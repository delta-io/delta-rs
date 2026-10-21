//! Translates the [`ScanParquet`] and [`ScanJson`] operators into a DataFusion table scan.
//!
//! A kernel scan gives a list of files, the schema of the rows, and the columns that hold one
//! value per file. `ScanLayout` splits the schema into columns read from the files, per-file
//! values, and the `FilePath` column. `FileScanProvider` serves the files as a DataFusion table:
//! one `DataSourceExec` per object store, one partition per file, and the per-file values as
//! partition values. Only the file source differs:
//!
//! - Parquet goes through DataFusion's `ParquetSource` ([`parquet`]).
//! - JSON goes through `CoercingJsonSource`, DataFusion's JSON source with the kernel's reader
//!   settings (`json`).
//!
//! Rows come back with the declared schema. Parquet reads nested fields as nullable, because
//! Spark writes them that way, and then converts them back. A NULL in a non-nullable nested field
//! is an error.
//!
//! Not supported:
//!
//! - Fields with `parquet.field.id`: DataFusion matches parquet columns by name only. The
//!   engine's parquet handler removes the ids before it builds a scan.
//! - The `RowIndex` metadata column.
//!
//! [`ScanParquet`]: delta_kernel::plans::ir::nodes::ScanParquet
//! [`ScanJson`]: delta_kernel::plans::ir::nodes::ScanJson

use std::sync::Arc;

use datafusion::arrow::datatypes::{
    Field as ArrowField, FieldRef, Schema as ArrowSchema, SchemaRef,
};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result as DFResult, ScalarValue, project_schema};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::provider_as_source;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{
    EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, TableType, ident,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use datafusion_datasource::TableSchema;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{FileType, ScanFile};
use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataColumnSpec, StructField, StructType,
};
use object_store::ObjectMeta;
use object_store::path::Path;

use crate::ExecutorError;
use crate::expressions::to_datafusion_scalar;
use crate::io::storage::group_by_store;

mod declared_type;
pub mod expr_adapter;
mod json;
pub mod parquet;

/// Lowers a scan over `files` into the `LogicalPlan` whose rows match `schema`.
///
/// The object store serving each file is resolved by the session that runs the plan.
///
/// # Errors
/// Returns an error for a schema this executor cannot serve (see the module docs) or a file
/// whose constants do not line up with `file_constant_columns`.
pub(super) fn lower_scan(
    file_type: FileType,
    files: &[ScanFile],
    file_constant_columns: &[String],
    schema: &StructType,
) -> crate::Result<LogicalPlan> {
    let layout = ScanLayout::try_new(file_type, schema, file_constant_columns)?;
    if files.is_empty() {
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(layout.output_schema()?)?),
        };
        return Ok(LogicalPlan::EmptyRelation(empty));
    }

    let provider = FileScanProvider::try_new(file_type, &layout, files)?;
    let table_name = match file_type {
        FileType::Parquet => "scan_parquet",
        FileType::Json => "scan_json",
    };
    // The provider serves file columns first and constants last. Project back to the declared
    // order, and alias each column so the plan's fields are unqualified like every other
    // operator's.
    let declared_order = layout
        .declared
        .iter()
        .zip(schema.fields())
        .map(|(&index, field)| {
            ident(layout.provider_schema.field(index).name()).alias(field.name())
        });
    let plan = LogicalPlanBuilder::scan(table_name, provider_as_source(Arc::new(provider)), None)?
        .project(declared_order)?
        .build()?;
    Ok(plan)
}

/// Where one field of a scan's declared schema comes from, while the layout is being built.
#[derive(Debug, Clone, Copy)]
enum FieldSource {
    /// Read from the file; the index is the field's position in the file schema.
    File(usize),
    /// A per-file constant; the index is the field's position among the constant columns.
    Constant(usize),
}

/// The declared schema of a scan, split by where each field comes from.
#[derive(Debug)]
struct ScanLayout {
    /// The schema the provider serves: the file fields, then the constant columns. The file-path
    /// column, when declared, is the last constant.
    provider_schema: SchemaRef,
    /// The fields read from each file, in declared order.
    file_schema: SchemaRef,
    /// The per-file constants: `file_constant_columns` in order, then the file-path column.
    constant_fields: Vec<FieldRef>,
    /// Whether the last constant is the `FilePath` metadata column.
    has_file_path: bool,
    /// For each declared field, its index in `provider_schema`.
    declared: Vec<usize>,
}

impl ScanLayout {
    fn try_new(
        file_type: FileType,
        schema: &StructType,
        file_constant_columns: &[String],
    ) -> crate::Result<Self> {
        let mut constant_fields = file_constant_columns
            .iter()
            .map(|name| {
                let field = schema.field(name).ok_or_else(|| {
                    ExecutorError::DataFusion(datafusion::common::plan_datafusion_err!(
                        "file-constant column `{name}` is not a field of the scan schema"
                    ))
                })?;
                arrow_field(field)
            })
            .collect::<crate::Result<Vec<FieldRef>>>()?;

        let mut file_fields = Vec::new();
        let mut file_path = None;
        let mut sources = Vec::with_capacity(schema.num_fields());
        for field in schema.fields() {
            if let Some(spec) = field.get_metadata_column_spec() {
                if spec != MetadataColumnSpec::FilePath {
                    return Err(ExecutorError::unsupported(format!(
                        "metadata column `{}` ({spec:?}) in a {file_type:?} scan",
                        field.name()
                    )));
                }
                file_path = Some(arrow_field(field)?);
                sources.push(FieldSource::Constant(file_constant_columns.len()));
            } else if let Some(index) = file_constant_columns
                .iter()
                .position(|name| name == field.name())
            {
                sources.push(FieldSource::Constant(index));
            } else {
                let has_field_id = field
                    .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                    .is_some()
                    || carries_parquet_field_id(field.data_type());
                if file_type == FileType::Parquet && has_field_id {
                    return Err(ExecutorError::unsupported(format!(
                        "field `{}` carries parquet.field.id; DataFusion matches parquet columns \
                         by name only",
                        field.name()
                    )));
                }
                sources.push(FieldSource::File(file_fields.len()));
                file_fields.push(arrow_field(field)?);
            }
        }

        let has_file_path = file_path.is_some();
        constant_fields.extend(file_path);
        let declared = sources
            .into_iter()
            .map(|source| match source {
                FieldSource::File(index) => index,
                FieldSource::Constant(index) => file_fields.len() + index,
            })
            .collect();
        let provider_fields: Vec<FieldRef> = file_fields
            .iter()
            .chain(&constant_fields)
            .cloned()
            .collect();
        Ok(Self {
            provider_schema: Arc::new(ArrowSchema::new(provider_fields)),
            file_schema: Arc::new(ArrowSchema::new(file_fields)),
            constant_fields,
            has_file_path,
            declared,
        })
    }

    /// The declared schema, in declared order.
    fn output_schema(&self) -> crate::Result<ArrowSchema> {
        Ok(self.provider_schema.project(&self.declared)?)
    }

    /// The values of `file`'s constant columns, in `constant_fields` order.
    ///
    /// # Errors
    /// Returns an error if the file carries a different number of constants than the scan declares,
    /// or a constant whose type is not its column's.
    fn constants_for(&self, file: &ScanFile) -> crate::Result<Vec<ScalarValue>> {
        let declared = self.constant_fields.len() - usize::from(self.has_file_path);
        if file.file_constants.len() != declared {
            return Err(ExecutorError::DataFusion(
                datafusion::common::plan_datafusion_err!(
                    "scan file {} carries {} constant(s), but the scan declares {declared}",
                    file.meta.location,
                    file.file_constants.len()
                ),
            ));
        }
        let mut values = Vec::with_capacity(self.constant_fields.len());
        for (scalar, field) in file.file_constants.iter().zip(&self.constant_fields) {
            let value = to_datafusion_scalar(scalar)?;
            if &value.data_type() != field.data_type() {
                return Err(ExecutorError::DataFusion(
                    datafusion::common::plan_datafusion_err!(
                        "constant for column `{}` of scan file {} is {:?}, expected {:?}",
                        field.name(),
                        file.meta.location,
                        value.data_type(),
                        field.data_type()
                    ),
                ));
            }
            values.push(value);
        }
        if self.has_file_path {
            values.push(ScalarValue::Utf8(Some(file.meta.location.to_string())));
        }
        Ok(values)
    }
}

/// Whether any nested field under `data_type` carries a parquet field id.
fn carries_parquet_field_id(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => fields.fields().any(|field| {
            field
                .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                .is_some()
                || carries_parquet_field_id(field.data_type())
        }),
        DataType::Array(array) => carries_parquet_field_id(array.element_type()),
        DataType::Map(map) => {
            carries_parquet_field_id(map.key_type()) || carries_parquet_field_id(map.value_type())
        }
        _ => false,
    }
}

/// `field` as an arrow field.
fn arrow_field(field: &StructField) -> crate::Result<FieldRef> {
    let arrow_field: ArrowField = field.try_into_arrow()?;
    Ok(Arc::new(arrow_field))
}

/// `file`'s path within its object store.
fn store_path(file: &ScanFile) -> crate::Result<Path> {
    Ok(Path::from_url_path(file.meta.location.path()).map_err(object_store::Error::from)?)
}

/// The files one object store serves, with their constants as partition values.
#[derive(Debug)]
struct StoreFiles {
    store_url: ObjectStoreUrl,
    /// One group per file, so every file is its own partition.
    file_groups: Vec<FileGroup>,
}

impl StoreFiles {
    fn try_new(
        store_url: ObjectStoreUrl,
        layout: &ScanLayout,
        files: &[&ScanFile],
    ) -> crate::Result<Self> {
        let file_groups = files
            .iter()
            .map(|file| {
                // From the `ObjectMeta`: `PartitionedFile::new` parses its path string again,
                // which encodes a `%` in the file's path a second time.
                let meta = ObjectMeta {
                    location: store_path(file)?,
                    last_modified: Default::default(),
                    size: file.meta.size,
                    e_tag: None,
                    version: None,
                };
                let partitioned =
                    PartitionedFile::from(meta).with_partition_values(layout.constants_for(file)?);
                Ok(FileGroup::new(vec![partitioned]))
            })
            .collect::<crate::Result<_>>()?;
        Ok(Self {
            store_url,
            file_groups,
        })
    }
}

/// A kernel scan's files as a DataFusion table: file columns first, constants last.
#[derive(Debug)]
struct FileScanProvider {
    schema: SchemaRef,
    file_type: FileType,
    /// The schema the file source reads: the file columns, then the constants.
    table_schema: TableSchema,
    /// One `DataSourceExec` per object store.
    stores: Vec<StoreFiles>,
}

impl FileScanProvider {
    fn try_new(
        file_type: FileType,
        layout: &ScanLayout,
        files: &[ScanFile],
    ) -> crate::Result<Self> {
        let table_schema = match file_type {
            FileType::Parquet => parquet::table_schema(layout),
            FileType::Json => json::table_schema(layout),
        };
        let stores = group_by_store(files)
            .into_iter()
            .map(|(url, files)| StoreFiles::try_new(url, layout, &files))
            .collect::<crate::Result<_>>()?;
        Ok(Self {
            schema: Arc::clone(&layout.provider_schema),
            file_type,
            table_schema,
            stores,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for FileScanProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plans = self
            .stores
            .iter()
            .map(|store| {
                let builder = match self.file_type {
                    FileType::Parquet => {
                        parquet::scan_config(state, &self.table_schema, &store.store_url)?
                    }
                    FileType::Json => json::scan_config(&self.table_schema, &store.store_url),
                };
                let config = builder
                    .with_file_groups(store.file_groups.clone())
                    .with_projection_indices(projection.cloned())?
                    .with_limit(limit)
                    .build();
                Ok(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>)
            })
            .collect::<DFResult<_>>()?;
        // `lower_scan` serves an empty file list as an `EmptyRelation`, so there is at least one
        // store; `UnionExec::try_new` returns a lone plan as is.
        let plan = UnionExec::try_new(plans)?;
        match self.file_type {
            FileType::Parquet => {
                parquet::with_declared_nullability(plan, &project_schema(&self.schema, projection)?)
            }
            FileType::Json => Ok(plan),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path as FsPath;

    use datafusion::arrow::array::{
        Array as _, AsArray as _, Int64Array, StringArray, StructArray,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Int64Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use delta_kernel::FileMeta;
    use delta_kernel::PlanBuilder;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::column_name;
    use delta_kernel::plans::ir::nodes::Agg;
    use delta_kernel::plans::{Operation, PlanExecutor as _};
    use delta_kernel::schema::{MetadataValue, StructField};
    use rstest::rstest;
    use tokio::runtime::Handle;
    use url::Url;

    use super::*;
    use crate::DataFusionPlanExecutor;

    // === Shared helpers ===

    fn executor() -> DataFusionPlanExecutor {
        DataFusionPlanExecutor::new(SessionContext::new().task_ctx(), Handle::current())
    }

    /// A scan file for `path` carrying `constants`.
    fn scan_file(path: &FsPath, constants: Vec<delta_kernel::expressions::Scalar>) -> ScanFile {
        ScanFile {
            meta: FileMeta {
                location: Url::from_file_path(path).unwrap(),
                last_modified: 0,
                size: std::fs::metadata(path).unwrap().len(),
            },
            file_constants: constants,
        }
    }

    fn long_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_from_iter([("x", Arc::new(Int64Array::from(values.to_vec())) as _)])
            .unwrap()
    }

    fn write_parquet(dir: &FsPath, name: &str, batch: &RecordBatch) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn write_json(dir: &FsPath, name: &str, lines: &[&str]) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, lines.join("\n")).unwrap();
        path
    }

    fn schema(fields: impl IntoIterator<Item = StructField>) -> StructType {
        StructType::try_new(fields).unwrap()
    }

    /// `x LONG` only.
    fn x_schema() -> StructType {
        schema([StructField::nullable("x", DataType::LONG)])
    }

    /// Runs the lowered scan on a session configured like [`crate::query::execute`].
    async fn run(
        file_type: FileType,
        files: &[ScanFile],
        constants: &[&str],
        schema: &StructType,
    ) -> crate::Result<Vec<RecordBatch>> {
        let constants: Vec<String> = constants.iter().map(|c| c.to_string()).collect();
        let plan = lower_scan(file_type, files, &constants, schema)?;
        let config = SessionConfig::new().with_repartition_file_scans(false);
        let session = SessionContext::new_with_config(config);
        Ok(session.execute_logical_plan(plan).await?.collect().await?)
    }

    fn longs(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(column)
                    .unwrap()
                    .as_primitive::<Int64Type>()
            })
            .collect()
    }

    fn strings(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|batch| batch.column_by_name(column).unwrap().as_string::<i32>())
            .map(|s| s.map(str::to_string))
            .collect()
    }

    /// A schema that puts a constant first, a file column second, and the file path last.
    fn versioned_schema() -> StructType {
        schema([
            StructField::not_null("version", DataType::LONG),
            StructField::nullable("x", DataType::LONG),
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
        ])
    }

    /// Writes `a` with rows 1, 2 and `b` with row 3 in `file_type`, tagged with versions 7 and 8.
    fn two_versioned_files(dir: &FsPath, file_type: FileType) -> Vec<ScanFile> {
        let (a, b) = match file_type {
            FileType::Parquet => (
                write_parquet(dir, "a.parquet", &long_batch(&[1, 2])),
                write_parquet(dir, "b.parquet", &long_batch(&[3])),
            ),
            FileType::Json => (
                write_json(dir, "a.json", &[r#"{"x": 1}"#, r#"{"x": 2}"#]),
                write_json(dir, "b.json", &[r#"{"x": 3}"#]),
            ),
        };
        vec![
            scan_file(&a, vec![7i64.into()]),
            scan_file(&b, vec![8i64.into()]),
        ]
    }

    /// [`two_versioned_files`] without their constants.
    fn unversioned_files(dir: &FsPath, file_type: FileType) -> Vec<ScanFile> {
        let mut files = two_versioned_files(dir, file_type);
        files
            .iter_mut()
            .for_each(|file| file.file_constants.clear());
        files
    }

    // === Both formats ===

    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn reads_columns_constants_and_file_path_in_declared_order(#[case] file_type: FileType) {
        let dir = tempfile::tempdir().unwrap();
        let files = two_versioned_files(dir.path(), file_type);
        let batches = run(file_type, &files, &["version"], &versioned_schema())
            .await
            .unwrap();

        let names: Vec<_> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, ["version", "x", "_file"]);

        let mut rows: Vec<_> = longs(&batches, "version")
            .into_iter()
            .zip(longs(&batches, "x"))
            .zip(strings(&batches, "_file"))
            .map(|((version, x), file)| (x.unwrap(), version.unwrap(), file.unwrap()))
            .collect();
        rows.sort();
        let (a, b) = (
            files[0].meta.location.to_string(),
            files[1].meta.location.to_string(),
        );
        assert_eq!(
            rows,
            [(1, 7, a.clone()), (2, 7, a), (3, 8, b)],
            "constants and the file path are broadcast per file"
        );
    }

    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn missing_nullable_column_is_null_filled(#[case] file_type: FileType) {
        let dir = tempfile::tempdir().unwrap();
        let files = unversioned_files(dir.path(), file_type);
        let schema = schema([
            StructField::nullable("x", DataType::LONG),
            StructField::nullable("missing", DataType::STRING),
        ]);
        let batches = run(file_type, &files, &[], &schema).await.unwrap();
        assert_eq!(longs(&batches, "x").len(), 3);
        assert!(strings(&batches, "missing").iter().all(Option::is_none));
    }

    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn missing_non_nullable_column_errors(#[case] file_type: FileType) {
        let dir = tempfile::tempdir().unwrap();
        let files = unversioned_files(dir.path(), file_type);
        let schema = schema([
            StructField::nullable("x", DataType::LONG),
            StructField::not_null("required", DataType::STRING),
        ]);
        let err = run(file_type, &files, &[], &schema).await.unwrap_err();
        assert!(err.to_string().contains("required"), "{err}");
    }

    /// A struct `s` with one `other` column, and the declared schema that adds `path` to it.
    fn struct_with_other(dir: &FsPath, file_type: FileType) -> std::path::PathBuf {
        match file_type {
            FileType::Parquet => {
                let inner = Arc::new(Int64Array::from(vec![1, 2]));
                let s = StructArray::from(vec![(
                    Arc::new(ArrowField::new("other", ArrowDataType::Int64, true)),
                    inner as _,
                )]);
                let batch = RecordBatch::try_from_iter([("s", Arc::new(s) as _)]).unwrap();
                write_parquet(dir, "s.parquet", &batch)
            }
            FileType::Json => write_json(
                dir,
                "s.json",
                &[r#"{"s": {"other": 1}}"#, r#"{"s": {"other": 2}}"#],
            ),
        }
    }

    fn struct_with_other_and(path: StructField) -> StructType {
        schema([StructField::nullable(
            "s",
            DataType::Struct(Box::new(schema([
                StructField::nullable("other", DataType::LONG),
                path,
            ]))),
        )])
    }

    /// A nested field missing from the file is NULL-filled when nullable, and an error when not,
    /// as in the kernel's default engine.
    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn missing_nested_field(#[case] file_type: FileType) {
        let dir = tempfile::tempdir().unwrap();
        let files = [scan_file(&struct_with_other(dir.path(), file_type), vec![])];

        let nullable = struct_with_other_and(StructField::nullable("path", DataType::STRING));
        let batches = run(file_type, &files, &[], &nullable).await.unwrap();
        let s = batches[0].column_by_name("s").unwrap().as_struct();
        assert_eq!(s.len(), 2);
        assert_eq!(s.column_by_name("path").unwrap().null_count(), 2);

        let not_null = struct_with_other_and(StructField::not_null("path", DataType::STRING));
        let err = run(file_type, &files, &[], &not_null).await.unwrap_err();
        assert!(err.to_string().contains("path"), "{err}");
    }

    /// Spark writes nested leaves as `optional`. One reads into a non-nullable declared field as
    /// long as it holds no NULLs, and the rows come back with the declared type.
    #[tokio::test]
    async fn parquet_optional_nested_leaf_reads_into_non_nullable_field() {
        let dir = tempfile::tempdir().unwrap();
        let x = Arc::new(ArrowField::new("x", ArrowDataType::Int64, true));
        let write = |name: &str, values: Vec<Option<i64>>| {
            let s = StructArray::from(vec![(x.clone(), Arc::new(Int64Array::from(values)) as _)]);
            let batch = RecordBatch::try_from_iter([("s", Arc::new(s) as _)]).unwrap();
            write_parquet(dir.path(), name, &batch)
        };
        let declared = schema([StructField::nullable(
            "s",
            DataType::Struct(Box::new(schema([StructField::not_null(
                "x",
                DataType::LONG,
            )]))),
        )]);

        let file = write("no_nulls.parquet", vec![Some(1), Some(2)]);
        let batches = run(
            FileType::Parquet,
            &[scan_file(&file, vec![])],
            &[],
            &declared,
        )
        .await
        .unwrap();
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ArrowDataType::Struct(vec![ArrowField::new("x", ArrowDataType::Int64, false)].into()),
            "declared nullability is restored above the read"
        );
        let s = batches[0].column_by_name("s").unwrap().as_struct();
        assert_eq!(s.column(0).as_primitive::<Int64Type>().values(), &[1, 2]);

        let file = write("nulls.parquet", vec![Some(1), None]);
        let err = run(
            FileType::Parquet,
            &[scan_file(&file, vec![])],
            &[],
            &declared,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("non-nullable"), "{err}");
    }

    /// A checkpoint's shape: `add` is NULL on non-add rows, and the parquet reader leaves its
    /// non-nullable `path` non-null there. The scan unions the struct's NULLs into its children,
    /// as the kernel's default engine does, whatever the declared nullability of the child.
    #[rstest]
    #[case::declared_not_null(StructField::not_null("path", DataType::STRING))]
    #[case::declared_nullable(StructField::nullable("path", DataType::STRING))]
    #[tokio::test]
    async fn parquet_null_struct_has_null_children(#[case] path: StructField) {
        let dir = tempfile::tempdir().unwrap();
        let add = StructArray::new(
            vec![ArrowField::new("path", ArrowDataType::Utf8, false)].into(),
            vec![Arc::new(StringArray::from(vec!["a.parquet", ""])) as _],
            Some(NullBuffer::from(vec![true, false])),
        );
        let batch = RecordBatch::try_from_iter([("add", Arc::new(add) as _)]).unwrap();
        let file = write_parquet(dir.path(), "checkpoint.parquet", &batch);
        let declared = schema([StructField::nullable(
            "add",
            DataType::Struct(Box::new(schema([path]))),
        )]);

        let batches = run(
            FileType::Parquet,
            &[scan_file(&file, vec![])],
            &[],
            &declared,
        )
        .await
        .unwrap();
        let add = batches[0].column_by_name("add").unwrap().as_struct();
        assert_eq!(
            add.column(0).as_string::<i32>().iter().collect::<Vec<_>>(),
            [Some("a.parquet"), None]
        );
    }

    /// Spark escapes a `:` in a partition value as `%3A`, so the directory name holds a `%`.
    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn reads_a_file_with_percent_in_its_path(#[case] file_type: FileType) {
        let dir = tempfile::tempdir().unwrap();
        let partition = dir.path().join("ts=12%3A30");
        std::fs::create_dir(&partition).unwrap();
        let file = match file_type {
            FileType::Parquet => write_parquet(&partition, "a.parquet", &long_batch(&[1])),
            FileType::Json => write_json(&partition, "a.json", &[r#"{"x": 1}"#]),
        };
        let batches = run(file_type, &[scan_file(&file, vec![])], &[], &x_schema())
            .await
            .unwrap();
        assert_eq!(longs(&batches, "x"), [Some(1)]);
    }

    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn no_files_is_an_empty_relation(#[case] file_type: FileType) {
        let batches = run(file_type, &[], &["version"], &versioned_schema())
            .await
            .unwrap();
        assert!(batches.iter().all(|b| b.num_rows() == 0));
    }

    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn row_index_metadata_column_is_refused(#[case] file_type: FileType) {
        let schema = schema([
            StructField::nullable("x", DataType::LONG),
            StructField::create_metadata_column("ridx", MetadataColumnSpec::RowIndex),
        ]);
        let err = lower_scan(file_type, &[], &[], &schema).unwrap_err();
        assert!(matches!(err, ExecutorError::Unsupported(_)), "{err}");
        assert!(err.to_string().contains("RowIndex"), "{err}");
    }

    #[rstest]
    #[case::parquet(FileType::Parquet)]
    #[case::json(FileType::Json)]
    #[tokio::test]
    async fn wrong_number_of_constants_errors(#[case] file_type: FileType) {
        let dir = tempfile::tempdir().unwrap();
        let mut files = two_versioned_files(dir.path(), file_type);
        files[1].file_constants.clear();
        let err = lower_scan(
            file_type,
            &files,
            &["version".to_string()],
            &versioned_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("carries 0 constant(s)"), "{err}");
    }

    #[tokio::test]
    async fn mistyped_constant_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = two_versioned_files(dir.path(), FileType::Json);
        files[0].file_constants = vec!["seven".into()];
        let err = lower_scan(
            FileType::Json,
            &files,
            &["version".to_string()],
            &versioned_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected Int64"), "{err}");
    }

    #[tokio::test]
    async fn unknown_constant_column_errors() {
        let err = lower_scan(FileType::Json, &[], &["nope".to_string()], &x_schema()).unwrap_err();
        assert!(err.to_string().contains("`nope`"), "{err}");
    }

    // === Parquet ===

    fn field_id_schema() -> StructType {
        schema([StructField::nullable("x", DataType::LONG)
            .with_metadata([("parquet.field.id", MetadataValue::Number(5))])])
    }

    /// Refused at the top level and when nested inside a struct.
    #[test]
    fn parquet_refuses_field_id_matching() {
        let nested = schema([StructField::nullable(
            "s",
            DataType::Struct(Box::new(field_id_schema())),
        )]);
        for schema in [field_id_schema(), nested] {
            let err = lower_scan(FileType::Parquet, &[], &[], &schema).unwrap_err();
            assert!(err.to_string().contains("parquet.field.id"), "{err}");
        }
    }

    #[tokio::test]
    async fn json_ignores_field_ids() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_json(dir.path(), "a.json", &[r#"{"x": 1}"#]);
        let batches = run(
            FileType::Json,
            &[scan_file(&file, vec![])],
            &[],
            &field_id_schema(),
        )
        .await
        .unwrap();
        assert_eq!(longs(&batches, "x"), [Some(1)]);
    }

    // === JSON ===

    #[tokio::test]
    async fn json_coerces_primitives_into_string_columns() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_json(dir.path(), "a.json", &[r#"{"s": false}"#, r#"{"s": 3}"#]);
        let schema = schema([StructField::nullable("s", DataType::STRING)]);
        let batches = run(FileType::Json, &[scan_file(&file, vec![])], &[], &schema)
            .await
            .unwrap();
        assert_eq!(
            strings(&batches, "s"),
            [Some("false".to_string()), Some("3".to_string())]
        );
    }

    #[tokio::test]
    async fn json_empty_file_yields_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_json(dir.path(), "empty.json", &[]);
        let batches = run(
            FileType::Json,
            &[scan_file(&file, vec![])],
            &[],
            &x_schema(),
        )
        .await
        .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    /// Through the executor itself: one partition per file keeps a file's rows in order, and a
    /// batch never spans files.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_keeps_rows_in_file_order() {
        let dir = tempfile::tempdir().unwrap();
        let lines: Vec<String> = (0..5000).map(|i| format!(r#"{{"x": {i}}}"#)).collect();
        let line_refs: Vec<&str> = lines.iter().map(String::as_str).collect();
        let a = write_json(dir.path(), "a.json", &line_refs);
        let b = write_json(dir.path(), "b.json", &[r#"{"x": -1}"#]);
        let plan = PlanBuilder::scan_json(
            [scan_file(&a, vec![]), scan_file(&b, vec![])],
            &[],
            Arc::new(x_schema()),
        )
        .unwrap()
        .build()
        .unwrap();

        let executor = executor();
        let batches: Vec<RecordBatch> = tokio::task::spawn_blocking(move || {
            executor
                .execute_op(Operation::QueryPlan(plan))
                .unwrap()
                .into_data()
                .unwrap()
                .map(|batch| {
                    ArrowEngineData::try_from_engine_data(batch.unwrap())
                        .unwrap()
                        .record_batch()
                        .clone()
                })
                .collect()
        })
        .await
        .unwrap();

        let per_batch: Vec<Vec<i64>> = batches
            .iter()
            .map(|batch| {
                longs(std::slice::from_ref(batch), "x")
                    .into_iter()
                    .flatten()
                    .collect()
            })
            .collect();
        for rows in &per_batch {
            assert!(
                rows.iter().all(|&x| x == -1) || rows.windows(2).all(|w| w[0] + 1 == w[1]),
                "a batch holds one file's rows, in file order: {rows:?}"
            );
        }
        let a_rows: Vec<i64> = per_batch
            .iter()
            .flatten()
            .copied()
            .filter(|&x| x >= 0)
            .collect();
        assert_eq!(a_rows, (0..5000).collect::<Vec<_>>());
    }

    // === Composition ===

    /// The shape of the kernel's P&M replay: a JSON scan and a parquet scan with the same schema,
    /// unioned and aggregated by the version constant.
    #[tokio::test]
    async fn union_of_json_and_parquet_scans_aggregates() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(schema([
            StructField::not_null("version", DataType::LONG),
            StructField::nullable("x", DataType::LONG),
        ]));
        let commits = two_versioned_files(dir.path(), FileType::Json);
        let checkpoint = vec![scan_file(
            &write_parquet(dir.path(), "cp.parquet", &long_batch(&[10, 20])),
            vec![5i64.into()],
        )];
        let plan = PlanBuilder::union_all([
            PlanBuilder::scan_json(commits, &["version"], schema.clone()).unwrap(),
            PlanBuilder::scan_parquet(checkpoint, &["version"], schema).unwrap(),
        ])
        .unwrap()
        .aggregate_ungrouped(|a| {
            a.max_non_null_by(
                column_name!("x"),
                column_name!("x"),
                column_name!("version"),
            )
            .aggregate_as(Agg::max(column_name!("version")), "newest")
        })
        .unwrap()
        .build()
        .unwrap();

        let executor = executor();
        let logical = crate::query::to_logical_plan(&executor, &plan).unwrap();
        let batches = SessionContext::new()
            .execute_logical_plan(logical)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(longs(&batches, "x"), [Some(3)], "x from the newest version");
        assert_eq!(longs(&batches, "newest"), [Some(8)]);
    }
}
