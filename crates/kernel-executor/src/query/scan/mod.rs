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
//! is an error. Parquet columns match by `parquet.field.id` first, then by name (`field_id`).
//!
//! Not supported: the `RowIndex` metadata column.
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
use delta_kernel::schema::{MetadataColumnSpec, StructField, StructType};
use object_store::ObjectMeta;
use object_store::path::Path;

use crate::ExecutorError;
use crate::expressions::to_datafusion_scalar;
use crate::io::storage::group_by_store;

mod declared_type;
pub mod expr_adapter;
mod field_id;
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
mod tests;
