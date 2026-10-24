//! The engine's handlers: each turns its call into an [`Operation`] for the executor.
//!
//! See [`super`] for how they differ from the kernel's own plan-based handlers.

use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::plans::{IoOperation, Operation, PlanBuilder, PlanExecutor, PlanResult};
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, SchemaRef, StructField, StructType,
};
use delta_kernel::{
    DeltaResult, DeltaResultIterator, DeltaResultIteratorStatic, EngineData, Error,
    FileDataReadResultIterator, FileMeta, FileSize, FileSlice, FilteredEngineData, JsonHandler,
    ParquetFooter, ParquetHandler, PredicateRef, StorageHandler,
};
use url::Url;

use crate::DataFusionPlanExecutor;

/// A [`StorageHandler`] that turns each call into an [`IoOperation`].
#[derive(Debug)]
pub(super) struct PlanBasedStorageHandler {
    pub(super) executor: Arc<DataFusionPlanExecutor>,
}

impl PlanBasedStorageHandler {
    fn execute_io(&self, op: IoOperation) -> DeltaResult<PlanResult> {
        self.executor.execute_op(Operation::IoOperation(op))
    }
}

impl StorageHandler for PlanBasedStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        Ok(self
            .execute_io(IoOperation::file_listing(path.clone()))?
            .into_file_meta()?)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        Ok(self
            .execute_io(IoOperation::read_bytes(files))?
            .into_bytes()?)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
        self.execute_io(IoOperation::atomic_copy(src.clone(), dest.clone()))?
            .into_unit()
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        self.execute_io(IoOperation::write_bytes(path.clone(), data, overwrite))?
            .into_unit()
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        self.execute_io(IoOperation::head_file(path.clone()))?
            .into_file_meta()?
            .next()
            .ok_or_else(|| Error::generic(format!("head {path}: no file meta returned")))?
    }

    /// No [`IoOperation`] for delete yet (kernel #2820): straight to the executor's storage.
    fn delete(&self, path: &Url) -> DeltaResult<()> {
        self.executor.storage().delete(path)
    }
}

/// Reads `files` one plan at a time, built when the iterator reaches the file.
fn scan_per_file(
    executor: Arc<DataFusionPlanExecutor>,
    files: &[FileMeta],
    schema: SchemaRef,
    scan: impl Fn(Vec<FileMeta>, SchemaRef) -> DeltaResult<PlanBuilder> + Send + 'static,
) -> DeltaResult<FileDataReadResultIterator> {
    let batches = Vec::from(files).into_iter().flat_map(move |file| {
        let batches: FileDataReadResultIterator = match scan(vec![file], schema.clone())
            .and_then(|builder| builder.build())
            .and_then(|plan| executor.execute_op(Operation::QueryPlan(plan)))
            .and_then(PlanResult::into_data)
        {
            Ok(batches) => batches,
            Err(err) => Box::new(std::iter::once(Err(err))),
        };
        batches
    });
    Ok(Box::new(batches))
}

/// A [`JsonHandler`] whose file reads are [`ScanJson`](delta_kernel::plans::ir::nodes::ScanJson)
/// plans.
#[derive(Debug)]
pub(super) struct PlanBasedJsonHandler {
    pub(super) executor: Arc<DataFusionPlanExecutor>,
}

impl JsonHandler for PlanBasedJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        delta_kernel::engine::parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        scan_per_file(
            self.executor.clone(),
            files,
            physical_schema,
            |files, schema| PlanBuilder::scan_json(files, &[], schema),
        )
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: DeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> DeltaResult<FileSize> {
        self.executor
            .file_formats()
            .write_json_file(path, data, overwrite)
    }
}

/// A [`ParquetHandler`] whose file reads are
/// [`ScanParquet`](delta_kernel::plans::ir::nodes::ScanParquet) plans.
#[derive(Debug)]
pub(super) struct PlanBasedParquetHandler {
    pub(super) executor: Arc<DataFusionPlanExecutor>,
}

impl ParquetHandler for PlanBasedParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let schema = match without_field_ids(&physical_schema)? {
            Some(stripped) => Arc::new(stripped),
            None => physical_schema,
        };
        scan_per_file(self.executor.clone(), files, schema, |files, schema| {
            PlanBuilder::scan_parquet(files, &[], schema)
        })
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        self.executor
            .file_formats()
            .write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        self.executor.read_parquet_footer(file.clone())
    }
}

/// `schema` with `parquet.field.id` removed at every depth, or `None` if it carries none.
fn without_field_ids(schema: &StructType) -> DeltaResult<Option<StructType>> {
    let mut changed = false;
    let fields: Vec<StructField> = schema
        .fields()
        .map(|field| strip_field_id(field, &mut changed))
        .collect::<DeltaResult<_>>()?;
    changed.then(|| StructType::try_new(fields)).transpose()
}

fn strip_field_id(field: &StructField, changed: &mut bool) -> DeltaResult<StructField> {
    let key = ColumnMetadataKey::ParquetFieldId.as_ref();
    let data_type = strip_field_ids_in_type(field.data_type(), changed)?;
    if !field.metadata.contains_key(key) {
        return Ok(
            StructField::new(field.name().clone(), data_type, field.is_nullable()).with_metadata(
                field
                    .metadata
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone())),
            ),
        );
    }
    *changed = true;
    Ok(
        StructField::new(field.name().clone(), data_type, field.is_nullable()).with_metadata(
            field
                .metadata
                .iter()
                .filter(|(name, _)| name.as_str() != key)
                .map(|(name, value)| (name.clone(), value.clone())),
        ),
    )
}

fn strip_field_ids_in_type(data_type: &DataType, changed: &mut bool) -> DeltaResult<DataType> {
    Ok(match data_type {
        DataType::Struct(fields) => {
            let fields: Vec<StructField> = fields
                .fields()
                .map(|field| strip_field_id(field, changed))
                .collect::<DeltaResult<_>>()?;
            DataType::Struct(Box::new(StructType::try_new(fields)?))
        }
        DataType::Array(array) => DataType::Array(Box::new(ArrayType::new(
            strip_field_ids_in_type(array.element_type(), changed)?,
            array.contains_null(),
        ))),
        DataType::Map(map) => DataType::Map(Box::new(MapType::new(
            strip_field_ids_in_type(map.key_type(), changed)?,
            strip_field_ids_in_type(map.value_type(), changed)?,
            map.value_contains_null(),
        ))),
        other => other.clone(),
    })
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::MetadataValue;

    use super::*;

    #[test]
    fn without_field_ids_strips_every_depth_and_keeps_other_metadata() {
        let plain = StructType::try_new([StructField::nullable("x", DataType::LONG)]).unwrap();
        assert!(without_field_ids(&plain).unwrap().is_none());

        let tagged = StructType::try_new([
            StructField::nullable("x", DataType::LONG).with_metadata([
                ("parquet.field.id", MetadataValue::Number(1)),
                ("comment", MetadataValue::String("keep".into())),
            ]),
            StructField::nullable(
                "s",
                DataType::Struct(Box::new(
                    StructType::try_new([StructField::nullable("y", DataType::LONG)
                        .with_metadata([("parquet.field.id", MetadataValue::Number(2))])])
                    .unwrap(),
                )),
            ),
        ])
        .unwrap();
        let stripped = without_field_ids(&tagged).unwrap().unwrap();
        let x = stripped.field("x").unwrap();
        assert!(
            x.get_config_value(&ColumnMetadataKey::ParquetFieldId)
                .is_none()
        );
        assert_eq!(x.metadata.len(), 1, "other metadata stays");
        let DataType::Struct(s) = stripped.field("s").unwrap().data_type() else {
            panic!("s stays a struct");
        };
        assert!(
            s.field("y")
                .unwrap()
                .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                .is_none()
        );
    }
}
