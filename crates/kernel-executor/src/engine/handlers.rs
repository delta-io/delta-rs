//! The engine's handlers: each turns its call into an [`Operation`] for the executor.
//!
//! See [`super`] for how they differ from the kernel's own plan-based handlers.

use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::plans::{IoOperation, Operation, PlanBuilder, PlanExecutor, PlanResult};
use delta_kernel::schema::SchemaRef;
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
        scan_per_file(
            self.executor.clone(),
            files,
            physical_schema,
            |files, schema| PlanBuilder::scan_parquet(files, &[], schema),
        )
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
