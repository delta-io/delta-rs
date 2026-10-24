//! [`IoOperation`] handlers.
//!
//! Each variant maps to one call on the executor's [`DataFusionStorageHandler`] or
//! [`DataFusionFileFormatHandler`], which resolve the object store for a URL through the
//! DataFusion session's object store registry. The variant's docs in the kernel name the
//! [`PlanResult`] it must return.
//!
//! [`DataFusionStorageHandler`]: self::storage::DataFusionStorageHandler
//! [`DataFusionFileFormatHandler`]: self::file_formats::DataFusionFileFormatHandler

use delta_kernel::plans::{IoOperation, PlanResult};
use delta_kernel::{DeltaResult, ParquetHandler as _, StorageHandler as _};

use crate::DataFusionPlanExecutor;

pub mod file_formats;
pub mod storage;

/// Runs one I/O operation and returns its result in the shape the kernel expects.
pub fn execute(executor: &DataFusionPlanExecutor, op: IoOperation) -> DeltaResult<PlanResult> {
    let storage = executor.storage();
    match op {
        // The `StorageHandler` iterators are not `Send`, which `PlanResult` requires, so both
        // are drained here. Streaming them is a follow-up once the handlers return `Send`
        // iterators.
        IoOperation::FileListing { url } => {
            let files: Vec<_> = storage.list_from(&url)?.collect();
            Ok(PlanResult::FileMeta(Box::new(files.into_iter())))
        }
        IoOperation::ReadBytes { files } => {
            let buffers: Vec<_> = storage.read_files(files)?.collect();
            Ok(PlanResult::Bytes(Box::new(buffers.into_iter())))
        }
        IoOperation::WriteBytes {
            url,
            data,
            overwrite,
        } => {
            storage.put(&url, data, overwrite)?;
            Ok(PlanResult::Unit)
        }
        IoOperation::HeadFile { url } => {
            let meta = storage.head(&url)?;
            Ok(PlanResult::FileMeta(Box::new(std::iter::once(Ok(meta)))))
        }
        IoOperation::AtomicCopy {
            source,
            destination,
        } => {
            storage.copy_atomic(&source, &destination)?;
            Ok(PlanResult::Unit)
        }
        IoOperation::ParquetFooter { file } => Ok(PlanResult::ParquetFooter(
            executor.file_formats().read_parquet_footer(&file)?,
        )),
    }
}
