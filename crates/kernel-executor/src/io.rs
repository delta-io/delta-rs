//! [`IoOperation`] handlers over the executor's [`ObjectStore`].
//!
//! Each variant maps to one object store call. The variant's docs in the kernel name the
//! [`PlanResult`] it must return.
//!
//! [`ObjectStore`]: object_store::ObjectStore

use delta_kernel::DeltaResult;
use delta_kernel::plans::{IoOperation, PlanResult};

use crate::{DataFusionPlanExecutor, Error};

/// Runs one I/O operation and returns its result in the shape the kernel expects.
pub fn execute(executor: &DataFusionPlanExecutor, op: IoOperation) -> DeltaResult<PlanResult> {
    let result = match op {
        IoOperation::FileListing { url } => file_listing(executor, url),
        IoOperation::ReadBytes { files } => read_bytes(executor, files),
        IoOperation::WriteBytes {
            url,
            data,
            overwrite,
        } => write_bytes(executor, url, data, overwrite),
        IoOperation::HeadFile { url } => head_file(executor, url),
        IoOperation::AtomicCopy {
            source,
            destination,
        } => atomic_copy(executor, source, destination),
        IoOperation::ParquetFooter { file } => parquet_footer(executor, file),
    };
    Ok(result?)
}

/// Lists files under `url` recursively as [`PlanResult::FileMeta`].
fn file_listing(_executor: &DataFusionPlanExecutor, url: url::Url) -> crate::Result<PlanResult> {
    Err(Error::unsupported(format!("file listing at {url}")))
}

/// Reads each file slice and returns one buffer per slice as [`PlanResult::Bytes`].
fn read_bytes(
    _executor: &DataFusionPlanExecutor,
    files: Vec<delta_kernel::FileSlice>,
) -> crate::Result<PlanResult> {
    Err(Error::unsupported(format!(
        "reading bytes from {} file slices",
        files.len()
    )))
}

/// Writes `data` to `url`, failing when the file exists and `overwrite` is false.
fn write_bytes(
    _executor: &DataFusionPlanExecutor,
    url: url::Url,
    _data: bytes::Bytes,
    _overwrite: bool,
) -> crate::Result<PlanResult> {
    Err(Error::unsupported(format!("writing bytes to {url}")))
}

/// Returns the metadata of the single file at `url` as [`PlanResult::FileMeta`].
fn head_file(_executor: &DataFusionPlanExecutor, url: url::Url) -> crate::Result<PlanResult> {
    Err(Error::unsupported(format!("head request for {url}")))
}

/// Copies `source` to `destination` without overwriting an existing destination.
fn atomic_copy(
    _executor: &DataFusionPlanExecutor,
    source: url::Url,
    destination: url::Url,
) -> crate::Result<PlanResult> {
    Err(Error::unsupported(format!(
        "atomic copy from {source} to {destination}"
    )))
}

/// Reads the Parquet footer of `file` as [`PlanResult::ParquetFooter`].
fn parquet_footer(
    _executor: &DataFusionPlanExecutor,
    file: delta_kernel::FileMeta,
) -> crate::Result<PlanResult> {
    Err(Error::unsupported(format!(
        "parquet footer of {}",
        file.location
    )))
}
