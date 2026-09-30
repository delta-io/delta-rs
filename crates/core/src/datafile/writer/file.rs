//! One data file in progress: its parquet writer, the multipart upload under it, and the
//! steps that finish or abort the file.

use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::OwnedSemaphorePermit;
use tracing::*;

use super::PartitionWriterConfig;
use super::parallel::ParallelArrowWriter;
use crate::errors::DeltaResult;
use crate::logstore::ObjectStoreRef;

const DEFAULT_UPLOAD_PART_SIZE: usize = 1024 * 1024 * 5;

fn upload_part_size() -> usize {
    static UPLOAD_SIZE: OnceLock<usize> = OnceLock::new();
    *UPLOAD_SIZE.get_or_init(|| {
        std::env::var("DELTARS_UPLOAD_PART_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|size| {
                if size < DEFAULT_UPLOAD_PART_SIZE {
                    // Minimum part size in GCS and S3
                    debug!("DELTARS_UPLOAD_PART_SIZE must be at least 5MB, therefore falling back on default of 5MB.");
                    DEFAULT_UPLOAD_PART_SIZE
                } else if size > 1024 * 1024 * 1024 * 5 {
                    // Maximum part size in GCS and S3
                    debug!("DELTARS_UPLOAD_PART_SIZE must not be higher than 5GB, therefore capping it at 5GB.");
                    1024 * 1024 * 1024 * 5
                } else {
                    size
                }
            })
            .unwrap_or(DEFAULT_UPLOAD_PART_SIZE)
    })
}

/// Finish a parquet file: encode the last row group and the footer, send what is left of the
/// file, and complete the upload. Most parts of a large file went out while it was written.
/// Returns the metadata for the file's Add action. Holds `_permit`, the file's
/// [`UploadBudget`] reservation, until the upload is done.
///
/// [`UploadBudget`]: super::UploadBudget
#[instrument(skip(arrow_writer, _permit), fields(rows = 0, size = 0))]
async fn finish_parquet_file(
    mut arrow_writer: ParallelArrowWriter<ParquetObjectWriter>,
    path: Path,
    _permit: OwnedSemaphorePermit,
) -> DeltaResult<(Path, usize, ParquetMetaData)> {
    let metadata = match arrow_writer.finish().await {
        Ok(metadata) => metadata,
        Err(e) => {
            // A failed completion leaves multipart parts behind that vacuum
            // cannot see; abort them (best-effort) before surfacing the error.
            //
            // `BufWriter::abort` panics (by design) once shutdown has begun, and
            // a `finish` that failed at the complete stage is exactly that state
            // — the upload is no longer reachable to abort. Catch the panic so
            // that case degrades to a leaked-parts warning instead of taking
            // down the write task.
            use futures::FutureExt as _;
            let mut buf_writer = arrow_writer.into_inner();
            let abort = std::panic::AssertUnwindSafe(buf_writer.abort()).catch_unwind();
            match abort.await {
                Ok(Ok(())) => {}
                Ok(Err(abort_err)) => {
                    warn!("failed to abort multipart upload after a failed finish: {abort_err}");
                }
                Err(_) => {
                    warn!(
                        "multipart upload failed during completion; its parts cannot be aborted \
                         and are left for the object store's lifecycle cleanup"
                    );
                }
            }
            return Err(e.into());
        }
    };
    let file_size = arrow_writer.bytes_written();
    // `bytes_written()` returns cumulative bytes flushed through AsyncArrowWriter,
    // including all row groups. After `finish()`, the parquet footer is written and
    // included in this counter (parquet-rs calls write_footer() then updates the
    // internal byte count before returning the metadata). If this ever understates
    // the physical object size, use `object_store.head(&path).size` as the source
    // of truth instead.
    Span::current().record("rows", metadata.file_metadata().num_rows());
    Span::current().record("size", file_size);
    debug!("multipart upload completed successfully");

    Ok((path, file_size, metadata))
}

/// [`ParquetObjectWriter`] for writing to parquet to an [`object_store::ObjectStore`].
///
/// Copied from parquet 59.2, which deprecated it in favor of passing a
/// [`BufWriter`] to [`AsyncArrowWriter`] directly (apache/arrow-rs#10354). That
/// route goes through `AsyncWrite`; this one keeps [`BufWriter::put`], which
/// "can write data without extra copying".
pub(super) struct ParquetObjectWriter(BufWriter);

impl ParquetObjectWriter {
    /// Abort the in-progress multipart upload, if any.
    async fn abort(&mut self) -> object_store::Result<()> {
        self.0.abort().await
    }
}

impl AsyncFileWriter for ParquetObjectWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .put(bs)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .shutdown()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        })
    }
}

pub(super) enum LazyArrowWriter {
    Initialized(Path, ObjectStoreRef, PartitionWriterConfig),
    Writing(Path, ParallelArrowWriter<ParquetObjectWriter>),
}

impl LazyArrowWriter {
    /// A file at `path` that creates its writers on the first batch.
    pub(super) fn new(
        path: Path,
        object_store: ObjectStoreRef,
        config: PartitionWriterConfig,
    ) -> Self {
        LazyArrowWriter::Initialized(path, object_store, config)
    }

    pub(super) async fn write_batch(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        match self {
            LazyArrowWriter::Initialized(path, object_store, config) => {
                let writer = ParquetObjectWriter(
                    BufWriter::with_capacity(
                        Arc::clone(object_store),
                        path.clone(),
                        upload_part_size(),
                    )
                    .with_max_concurrency(config.max_concurrency_tasks),
                );
                let mut arrow_writer = ParallelArrowWriter::try_new(
                    writer,
                    config.file_schema.clone(),
                    config.writer_properties.clone(),
                    Some(config.arrow_options.clone()),
                )?;
                // A large first batch can complete row groups and start a multipart
                // upload before this call returns. On failure, `self` is still
                // `Initialized` — unreachable by the outer abort paths — so the
                // upload must be aborted here before surfacing the error.
                if let Err(e) = arrow_writer.write(batch).await {
                    let mut buf_writer = arrow_writer.into_inner();
                    if let Err(abort_err) = buf_writer.abort().await {
                        warn!(
                            "failed to abort multipart upload after a failed first write: {abort_err}"
                        );
                    }
                    return Err(e.into());
                }
                *self = LazyArrowWriter::Writing(path.clone(), arrow_writer);
            }
            LazyArrowWriter::Writing(_, arrow_writer) => {
                arrow_writer.write(batch).await?;
            }
        }

        Ok(())
    }

    pub(super) fn estimated_size(&self) -> usize {
        match self {
            LazyArrowWriter::Initialized(_, _, _) => 0,
            LazyArrowWriter::Writing(_, arrow_writer) => {
                arrow_writer.bytes_written() + arrow_writer.in_progress_size()
            }
        }
    }

    /// Upper bound on the memory this file's upload still holds after the writer closes
    /// it. Two things are outstanding at that moment.
    ///
    /// First, the row group being built is still in memory. `memory_size` is parquet's
    /// figure for it, which parquet documents as at least what that row group will
    /// encode to.
    ///
    /// Second, bytes already sent to the object store that it has not acknowledged. The
    /// multipart uploader sends them as parts of `upload_part_size` and keeps up to
    /// `max_concurrency_tasks` of them in flight. Closing the file flushes the last row
    /// group, which starts parts for all of it at once, so the worst case is that row
    /// group plus `max_concurrency_tasks - 1` full parts left over from earlier ones.
    /// The store cannot be holding bytes the writer never flushed, so this second term
    /// is capped at `bytes_written`.
    pub(super) fn pending_upload_bytes(&self, max_concurrency_tasks: usize) -> usize {
        match self {
            LazyArrowWriter::Initialized(_, _, _) => 0,
            LazyArrowWriter::Writing(_, arrow_writer) => {
                let buffered = arrow_writer.memory_size();
                let last_row_group = arrow_writer
                    .flushed_row_groups()
                    .last()
                    .map_or(0, |rg| rg.compressed_size().max(0) as usize);
                let older_parts = max_concurrency_tasks.saturating_sub(1) * upload_part_size();
                let in_flight_parts =
                    (last_row_group + older_parts).min(arrow_writer.bytes_written());
                buffered + in_flight_parts
            }
        }
    }

    /// Finish the file in the returned future, see [`finish_parquet_file`]. The future holds
    /// `permit` until the upload is done. `None` when the file never got a row, so there is
    /// nothing to write.
    pub(super) fn finish(
        self,
        permit: OwnedSemaphorePermit,
    ) -> Option<impl Future<Output = DeltaResult<(Path, usize, ParquetMetaData)>> + Send + 'static>
    {
        match self {
            LazyArrowWriter::Initialized(_, _, _) => None,
            LazyArrowWriter::Writing(path, arrow_writer) => {
                Some(finish_parquet_file(arrow_writer, path, permit))
            }
        }
    }

    /// Abort the in-progress multipart upload, if any. Dropping the writer
    /// instead would leak upload parts, which vacuum cannot see.
    pub(super) async fn abort(self) -> DeltaResult<()> {
        if let LazyArrowWriter::Writing(_, arrow_writer) = self {
            let mut buf_writer = arrow_writer.into_inner();
            buf_writer.abort().await?;
        }
        Ok(())
    }

    pub(super) fn in_progress_rows(&self) -> usize {
        match self {
            LazyArrowWriter::Initialized(_, _, _) => 0,
            LazyArrowWriter::Writing(_, arrow_writer) => arrow_writer.in_progress_rows(),
        }
    }
}
