//! Data-file read/write abstractions, in two tiers:
//!
//! * **File tier** ([`DataFileWriter`], [`DataFileReader`]) — the per-file
//!   seam where parquet `WriterProperties` attach.
//!   Impl: [`writer::PartitionWriter`].
//! * **Dataset tier** ([`DeltaDataWriter`], [`DeltaDataReader`]) — composes the
//!   file tier across a table. Impl: [`writer::DeltaWriter`].
//!
//! Both tiers operate on a DataFusion-free stream of [`BatchFuture`]s. The
//! DataFusion-capable surface lives in the gated [`datafusion_ext`] module.

use arrow_array::RecordBatch;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream, StreamExt as _};

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;

pub mod properties;
pub mod reader;
pub mod writer;

#[cfg(feature = "datafusion")]
pub mod datafusion_ext;

pub use properties::ReaderProperties;

/// A future resolving to a single [`RecordBatch`] — the unit of late materialization.
pub type BatchFuture = BoxFuture<'static, DeltaResult<RecordBatch>>;

/// A stream of [`BatchFuture`]s; draining it with bounded concurrency
/// (e.g. [`futures::StreamExt::buffered`]) yields parallel reads/writes.
pub type RecordBatchFutureStream = BoxStream<'static, BatchFuture>;

/// Adapt a fallible `RecordBatch` stream into a [`RecordBatchFutureStream`]:
/// each item becomes a ready [`BatchFuture`] with its error mapped into
/// [`DeltaTableError`]. Shared by the parquet file reader ([`reader`]) and the
/// DataFusion stream adapters ([`datafusion_ext`]).
pub(crate) fn results_to_future_stream<S, E>(stream: S) -> RecordBatchFutureStream
where
    S: Stream<Item = Result<RecordBatch, E>> + Send + 'static,
    E: Into<DeltaTableError> + Send + 'static,
{
    stream
        .map(|res| -> BatchFuture { Box::pin(async move { res.map_err(Into::into) }) })
        .boxed()
}

/// File tier: writes a single Delta data file (or size-split set for one
/// partition). The per-file seam where parquet `WriterProperties`/encryption
/// attach. Impl: [`writer::PartitionWriter`].
#[async_trait::async_trait]
pub trait DataFileWriter: Send {
    /// Buffer a record batch, writing to one or more parquet files as needed.
    /// The batch must match the writer's (partition-stripped) file schema.
    async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()>;

    /// Finish writing and return the uncommitted [`Add`] actions for the files
    /// that were produced.
    async fn close(self: Box<Self>) -> DeltaResult<Vec<Add>>;
}

/// File tier: reads a single parquet data file (the per-file decryption seam,
/// mirroring [`DataFileWriter`]). Impl: [`reader::ParquetFileReader`].
#[async_trait::async_trait]
pub trait DataFileReader: Send + Sync {
    /// Read the parquet data file at `path` into a stream of record batches.
    async fn read_file(
        &self,
        path: object_store::path::Path,
    ) -> DeltaResult<RecordBatchFutureStream>;
}

/// Options for a basic (DataFusion-free) read. Richer predicate/projection
/// pushdown is the DataFusion extension's job ([`datafusion_ext::DeltaDataReaderExt`]).
#[derive(Debug, Default, Clone)]
pub struct ReadOptions {
    /// Project to this subset of (logical) column names. `None` reads all columns.
    pub projection: Option<Vec<String>>,
    /// Stop after returning at least this many rows. `None` reads the whole table.
    pub limit: Option<usize>,
}

impl ReadOptions {
    /// Project to the given logical column names.
    pub fn with_projection(mut self, projection: impl Into<Vec<String>>) -> Self {
        self.projection = Some(projection.into());
        self
    }

    /// Limit the number of rows returned.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Dataset tier: a DataFusion-free writer that drains a batch stream into a
/// table's data files (partitioning and composing a [`DataFileWriter`] per
/// partition). Batches must already conform to the table schema and constraints
/// (callers on the basic path validate themselves).
#[async_trait::async_trait]
pub trait DeltaDataWriter: Send {
    /// Drain the batch-future stream into data files, returning the uncommitted
    /// [`Add`] actions (still to be committed via a transaction).
    async fn write_all(self: Box<Self>, batches: RecordBatchFutureStream) -> DeltaResult<Vec<Add>>;
}

/// Dataset tier: a DataFusion-free reader that composes the file tier
/// ([`DataFileReader`]) across a table's data files, applying deletion
/// vectors, partition values, and column-mapping transforms.
#[async_trait::async_trait]
pub trait DeltaDataReader: Send + Sync {
    /// Read the selected data as a stream of batch futures.
    async fn read(&self, options: ReadOptions) -> DeltaResult<RecordBatchFutureStream>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_options_builders() {
        let options = ReadOptions::default()
            .with_projection(vec!["a".to_string(), "b".to_string()])
            .with_limit(5);
        assert_eq!(
            options.projection,
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(options.limit, Some(5));
    }
}
