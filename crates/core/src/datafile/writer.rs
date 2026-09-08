//! Abstractions and implementations for writing data to delta tables

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use bytes::Bytes;
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use tokio::io::AsyncWriteExt as _;
use tokio::task::JoinSet;
use tracing::*;

use crate::datafile::{BatchStream, DataFileWriter, DeltaDataWriter};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Add, PartitionsExt};
use crate::logstore::ObjectStoreRef;
use crate::parquet_utils::default_writer_properties;
use crate::writer::record_batch::{PartitionResult, divide_by_partition_values};
use crate::writer::stats::create_add;
use crate::writer::utils::{
    arrow_schema_without_partitions, next_data_path, record_batch_without_partitions,
};

use parquet::file::metadata::ParquetMetaData;

const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;
const DEFAULT_UPLOAD_PART_SIZE: usize = 1024 * 1024 * 5;
const DEFAULT_MAX_CONCURRENCY_TASKS: usize = 10;

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

fn get_max_concurrency_tasks() -> usize {
    static MAX_CONCURRENCY_TASKS: OnceLock<usize> = OnceLock::new();
    *MAX_CONCURRENCY_TASKS.get_or_init(|| {
        std::env::var("DELTARS_MAX_CONCURRENCY_TASKS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONCURRENCY_TASKS)
    })
}

const DEFAULT_WRITER_BATCH_CHANNEL_SIZE: usize = 10;

fn parse_writer_batch_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .filter(|size| *size > 0)
        .unwrap_or(DEFAULT_WRITER_BATCH_CHANNEL_SIZE)
}

/// How many record batches may be in flight on a write path. It bounds the
/// producer→writer channel capacity in `write_streams` (and the change-data
/// fan-in), and the `buffered()` drain depth in [`DeltaDataWriter::write_all`].
/// Tunable via `DELTARS_WRITER_BATCH_CHANNEL_SIZE` (default 10); read once.
pub(crate) fn writer_batch_concurrency() -> usize {
    static CONCURRENCY: OnceLock<usize> = OnceLock::new();
    *CONCURRENCY.get_or_init(|| {
        parse_writer_batch_concurrency(
            std::env::var("DELTARS_WRITER_BATCH_CHANNEL_SIZE")
                .ok()
                .as_deref(),
        )
    })
}

fn roll_on_row_group_boundary_default() -> bool {
    static ROLL_ON_ROW_GROUP_BOUNDARY: OnceLock<bool> = OnceLock::new();
    *ROLL_ON_ROW_GROUP_BOUNDARY.get_or_init(|| {
        std::env::var("DELTARS_ROLL_ON_ROW_GROUP_BOUNDARY")
            .map(|s| matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
    })
}

/// Upload a parquet file to object store and return metadata for creating an Add action
#[instrument(skip(arrow_writer), fields(rows = 0, size = 0))]
async fn upload_parquet_file(
    mut arrow_writer: AsyncArrowWriter<ParquetObjectWriter>,
    path: Path,
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

fn sort_completed_writes_by_path<T>(results: &mut [(Path, usize, T)]) {
    results.sort_unstable_by(|a, b| a.0.cmp(&b.0));
}

#[derive(thiserror::Error, Debug)]
enum WriteError {
    #[error("Unexpected Arrow schema: got: {schema}, expected: {expected_schema}")]
    SchemaMismatch {
        schema: ArrowSchemaRef,
        expected_schema: ArrowSchemaRef,
    },

    #[error("Error creating add action: {source}")]
    CreateAdd {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Error handling Arrow data: {source}")]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error("Error partitioning record batch: {0}")]
    Partitioning(String),
}

impl From<WriteError> for DeltaTableError {
    fn from(err: WriteError) -> Self {
        match err {
            WriteError::SchemaMismatch { .. } => DeltaTableError::SchemaMismatch {
                msg: err.to_string(),
            },
            WriteError::Arrow { source } => DeltaTableError::Arrow { source },
            _ => DeltaTableError::GenericError {
                source: Box::new(err),
            },
        }
    }
}

/// Configuration to write data into Delta tables
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Schema of the delta table
    table_schema: ArrowSchemaRef,
    /// Column names for columns the table is partitioned by
    partition_columns: Vec<String>,
    /// Properties passed to underlying parquet writer
    writer_properties: WriterProperties,
    /// Size above which we will write a buffered parquet file to disk.
    /// If None, the writer will not create a new file until the writer is closed.
    target_file_size: Option<NonZeroU64>,
    /// Row chunks passed to parquet writer. This and the internal parquet writer settings
    /// determine how fine granular we can track / control the size of resulting files.
    write_batch_size: usize,
    /// Num index cols to collect stats for
    num_indexed_cols: DataSkippingNumIndexedCols,
    /// Stats columns, specific columns to collect stats from, takes precedence over num_indexed_cols
    stats_columns: Option<Vec<String>>,
    /// When set, write data files under a random prefix directory of this length instead of
    /// Hive-style partition dirs — keeps physical (UUID) column names out of paths under CM.
    random_prefix_length: Option<usize>,
}

impl WriterConfig {
    /// Create a new instance of [WriterConfig].
    pub fn new(
        table_schema: ArrowSchemaRef,
        partition_columns: Vec<String>,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<NonZeroU64>,
        write_batch_size: Option<usize>,
        num_indexed_cols: DataSkippingNumIndexedCols,
        stats_columns: Option<Vec<String>>,
    ) -> Self {
        let writer_properties =
            writer_properties.unwrap_or_else(|| default_writer_properties(Compression::SNAPPY));
        let write_batch_size = write_batch_size.unwrap_or(DEFAULT_WRITE_BATCH_SIZE);

        Self {
            table_schema,
            partition_columns,
            writer_properties,
            target_file_size,
            write_batch_size,
            num_indexed_cols,
            stats_columns,
            random_prefix_length: None,
        }
    }

    /// Write data files under a random prefix of `length` chars instead of Hive-style dirs
    /// (column-mapped tables); `None` keeps the Hive layout.
    pub fn with_random_prefix_length(mut self, length: Option<usize>) -> Self {
        self.random_prefix_length = length;
        self
    }

    /// Schema of files written to disk
    pub fn file_schema(&self) -> ArrowSchemaRef {
        arrow_schema_without_partitions(&self.table_schema, &self.partition_columns)
    }
}

/// A parquet writer implementation tailored to the needs of writing data to a delta table.
pub struct DeltaWriter {
    /// An object store pointing at Delta table root
    object_store: ObjectStoreRef,
    /// configuration for the writers
    config: WriterConfig,
    /// Physical file schema (table schema with partition columns removed), derived
    /// once at construction. The per-batch write paths read this instead of calling
    /// `WriterConfig::file_schema()`, which reallocates the schema on every call.
    /// Invariant: it depends only on the config's table schema + partition columns,
    /// so any future setter for those must refresh this field.
    file_schema: ArrowSchemaRef,
    /// partition writers for individual partitions
    partition_writers: HashMap<Path, PartitionWriter>,
}

impl DeltaWriter {
    /// Create a new instance of [`DeltaWriter`]
    pub fn new(object_store: ObjectStoreRef, config: WriterConfig) -> Self {
        let file_schema = config.file_schema();
        Self {
            object_store,
            config,
            file_schema,
            partition_writers: HashMap::new(),
        }
    }

    /// Apply custom writer_properties to the underlying parquet writer
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.config.writer_properties = writer_properties;
        self
    }

    fn divide_by_partition_values(
        &mut self,
        values: &RecordBatch,
    ) -> DeltaResult<Vec<PartitionResult>> {
        Ok(divide_by_partition_values(
            self.file_schema.clone(),
            &self.config.partition_columns,
            values,
        )
        .map_err(|err| WriteError::Partitioning(err.to_string()))?)
    }

    /// Build a fresh [`PartitionWriter`] for the given partition values.
    fn build_partition_writer(
        &self,
        partition_values: IndexMap<String, Scalar>,
    ) -> DeltaResult<PartitionWriter> {
        let prefix_override = match self.config.random_prefix_length {
            Some(length) => Some(Path::parse(random_prefix(length))?),
            None => None,
        };
        let config = PartitionWriterConfig::try_new(
            self.file_schema.clone(),
            partition_values,
            Some(self.config.writer_properties.clone()),
            self.config.target_file_size,
            Some(self.config.write_batch_size),
            None,
            prefix_override,
        )?;
        PartitionWriter::try_with_config(
            self.object_store.clone(),
            config,
            self.config.num_indexed_cols,
            self.config.stats_columns.clone(),
        )
    }

    /// Find-or-create the partition writer for `key` and stream `batch` into it.
    /// `make_values` supplies the new writer's partition values and is called only
    /// when a writer is created. The writer is inserted only after its first write
    /// succeeds, so a failed first write leaves no half-open writer behind.
    async fn write_keyed(
        &mut self,
        key: Path,
        batch: &RecordBatch,
        make_values: impl FnOnce() -> IndexMap<String, Scalar>,
    ) -> DeltaResult<()> {
        match self.partition_writers.get_mut(&key) {
            Some(writer) => writer.write(batch).await?,
            None => {
                let mut writer = self.build_partition_writer(make_values())?;
                if let Err(e) = writer.write(batch).await {
                    // The writer was never inserted, so a later `DeltaWriter::abort`
                    // can't reach it — clean up its upload here.
                    if let Err(abort_err) = writer.abort().await {
                        warn!("failed to abort an in-progress multipart upload: {abort_err}");
                    }
                    return Err(e);
                }
                let _ = self.partition_writers.insert(key, writer);
            }
        }
        Ok(())
    }

    /// Write a batch to the partition induced by `partition_values`. The batch must
    /// be pre-partitioned (all rows in one partition) but still include the
    /// partition columns; they are stripped before encoding.
    pub async fn write_partition(
        &mut self,
        record_batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
    ) -> DeltaResult<()> {
        let key = Path::parse(partition_values.hive_partition_path())?;
        let record_batch =
            record_batch_without_partitions(&record_batch, &self.config.partition_columns)?;
        self.write_keyed(key, &record_batch, || partition_values.clone())
            .await
    }

    /// Fast path for unpartitioned tables: a single partition writer keyed by the
    /// empty path, skipping the per-batch partition split and projection (an
    /// identity for an unpartitioned schema) — the batch goes straight to the writer.
    async fn write_unpartitioned(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        self.write_keyed(Path::default(), batch, IndexMap::new)
            .await
    }

    /// Buffers record batches in-memory per partition up to appx. `target_file_size` for a partition.
    /// Flushes data to storage once a full file can be written.
    ///
    /// The `close` method has to be invoked to write all data still buffered
    /// and get the list of all written files.
    pub async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        if self.config.partition_columns.is_empty() {
            return self.write_unpartitioned(batch).await;
        }
        for result in self.divide_by_partition_values(batch)? {
            self.write_partition(result.record_batch, &result.partition_values)
                .await?;
        }
        Ok(())
    }

    /// Approximate encoded (parquet) size written across all partitions and
    /// not yet returned as `Add`s (which only surface at `close`).
    pub(crate) fn buffered_size(&self) -> usize {
        self.partition_writers
            .values()
            .map(PartitionWriter::buffered_size)
            .sum()
    }

    /// Abandon the writer, aborting every partition's in-progress multipart
    /// upload. Already-completed files are left as orphans for vacuum.
    pub async fn abort(mut self) -> DeltaResult<()> {
        let mut first_err = None;
        for (_, writer) in std::mem::take(&mut self.partition_writers) {
            if let Err(e) = writer.abort().await {
                warn!("failed to abort an in-progress multipart upload: {e}");
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Best-effort [`abort`](Self::abort) for synchronous callers: spawns the
    /// cleanup on the current tokio runtime, or logs and leaks the upload
    /// parts when there is none. `abort` warns per failed partition, so the
    /// returned error needs no extra logging here.
    pub(crate) fn abort_detached(self) {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let _ = self.abort().await;
                });
            }
            Err(_) => warn!(
                "no tokio runtime available; abandoning in-progress multipart uploads without abort"
            ),
        }
    }

    /// Close the writer and get the new [Add] actions.
    ///
    /// This will flush all remaining data.
    pub async fn close(mut self) -> DeltaResult<Vec<Add>> {
        let writers = std::mem::take(&mut self.partition_writers);
        // The common (unpartitioned) case has a single writer; close it directly and
        // skip the concurrent-fan-out machinery (and the `num_cpus` probe).
        if writers.len() <= 1 {
            let mut actions = Vec::new();
            for (_, writer) in writers {
                actions.extend(writer.close().await?);
            }
            return Ok(actions);
        }
        // On error, keep draining the remaining closes rather than short-circuiting:
        // cancelling them mid-upload would leak multipart parts vacuum can't see,
        // while completed files are orphans it can reclaim.
        let mut close_stream = futures::stream::iter(writers)
            .map(|(_, writer)| writer.close())
            .buffered(num_cpus::get());
        let mut actions = Vec::new();
        let mut first_err: Option<DeltaTableError> = None;
        while let Some(result) = close_stream.next().await {
            match result {
                Ok(writer_actions) => actions.extend(writer_actions),
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(actions)
    }
}

/// Per-batch write metrics accumulated while draining a batch stream.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DrainMetrics {
    /// Cumulative time spent inside [`DeltaWriter::write`] (ms).
    pub write_time_ms: u64,
    /// Total rows written.
    pub rows_written: u64,
}

/// Write every batch from `batches` through `writer`, accumulating the total
/// write time and row count. This is the single per-batch drain loop shared by
/// the basic [`DeltaDataWriter::write_all`] and the DataFusion producer/consumer
/// path (`write_streams`).
pub(crate) async fn write_batches_timed<S>(
    writer: &mut DeltaWriter,
    mut batches: S,
) -> DeltaResult<DrainMetrics>
where
    S: Stream<Item = DeltaResult<RecordBatch>> + Unpin,
{
    let mut metrics = DrainMetrics::default();
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        metrics.rows_written += batch.num_rows() as u64;
        let wstart = std::time::Instant::now();
        writer.write(&batch).await?;
        metrics.write_time_ms += wstart.elapsed().as_millis() as u64;
    }
    Ok(metrics)
}

#[async_trait::async_trait]
impl DeltaDataWriter for DeltaWriter {
    async fn write_all(mut self: Box<Self>, batches: BatchStream) -> DeltaResult<Vec<Add>> {
        // Batches are written in input-stream order, so the file order follows
        // the input (which a caller that merges partition streams may itself
        // interleave).
        if let Err(e) = write_batches_timed(&mut self, batches).await {
            // Abort rather than drop: dropping leaks the open multipart uploads.
            // Log abort failures but always return the original write error — callers cannot
            // meaningfully act on a secondary abort error, and leaked parts are cleaned
            // up by vacuum's lifecycle policy.
            if let Err(abort_err) = (*self).abort().await {
                warn!(
                    "failed to abort in-progress multipart uploads after write error: {abort_err}"
                );
            }
            return Err(e);
        }
        (*self).close().await
    }
}

/// Random hex (URI-safe) directory prefix of `length` chars, used to keep physical column
/// names out of data-file paths on column-mapped tables.
fn random_prefix(length: usize) -> String {
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    uuid[..length.min(uuid.len())].to_string()
}

/// Write configuration for partition writers
#[derive(Debug, Clone)]
pub struct PartitionWriterConfig {
    /// Schema of the data written to disk
    file_schema: ArrowSchemaRef,
    /// Prefix applied to all paths
    prefix: Path,
    /// Values for all partition columns
    partition_values: IndexMap<String, Scalar>,
    /// Properties passed to underlying parquet writer
    writer_properties: WriterProperties,
    /// Size above which we will write a buffered parquet file to disk.
    /// If None, the writer will not create a new file until the writer is closed.
    target_file_size: Option<NonZeroU64>,
    /// Row chunks passed to parquet writer. This and the internal parquet writer settings
    /// determine how fine granular we can track / control the size of resulting files.
    write_batch_size: usize,
    /// Concurrency level for writing to object store
    max_concurrency_tasks: usize,
    /// Defer the `target_file_size` roll until the current row group is complete, so no
    /// file ends in a truncated row group. See
    /// [`PartitionWriterConfig::with_roll_on_row_group_boundary`].
    roll_on_row_group_boundary: bool,
}

impl PartitionWriterConfig {
    /// Create a new instance of [PartitionWriterConfig]
    pub fn try_new(
        file_schema: ArrowSchemaRef,
        partition_values: IndexMap<String, Scalar>,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<NonZeroU64>,
        write_batch_size: Option<usize>,
        max_concurrency_tasks: Option<usize>,
        prefix_override: Option<Path>,
    ) -> DeltaResult<Self> {
        let prefix = match prefix_override {
            Some(prefix) => prefix,
            None => Path::parse(partition_values.hive_partition_path())?,
        };
        let writer_properties =
            writer_properties.unwrap_or_else(|| default_writer_properties(Compression::SNAPPY));
        if write_batch_size == Some(0) {
            return Err(DeltaTableError::generic(
                "write_batch_size must be greater than 0",
            ));
        }
        let write_batch_size = write_batch_size.unwrap_or(DEFAULT_WRITE_BATCH_SIZE);

        Ok(Self {
            file_schema,
            prefix,
            partition_values,
            writer_properties,
            target_file_size,
            write_batch_size,
            max_concurrency_tasks: max_concurrency_tasks.unwrap_or_else(get_max_concurrency_tasks),
            roll_on_row_group_boundary: roll_on_row_group_boundary_default(),
        })
    }

    /// Defer the `target_file_size` file roll until the parquet writer's current row group is
    /// complete, so no file ends in a truncated row group.
    ///
    /// A row group cannot span files, so the plain byte roll cuts each file's last row group
    /// wherever the target happens to land. Columnar readers that map one row group to one
    /// in-memory segment degrade on those runt tail groups. With this enabled, every row group
    /// is exactly the configured `max_row_group_row_count` — the single end-of-data remainder
    /// written at close is the one exception — and a file may overshoot `target_file_size` by
    /// up to one row group.
    ///
    /// Only effective when the writer properties bound row groups by row count alone
    /// (`max_row_group_row_count` set, `max_row_group_bytes` unset); byte-bounded row groups
    /// keep the legacy roll behavior. Defaults to the `DELTARS_ROLL_ON_ROW_GROUP_BOUNDARY`
    /// env var ("1"/"true"/"yes"), else `false`.
    pub fn with_roll_on_row_group_boundary(mut self, roll_on_row_group_boundary: bool) -> Self {
        self.roll_on_row_group_boundary = roll_on_row_group_boundary;
        self
    }
}

/// [`ParquetObjectWriter`] for writing to parquet to an [`object_store::ObjectStore`].
///
/// Copied from parquet 59.2, which deprecated it in favor of passing a
/// [`BufWriter`] to [`AsyncArrowWriter`] directly (apache/arrow-rs#10354). That
/// route goes through `AsyncWrite`; this one keeps [`BufWriter::put`], which
/// "can write data without extra copying".
struct ParquetObjectWriter(BufWriter);

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

enum LazyArrowWriter {
    Initialized(Path, ObjectStoreRef, PartitionWriterConfig),
    Writing(Path, AsyncArrowWriter<ParquetObjectWriter>),
}

impl LazyArrowWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
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
                let mut arrow_writer = AsyncArrowWriter::try_new(
                    writer,
                    config.file_schema.clone(),
                    Some(config.writer_properties.clone()),
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

    fn estimated_size(&self) -> usize {
        match self {
            LazyArrowWriter::Initialized(_, _, _) => 0,
            LazyArrowWriter::Writing(_, arrow_writer) => {
                arrow_writer.bytes_written() + arrow_writer.in_progress_size()
            }
        }
    }

    /// Abort the in-progress multipart upload, if any. Dropping the writer
    /// instead would leak upload parts, which vacuum cannot see.
    async fn abort(self) -> DeltaResult<()> {
        if let LazyArrowWriter::Writing(_, arrow_writer) = self {
            let mut buf_writer = arrow_writer.into_inner();
            buf_writer.abort().await?;
        }
        Ok(())
    }

    fn in_progress_rows(&self) -> usize {
        match self {
            LazyArrowWriter::Initialized(_, _, _) => 0,
            LazyArrowWriter::Writing(_, arrow_writer) => arrow_writer.in_progress_rows(),
        }
    }
}

/// Partition writer implementation
/// This writer takes in table data as RecordBatches and writes it out to partitioned parquet files.
/// It buffers data in memory until it reaches a certain size, then writes it out to optimize file sizes.
/// When you complete writing you get back a list of Add actions that can be used to update the Delta table commit log.
pub struct PartitionWriter {
    object_store: ObjectStoreRef,
    writer_id: uuid::Uuid,
    config: PartitionWriterConfig,
    writer: LazyArrowWriter,
    part_counter: usize,
    /// Num index cols to collect stats for
    num_indexed_cols: DataSkippingNumIndexedCols,
    /// Stats columns, specific columns to collect stats from, takes precedence over num_indexed_cols
    stats_columns: Option<Vec<String>>,
    in_flight_writers: JoinSet<DeltaResult<(Path, usize, ParquetMetaData)>>,
    /// Approximate encoded size of files already rolled to background upload;
    /// keeps `buffered_size` monotonic across rolls.
    rolled_bytes: usize,
}

impl PartitionWriter {
    /// Create a new instance of [`PartitionWriter`] from [`PartitionWriterConfig`]
    pub fn try_with_config(
        object_store: ObjectStoreRef,
        config: PartitionWriterConfig,
        num_indexed_cols: DataSkippingNumIndexedCols,
        stats_columns: Option<Vec<String>>,
    ) -> DeltaResult<Self> {
        let writer_id = uuid::Uuid::new_v4();
        let first_path = next_data_path(&config.prefix, 0, &writer_id, &config.writer_properties);
        let writer = Self::create_writer(object_store.clone(), first_path.clone(), &config);

        Ok(Self {
            object_store,
            writer_id,
            config,
            writer,
            part_counter: 0,
            num_indexed_cols,
            stats_columns,
            in_flight_writers: JoinSet::new(),
            rolled_bytes: 0,
        })
    }

    fn create_writer(
        object_store: ObjectStoreRef,
        path: Path,
        config: &PartitionWriterConfig,
    ) -> LazyArrowWriter {
        LazyArrowWriter::Initialized(path, object_store, config.clone())
    }

    fn next_data_path(&mut self) -> Path {
        self.part_counter += 1;

        next_data_path(
            &self.config.prefix,
            self.part_counter,
            &self.writer_id,
            &self.config.writer_properties,
        )
    }

    fn reset_writer(&mut self) -> DeltaResult<()> {
        let next_path = self.next_data_path();
        let new_writer = Self::create_writer(self.object_store.clone(), next_path, &self.config);
        let state = std::mem::replace(&mut self.writer, new_writer);

        if let LazyArrowWriter::Writing(path, arrow_writer) = state {
            self.rolled_bytes += arrow_writer.bytes_written() + arrow_writer.in_progress_size();
            self.in_flight_writers
                .spawn(upload_parquet_file(arrow_writer, path));
        }
        Ok(())
    }

    /// Approximate encoded (parquet) size written since creation: the
    /// in-progress file plus already-rolled files. Monotonic, so usable as a
    /// flush threshold.
    fn buffered_size(&self) -> usize {
        self.rolled_bytes + self.writer.estimated_size()
    }

    /// Rows the parquet writer will accept before its current row group completes, when the
    /// group-aligned roll is enabled and row groups are bounded by row count alone. `None`
    /// means slices need no alignment: the feature is off, no row-count bound is configured
    /// (both bounds unset writes a single row group per file), or row groups are byte-bounded
    /// and can close early on bytes, where a row-count-aligned slice cannot hit the boundary
    /// reliably.
    fn rows_to_row_group_boundary(&self) -> Option<usize> {
        if !self.config.roll_on_row_group_boundary {
            return None;
        }
        if self
            .config
            .writer_properties
            .max_row_group_bytes()
            .is_some()
        {
            return None;
        }
        let max_rows = self.config.writer_properties.max_row_group_row_count()?;
        Some(max_rows - (self.writer.in_progress_rows() % max_rows))
    }

    /// Buffers record batches in-memory up to appx. `target_file_size`.
    /// Flushes data to storage once a full file can be written.
    ///
    /// The `close` method has to be invoked to write all data still buffered
    /// and get the list of all written files.
    pub async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        if batch.schema() != self.config.file_schema {
            return Err(WriteError::SchemaMismatch {
                schema: batch.schema(),
                expected_schema: self.config.file_schema.clone(),
            }
            .into());
        }

        // Don't materialize the lazy writer for a 0-row batch — `close` would
        // upload an empty file and emit a spurious `Add`.
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let Some(target_file_size) = self.config.target_file_size else {
            // No size target means no file rolling, but still slice at row-group
            // boundaries: the async writer only uploads as row groups complete
            // within a `write_batch` call, so one huge call would buffer every
            // row group in memory first.
            let step = self
                .config
                .writer_properties
                .max_row_group_row_count()
                .unwrap_or(self.config.write_batch_size)
                .max(1);
            let max_offset = batch.num_rows();
            for offset in (0..max_offset).step_by(step) {
                let length = usize::min(step, max_offset - offset);
                self.writer
                    .write_batch(&batch.slice(offset, length))
                    .await?;
            }
            return Ok(());
        };

        // With a target file size we slice the batch so we can check the encoded
        // size between chunks and roll a new file once the target is reached.
        let max_offset = batch.num_rows();
        let mut offset = 0;
        while offset < max_offset {
            let mut length = usize::min(self.config.write_batch_size, max_offset - offset);
            // Never let a slice straddle a row-group boundary: the roll below may only fire
            // when the writer sits exactly on one (no rows buffered in an open group).
            let boundary = self.rows_to_row_group_boundary();
            if let Some(to_boundary) = boundary {
                length = usize::min(length, to_boundary);
            }
            self.writer
                .write_batch(&batch.slice(offset, length))
                .await?;
            offset += length;
            let estimated_size = self.writer.estimated_size();
            // flush currently buffered data to disk once we meet or exceed the target file
            // size — with the group-aligned roll, only once the open row group completed.
            if estimated_size as u64 >= target_file_size.get()
                && (boundary.is_none() || self.writer.in_progress_rows() == 0)
            {
                debug!("Writing file with estimated size {estimated_size:?} in background.");
                self.reset_writer()?;
            }
        }

        Ok(())
    }

    /// Close the writer and get the new [Add] actions.
    ///
    /// This will flush any remaining data and collect all Add actions from background tasks.
    pub async fn close(mut self) -> DeltaResult<Vec<Add>> {
        if let LazyArrowWriter::Writing(path, arrow_writer) = self.writer {
            self.in_flight_writers
                .spawn(upload_parquet_file(arrow_writer, path));
        }

        // On a failed upload, keep draining the siblings rather than returning
        // early: dropping the JoinSet would cancel them mid-multipart, leaking
        // parts vacuum can't see (completed files are orphans it can reclaim).
        let mut results = Vec::new();
        let mut first_err: Option<DeltaTableError> = None;
        while let Some(result) = self.in_flight_writers.join_next().await {
            match result {
                Ok(Ok(data)) => results.push(data),
                Ok(Err(e)) => {
                    first_err.get_or_insert(e);
                }
                Err(e) => {
                    first_err.get_or_insert(DeltaTableError::GenericError {
                        source: Box::new(e),
                    });
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }

        sort_completed_writes_by_path(&mut results);

        let adds = results
            .into_iter()
            .map(|(path, file_size, metadata)| {
                create_add(
                    &self.config.partition_values,
                    path.to_string(),
                    file_size as i64,
                    &metadata,
                    self.num_indexed_cols,
                    &self.stats_columns,
                )
                .map_err(|err| WriteError::CreateAdd {
                    source: Box::new(err),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(adds)
    }

    /// Abandon the writer: let in-flight size-roll uploads finish (cancelling
    /// mid-upload would leak parts vacuum cannot see; completed files are
    /// orphans it can reclaim), then abort the open file's multipart upload.
    pub async fn abort(mut self) -> DeltaResult<()> {
        while let Some(result) = self.in_flight_writers.join_next().await {
            // Outcome irrelevant: nothing from this writer is committed.
            let _ = result;
        }
        self.writer.abort().await
    }
}

// Expose the inherent `write`/`close` behind the [`DataFileWriter`] trait (the
// per-file seam). Fully-qualified calls select the inherent methods.
#[async_trait::async_trait]
impl DataFileWriter for PartitionWriter {
    async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        PartitionWriter::write(self, batch).await
    }

    async fn close(self: Box<Self>) -> DeltaResult<Vec<Add>> {
        PartitionWriter::close(*self).await
    }

    async fn abort(self: Box<Self>) -> DeltaResult<()> {
        PartitionWriter::abort(*self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeltaTableBuilder;
    use crate::crate_version;
    use crate::logstore::tests::flatten_list_stream as list;
    use crate::table::config::DEFAULT_NUM_INDEX_COLS;
    use crate::writer::test_utils::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use object_store::ObjectStoreExt as _;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::schema::types::ColumnPath;
    use std::sync::Arc;

    #[test]
    fn writer_batch_concurrency_zero_falls_back_to_default() {
        assert_eq!(
            parse_writer_batch_concurrency(Some("0")),
            DEFAULT_WRITER_BATCH_CHANNEL_SIZE
        );
    }

    #[test]
    fn writer_batch_concurrency_positive_value_is_used() {
        assert_eq!(parse_writer_batch_concurrency(Some("8")), 8);
    }

    #[test]
    fn writer_batch_concurrency_invalid_value_falls_back_to_default() {
        assert_eq!(
            parse_writer_batch_concurrency(Some("abc")),
            DEFAULT_WRITER_BATCH_CHANNEL_SIZE
        );
    }

    #[test]
    fn writer_batch_concurrency_missing_value_falls_back_to_default() {
        assert_eq!(
            parse_writer_batch_concurrency(None),
            DEFAULT_WRITER_BATCH_CHANNEL_SIZE
        );
    }

    fn get_delta_writer(
        object_store: ObjectStoreRef,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<NonZeroU64>,
        write_batch_size: Option<usize>,
    ) -> DeltaWriter {
        let config = WriterConfig::new(
            batch.schema(),
            vec![],
            writer_properties,
            target_file_size,
            write_batch_size,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        );
        DeltaWriter::new(object_store, config)
    }

    fn get_partition_writer(
        object_store: ObjectStoreRef,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<NonZeroU64>,
        write_batch_size: Option<usize>,
    ) -> PartitionWriter {
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            writer_properties,
            target_file_size,
            write_batch_size,
            None,
            None,
        )
        .unwrap();
        PartitionWriter::try_with_config(
            object_store,
            config,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        )
        .unwrap()
    }

    fn assert_default_created_by(writer_properties: &WriterProperties) {
        assert_eq!(
            writer_properties.created_by(),
            format!("delta-rs version {}", crate_version())
        );
    }

    #[tokio::test]
    async fn test_failed_first_write_cleans_up_without_panicking() {
        use crate::test_utils::failing_store::FailingMultipartStore;
        use arrow::array::Int64Array;
        use std::sync::atomic::Ordering;

        // Fail the multipart *creation*, so the failure surfaces inside the very
        // first `write` call — while the lazy writer is still in its
        // `Initialized` state, whose error path (unlike the `Writing` one) is
        // handled inline rather than by the outer abort machinery.
        let store = Arc::new(FailingMultipartStore::default());
        store.fail_multipart_create.store(true, Ordering::Release);

        // Two int64 columns × 1M rows ≈ 16MB encoded (uncompressed, no
        // dictionary): the first `write` call completes a full row group
        // (default cap 1M rows) and flushes it, forcing a multipart upload.
        let rows = 1024 * 1024;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let values = || Arc::new(Int64Array::from_iter_values(0..rows as i64));
        let batch = RecordBatch::try_new(schema, vec![values(), values()]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            Some(props),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let mut writer = PartitionWriter::try_with_config(
            store,
            config,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        )
        .unwrap();

        let result = writer.write(&batch).await;
        assert!(result.is_err(), "injected multipart failure must surface");
        // The first-write error path must leave the writer cleanly abortable —
        // no leaked upload, no panic.
        writer.abort().await.unwrap();
    }

    #[test]
    fn test_writer_config_defaults_include_delta_rs_created_by() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let config = WriterConfig::new(
            schema,
            vec![],
            None,
            None,
            None,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        );

        assert_default_created_by(&config.writer_properties);
        assert_eq!(
            config
                .writer_properties
                .compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
    }

    #[test]
    fn test_partition_writer_config_defaults_include_delta_rs_created_by() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let config =
            PartitionWriterConfig::try_new(schema, IndexMap::new(), None, None, None, None, None)
                .unwrap();

        assert_default_created_by(&config.writer_properties);
        assert_eq!(
            config
                .writer_properties
                .compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
    }

    #[tokio::test]
    async fn test_write_partition() {
        let log_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap();
        let object_store = log_store.object_store(None);
        let batch = get_record_batch(None, false);

        // write single un-partitioned batch
        let mut writer = get_partition_writer(object_store.clone(), &batch, None, None, None);
        writer.write(&batch).await.unwrap();
        let files = list(object_store.as_ref(), None).await.unwrap();
        assert_eq!(files.len(), 0);
        let adds = writer.close().await.unwrap();
        let files = list(object_store.as_ref(), None).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files.len(), adds.len());
        let head = object_store
            .head(&Path::from(adds[0].path.clone()))
            .await
            .unwrap();
        assert_eq!(head.size, adds[0].size as u64)
    }

    #[tokio::test]
    async fn test_write_partition_with_parts() {
        let base_int = Arc::new(Int32Array::from((0..10000).collect::<Vec<i32>>()));
        let base_str = Arc::new(StringArray::from(vec!["A"; 10000]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![base_str, base_int]).unwrap();

        let object_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store(None);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1024))
            .build();
        // configure small target file size and and row group size so we can observe multiple files written
        let mut writer = get_partition_writer(
            object_store,
            &batch,
            Some(properties),
            Some(NonZeroU64::new(10_000).unwrap()),
            None,
        );
        writer.write(&batch).await.unwrap();

        // check that we have written more then once file, and no more then 1 is below target size
        let adds = writer.close().await.unwrap();
        assert!(adds.len() > 1);
        let target_file_count = adds
            .iter()
            .fold(0, |acc, add| acc + (add.size > 10_000) as i32);
        assert!(target_file_count >= adds.len() as i32 - 1)
    }

    /// Write 10_000 rows in 1024-row groups with 700-row write batches and a tiny byte
    /// target, then return the row counts of every written row group. The 700-row batch
    /// size never lands on a 1024 multiple, so the byte target is always crossed with a
    /// row group open — only the group-aligned roll keeps groups intact.
    async fn write_and_collect_group_sizes(roll_on_row_group_boundary: bool) -> Vec<i64> {
        let base_int = Arc::new(Int32Array::from((0..10000).collect::<Vec<i32>>()));
        let base_str = Arc::new(StringArray::from(vec!["A"; 10000]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![base_str, base_int]).unwrap();

        let object_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store(None);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1024))
            .build();
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            Some(properties),
            Some(NonZeroU64::new(10_000).unwrap()),
            Some(700),
            None,
            None,
        )
        .unwrap()
        .with_roll_on_row_group_boundary(roll_on_row_group_boundary);
        let mut writer = PartitionWriter::try_with_config(
            object_store.clone(),
            config,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        let adds = writer.close().await.unwrap();
        // the byte target still splits the write into multiple files
        assert!(adds.len() > 1);

        let mut group_sizes = Vec::new();
        for add in &adds {
            let bytes = object_store
                .get(&Path::from(add.path.clone()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let reader = SerializedFileReader::new(bytes).unwrap();
            for rg in reader.metadata().row_groups() {
                group_sizes.push(rg.num_rows());
            }
        }
        assert_eq!(group_sizes.iter().sum::<i64>(), 10_000);
        group_sizes
    }

    #[tokio::test]
    async fn test_roll_on_row_group_boundary_never_truncates_groups() {
        // With the group-aligned roll, every row group is exactly 1024 rows except the
        // single end-of-data remainder (10_000 = 9 * 1024 + 784).
        let group_sizes = write_and_collect_group_sizes(true).await;
        let runts: Vec<_> = group_sizes.iter().filter(|n| **n != 1024).collect();
        assert_eq!(runts.len(), 1);
        assert_eq!(*runts[0], 10_000 % 1024);

        // Negative control: the same setup with the plain byte roll cuts row groups
        // mid-file, so the assertions above genuinely depend on the feature.
        let group_sizes = write_and_collect_group_sizes(false).await;
        let runts: Vec<_> = group_sizes.iter().filter(|n| **n != 1024).collect();
        assert!(runts.len() > 1);
    }

    #[tokio::test]
    async fn test_zero_write_batch_size_is_rejected() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let result = PartitionWriterConfig::try_new(
            schema,
            IndexMap::new(),
            None,
            None,
            Some(0),
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unflushed_row_group_size() {
        let base_int = Arc::new(Int32Array::from((0..10000).collect::<Vec<i32>>()));
        let base_str = Arc::new(StringArray::from(vec!["A"; 10000]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![base_str, base_int]).unwrap();

        let object_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store(None);
        // configure small target file size so we can observe multiple files written
        let mut writer = get_partition_writer(
            object_store,
            &batch,
            None,
            Some(NonZeroU64::new(10_000).unwrap()),
            None,
        );
        writer.write(&batch).await.unwrap();

        // check that we have written more then once file, and no more then 1 is below target size
        let adds = writer.close().await.unwrap();
        assert!(adds.len() > 1);
        let target_file_count = adds
            .iter()
            .fold(0, |acc, add| acc + (add.size > 10_000) as i32);
        assert!(target_file_count >= adds.len() as i32 - 1)
    }

    #[tokio::test]
    async fn test_do_not_write_empty_file_on_close() {
        let base_int = Arc::new(Int32Array::from((0..10000_i32).collect::<Vec<i32>>()));
        let base_str = Arc::new(StringArray::from(vec!["A"; 10000]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![base_str, base_int]).unwrap();

        let object_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store(None);
        // configure high batch size and low file size to observe one file written and flushed immediately
        // upon writing batch, then ensures the buffer is empty upon closing writer
        let mut writer = get_partition_writer(
            object_store,
            &batch,
            None,
            Some(NonZeroU64::new(9000).unwrap()),
            Some(10000),
        );
        writer.write(&batch).await.unwrap();

        let adds = writer.close().await.unwrap();
        assert_eq!(adds.len(), 1);
    }

    #[test]
    fn test_sort_completed_writes_by_path() {
        let mut results = vec![
            (Path::from("part-00002.parquet"), 3, 2_u8),
            (Path::from("part-00000.parquet"), 1, 0_u8),
            (Path::from("part-00001.parquet"), 2, 1_u8),
        ];

        sort_completed_writes_by_path(&mut results);

        let ordered_paths = results
            .iter()
            .map(|(path, _, _)| path.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            ordered_paths,
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet"
            ]
        );
    }

    #[tokio::test]
    async fn test_write_mismatched_schema() {
        let log_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap();
        let object_store = log_store.object_store(None);
        let batch = get_record_batch(None, false);

        // write single un-partitioned batch
        let mut writer = get_delta_writer(object_store.clone(), &batch, None, None, None);
        writer.write(&batch).await.unwrap();
        // Ensure the write hasn't been flushed
        let files = list(object_store.as_ref(), None).await.unwrap();
        assert_eq!(files.len(), 0);

        // Create a second batch with a different schema
        let second_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let second_batch = RecordBatch::try_new(
            second_schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("will"), Some("robert")])),
            ],
        )
        .unwrap();

        let result = writer.write(&second_batch).await;
        assert!(result.is_err());

        match result {
            Ok(_) => {
                panic!("Should not have successfully written");
            }
            Err(e) => {
                match e {
                    DeltaTableError::SchemaMismatch { .. } => {
                        // this is expected
                    }
                    others => {
                        panic!("Got the wrong error: {others:?}");
                    }
                }
            }
        };
    }
}
