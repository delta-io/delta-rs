//! Abstractions and implementations for writing data to delta tables

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use futures::{Stream, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::task::JoinSet;
use tracing::*;

use crate::datafile::{DataFileWriter, DeltaDataWriter, RecordBatchFutureStream};
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

/// Upload a parquet file to object store and return metadata for creating an Add action
#[instrument(skip(arrow_writer), fields(rows = 0, size = 0))]
async fn upload_parquet_file(
    mut arrow_writer: AsyncArrowWriter<ParquetObjectWriter>,
    path: Path,
) -> DeltaResult<(Path, usize, ParquetMetaData)> {
    let metadata = arrow_writer.finish().await?;
    let file_size = arrow_writer.bytes_written();
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

    /// Write a batch to the partition induced by the partition_values. The record batch is expected
    /// to be pre-partitioned and only contain rows that belong into the same partition.
    /// However, it should still contain the partition columns.
    pub async fn write_partition(
        &mut self,
        record_batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
    ) -> DeltaResult<()> {
        let partition_key = Path::parse(partition_values.hive_partition_path())?;

        let record_batch =
            record_batch_without_partitions(&record_batch, &self.config.partition_columns)?;

        match self.partition_writers.get_mut(&partition_key) {
            Some(writer) => {
                writer.write(&record_batch).await?;
            }
            None => {
                let mut writer = self.build_partition_writer(partition_values.clone())?;
                writer.write(&record_batch).await?;
                let _ = self.partition_writers.insert(partition_key, writer);
            }
        }

        Ok(())
    }

    /// Fast path for unpartitioned tables: a single partition writer keyed by the
    /// empty path, skipping the per-batch partition split and projection (an
    /// identity for an unpartitioned schema) — the batch goes straight to the writer.
    async fn write_unpartitioned(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        let partition_key = Path::default();
        match self.partition_writers.get_mut(&partition_key) {
            Some(writer) => writer.write(batch).await?,
            None => {
                let mut writer = self.build_partition_writer(IndexMap::new())?;
                writer.write(batch).await?;
                let _ = self.partition_writers.insert(partition_key, writer);
            }
        }
        Ok(())
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

    /// Total encoded (parquet) size currently buffered across all open partition
    /// files (i.e. data written but not yet finalized into a closed file).
    pub(crate) fn buffered_size(&self) -> usize {
        self.partition_writers
            .values()
            .map(PartitionWriter::buffered_size)
            .sum()
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
        let actions = futures::stream::iter(writers)
            .map(|(_, writer)| async move {
                let writer_actions = writer.close().await?;
                Ok::<_, DeltaTableError>(writer_actions)
            })
            .buffered(num_cpus::get())
            .try_fold(Vec::new(), |mut acc, actions| {
                acc.extend(actions);
                futures::future::ready(Ok(acc))
            })
            .await?;

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
    async fn write_all(
        mut self: Box<Self>,
        batches: RecordBatchFutureStream,
    ) -> DeltaResult<Vec<Add>> {
        // Resolve up to `writer_batch_concurrency()` batch futures ahead and write
        // them in input-stream order (`buffered`, not `buffer_unordered`), so this
        // adds no reordering of its own — the file order follows the input (which a
        // caller that merges partition streams may itself interleave). Today's
        // callers wrap already-materialized batches in ready futures (so this is
        // just a bounded drain, metrics unused), but the bound is honored for any
        // future streaming caller.
        let buffered = batches.buffered(writer_batch_concurrency().max(1));
        write_batches_timed(&mut self, buffered).await?;
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
        let write_batch_size = write_batch_size.unwrap_or(DEFAULT_WRITE_BATCH_SIZE);

        Ok(Self {
            file_schema,
            prefix,
            partition_values,
            writer_properties,
            target_file_size,
            write_batch_size,
            max_concurrency_tasks: max_concurrency_tasks.unwrap_or_else(get_max_concurrency_tasks),
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
                let writer = ParquetObjectWriter::from_buf_writer(
                    BufWriter::with_capacity(
                        object_store.clone(),
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
                arrow_writer.write(batch).await?;
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
            self.in_flight_writers
                .spawn(upload_parquet_file(arrow_writer, path));
        }
        Ok(())
    }

    /// Encoded (parquet) size currently buffered in the in-progress file.
    fn buffered_size(&self) -> usize {
        self.writer.estimated_size()
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

        let Some(target_file_size) = self.config.target_file_size else {
            // No size target: hand the whole batch to the arrow writer in one call.
            // It still forms row groups internally, so the output is identical, but
            // we avoid fragmenting a large batch into many tiny `write_batch` calls.
            self.writer.write_batch(batch).await?;
            return Ok(());
        };

        // With a target file size we slice the batch so we can check the encoded
        // size between chunks and roll a new file once the target is reached.
        let max_offset = batch.num_rows();
        for offset in (0..max_offset).step_by(self.config.write_batch_size) {
            let length = usize::min(self.config.write_batch_size, max_offset - offset);
            self.writer
                .write_batch(&batch.slice(offset, length))
                .await?;
            let estimated_size = self.writer.estimated_size();
            // flush currently buffered data to disk once we meet or exceed the target file size.
            if estimated_size as u64 >= target_file_size.get() {
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

        let mut results = Vec::new();
        while let Some(result) = self.in_flight_writers.join_next().await {
            match result {
                Ok(Ok(data)) => results.push(data),
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(e) => {
                    return Err(DeltaTableError::GenericError {
                        source: Box::new(e),
                    });
                }
            }
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
