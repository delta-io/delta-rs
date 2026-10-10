//! Dataset tier: [`DeltaWriter`] sends each batch to the [`PartitionWriter`] of its partition.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::expressions::Scalar;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use object_store::path::Path;
use parquet::file::properties::WriterProperties;
use tracing::*;

use super::{PartitionWriter, PartitionWriterConfig, UploadBudget, WriteError};
use crate::datafile::writer::parallel::ArrowWriterOptions;
use crate::datafile::{BatchStream, DeltaDataWriter, DeltaWriterProperties};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Add, PartitionsExt};
use crate::logstore::ObjectStoreRef;
use crate::writer::partition_split::{PartitionResult, divide_by_partition_values};
use crate::writer::utils::{arrow_schema_without_partitions, record_batch_without_partitions};

/// Configuration to write data into Delta tables
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Schema of the delta table
    table_schema: ArrowSchemaRef,
    /// Column names for columns the table is partitioned by
    partition_columns: Vec<String>,
    /// How the data files are encoded
    props: DeltaWriterProperties,
    /// When set, write data files under a random prefix directory of this length instead of
    /// Hive-style partition dirs — keeps physical (UUID) column names out of paths under CM.
    random_prefix_length: Option<usize>,
    /// Directory under the table root the data files go below (`_change_data`);
    /// `None` is the root.
    path_prefix: Option<Path>,
    /// [`UploadBudget`] for closed files still uploading. Every writer built from this
    /// config, or from a clone of it, shares it.
    upload_budget: UploadBudget,
}

impl WriterConfig {
    /// Create a new instance of [WriterConfig].
    pub fn new(
        table_schema: ArrowSchemaRef,
        partition_columns: Vec<String>,
        props: DeltaWriterProperties,
    ) -> Self {
        Self {
            table_schema,
            partition_columns,
            random_prefix_length: None,
            path_prefix: None,
            upload_budget: UploadBudget::for_write(props.target_file_size()),
            props,
        }
    }

    /// Write every data file below `prefix` under the table root, so the returned
    /// [`Add`] paths stay table-relative.
    pub fn with_path_prefix(mut self, prefix: Option<Path>) -> Self {
        self.path_prefix = prefix;
        self
    }

    /// Draw on `budget` instead of the fresh one [`WriterConfig::new`] makes, so configs
    /// that are not clones of each other still share one bound.
    pub(crate) fn with_upload_budget(mut self, budget: UploadBudget) -> Self {
        self.upload_budget = budget;
        self
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
        self.config.props.parquet = Some(writer_properties);
        self
    }

    /// Apply custom arrow_options to the underlying arrow writer
    pub fn with_arrow_options(mut self, arrow_options: ArrowWriterOptions) -> Self {
        self.config.props.arrow = arrow_options;
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
        let partition_prefix = match self.config.random_prefix_length {
            Some(length) => Path::parse(random_prefix(length))?,
            None => Path::parse(partition_values.hive_partition_path())?,
        };
        let prefix = match &self.config.path_prefix {
            Some(path_prefix) => path_prefix
                .parts()
                .chain(partition_prefix.parts())
                .collect(),
            None => partition_prefix,
        };
        let config = PartitionWriterConfig::try_new(
            self.file_schema.clone(),
            partition_values,
            self.config.props.clone(),
            None,
            Some(prefix),
        )?
        .with_upload_budget(self.config.upload_budget.clone());
        PartitionWriter::try_with_config(self.object_store.clone(), config)
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
        // skip the concurrent-fan-out machinery (and the `available_parallelism` probe).
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
            .buffered(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1),
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeltaTableBuilder;
    use crate::datafile::writer::test_utils::{assert_default_created_by, test_props};
    use crate::logstore::tests::flatten_list_stream as list;
    use crate::writer::test_utils::get_record_batch;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use parquet::basic::Compression;
    use parquet::schema::types::ColumnPath;
    use std::num::NonZeroU64;
    use std::sync::Arc;

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
            test_props(writer_properties, None, target_file_size, write_batch_size),
        );
        DeltaWriter::new(object_store, config)
    }

    #[test]
    fn test_writer_config_defaults_include_delta_rs_created_by() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let config = WriterConfig::new(schema, vec![], DeltaWriterProperties::default());

        let writer_properties = config.props.parquet_properties_or_default();
        assert_default_created_by(writer_properties);
        assert_eq!(
            writer_properties.compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
    }

    #[tokio::test]
    async fn test_write_mismatched_schema() {
        let log_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap();
        let object_store = log_store.object_store();
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

    #[tokio::test]
    async fn path_prefix_puts_files_below_it() {
        let object_store: ObjectStoreRef = Arc::new(object_store::memory::InMemory::new());
        let batch = get_record_batch(None, false);
        let config = WriterConfig::new(
            batch.schema(),
            vec!["modified".to_string()],
            DeltaWriterProperties::default(),
        )
        .with_path_prefix(Some(Path::from("_change_data")));
        let mut writer = DeltaWriter::new(object_store, config);
        writer.write(&batch).await.unwrap();
        let adds = writer.close().await.unwrap();
        assert!(!adds.is_empty());
        for add in adds {
            assert!(
                add.path.starts_with("_change_data/modified="),
                "{}",
                add.path
            );
        }
    }

    #[test]
    fn test_writer_config_clones_share_one_upload_budget() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let config = WriterConfig::new(schema.clone(), vec![], DeltaWriterProperties::default());
        let clone = config.clone();
        // Every writer of one write is built from clones of one config, so the
        // partitioned path shares a single bound.
        assert!(Arc::ptr_eq(
            &config.upload_budget.semaphore,
            &clone.upload_budget.semaphore
        ));
        let other = WriterConfig::new(schema, vec![], DeltaWriterProperties::default());
        // A separate write gets its own budget.
        assert!(!Arc::ptr_eq(
            &config.upload_budget.semaphore,
            &other.upload_budget.semaphore
        ));
        // Partition writers draw on the budget of the config they were built from.
        let writer = DeltaWriter::new(Arc::new(object_store::memory::InMemory::new()), config);
        let partition = writer.build_partition_writer(IndexMap::new()).unwrap();
        assert!(Arc::ptr_eq(
            &partition.config.upload_budget.semaphore,
            &writer.config.upload_budget.semaphore
        ));
    }
}
