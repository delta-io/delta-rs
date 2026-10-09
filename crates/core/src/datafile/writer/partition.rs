//! File tier: [`PartitionWriter`] writes the data files of one partition. It starts a new
//! file when the current one reaches the target file size.

use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use object_store::path::Path;
use parquet::file::metadata::ParquetMetaData;
use tokio::task::JoinSet;
use tracing::*;

use crate::datafile::writer::WriteError;
use crate::datafile::writer::file::LazyArrowWriter;
use crate::datafile::writer::upload_budget::UploadBudget;
use crate::datafile::{DEFAULT_WRITE_BATCH_SIZE, DataFileWriter, DeltaWriterProperties};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Add, PartitionsExt};
use crate::logstore::ObjectStoreRef;
use crate::writer::stats::create_add;
use crate::writer::utils::next_data_path;

const DEFAULT_MAX_CONCURRENCY_TASKS: usize = 10;

fn get_max_concurrency_tasks() -> usize {
    static MAX_CONCURRENCY_TASKS: OnceLock<usize> = OnceLock::new();
    *MAX_CONCURRENCY_TASKS.get_or_init(|| {
        std::env::var("DELTARS_MAX_CONCURRENCY_TASKS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONCURRENCY_TASKS)
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

fn sort_completed_writes_by_path<T>(results: &mut [(Path, usize, T)]) {
    results.sort_unstable_by(|a, b| a.0.cmp(&b.0));
}

/// Write configuration for partition writers
#[derive(Debug, Clone)]
pub struct PartitionWriterConfig {
    /// Schema of the data written to disk
    pub(super) file_schema: ArrowSchemaRef,
    /// Prefix applied to all paths
    prefix: Path,
    /// Values for all partition columns
    partition_values: IndexMap<String, Scalar>,
    /// How the files are encoded
    pub(super) props: DeltaWriterProperties,
    /// Row chunks passed to parquet writer. This and the internal parquet writer settings
    /// determine how fine granular we can track / control the size of resulting files.
    write_batch_size: usize,
    /// Concurrency level for writing to object store
    pub(super) max_concurrency_tasks: usize,
    /// Defer the `target_file_size` roll until the current row group is complete, so no
    /// file ends in a truncated row group. See
    /// [`PartitionWriterConfig::with_roll_on_row_group_boundary`].
    roll_on_row_group_boundary: bool,
    /// [`UploadBudget`] for closed files still uploading. Cloning the config shares it,
    /// so every file this writer closes draws on one bound.
    pub(super) upload_budget: UploadBudget,
}

impl PartitionWriterConfig {
    /// Create a new instance of [PartitionWriterConfig]
    pub fn try_new(
        file_schema: ArrowSchemaRef,
        partition_values: IndexMap<String, Scalar>,
        props: DeltaWriterProperties,
        max_concurrency_tasks: Option<usize>,
        prefix_override: Option<Path>,
    ) -> DeltaResult<Self> {
        let prefix = match prefix_override {
            Some(prefix) => prefix,
            None => Path::parse(partition_values.hive_partition_path())?,
        };
        if props.write_batch_size() == Some(0) {
            return Err(DeltaTableError::generic(
                "write_batch_size must be greater than 0",
            ));
        }
        let write_batch_size = props.write_batch_size().unwrap_or(DEFAULT_WRITE_BATCH_SIZE);

        Ok(Self {
            file_schema,
            prefix,
            partition_values,
            write_batch_size,
            max_concurrency_tasks: max_concurrency_tasks.unwrap_or_else(get_max_concurrency_tasks),
            roll_on_row_group_boundary: roll_on_row_group_boundary_default(),
            upload_budget: UploadBudget::for_write(props.target_file_size()),
            props,
        })
    }

    /// Draw on `budget` instead of the fresh one [`Self::try_new`] makes, so
    /// configs that are not clones of each other still share one bound.
    pub(crate) fn with_upload_budget(mut self, budget: UploadBudget) -> Self {
        self.upload_budget = budget;
        self
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

/// Partition writer implementation
/// This writer takes in table data as RecordBatches and writes it out to partitioned parquet files.
/// It buffers data in memory until it reaches a certain size, then writes it out to optimize file sizes.
/// When you complete writing you get back a list of Add actions that can be used to update the Delta table commit log.
pub struct PartitionWriter {
    object_store: ObjectStoreRef,
    writer_id: uuid::Uuid,
    pub(super) config: PartitionWriterConfig,
    writer: LazyArrowWriter,
    part_counter: usize,
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
    ) -> DeltaResult<Self> {
        let writer_id = uuid::Uuid::new_v4();
        let first_path = next_data_path(
            &config.prefix,
            0,
            &writer_id,
            config.props.parquet_properties_or_default(),
        );
        let writer = Self::create_writer(object_store.clone(), first_path.clone(), &config);

        Ok(Self {
            object_store,
            writer_id,
            config,
            writer,
            part_counter: 0,
            in_flight_writers: JoinSet::new(),
            rolled_bytes: 0,
        })
    }

    fn create_writer(
        object_store: ObjectStoreRef,
        path: Path,
        config: &PartitionWriterConfig,
    ) -> LazyArrowWriter {
        LazyArrowWriter::new(path, object_store, config.clone())
    }

    /// Bytes a background upload of the current file would hold; see
    /// [`LazyArrowWriter::pending_upload_bytes`].
    fn pending_upload_bytes(&self) -> usize {
        self.writer
            .pending_upload_bytes(self.config.max_concurrency_tasks)
    }

    fn next_data_path(&mut self) -> Path {
        self.part_counter += 1;

        next_data_path(
            &self.config.prefix,
            self.part_counter,
            &self.writer_id,
            self.config.props.parquet_properties_or_default(),
        )
    }

    async fn reset_writer(&mut self) -> DeltaResult<()> {
        // Reserve before taking the file out, so a cancelled wait leaves the
        // writer intact and abortable.
        let permit = self
            .config
            .upload_budget
            .reserve(self.pending_upload_bytes())
            .await;
        let next_path = self.next_data_path();
        let new_writer = Self::create_writer(self.object_store.clone(), next_path, &self.config);
        let file = std::mem::replace(&mut self.writer, new_writer);

        self.rolled_bytes += file.estimated_size();
        if let Some(finish) = file.finish(permit) {
            self.in_flight_writers.spawn(finish);
        }
        Ok(())
    }

    /// Approximate encoded (parquet) size written since creation: the
    /// in-progress file plus already-rolled files. Monotonic, so usable as a
    /// flush threshold.
    pub(super) fn buffered_size(&self) -> usize {
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
        let writer_properties = self.config.props.parquet_properties_or_default();
        if writer_properties.max_row_group_bytes().is_some() {
            return None;
        }
        let max_rows = writer_properties.max_row_group_row_count()?;
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

        let Some(target_file_size) = self.config.props.target_file_size() else {
            // No size target means no file rolling, but still slice at row-group
            // boundaries: the async writer only uploads as row groups complete
            // within a `write_batch` call, so one huge call would buffer every
            // row group in memory first.
            let step = self
                .config
                .props
                .parquet_properties_or_default()
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
                self.reset_writer().await?;
            }
        }

        Ok(())
    }

    /// Close the writer and get the new [Add] actions.
    ///
    /// This will flush any remaining data and collect all Add actions from background tasks.
    pub async fn close(mut self) -> DeltaResult<Vec<Add>> {
        // A file that never got a row reserves 0 bytes, which never waits.
        let permit = self
            .config
            .upload_budget
            .reserve(self.pending_upload_bytes())
            .await;
        if let Some(finish) = self.writer.finish(permit) {
            self.in_flight_writers.spawn(finish);
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
                    self.config.props.stats().num_indexed_cols,
                    &self.config.props.stats().stats_columns,
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
    use crate::datafile::writer::test_utils::{assert_default_created_by, test_props};
    use crate::logstore::tests::flatten_list_stream as list;
    use crate::writer::test_utils::get_record_batch;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use object_store::ObjectStoreExt as _;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::schema::types::ColumnPath;
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use crate::datafile::writer::ArrowWriterOptions;

    fn get_partition_writer(
        object_store: ObjectStoreRef,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<NonZeroU64>,
        write_batch_size: Option<usize>,
    ) -> PartitionWriter {
        partition_writer_sharing_budget(
            object_store,
            batch,
            writer_properties,
            target_file_size,
            write_batch_size,
            None,
            None,
        )
    }

    /// [`get_partition_writer`] plus the two knobs only the upload-budget tests set:
    /// a path prefix, so sibling writers do not collide, and a shared budget.
    fn partition_writer_sharing_budget(
        object_store: ObjectStoreRef,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<NonZeroU64>,
        write_batch_size: Option<usize>,
        prefix: Option<Path>,
        budget: Option<&UploadBudget>,
    ) -> PartitionWriter {
        let mut config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            test_props(writer_properties, None, target_file_size, write_batch_size),
            None,
            prefix,
        )
        .unwrap();
        if let Some(budget) = budget {
            config = config.with_upload_budget(budget.clone());
        }
        PartitionWriter::try_with_config(object_store, config).unwrap()
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
            test_props(Some(props), None, None, None),
            None,
            None,
        )
        .unwrap();
        let mut writer = PartitionWriter::try_with_config(store, config).unwrap();

        let result = writer.write(&batch).await;
        assert!(result.is_err(), "injected multipart failure must surface");
        // The first-write error path must leave the writer cleanly abortable —
        // no leaked upload, no panic.
        writer.abort().await.unwrap();
    }

    #[test]
    fn test_partition_writer_config_defaults_include_delta_rs_created_by() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let config = PartitionWriterConfig::try_new(
            schema,
            IndexMap::new(),
            DeltaWriterProperties::default(),
            None,
            None,
        )
        .unwrap();

        let writer_properties = config.props.parquet_properties_or_default();
        assert_default_created_by(writer_properties);
        assert_eq!(
            writer_properties.compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
    }

    #[tokio::test]
    async fn test_write_partition() {
        let log_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap();
        let object_store = log_store.object_store();
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

    #[rstest::rstest]
    #[case::parallel(true)]
    #[case::serial(false)]
    #[tokio::test]
    async fn test_write_partition_with_either_encoding(#[case] parallel: bool) {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let batch = get_record_batch(None, false);
        let object_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            test_props(
                Some(properties),
                Some(ArrowWriterOptions::new().with_enable_parallel_encoding(parallel)),
                None,
                Some(2),
            ),
            None,
            None,
        )
        .unwrap();
        let mut writer = PartitionWriter::try_with_config(object_store.clone(), config).unwrap();
        writer.write(&batch).await.unwrap();
        let adds = writer.close().await.unwrap();
        assert_eq!(adds.len(), 1);

        let bytes = object_store
            .get(&Path::from(adds[0].path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        assert!(reader.metadata().num_row_groups() > 1);
        let read: Vec<RecordBatch> = reader.build().unwrap().map(Result::unwrap).collect();
        let read = arrow::compute::concat_batches(&batch.schema(), &read).unwrap();
        assert_eq!(read, batch);
    }

    /// A batch may not straddle the row-group limit: the rows that fit complete the open group
    /// and the rest start the next one. Both encodings, on both slicing paths of
    /// [`PartitionWriter::write`]: a write batch above the limit (with a target file size), and
    /// a second write that starts with rows already buffered in an open group (without one).
    #[rstest::rstest]
    #[case::parallel_with_target(true, Some(10 * 1024 * 1024))]
    #[case::serial_with_target(false, Some(10 * 1024 * 1024))]
    #[case::parallel_without_target(true, None)]
    #[case::serial_without_target(false, None)]
    #[tokio::test]
    async fn test_row_groups_never_exceed_max_row_count(
        #[case] parallel: bool,
        #[case] target_file_size: Option<u64>,
    ) {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let object_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let config = PartitionWriterConfig::try_new(
            schema,
            IndexMap::new(),
            test_props(
                Some(properties),
                Some(ArrowWriterOptions::new().with_enable_parallel_encoding(parallel)),
                target_file_size.and_then(NonZeroU64::new),
                Some(8),
            ),
            None,
            None,
        )
        .unwrap();
        let mut writer = PartitionWriter::try_with_config(object_store.clone(), config).unwrap();
        writer.write(&batch).await.unwrap();
        writer.write(&batch).await.unwrap();
        let adds = writer.close().await.unwrap();
        assert_eq!(adds.len(), 1);

        let bytes = object_store
            .get(&Path::from(adds[0].path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = SerializedFileReader::new(bytes).unwrap();
        let group_sizes: Vec<i64> = reader
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .collect();
        assert_eq!(group_sizes, [3, 3, 3, 3, 3, 3, 2]);
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
            .object_store();
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
            .object_store();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1024))
            .build();
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            test_props(
                Some(properties),
                None,
                Some(NonZeroU64::new(10_000).unwrap()),
                Some(700),
            ),
            None,
            None,
        )
        .unwrap()
        .with_roll_on_row_group_boundary(roll_on_row_group_boundary);
        let mut writer = PartitionWriter::try_with_config(object_store.clone(), config).unwrap();
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
            test_props(None, None, None, Some(0)),
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
            .object_store();
        // configure small target file size so we can observe multiple files written;
        // small slices, so the size is checked often enough to roll at this target
        let mut writer = get_partition_writer(
            object_store,
            &batch,
            None,
            Some(NonZeroU64::new(10_000).unwrap()),
            Some(1024),
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
            .object_store();
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

    fn string_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]))
    }

    /// `rows` pseudo-random 64-char strings (about 64 KiB of string data per
    /// 1024 rows) that compression cannot shrink, so a small `target_file_size`
    /// closes files quickly.
    fn incompressible_batch(rows: usize) -> RecordBatch {
        use rand::{RngExt, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(42);
        let strings: Vec<String> = (0..rows)
            .map(|_| format!("{:032x}{:032x}", rng.random::<u128>(), rng.random::<u128>()))
            .collect();
        RecordBatch::try_new(
            string_schema(),
            vec![
                Arc::new(Int32Array::from((0..rows as i32).collect::<Vec<_>>())),
                Arc::new(StringArray::from(strings)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_upload_budget_is_shared_across_writers() {
        use crate::test_utils::slow_store::SlowCountingStore;
        use std::time::Duration;

        // Files roll once their estimate passes 256 KiB, i.e. between 256 KiB and
        // 256 KiB plus one 1024-row slice, so a 700 KiB budget admits at most two
        // uploads at a time across *both* writers. Every further roll must wait
        // for one of them to land.
        let store = Arc::new(SlowCountingStore::new(Duration::from_millis(50)));
        let budget = UploadBudget::new(700 * 1024);
        let batch = incompressible_batch(8192);
        let roll_at = Some(NonZeroU64::new(256 * 1024).unwrap());
        let mut left = partition_writer_sharing_budget(
            store.clone(),
            &batch,
            None,
            roll_at,
            Some(1024),
            Some(Path::from("left")),
            Some(&budget),
        );
        let mut right = partition_writer_sharing_budget(
            store.clone(),
            &batch,
            None,
            roll_at,
            Some(1024),
            Some(Path::from("right")),
            Some(&budget),
        );

        for _ in 0..8 {
            left.write(&batch).await.unwrap();
            right.write(&batch).await.unwrap();
        }
        let left_adds = left.close().await.unwrap();
        let right_adds = right.close().await.unwrap();

        // Each ~512 KiB batch rolls at least one file per writer.
        assert!(
            left_adds.len() >= 8 && right_adds.len() >= 8,
            "expected many rolled files, got {} and {}",
            left_adds.len(),
            right_adds.len()
        );
        let max_in_flight = store.max_in_flight();
        assert!(
            (1..=2).contains(&max_in_flight),
            "a two-file budget must cap concurrent uploads at two, saw {max_in_flight}"
        );
        assert_eq!(
            budget.available_bytes(),
            budget.bytes,
            "every reservation must be released once its upload has landed"
        );
    }

    #[tokio::test]
    async fn test_file_larger_than_upload_budget_still_uploads() {
        use crate::test_utils::slow_store::SlowCountingStore;
        use std::time::Duration;

        // A one-byte budget is smaller than any file. Uploads run one at a time
        // instead of deadlocking, and every file still lands.
        let store = Arc::new(SlowCountingStore::new(Duration::from_millis(10)));
        let budget = UploadBudget::new(1);
        let batch = incompressible_batch(8192);
        let mut writer = partition_writer_sharing_budget(
            store.clone(),
            &batch,
            None,
            Some(NonZeroU64::new(256 * 1024).unwrap()),
            Some(1024),
            None,
            Some(&budget),
        );

        for _ in 0..4 {
            writer.write(&batch).await.unwrap();
        }
        let adds = writer.close().await.unwrap();

        assert!(adds.len() >= 4, "expected rolled files, got {}", adds.len());
        assert_eq!(store.max_in_flight(), 1);
        assert_eq!(budget.available_bytes(), 1);
        for add in &adds {
            let meta = store.head(&Path::from(add.path.as_str())).await.unwrap();
            assert_eq!(meta.size as i64, add.size);
        }
    }
}
