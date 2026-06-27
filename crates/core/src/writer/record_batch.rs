//! Main writer API to write record batches to Delta Table
//!
//! Writes Arrow record batches to a Delta Table, handling partitioning and file statistics.
//! Each Parquet file is buffered in-memory and only written once `flush()` is called on
//! the writer. Once written, add actions are returned by the writer. It's the users responsibility
//! to create the transaction using those actions.

use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, UInt32Array, new_null_array};
use arrow_ord::partition::partition;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow_select::take::take;
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use indexmap::IndexMap;
use object_store::ObjectStore;
use parquet::file::properties::WriterProperties;
use tracing::log::*;

use super::{DeltaWriter, DeltaWriterError, WriteMode, ensure_legacy_writer_supports_table};
use crate::DeltaTable;
use crate::datafile::writer::DeltaWriter as DataFileDeltaWriter;
use crate::errors::DeltaTableError;
use crate::kernel::schema::cast::{cast_record_batch, normalize_for_delta};
use crate::kernel::schema::merge_arrow_schema;
use crate::kernel::transaction::CommitProperties;
use crate::kernel::{Action, Add, scalars::ScalarExt};
use crate::kernel::{MetadataExt as _, Version};
use crate::parquet_utils::default_writer_properties;
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::DEFAULT_NUM_INDEX_COLS;

/// Writes messages to a delta lake table.
pub struct RecordBatchWriter {
    storage: Arc<dyn ObjectStore>,
    arrow_schema_ref: ArrowSchemaRef,
    original_schema_ref: ArrowSchemaRef,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    /// Streaming sink (created lazily on first write). Batches are encoded into
    /// it incrementally; it is sealed at `flush` and rotated when a `MergeSchema`
    /// write widens the schema.
    sink: Option<DataFileDeltaWriter>,
    /// `Add` actions from sinks already sealed in this write window (schema-widening
    /// rotations); returned together at the next `flush`.
    pending_adds: Vec<Add>,
    /// Batches streamed since the last `flush` (for `buffered_record_batch_count`).
    buffered_batch_count: usize,
    /// Optional target file size; when set, the sink rolls a new file once an
    /// in-progress file reaches it. `None` (default) keeps one file per partition.
    target_file_size: Option<NonZeroU64>,
    num_indexed_cols: DataSkippingNumIndexedCols,
    stats_columns: Option<Vec<String>>,
    commit_properties: Option<CommitProperties>,
}

impl std::fmt::Debug for RecordBatchWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordBatchWriter")
    }
}

impl RecordBatchWriter {
    /// Create a new [`RecordBatchWriter`] instance
    pub fn try_new(
        table_uri: impl AsRef<str>,
        schema: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self, DeltaTableError> {
        let table_url = url::Url::parse(table_uri.as_ref())
            .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
        let delta_table = DeltaTableBuilder::from_url(table_url)?
            .with_storage_options(storage_options.unwrap_or_default())
            .build()?;
        // Initialize writer properties for the underlying arrow writer
        let writer_properties = default_writer_properties(parquet::basic::Compression::SNAPPY);

        // if metadata fails to load, use an empty hashmap and default values for num_indexed_cols and stats_columns
        let configuration = delta_table.snapshot().map_or_else(
            |_| HashMap::new(),
            |snapshot| snapshot.metadata().configuration().clone(),
        );

        Ok(Self::new_with_table(
            delta_table,
            schema,
            partition_columns,
            configuration,
            writer_properties,
        ))
    }

    /// Create a new [`RecordBatchWriter`] for an existing table after validating table metadata.
    pub async fn try_new_checked(
        table_uri: impl AsRef<str>,
        schema: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self, DeltaTableError> {
        let table_url = url::Url::parse(table_uri.as_ref())
            .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
        let delta_table = DeltaTableBuilder::from_url(table_url)?
            .with_storage_options(storage_options.unwrap_or_default())
            .load()
            .await?;
        ensure_legacy_writer_supports_table(&delta_table, "RecordBatchWriter")?;

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = default_writer_properties(parquet::basic::Compression::SNAPPY);
        let configuration = delta_table.snapshot()?.metadata().configuration().clone();

        Ok(Self::new_with_table(
            delta_table,
            schema,
            partition_columns,
            configuration,
            writer_properties,
        ))
    }

    /// Add the [CommitProperties] to the [RecordBatchWriter] to be used when the writer flushes
    /// the write into storage.
    ///
    /// This can be useful for situations where slight modifications to the commit behavior are
    /// required.
    pub fn with_commit_properties(mut self, properties: CommitProperties) -> Self {
        self.commit_properties = Some(properties);
        self
    }

    /// Creates a [`RecordBatchWriter`] to write data to provided Delta Table
    pub fn for_table(table: &DeltaTable) -> Result<Self, DeltaTableError> {
        ensure_legacy_writer_supports_table(table, "RecordBatchWriter")?;

        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.snapshot()?.metadata();
        let arrow_schema: ArrowSchema = (&metadata.parse_schema()?).try_into_arrow()?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns().into();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = default_writer_properties(parquet::basic::Compression::SNAPPY);
        let configuration = table.snapshot()?.metadata().configuration().clone();

        Ok(Self::from_parts(
            table.object_store(),
            arrow_schema_ref,
            partition_columns,
            &configuration,
            writer_properties,
        ))
    }

    /// Creates a [`RecordBatchWriter`] to write data to an [`BlindDeltaTable`].
    ///
    /// This is optimized for append-only writes where file statistics are not needed
    /// during table loading.
    ///
    /// [`BlindDeltaTable`]: crate::table::AppendableDeltaTable
    pub fn for_blind_appends(
        table: &crate::table::BlindDeltaTable,
    ) -> Result<Self, DeltaTableError> {
        let metadata = table.metadata();
        let arrow_schema: ArrowSchema = (&metadata.parse_schema()?).try_into_arrow()?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns().to_vec();

        let writer_properties = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let configuration = metadata.configuration().clone();

        Ok(Self::from_parts(
            table.object_store(),
            arrow_schema_ref,
            partition_columns,
            &configuration,
            writer_properties,
        ))
    }

    fn new_with_table(
        delta_table: DeltaTable,
        schema: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        configuration: HashMap<String, String>,
        writer_properties: WriterProperties,
    ) -> Self {
        Self::from_parts(
            delta_table.object_store(),
            normalize_for_delta(&schema),
            partition_columns.unwrap_or_default(),
            &configuration,
            writer_properties,
        )
    }

    /// Build a writer from already-resolved parts, parsing the data-skipping
    /// stats configuration once. All constructors funnel through this so the
    /// config parsing and field defaults live in a single place.
    fn from_parts(
        storage: Arc<dyn ObjectStore>,
        arrow_schema_ref: ArrowSchemaRef,
        partition_columns: Vec<String>,
        configuration: &HashMap<String, String>,
        writer_properties: WriterProperties,
    ) -> Self {
        let num_indexed_cols = configuration
            .get("delta.dataSkippingNumIndexedCols")
            .and_then(|v| {
                v.parse::<u64>()
                    .ok()
                    .map(DataSkippingNumIndexedCols::NumColumns)
            })
            .unwrap_or(DataSkippingNumIndexedCols::NumColumns(
                DEFAULT_NUM_INDEX_COLS,
            ));
        let stats_columns = configuration
            .get("delta.dataSkippingStatsColumns")
            .map(|v| v.split(',').map(|s| s.to_string()).collect());

        Self {
            storage,
            arrow_schema_ref: arrow_schema_ref.clone(),
            original_schema_ref: arrow_schema_ref,
            writer_properties,
            partition_columns,
            sink: None,
            pending_adds: Vec::new(),
            buffered_batch_count: 0,
            target_file_size: None,
            num_indexed_cols,
            stats_columns,
            commit_properties: None,
        }
    }

    /// Approximate total encoded (parquet) size of the data buffered across all
    /// in-progress files in the underlying dataset sink (the writer keeps one open
    /// file per partition). May be used by the caller to decide when to finalize
    /// the buffered writes by calling [`flush`](Self::flush).
    pub fn buffer_len(&self) -> usize {
        self.sink
            .as_ref()
            .map_or(0, DataFileDeltaWriter::buffered_size)
    }

    /// Returns the number of record batches streamed since the last flush.
    pub fn buffered_record_batch_count(&self) -> usize {
        self.buffered_batch_count
    }

    /// Resets internal state, discarding any data buffered since the last flush.
    ///
    /// This only drops in-memory buffers; it does not touch object storage. If a
    /// `target_file_size` is set, or a schema-widening `MergeSchema` write has
    /// rotated the sink since the last flush, some parquet files may already have
    /// been finalized and uploaded. `reset` neither deletes those files nor keeps
    /// their pending `Add` actions, so they remain in storage unreferenced by the
    /// log (reclaimed only by a later vacuum). Call [`flush`](Self::flush) instead
    /// to commit buffered data.
    pub fn reset(&mut self) {
        self.sink = None;
        self.pending_adds.clear();
        self.buffered_batch_count = 0;
    }

    /// Sets a target file size; once an in-progress file reaches it the writer
    /// finalizes it and rolls a new one. Without this the writer emits a single
    /// file per partition per flush (the default).
    ///
    /// A `target_file_size` of `0` means "no limit": size-based rolling is
    /// disabled and the writer keeps one file per partition per flush, exactly
    /// as if this method had not been called.
    pub fn with_target_file_size(mut self, target_file_size: u64) -> Self {
        self.target_file_size = NonZeroU64::new(target_file_size);
        self
    }

    /// Returns the arrow schema representation of the delta table schema defined for the wrapped
    /// table.
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.arrow_schema_ref.clone()
    }

    /// Sets the writer properties for the underlying arrow writer.
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = writer_properties;
        self
    }

    /// Write a record batch that belongs entirely to the partition identified by
    /// `partition_values`, streaming it into the partition's open file.
    ///
    /// The batch may be provided with or without its partition columns (the sink
    /// strips them before encoding). Returns the writer's current arrow schema.
    /// `MergeSchema` is rejected for partitioned tables, matching [`write`](Self::write).
    pub async fn write_partition(
        &mut self,
        record_batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
        mode: WriteMode,
    ) -> Result<ArrowSchemaRef, DeltaTableError> {
        if mode == WriteMode::MergeSchema && !self.partition_columns.is_empty() {
            return Err(DeltaTableError::Generic(
                "Merging Schemas with partition columns present is currently unsupported"
                    .to_owned(),
            ));
        }
        if self.sink.is_none() {
            self.sink = Some(self.new_sink());
        }
        let sink = self.sink.as_mut().expect("sink was just created");
        if let Err(e) = sink.write_partition(record_batch, partition_values).await {
            // Mirror `write_into_sink`: the streaming sink can't roll back its
            // in-progress upload, so drop it (and any sealed-but-uncommitted
            // files) rather than leave a later flush to commit a partial window.
            self.sink = None;
            self.pending_adds.clear();
            self.buffered_batch_count = 0;
            return Err(e);
        }
        self.buffered_batch_count += 1;
        Ok(self.arrow_schema_ref.clone())
    }

    /// Build a fresh streaming sink for the current schema/partitioning/target size.
    fn new_sink(&self) -> DataFileDeltaWriter {
        super::build_streaming_sink(
            self.storage.clone(),
            self.arrow_schema_ref.clone(),
            self.partition_columns.clone(),
            self.writer_properties.clone(),
            self.target_file_size,
            self.num_indexed_cols,
            self.stats_columns.clone(),
        )
    }

    /// Finalize the current sink (if any), collecting its [`Add`] actions into
    /// `pending_adds` for the next flush.
    async fn seal_sink(&mut self) -> Result<(), DeltaTableError> {
        if let Some(sink) = self.sink.take() {
            self.pending_adds.extend(sink.close().await?);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl DeltaWriter<RecordBatch> for RecordBatchWriter {
    /// Write a chunk of values into the internal write buffers with the default write mode
    async fn write(&mut self, values: RecordBatch) -> Result<(), DeltaTableError> {
        self.write_with_mode(values, WriteMode::Default).await
    }
    /// Stream a record batch (partition columns included) into the dataset
    /// writer, resolving schema evolution. Partitioning and parquet encoding
    /// happen incrementally; files are finalized at flush (or when a target file
    /// size is reached, or when a `MergeSchema` write widens the schema).
    async fn write_with_mode(
        &mut self,
        values: RecordBatch,
        mode: WriteMode,
    ) -> Result<(), DeltaTableError> {
        if mode == WriteMode::MergeSchema && !self.partition_columns.is_empty() {
            return Err(DeltaTableError::Generic(
                "Merging Schemas with partition columns present is currently unsupported"
                    .to_owned(),
            ));
        }
        let values = if values.schema() != self.arrow_schema_ref {
            let normalized = normalize_for_delta(&values.schema());
            if normalized != values.schema() {
                cast_record_batch(&values, normalized, true, false)?
            } else {
                values
            }
        } else {
            values
        };

        let batch = if values.schema() != self.arrow_schema_ref {
            match mode {
                WriteMode::MergeSchema => {
                    debug!("The writer and record batch schemas do not match, merging");
                    let merged = merge_arrow_schema(
                        self.arrow_schema_ref.clone(),
                        values.schema().clone(),
                        true,
                    )?;
                    if merged != self.arrow_schema_ref {
                        // Genuine widening: data already streamed under the narrower
                        // schema can't be re-encoded, so seal the current sink (its
                        // files simply omit the new column, which reads back as null)
                        // and continue with the widened schema in a fresh sink.
                        self.seal_sink().await?;
                        self.arrow_schema_ref = merged;
                    }
                    // Conform the incoming batch to the (merged) schema; a column-type
                    // change errors here, at write time.
                    conform_to_schema(&values, &self.arrow_schema_ref)?
                }
                WriteMode::Default => {
                    return Err(DeltaWriterError::SchemaMismatch {
                        record_batch_schema: values.schema(),
                        expected_schema: self.arrow_schema_ref.clone(),
                    }
                    .into());
                }
            }
        } else {
            values
        };

        if self.sink.is_none() {
            self.sink = Some(self.new_sink());
        }
        if let Err(e) =
            super::write_into_sink(&mut self.sink, &mut self.buffered_batch_count, &batch).await
        {
            // `write_into_sink` already dropped the poisoned sink; also drop any
            // sealed-but-uncommitted files from earlier `MergeSchema` rotations so
            // a later flush() can't commit only a partial subset of this now-aborted
            // write window.
            self.pending_adds.clear();
            return Err(e);
        }
        Ok(())
    }

    /// Finalize all files written since the last flush and return their [`Add`]
    /// actions, resetting internal state to handle another flush window.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        if let Err(e) = self.seal_sink().await {
            // The in-progress file failed to finalize; drop the whole window
            // (including earlier sealed files) rather than leave `pending_adds`
            // to be re-committed by a retried flush.
            self.pending_adds.clear();
            self.buffered_batch_count = 0;
            return Err(e);
        }
        self.buffered_batch_count = 0;
        Ok(std::mem::take(&mut self.pending_adds))
    }

    /// Flush the internal write buffers to files in the delta table folder structure.
    /// and commit the changes to the Delta log, creating a new table version.
    async fn flush_and_commit(
        &mut self,
        table: &mut DeltaTable,
    ) -> Result<Version, DeltaTableError> {
        use crate::kernel::StructType;
        let mut adds: Vec<Action> = self.flush().await?.drain(..).map(Action::Add).collect();

        // The schema only ever changes via a `MergeSchema` widening, so a difference
        // from the original schema is exactly the signal to evolve the table metadata.
        let evolve_schema = self.arrow_schema_ref != self.original_schema_ref;
        if evolve_schema {
            let schema: StructType = self.arrow_schema_ref.clone().try_into_kernel()?;
            if !self.partition_columns.is_empty() {
                return Err(DeltaTableError::Generic(
                    "Merging Schemas with partition columns present is currently unsupported"
                        .to_owned(),
                ));
            }
            // TODO: we are using the metadata from the passed table, but actually have no guarantee that this is
            // the same table that was used to create the writer instance. Previously we were erasing current config
            // assigning a new table ID, which we should not be doing when evolving the schema.
            let current_meta = table.snapshot()?.metadata().clone();
            let metadata = current_meta.with_schema(&schema)?;
            adds.push(Action::Metadata(metadata));
        }
        let version = super::flush_and_commit(adds, table, self.commit_properties.clone()).await?;
        if evolve_schema {
            // The widened schema is now committed; treat it as the baseline so a
            // subsequent commit doesn't re-emit the same metadata.
            self.original_schema_ref = self.arrow_schema_ref.clone();
        }
        Ok(version)
    }
}

/// Helper container for partitioned record batches
#[derive(Clone, Debug)]
pub struct PartitionResult {
    /// values found in partition columns
    pub partition_values: IndexMap<String, Scalar>,
    /// remaining dataset with partition column values removed
    pub record_batch: RecordBatch,
}

/// Project `batch` onto `schema`, null-filling absent fields. Present columns are
/// carried over unchanged (and so must already match), which makes a column-type
/// change across a schema merge a hard error rather than a silent cast.
fn conform_to_schema(
    batch: &RecordBatch,
    schema: &ArrowSchemaRef,
) -> Result<RecordBatch, DeltaWriterError> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        match batch.column_by_name(field.name()) {
            // Present column whose type matches: carried over unchanged.
            Some(column) if column.data_type() == field.data_type() => cols.push(column.clone()),
            // Present but the type differs — e.g. a later MergeSchema write widened
            // this column's type while this batch was buffered under the old type.
            // Report a clear schema mismatch rather than a low-level arrow error.
            Some(_) => {
                return Err(DeltaWriterError::SchemaMismatch {
                    record_batch_schema: batch.schema(),
                    expected_schema: schema.clone(),
                });
            }
            None => cols.push(new_null_array(field.data_type(), batch.num_rows())),
        }
    }
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

/// Partition a RecordBatch along partition columns
pub(crate) fn divide_by_partition_values(
    arrow_schema: ArrowSchemaRef,
    partition_columns: &[String],
    values: &RecordBatch,
) -> Result<Vec<PartitionResult>, DeltaWriterError> {
    let mut partitions = Vec::new();

    if partition_columns.is_empty() {
        partitions.push(PartitionResult {
            partition_values: IndexMap::new(),
            record_batch: values.clone(),
        });
        return Ok(partitions);
    }

    let schema = values.schema();

    let projection = partition_columns
        .iter()
        .map(|n| Ok(schema.index_of(n)?))
        .collect::<Result<Vec<_>, DeltaWriterError>>()?;
    let sort_columns = values.project(&projection)?;

    let indices = lexsort_to_indices(sort_columns.columns());
    let sorted_partition_columns = partition_columns
        .iter()
        .map(|c| Ok(take(values.column(schema.index_of(c)?), &indices, None)?))
        .collect::<Result<Vec<_>, DeltaWriterError>>()?;

    let partition_ranges = partition(sorted_partition_columns.as_slice())?;

    for range in partition_ranges.ranges().into_iter() {
        // get row indices for current partition
        let idx: UInt32Array = (range.start..range.end)
            .map(|i| Some(indices.value(i)))
            .collect();

        let partition_key_iter = sorted_partition_columns
            .iter()
            .map(|col| {
                Scalar::from_array(&col.slice(range.start, range.end - range.start), 0).ok_or(
                    DeltaWriterError::MissingPartitionColumn("failed to parse".into()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let partition_values = partition_columns
            .iter()
            .cloned()
            .zip(partition_key_iter)
            .collect();
        let batch_data = arrow_schema
            .fields()
            .iter()
            .map(|f| Ok(values.column(schema.index_of(f.name())?).clone()))
            .map(move |col: Result<_, ArrowError>| take(col?.as_ref(), &idx, None))
            .collect::<Result<Vec<_>, _>>()?;

        partitions.push(PartitionResult {
            partition_values,
            record_batch: RecordBatch::try_new(arrow_schema.clone(), batch_data)?,
        });
    }

    Ok(partitions)
}

fn lexsort_to_indices(arrays: &[ArrayRef]) -> UInt32Array {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(arrays).unwrap();
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by_key(|(_, a)| *a);
    UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
}

#[cfg(test)]
mod tests {
    use arrow::json::ReaderBuilder;
    use arrow_schema::Schema as ArrowSchema;
    use delta_kernel::schema::StructType;

    use crate::DeltaResult;
    use crate::kernel::PartitionsExt;
    use crate::operations::create::CreateBuilder;
    use crate::writer::test_utils::*;

    use super::*;
    use crate::writer::utils::arrow_schema_without_partitions;

    /// Partition a record batch through the writer's schema/partition columns,
    /// for tests that assert on the resulting [`PartitionResult`]s.
    fn divide_writer_batch(
        writer: &RecordBatchWriter,
        values: &RecordBatch,
    ) -> Result<Vec<PartitionResult>, DeltaWriterError> {
        divide_by_partition_values(
            arrow_schema_without_partitions(&writer.arrow_schema_ref, &writer.partition_columns),
            &writer.partition_columns,
            values,
        )
    }

    #[test]
    fn test_conform_to_schema_null_fills_missing_columns() {
        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field};
        use std::sync::Arc;

        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "a",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();
        let target = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true), // absent from the batch -> null-filled
        ]));
        let out = conform_to_schema(&batch, &target).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.column(1).null_count(), 2);
    }

    #[test]
    fn test_conform_to_schema_type_change_reports_schema_mismatch() {
        use arrow_array::StringArray;
        use arrow_schema::{DataType, Field};
        use std::sync::Arc;

        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "c",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some("x")]))],
        )
        .unwrap();
        // A column present with a different (e.g. widened) type must report a
        // clear SchemaMismatch, not a low-level arrow type-mismatch error.
        let target = Arc::new(ArrowSchema::new(vec![Field::new(
            "c",
            DataType::LargeUtf8,
            true,
        )]));
        let err = conform_to_schema(&batch, &target).unwrap_err();
        assert!(
            matches!(err, DeltaWriterError::SchemaMismatch { .. }),
            "expected SchemaMismatch, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_buffer_len_includes_unflushed_row_group() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        writer.write(batch).await.unwrap();

        assert!(writer.buffer_len() > 0);
    }

    #[tokio::test]
    async fn test_record_batch_writer_for_table_defaults_include_delta_rs_created_by() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;

        let writer = RecordBatchWriter::for_table(&table).unwrap();

        assert_eq!(
            writer.writer_properties.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
    }

    #[tokio::test]
    async fn test_record_batch_writer_try_new_defaults_include_delta_rs_created_by() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let table_uri = crate::ensure_table_uri(table_path).unwrap();

        let writer = RecordBatchWriter::try_new(table_uri, schema, None, None).unwrap();

        assert_eq!(
            writer.writer_properties.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
    }

    #[tokio::test]
    async fn test_record_batch_writer_try_new_checked_defaults_include_delta_rs_created_by() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let table_uri = crate::ensure_table_uri(table_path).unwrap();

        let writer = RecordBatchWriter::try_new_checked(table_uri, schema, None, None)
            .await
            .unwrap();

        assert_eq!(
            writer.writer_properties.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
    }

    #[tokio::test]
    async fn test_divide_record_batch_no_partition() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = divide_writer_batch(&writer, &batch).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].record_batch, batch)
    }

    #[tokio::test]
    async fn test_divide_record_batch_single_partition() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = divide_writer_batch(&writer, &batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];
        validate_partition_map(partitions, expected_keys)
    }

    /*
     * This test is a little messy but demonstrates a bug when
     * trying to write data to a Delta Table that has a map column and partition columns
     *
     * For readability the schema and data for the write are defined in JSON
     */
    #[tokio::test]
    async fn test_divide_record_batch_with_map_single_partition() {
        let table = crate::writer::test_utils::create_bare_table();
        let partition_cols = ["modified".to_string()];
        let delta_schema = r#"
        {"type" : "struct",
        "fields" : [
            {"name" : "id", "type" : "string", "nullable" : false, "metadata" : {}},
            {"name" : "value", "type" : "integer", "nullable" : false, "metadata" : {}},
            {"name" : "modified", "type" : "string", "nullable" : false, "metadata" : {}},
            {"name" : "metadata", "type" :
                {"type" : "map", "keyType" : "string", "valueType" : "string", "valueContainsNull" : true},
                "nullable" : false, "metadata" : {}}
            ]
        }"#;

        let delta_schema: StructType =
            serde_json::from_str(delta_schema).expect("Failed to parse schema");

        let table = table
            .create()
            .with_partition_columns(partition_cols.to_vec())
            .with_columns(delta_schema.fields().cloned())
            .await
            .unwrap();

        let buf = r#"
            {"id" : "0xdeadbeef", "value" : 42, "modified" : "2021-02-01",
                "metadata" : {"some-key" : "some-value"}}
            {"id" : "0xdeadcaf", "value" : 3, "modified" : "2021-02-02",
                "metadata" : {"some-key" : "some-value"}}"#
            .as_bytes();

        let schema: ArrowSchema = (&delta_schema).try_into_arrow().unwrap();

        // Using a batch size of two since the buf above only has two records
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(2)
            .build_decoder()
            .expect("Failed to build decoder");

        decoder
            .decode(buf)
            .expect("Failed to deserialize the JSON in the buffer");
        let batch = decoder.flush().expect("Failed to flush").unwrap();

        let writer = RecordBatchWriter::for_table(&table).unwrap();
        let partitions = divide_writer_batch(&writer, &batch).unwrap();

        let expected_keys = [
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];

        assert_eq!(partitions.len(), expected_keys.len());
        for result in partitions {
            let partition_key = result.partition_values.hive_partition_path();
            assert!(expected_keys.contains(&partition_key));
        }
    }

    /// This test reproduces an issue where the [RecordBatchWriter] can have its schema "reordered"
    /// by differently ordered JSON objects that were deserialized and written by arrow's [Decoder]
    #[tokio::test]
    async fn test_writer_schema_mutation() -> DeltaResult<()> {
        let delta_schema = r#"
        {"type" : "struct",
        "fields" : [
            {"name" : "id", "type" : "string", "nullable" : false, "metadata" : {}},
            {"name" : "value", "type" : "integer", "nullable" : false, "metadata" : {}},
            {"name" : "modified", "type" : "string", "nullable" : false, "metadata" : {}},
            {"name" : "metadata", "type" :
                {"type" : "map", "keyType" : "string", "valueType" : "string", "valueContainsNull" : true},
                "nullable" : false, "metadata" : {}}
            ]
        }"#;

        let delta_schema: StructType =
            serde_json::from_str(delta_schema).expect("Failed to parse schema");

        let mut table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(vec!["modified"])
            .await
            .unwrap();
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        let original_schema = writer.arrow_schema().clone();

        for data in &[
            r#"{"id" : "0xdeadbeef", "value" : 1, "modified" : "2021-02-01",
                "metadata" : {"some-key" : "some-value"}}"#,
        ] {
            let data = data.as_bytes();

            // Using a batch size of two since the buf above only has two records
            let mut decoder = ReaderBuilder::new(writer.arrow_schema())
                .with_batch_size(1)
                .build_decoder()
                .expect("Failed to build decoder");

            decoder
                .decode(data)
                .expect("Failed to deserialize the JSON in the buffer");

            if let Some(batch) = decoder.flush().unwrap() {
                pretty_assertions::assert_eq!(batch.schema(), original_schema);
                writer.write(batch).await?;
            }

            pretty_assertions::assert_eq!(writer.arrow_schema(), original_schema)
        }
        writer.flush_and_commit(&mut table).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_divide_record_batch_multiple_partitions() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = divide_writer_batch(&writer, &batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01/id=A"),
            String::from("modified=2021-02-01/id=B"),
            String::from("modified=2021-02-02/id=A"),
            String::from("modified=2021-02-02/id=B"),
        ];
        validate_partition_map(partitions, expected_keys)
    }

    #[tokio::test]
    async fn test_write_no_partitions() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 1);
    }

    #[tokio::test]
    async fn test_write_multiple_partitions() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 4);

        let expected_keys = vec![
            String::from("modified=2021-02-01/id=A"),
            String::from("modified=2021-02-01/id=B"),
            String::from("modified=2021-02-02/id=A"),
            String::from("modified=2021-02-02/id=B"),
        ];
        let table_dir = table
            .table_url()
            .to_file_path()
            .expect("Failed to turn table URL back into file path");
        for key in expected_keys {
            let partition_dir = table_dir.join(key);
            assert!(partition_dir.exists())
        }
    }

    fn validate_partition_map(partitions: Vec<PartitionResult>, expected_keys: Vec<String>) {
        assert_eq!(partitions.len(), expected_keys.len());
        for result in partitions {
            let partition_key = result.partition_values.hive_partition_path();
            assert!(expected_keys.contains(&partition_key));
            let ref_batch = get_record_batch(Some(partition_key.clone()), false);
            assert_eq!(ref_batch, result.record_batch);
        }
    }

    /// Validates <https://github.com/delta-io/delta-rs/issues/1806>
    #[tokio::test]
    async fn test_write_tilde() {
        let table_schema = crate::writer::test_utils::get_delta_schema();
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table_dir = tempfile::Builder::new()
            .prefix("example~with~tilde")
            .tempdir()
            .unwrap();
        let table_path = table_dir.path();

        let table = CreateBuilder::new()
            .with_location(table_path.to_str().unwrap())
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(partition_cols)
            .await
            .unwrap();

        let batch = get_record_batch(None, false);
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 4);
    }

    // The following sets of tests are related to #1386 and mergeSchema support
    // <https://github.com/delta-io/delta-rs/issues/1386>
    #[cfg(feature = "datafusion")]
    mod schema_evolution {
        use super::*;

        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field};

        use itertools::Itertools;

        #[tokio::test]
        async fn test_write_mismatched_schema() {
            let table_dir = tempfile::tempdir().unwrap();
            let table_path = table_dir.path().to_str().unwrap();

            let batch = get_record_batch(None, false);
            let partition_cols = vec![];
            let table = create_initialized_table(table_path, &partition_cols).await;
            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            // Write the first batch with the first schema to the table
            writer.write(batch).await.unwrap();
            let adds = writer.flush().await.unwrap();
            assert_eq!(adds.len(), 1);

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

            let result = writer.write(second_batch).await;
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
        async fn test_write_schema_evolution() {
            let table_schema = get_delta_schema();
            let table_dir = tempfile::tempdir().unwrap();
            let table_path = table_dir.path();

            let mut table = CreateBuilder::new()
                .with_location(table_path.to_str().unwrap())
                .with_table_name("test-table")
                .with_comment("A table for running tests")
                .with_columns(table_schema.fields().cloned())
                .await
                .unwrap();
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), Some(0));

            let batch = get_record_batch(None, false);
            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            writer.write(batch).await.unwrap();
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 1);
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), Some(1));

            // Create a second batch with a different schema
            let second_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("vid", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ]));
            let second_batch = RecordBatch::try_new(
                second_schema,
                vec![
                    Arc::new(Int32Array::from(vec![Some(1), Some(2)])), // vid
                    Arc::new(StringArray::from(vec![Some("will"), Some("robert")])), // name
                ],
            )
            .unwrap();

            let result = writer
                .write_with_mode(second_batch, WriteMode::MergeSchema)
                .await;
            assert!(
                result.is_ok(),
                "Failed to write with WriteMode::MergeSchema, {result:?}",
            );
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 2);
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), Some(2));

            let new_schema = table.snapshot().unwrap().metadata().parse_schema().unwrap();
            let expected_columns = vec!["id", "value", "modified", "vid", "name"];
            let found_columns: Vec<&String> = new_schema.fields().map(|f| f.name()).collect();
            assert_eq!(
                expected_columns, found_columns,
                "The new table schema does not contain all evolved columns as expected"
            );
        }

        #[tokio::test]
        async fn test_schema_evolution_not_dropped_by_trailing_default_write() {
            // Regression: a MergeSchema widening followed, in the same flush window,
            // by a Default write that already matches the widened schema must still
            // evolve the table metadata at commit. (Schema evolution is keyed off the
            // schema diff, not a flag the trailing Default write could clear.)
            let table_schema = get_delta_schema();
            let table_dir = tempfile::tempdir().unwrap();
            let mut table = CreateBuilder::new()
                .with_location(table_dir.path().to_str().unwrap())
                .with_columns(table_schema.fields().cloned())
                .await
                .unwrap();
            table.load().await.unwrap();

            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            // Widen the schema via MergeSchema.
            let wider = Arc::new(ArrowSchema::new(vec![
                Field::new("vid", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ]));
            let wide_batch = RecordBatch::try_new(
                wider,
                vec![
                    Arc::new(Int32Array::from(vec![Some(1)])),
                    Arc::new(StringArray::from(vec![Some("will")])),
                ],
            )
            .unwrap();
            writer
                .write_with_mode(wide_batch, WriteMode::MergeSchema)
                .await
                .unwrap();

            // A Default write that already matches the widened schema, before commit.
            let merged = writer.arrow_schema();
            let matching = conform_to_schema(&get_record_batch(None, false), &merged).unwrap();
            writer.write(matching).await.unwrap();

            writer.flush_and_commit(&mut table).await.unwrap();
            table.load().await.unwrap();

            let committed = table.snapshot().unwrap().metadata().parse_schema().unwrap();
            let names: Vec<&str> = committed.fields().map(|f| f.name().as_str()).collect();
            assert!(
                names.contains(&"vid") && names.contains(&"name"),
                "trailing Default write dropped the evolved columns: {names:?}",
            );
        }

        #[tokio::test]
        async fn test_write_schema_evolution_multiple_buffered_batches() {
            // Buffer several batches under different schemas before a single
            // flush: the prior (base-schema) batches must be conformed to the
            // merged schema at flush, not rebuilt on every widening write.
            let table_schema = get_delta_schema();
            let table_dir = tempfile::tempdir().unwrap();
            let mut table = CreateBuilder::new()
                .with_location(table_dir.path().to_str().unwrap())
                .with_table_name("test-table")
                .with_columns(table_schema.fields().cloned())
                .await
                .unwrap();
            table.load().await.unwrap();

            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            // Two batches in the base schema (Default mode), buffered.
            writer.write(get_record_batch(None, false)).await.unwrap();
            writer.write(get_record_batch(None, false)).await.unwrap();

            // A third, wider batch — buffered, not flushed between writes.
            let wider = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("vid", DataType::Int32, true),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                    Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                ],
            )
            .unwrap();
            writer
                .write_with_mode(wider, WriteMode::MergeSchema)
                .await
                .unwrap();
            assert_eq!(writer.buffered_record_batch_count(), 3);

            // A single flush conforms the two base batches to the merged schema.
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 1);
            table.load().await.unwrap();

            let schema = table.snapshot().unwrap().metadata().parse_schema().unwrap();
            let found: Vec<&String> = schema.fields().map(|f| f.name()).collect();
            assert_eq!(found, vec!["id", "value", "modified", "vid", "name"]);
        }

        #[tokio::test]
        async fn test_write_schema_evolution_with_partition_columns_should_fail_as_unsupported() {
            let table_schema = get_delta_schema();
            let table_dir = tempfile::tempdir().unwrap();
            let table_path = table_dir.path();

            let mut table = CreateBuilder::new()
                .with_location(table_path.to_str().unwrap())
                .with_table_name("test-table")
                .with_comment("A table for running tests")
                .with_columns(table_schema.fields().cloned())
                .with_partition_columns(["id"])
                .await
                .unwrap();
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), Some(0));

            let batch = get_record_batch(None, false);
            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            writer.write(batch).await.unwrap();
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 1);
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), Some(1));

            // Create a second batch with appended columns
            let second_batch = {
                let second = get_record_batch(None, false);
                let second_schema = ArrowSchema::new(
                    second
                        .schema()
                        .fields
                        .iter()
                        .cloned()
                        .chain([
                            Field::new("vid", DataType::Int32, true).into(),
                            Field::new("name", DataType::Utf8, true).into(),
                        ])
                        .collect_vec(),
                );

                let len = second.num_rows();

                let second_arrays = second
                    .columns()
                    .iter()
                    .cloned()
                    .chain([
                        Arc::new(Int32Array::from(vec![Some(1); len])) as _, // vid
                        Arc::new(StringArray::from(vec![Some("will"); len])) as _, // name
                    ])
                    .collect_vec();

                RecordBatch::try_new(second_schema.into(), second_arrays).unwrap()
            };

            let result = writer
                .write_with_mode(second_batch, WriteMode::MergeSchema)
                .await;

            assert!(result.is_err());

            match result.unwrap_err() {
                DeltaTableError::Generic(s) => {
                    assert_eq!(
                        s,
                        "Merging Schemas with partition columns present is currently unsupported"
                    )
                }
                e => panic!("unexpected error: {e:?}"),
            }
        }

        #[tokio::test]
        async fn test_schema_evolution_column_type_mismatch() {
            let table_dir = tempfile::tempdir().unwrap();
            let table_path = table_dir.path().to_str().unwrap();

            let batch = get_record_batch(None, false);
            let partition_cols = vec![];
            let mut table = create_initialized_table(table_path, &partition_cols).await;

            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            // Write the first batch with the first schema to the table
            writer.write(batch).await.unwrap();
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 1);

            // Create a second batch with a different schema
            let second_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ]));
            let second_batch = RecordBatch::try_new(
                second_schema,
                vec![
                    Arc::new(Int32Array::from(vec![Some(1), Some(2)])), // vid
                    Arc::new(StringArray::from(vec![Some("will"), Some("robert")])), // name
                ],
            )
            .unwrap();

            let result = writer
                .write_with_mode(second_batch, WriteMode::MergeSchema)
                .await;
            assert!(
                result.is_err(),
                "Did not expect to successfully add new writes with different column types: {result:?}",
            );
        }

        #[tokio::test]
        async fn test_schema_evolution_with_nonnullable_col() {
            use crate::kernel::{
                DataType as DeltaDataType, PrimitiveType, StructField, StructType,
            };

            let table_schema = StructType::try_new(vec![
                StructField::new(
                    "id".to_string(),
                    DeltaDataType::Primitive(PrimitiveType::String),
                    false,
                ),
                StructField::new(
                    "value".to_string(),
                    DeltaDataType::Primitive(PrimitiveType::Integer),
                    true,
                ),
                StructField::new(
                    "modified".to_string(),
                    DeltaDataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .unwrap();
            let table_dir = tempfile::tempdir().unwrap();
            let table_path = table_dir.path();

            let mut table = CreateBuilder::new()
                .with_location(table_path.to_str().unwrap())
                .with_table_name("test-table")
                .with_comment("A table for running tests")
                .with_columns(table_schema.fields().cloned())
                .await
                .unwrap();
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), Some(0));

            // Hand-crafting the first RecordBatch to ensure that a write with non-nullable columns
            // works properly before attempting the second write
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
                Field::new("modified", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(vec![Some("1"), Some("2")])), // id
                    Arc::new(new_null_array(&DataType::Int32, 2)),           // value
                    Arc::new(new_null_array(&DataType::Utf8, 2)),            // modified
                ],
            )
            .unwrap();

            // Write the first batch with the first schema to the table
            let mut writer = RecordBatchWriter::for_table(&table).unwrap();
            writer.write(batch).await.unwrap();
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 1);

            // Create a second batch with a different schema
            let second_schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "name",
                DataType::Utf8,
                true,
            )]));
            let second_batch = RecordBatch::try_new(
                second_schema,
                vec![
                    Arc::new(StringArray::from(vec![Some("will"), Some("robert")])), // name
                ],
            )
            .unwrap();

            let result = writer
                .write_with_mode(second_batch, WriteMode::MergeSchema)
                .await;
            assert!(
                result.is_err(),
                "Should not have been able to write with a missing non-nullable column: {result:?}",
            );
        }
    }

    #[cfg(feature = "datafusion")]
    mod datafusion_tests {
        use super::*;

        use futures::TryStreamExt;

        #[tokio::test]
        async fn test_write_data_skipping_stats_columns() {
            let batch = get_record_batch(None, false);
            let partition_cols: &[String] = &[];
            let table_schema: StructType = get_delta_schema();
            let table_dir = tempfile::tempdir().unwrap();
            let table_path = table_dir.path();
            let config: HashMap<String, Option<String>> = vec![(
                "delta.dataSkippingStatsColumns".to_string(),
                Some("id,value".to_string()),
            )]
            .into_iter()
            .collect();

            let mut table = CreateBuilder::new()
                .with_location(table_path.to_str().unwrap())
                .with_table_name("test-table")
                .with_comment("A table for running tests")
                .with_columns(table_schema.fields().cloned())
                .with_configuration(config)
                .with_partition_columns(partition_cols)
                .await
                .unwrap();

            let mut writer = RecordBatchWriter::for_table(&table).unwrap();
            let partitions = divide_writer_batch(&writer, &batch).unwrap();

            assert_eq!(partitions.len(), 1);
            assert_eq!(partitions[0].record_batch, batch);
            writer.write(batch).await.unwrap();
            writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(table.version(), Some(1));
            let add_actions: Vec<_> = table
                .snapshot()
                .unwrap()
                .snapshot()
                .file_views(&table.log_store, None)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(add_actions.len(), 1);
            let expected_stats = "{\"numRecords\":11,\"minValues\":{\"value\":1,\"id\":\"A\"},\"maxValues\":{\"id\":\"B\",\"value\":11},\"nullCount\":{\"id\":0,\"value\":0}}";
            assert_eq!(
                expected_stats.parse::<serde_json::Value>().unwrap(),
                add_actions
                    .into_iter()
                    .next()
                    .unwrap()
                    .stats()
                    .unwrap()
                    .parse::<serde_json::Value>()
                    .unwrap()
            );
        }

        #[tokio::test]
        async fn test_write_data_skipping_num_indexed_colsn() {
            let batch = get_record_batch(None, false);
            let partition_cols: &[String] = &[];
            let table_schema: StructType = get_delta_schema();
            let table_dir = tempfile::tempdir().unwrap();
            let table_path = table_dir.path();
            let config: HashMap<String, Option<String>> = vec![(
                "delta.dataSkippingNumIndexedCols".to_string(),
                Some("1".to_string()),
            )]
            .into_iter()
            .collect();

            let mut table = CreateBuilder::new()
                .with_location(table_path.to_str().unwrap())
                .with_table_name("test-table")
                .with_comment("A table for running tests")
                .with_columns(table_schema.fields().cloned())
                .with_configuration(config)
                .with_partition_columns(partition_cols)
                .await
                .unwrap();

            let mut writer = RecordBatchWriter::for_table(&table).unwrap();
            let partitions = divide_writer_batch(&writer, &batch).unwrap();

            assert_eq!(partitions.len(), 1);
            assert_eq!(partitions[0].record_batch, batch);
            writer.write(batch).await.unwrap();
            writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(table.version(), Some(1));
            let add_actions: Vec<_> = table
                .snapshot()
                .unwrap()
                .snapshot()
                .file_views(&table.log_store, None)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(add_actions.len(), 1);
            let expected_stats = "{\"numRecords\":11,\"minValues\":{\"id\":\"A\"},\"maxValues\":{\"id\":\"B\"},\"nullCount\":{\"id\":0}}";
            assert_eq!(
                expected_stats.parse::<serde_json::Value>().unwrap(),
                add_actions
                    .into_iter()
                    .next()
                    .unwrap()
                    .stats()
                    .unwrap()
                    .parse::<serde_json::Value>()
                    .unwrap()
            );
        }
    }
}
