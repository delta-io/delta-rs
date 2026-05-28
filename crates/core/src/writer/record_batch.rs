//! Main writer API to write record batches to Delta Table
//!
//! Writes Arrow record batches to a Delta Table, handling partitioning and file statistics.
//! Record batches are buffered in-memory until `flush()` or `flush_and_commit()` is called.
#![allow(deprecated)]

use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, new_null_array};
use arrow_ord::partition::partition;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow_select::take::take;
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use indexmap::IndexMap;
use object_store::ObjectStore;
use parquet::{basic::Compression, file::properties::WriterProperties};
use tracing::log::*;

use super::utils::arrow_schema_without_partitions;
use super::{DeltaWriter, DeltaWriterError, WriteMode};
use crate::DeltaTable;
use crate::errors::DeltaTableError;
use crate::kernel::schema::cast::normalize_for_delta;
use crate::kernel::schema::merge_arrow_schema;
use crate::kernel::transaction::CommitProperties;
use crate::kernel::{Action, Add, scalars::ScalarExt};
use crate::kernel::{MetadataExt as _, Version};
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::DEFAULT_NUM_INDEX_COLS;

/// Writes messages to a delta lake table.
///
/// # Deprecation
///
/// `RecordBatchWriter` is deprecated. Use [`crate::DeltaTable`] write operations
/// (e.g. [`crate::operations::write::WriteBuilder`]) instead, which support all
/// modern Delta features including encryption.
#[deprecated(
    since = "0.32.0",
    note = "Use DeltaTable write operations instead. \
            See https://delta-io.github.io/delta-rs/usage/writing/index.html"
)]
pub struct RecordBatchWriter {
    storage: Arc<dyn ObjectStore>,
    arrow_schema_ref: ArrowSchemaRef,
    original_schema_ref: ArrowSchemaRef,
    /// Passed to write_data_plan (allows callers to customise compression, etc.).
    writer_properties: WriterProperties,
    should_evolve: bool,
    partition_columns: Vec<String>,
    /// Buffered full-schema record batches (partition columns included).
    batches: Vec<RecordBatch>,
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

        // if metadata fails to load, use an empty hashmap and default values
        let configuration = delta_table.snapshot().map_or_else(
            |_| HashMap::new(),
            |snapshot| snapshot.metadata().configuration().clone(),
        );

        let schema = normalize_for_delta(&schema);

        Ok(Self {
            storage: delta_table.object_store(),
            arrow_schema_ref: schema.clone(),
            original_schema_ref: schema,
            writer_properties: default_writer_properties(),
            partition_columns: partition_columns.unwrap_or_default(),
            should_evolve: false,
            batches: Vec::new(),
            num_indexed_cols: configuration
                .get("delta.dataSkippingNumIndexedCols")
                .and_then(|v| {
                    v.parse::<u64>()
                        .ok()
                        .map(DataSkippingNumIndexedCols::NumColumns)
                })
                .unwrap_or(DataSkippingNumIndexedCols::NumColumns(
                    DEFAULT_NUM_INDEX_COLS,
                )),
            stats_columns: configuration
                .get("delta.dataSkippingStatsColumns")
                .map(|v| v.split(',').map(|s| s.to_string()).collect()),
            commit_properties: None,
        })
    }

    /// Add the [`CommitProperties`] to the [`RecordBatchWriter`] to be used when the writer
    /// flushes the write into storage.
    pub fn with_commit_properties(mut self, properties: CommitProperties) -> Self {
        self.commit_properties = Some(properties);
        self
    }

    /// Creates a [`RecordBatchWriter`] to write data to the provided Delta Table
    pub fn for_table(table: &DeltaTable) -> Result<Self, DeltaTableError> {
        super::ensure_legacy_writer_supports_table(table, "RecordBatchWriter")?;
        let metadata = table.snapshot()?.metadata();
        let arrow_schema: ArrowSchema = (&metadata.parse_schema()?).try_into_arrow()?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns().to_vec();

        let configuration = table.snapshot()?.metadata().configuration().clone();

        Ok(Self {
            storage: table.object_store(),
            arrow_schema_ref: arrow_schema_ref.clone(),
            original_schema_ref: arrow_schema_ref.clone(),
            writer_properties: default_writer_properties(),
            partition_columns,
            should_evolve: false,
            batches: Vec::new(),
            num_indexed_cols: configuration
                .get("delta.dataSkippingNumIndexedCols")
                .and_then(|v| {
                    v.parse::<u64>()
                        .ok()
                        .map(DataSkippingNumIndexedCols::NumColumns)
                })
                .unwrap_or(DataSkippingNumIndexedCols::NumColumns(
                    DEFAULT_NUM_INDEX_COLS,
                )),
            stats_columns: configuration
                .get("delta.dataSkippingStatsColumns")
                .map(|v| v.split(',').map(|s| s.to_string()).collect()),
            commit_properties: None,
        })
    }

    /// Creates a [`RecordBatchWriter`] for an existing table after loading and validating metadata.
    pub async fn try_new_checked(
        table_uri: impl AsRef<str>,
        schema: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self, DeltaTableError> {
        let table_url = url::Url::parse(table_uri.as_ref())
            .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
        let table = crate::table::builder::DeltaTableBuilder::from_url(table_url)?
            .with_storage_options(storage_options.unwrap_or_default())
            .load()
            .await?;
        super::ensure_legacy_writer_supports_table(&table, "RecordBatchWriter")?;
        let _ = (schema, partition_columns);
        Self::for_table(&table)
    }

    /// Returns the current byte length of the in memory buffer (approximate).
    pub fn buffer_len(&self) -> usize {
        self.batches
            .iter()
            .map(|b| b.num_rows() * b.num_columns() * 8)
            .sum()
    }

    /// Returns the number of records held in the current buffer.
    pub fn buffered_record_batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Resets internal state.
    pub fn reset(&mut self) {
        self.batches.clear();
    }

    /// Returns the arrow schema representation of the delta table schema defined for the wrapped table.
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.arrow_schema_ref.clone()
    }

    /// Write a batch to the specified partition.
    ///
    /// Partition column values are added back to form a full-schema batch.
    pub async fn write_partition(
        &mut self,
        record_batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
        mode: WriteMode,
    ) -> Result<ArrowSchemaRef, DeltaTableError> {
        let n = record_batch.num_rows();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.arrow_schema_ref.fields().len());

        for field in self.arrow_schema_ref.fields() {
            if let Ok(idx) = record_batch.schema().index_of(field.name()) {
                cols.push(record_batch.column(idx).clone());
            } else if let Some(scalar) = partition_values.get(field.name()) {
                cols.push(
                    scalar_to_array(scalar, n, field.data_type())
                        .map_err(|e| DeltaTableError::Generic(e.to_string()))?,
                );
            } else {
                cols.push(new_null_array(field.data_type(), n));
            }
        }

        let full_batch = RecordBatch::try_new(self.arrow_schema_ref.clone(), cols)
            .map_err(|e| DeltaTableError::Arrow { source: e })?;

        self.write_with_mode(full_batch, mode).await?;
        Ok(self.arrow_schema_ref.clone())
    }

    /// Sets the writer properties for the underlying arrow writer.
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = writer_properties;
        self
    }

    fn divide_by_partition_values(
        &mut self,
        values: &RecordBatch,
    ) -> Result<Vec<PartitionResult>, DeltaWriterError> {
        divide_by_partition_values(
            arrow_schema_without_partitions(&self.arrow_schema_ref, &self.partition_columns),
            self.partition_columns.clone(),
            values,
        )
    }
}

#[async_trait::async_trait]
impl DeltaWriter<RecordBatch> for RecordBatchWriter {
    async fn write(&mut self, values: RecordBatch) -> Result<(), DeltaTableError> {
        self.write_with_mode(values, WriteMode::Default).await
    }

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
        self.should_evolve = mode == WriteMode::MergeSchema;

        // Schema normalization
        let values = if values.schema() != self.arrow_schema_ref {
            let normalized = normalize_for_delta(&values.schema());
            if normalized != values.schema() {
                crate::kernel::schema::cast::cast_record_batch(&values, normalized, true, false)?
            } else {
                values
            }
        } else {
            values
        };

        // Schema validation / evolution
        let values = if values.schema() != self.arrow_schema_ref {
            match mode {
                WriteMode::MergeSchema => {
                    debug!("The writer and record batch schemas do not match, merging");

                    // Non-nullable columns missing from the batch must fail
                    for field in self.arrow_schema_ref.fields() {
                        if !field.is_nullable()
                            && values.column_by_name(field.name()).is_none()
                        {
                            return Err(DeltaTableError::Arrow {
                                source: ArrowError::SchemaError(format!(
                                    "Column '{}' is non-nullable but not present in batch",
                                    field.name()
                                )),
                            });
                        }
                    }

                    let merged = merge_arrow_schema(
                        self.arrow_schema_ref.clone(),
                        values.schema(),
                        true,
                    )
                    .map_err(|e| DeltaTableError::Arrow { source: e })?;
                    self.arrow_schema_ref = merged.clone();

                    let mut cols = vec![];
                    for field in merged.fields() {
                        if let Some(col) = values.column_by_name(field.name()) {
                            cols.push(col.clone());
                        } else {
                            cols.push(new_null_array(field.data_type(), values.num_rows()));
                        }
                    }
                    RecordBatch::try_new(merged, cols)
                        .map_err(|e| DeltaTableError::Arrow { source: e })?
                }
                WriteMode::Default => {
                    return Err(DeltaTableError::SchemaMismatch {
                        msg: format!(
                            "RecordBatch schema does not match: RecordBatch schema: {}, expected: {}",
                            values.schema(),
                            self.arrow_schema_ref
                        ),
                    });
                }
            }
        } else {
            values
        };

        self.batches.push(values);
        Ok(())
    }

    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        flush_batches(self).await
    }

    async fn flush_and_commit(
        &mut self,
        table: &mut DeltaTable,
    ) -> Result<Version, DeltaTableError> {
        flush_and_commit_via_exec(self, table).await
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DataFusion-backed write helpers (cfg-gated)
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "datafusion")]
async fn flush_batches(writer: &mut RecordBatchWriter) -> Result<Vec<Add>, DeltaTableError> {
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::SessionConfig;

    use crate::kernel::Action;
    use crate::operations::write::configs::WriterStatsConfig;
    use crate::operations::write::execution::write_execution_plan;

    let batches = std::mem::take(&mut writer.batches);
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = writer.arrow_schema_ref.clone();
    let ctx = SessionContext::new_with_config(SessionConfig::new());
    let mem_table = MemTable::try_new(schema, vec![batches])
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
    let exec = ctx
        .read_table(Arc::new(mem_table))
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?
        .create_physical_plan()
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

    let stats_config =
        WriterStatsConfig::new(writer.num_indexed_cols, writer.stats_columns.clone());

    let actions = write_execution_plan(
        None,
        &ctx.state(),
        exec,
        writer.partition_columns.clone(),
        writer.storage.clone(),
        None,
        None,
        Some(writer.writer_properties.clone()),
        stats_config,
    )
    .await?;

    Ok(actions
        .into_iter()
        .filter_map(|a| match a {
            Action::Add(add) => Some(add),
            _ => None,
        })
        .collect())
}

#[cfg(not(feature = "datafusion"))]
async fn flush_batches(writer: &mut RecordBatchWriter) -> Result<Vec<Add>, DeltaTableError> {
    use bytes::Bytes;
    use object_store::path::Path;
    use parquet::arrow::ArrowWriter;
    use uuid::Uuid;

    use super::stats::create_add;
    use super::utils::{next_data_path, ShareableBuffer};
    use crate::kernel::PartitionsExt;
    use crate::logstore::ObjectStoreRetryExt;

    let batches = std::mem::take(&mut writer.batches);
    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Partition batches and write one file per partition
    let mut adds: Vec<Add> = Vec::new();
    let mut partition_map: HashMap<String, (IndexMap<String, Scalar>, Vec<RecordBatch>)> =
        HashMap::new();

    for batch in batches {
        for result in divide_by_partition_values(
            arrow_schema_without_partitions(&writer.arrow_schema_ref, &writer.partition_columns),
            writer.partition_columns.clone(),
            &batch,
        )? {
            let key = result.partition_values.hive_partition_path();
            partition_map
                .entry(key)
                .or_insert_with(|| (result.partition_values.clone(), Vec::new()))
                .1
                .push(result.record_batch);
        }
    }

    for (_, (partition_values, part_batches)) in partition_map {
        let stripped_schema =
            arrow_schema_without_partitions(&writer.arrow_schema_ref, &writer.partition_columns);
        let buffer = ShareableBuffer::default();
        let mut arrow_writer = ArrowWriter::try_new(
            buffer.clone(),
            stripped_schema,
            Some(writer.writer_properties.clone()),
        )
        .map_err(|e| DeltaTableError::Parquet { source: e })?;
        for batch in &part_batches {
            arrow_writer
                .write(batch)
                .map_err(|e| DeltaTableError::Parquet { source: e })?;
        }
        let metadata = arrow_writer
            .close()
            .map_err(|e| DeltaTableError::Parquet { source: e })?;
        let obj_bytes = Bytes::from(buffer.to_vec());
        let file_size = obj_bytes.len() as i64;
        let prefix = Path::parse(partition_values.hive_partition_path())?;
        let uuid = Uuid::new_v4();
        let path = next_data_path(&prefix, 0, &uuid, &writer.writer_properties);
        writer
            .storage
            .put_with_retries(&path, obj_bytes.into(), 15)
            .await?;
        adds.push(create_add(
            &partition_values,
            path.to_string(),
            file_size,
            &metadata,
            writer.num_indexed_cols,
            &writer.stats_columns,
        )?);
    }
    Ok(adds)
}

#[cfg(feature = "datafusion")]
async fn flush_and_commit_via_exec(
    writer: &mut RecordBatchWriter,
    table: &mut DeltaTable,
) -> Result<Version, DeltaTableError> {
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::SessionConfig;

    use crate::kernel::StructType;
    use crate::operations::write::execution::write_exec_plan;

    let batches = std::mem::take(&mut writer.batches);

    if batches.is_empty() {
        if writer.arrow_schema_ref != writer.original_schema_ref && writer.should_evolve {
            let schema: StructType = writer.arrow_schema_ref.clone().try_into_kernel()?;
            let current_meta = table.snapshot()?.metadata().clone();
            let metadata = current_meta.with_schema(&schema)?;
            return super::flush_and_commit(
                vec![Action::Metadata(metadata)],
                table,
                writer.commit_properties.clone(),
            )
            .await;
        }
        return Ok(table.version().unwrap_or(0));
    }

    let schema = writer.arrow_schema_ref.clone();
    let ctx = SessionContext::new_with_config(SessionConfig::new());
    let mem_table = MemTable::try_new(schema, vec![batches])
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
    let exec = ctx
        .read_table(Arc::new(mem_table))
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?
        .create_physical_plan()
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

    let state = table.snapshot()?;
    let table_config = state.snapshot().table_configuration();
    let (mut actions, _) = write_exec_plan(
        &ctx.state(),
        table.log_store().as_ref(),
        table_config,
        exec,
        None,
        None,
        false,
    )
    .await?;

    if writer.arrow_schema_ref != writer.original_schema_ref && writer.should_evolve {
        if !writer.partition_columns.is_empty() {
            return Err(DeltaTableError::Generic(
                "Merging Schemas with partition columns present is currently unsupported"
                    .to_owned(),
            ));
        }
        let schema: StructType = writer.arrow_schema_ref.clone().try_into_kernel()?;
        let current_meta = table.snapshot()?.metadata().clone();
        let metadata = current_meta.with_schema(&schema)?;
        actions.push(Action::Metadata(metadata));
    }

    let actions: Vec<Action> = actions
        .into_iter()
        .filter(|a| matches!(a, Action::Add(_) | Action::Metadata(_)))
        .collect();

    super::flush_and_commit(actions, table, writer.commit_properties.clone()).await
}

#[cfg(not(feature = "datafusion"))]
async fn flush_and_commit_via_exec(
    writer: &mut RecordBatchWriter,
    table: &mut DeltaTable,
) -> Result<Version, DeltaTableError> {
    use crate::kernel::StructType;

    let mut adds: Vec<Action> = writer
        .flush()
        .await?
        .into_iter()
        .map(Action::Add)
        .collect();

    if writer.arrow_schema_ref != writer.original_schema_ref && writer.should_evolve {
        let schema: StructType = writer.arrow_schema_ref.clone().try_into_kernel()?;
        if !writer.partition_columns.is_empty() {
            return Err(DeltaTableError::Generic(
                "Merging Schemas with partition columns present is currently unsupported"
                    .to_owned(),
            ));
        }
        let current_meta = table.snapshot()?.metadata().clone();
        let metadata = current_meta.with_schema(&schema)?;
        adds.push(Action::Metadata(metadata));
    }
    super::flush_and_commit(adds, table, writer.commit_properties.clone()).await
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(format!("delta-rs version {}", crate::crate_version()))
        .build()
}

/// Create a constant Arrow array of length `n` from a kernel [`Scalar`].
fn scalar_to_array(
    scalar: &Scalar,
    n: usize,
    expected_type: &arrow_schema::DataType,
) -> Result<ArrayRef, DeltaWriterError> {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        TimestampMicrosecondArray,
    };

    let array: ArrayRef = match scalar {
        Scalar::Null(_) => new_null_array(expected_type, n),
        Scalar::String(s) => Arc::new(StringArray::from(vec![s.as_str(); n])),
        Scalar::Integer(v) => Arc::new(Int32Array::from(vec![*v; n])),
        Scalar::Long(v) => Arc::new(Int64Array::from(vec![*v; n])),
        Scalar::Short(v) => Arc::new(Int16Array::from(vec![*v; n])),
        Scalar::Byte(v) => Arc::new(Int8Array::from(vec![*v; n])),
        Scalar::Float(v) => Arc::new(Float32Array::from(vec![*v; n])),
        Scalar::Double(v) => Arc::new(Float64Array::from(vec![*v; n])),
        Scalar::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; n])),
        Scalar::Date(v) => Arc::new(Date32Array::from(vec![*v; n])),
        Scalar::Timestamp(v) | Scalar::TimestampNtz(v) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v; n]))
        }
        Scalar::Binary(bytes) => {
            let refs: Vec<&[u8]> = vec![bytes.as_slice(); n];
            Arc::new(BinaryArray::from(refs))
        }
        Scalar::Decimal(d) => Arc::new(
            Decimal128Array::from(vec![d.bits(); n])
                .with_precision_and_scale(d.precision(), d.scale() as i8)
                .map_err(|e| DeltaWriterError::Arrow { source: e })?,
        ),
        _ => {
            return Err(DeltaWriterError::Arrow {
                source: ArrowError::InvalidArgumentError(format!(
                    "Unsupported scalar type for partition column: {scalar:?}"
                )),
            });
        }
    };
    Ok(array)
}

/// Helper container for partitioned record batches
#[derive(Clone, Debug)]
pub struct PartitionResult {
    /// values found in partition columns
    pub partition_values: IndexMap<String, Scalar>,
    /// remaining dataset with partition column values removed
    pub record_batch: RecordBatch,
}

/// Partition a RecordBatch along partition columns
pub(crate) fn divide_by_partition_values(
    arrow_schema: ArrowSchemaRef,
    partition_columns: Vec<String>,
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
            .clone()
            .into_iter()
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
    sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
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

    #[tokio::test]
    #[allow(deprecated)]
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
    #[allow(deprecated)]
    async fn test_divide_record_batch_no_partition() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].record_batch, batch)
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_divide_record_batch_single_partition() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

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
    #[allow(deprecated)]
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

        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(2)
            .build_decoder()
            .expect("Failed to build decoder");

        decoder
            .decode(buf)
            .expect("Failed to deserialize the JSON in the buffer");
        let batch = decoder.flush().expect("Failed to flush").unwrap();

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        let partitions = writer.divide_by_partition_values(&batch).unwrap();

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
    #[tokio::test]
    #[allow(deprecated)]
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
    #[allow(deprecated)]
    async fn test_divide_record_batch_multiple_partitions() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01/id=A"),
            String::from("modified=2021-02-01/id=B"),
            String::from("modified=2021-02-02/id=A"),
            String::from("modified=2021-02-02/id=B"),
        ];
        validate_partition_map(partitions, expected_keys)
    }

    #[tokio::test]
    #[allow(deprecated)]
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
    #[allow(deprecated)]
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
    #[allow(deprecated)]
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
        #[allow(deprecated)]
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
        #[allow(deprecated)]
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
        #[allow(deprecated)]
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
        #[allow(deprecated)]
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
        #[allow(deprecated)]
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

            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
                Field::new("modified", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(vec![Some("1"), Some("2")])), // id
                    Arc::new(arrow_array::new_null_array(&DataType::Int32, 2)),   // value
                    Arc::new(arrow_array::new_null_array(&DataType::Utf8, 2)),    // modified
                ],
            )
            .unwrap();

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
                vec![Arc::new(StringArray::from(vec![
                    Some("will"),
                    Some("robert"),
                ]))],
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
        #[allow(deprecated)]
        async fn test_write_data_skipping_stats_columns() {
            use std::collections::HashMap;
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
            let partitions = writer.divide_by_partition_values(&batch).unwrap();

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
        #[allow(deprecated)]
        async fn test_write_data_skipping_num_indexed_colsn() {
            use std::collections::HashMap;
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
            let partitions = writer.divide_by_partition_values(&batch).unwrap();

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
