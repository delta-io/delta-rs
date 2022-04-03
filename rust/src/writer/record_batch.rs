//! Main writer API to write record batches to Delta Table
//!
//! Writes Arrow record batches to a Delta Table, handling partitioning and file statistics.
//! Each Parquet file is buffered in-memory and only written once `flush()` is called on
//! the writer. Once written, add actions are returned by the writer. It's the users responsibility
//! to create the transaction using those actions.
//!
//! # Examples
//!
//! Write to an existing Delta Lake table:
//! ```rust ignore
//! let table = DeltaTable::try_from_uri("../path/to/table")
//! let batch: RecordBatch = ...
//! let mut writer = RecordBatchWriter::for_table(table, /*storage_options=*/ HashMap::new())
//! writer.write(batch)?;
//! let actions: Vec<action::Action> = writer.flush()?.iter()
//!     .map(|add| Action::add(add.into()))
//!     .collect();
//! let mut transaction = table.create_transaction(Some(DeltaTransactionOptions::new(/*max_retry_attempts=*/3)));
//! transaction.add_actions(actions);
//! async {
//!     transaction.commit(Some(DeltaOperation::Write {
//!         SaveMode::Append,
//!         partitionBy: Some(table.get_metadata().partition_columns),
//!         predicate: None,
//!     }))
//! }
//! ```
use super::{
    stats::{create_add, NullCounts},
    utils::{cursor_from_bytes, get_partition_key, next_data_path, stringified_partition_value},
    DeltaWriter, DeltaWriterError,
};
use crate::writer::stats::apply_null_counts;
use crate::{
    action::Add, get_backend_for_uri_with_options, DeltaTable, DeltaTableMetaData, Schema,
    StorageBackend,
};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{Array, UInt32Array},
    compute::{lexicographical_partition_ranges, lexsort_to_indices, take, SortColumn},
    datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    error::ArrowError,
};
use parquet::{arrow::ArrowWriter, errors::ParquetError, file::writer::InMemoryWriteableCursor};
use parquet::{basic::Compression, file::properties::WriterProperties};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

/// Writes messages to a delta lake table.
pub struct RecordBatchWriter {
    pub(crate) storage: Box<dyn StorageBackend>,
    pub(crate) table_uri: String,
    pub(crate) arrow_schema_ref: Arc<arrow::datatypes::Schema>,
    pub(crate) writer_properties: WriterProperties,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) arrow_writers: HashMap<String, PartitionWriter>,
}

impl std::fmt::Debug for RecordBatchWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordBatchWriter")
    }
}

impl RecordBatchWriter {
    /// Create a new RecordBatchWriter instance
    pub fn try_new(
        table_uri: String,
        schema: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self, DeltaWriterError> {
        let storage =
            get_backend_for_uri_with_options(&table_uri, storage_options.unwrap_or_default())?;

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage,
            table_uri: table_uri.clone(),
            arrow_schema_ref: schema,
            writer_properties,
            partition_columns: partition_columns.unwrap_or_default(),
            arrow_writers: HashMap::new(),
        })
    }

    /// Creates a RecordBatchWriter to write data to provided Delta Table
    pub fn for_table(
        table: &DeltaTable,
        storage_options: HashMap<String, String>,
    ) -> Result<RecordBatchWriter, DeltaWriterError> {
        let storage = get_backend_for_uri_with_options(&table.table_uri, storage_options)?;

        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.get_metadata()?;
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema.clone())?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns.clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage,
            table_uri: table.table_uri.clone(),
            arrow_schema_ref,
            writer_properties,
            partition_columns,
            arrow_writers: HashMap::new(),
        })
    }

    /// Retrieves the latest schema from table, compares to the current and updates if changed.
    /// When schema is updated then `true` is returned which signals the caller that parquet
    /// created file or arrow batch should be revisited.
    // TODO Test schema update scenarios
    pub fn update_schema(
        &mut self,
        metadata: &DeltaTableMetaData,
    ) -> Result<bool, DeltaWriterError> {
        let schema: ArrowSchema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema)?;

        let schema_updated = self.arrow_schema_ref.as_ref() != &schema
            || self.partition_columns != metadata.partition_columns;

        if schema_updated {
            let _ = std::mem::replace(&mut self.arrow_schema_ref, Arc::new(schema));
            let _ = std::mem::replace(
                &mut self.partition_columns,
                metadata.partition_columns.clone(),
            );
        }

        Ok(schema_updated)
    }

    fn divide_by_partition_values(
        &mut self,
        values: &RecordBatch,
    ) -> Result<Vec<PartitionResult>, DeltaWriterError> {
        divide_by_partition_values(
            self.partition_arrow_schema(),
            self.partition_columns.clone(),
            values,
        )
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.arrow_writers.values().map(|w| w.buffer_len()).sum()
    }

    /// Returns the number of records held in the current buffer.
    pub fn buffered_record_batch_count(&self) -> usize {
        self.arrow_writers
            .values()
            .map(|w| w.buffered_record_batch_count)
            .sum()
    }

    /// Resets internal state.
    pub fn reset(&mut self) {
        self.arrow_writers.clear();
    }

    /// Returns the arrow schema representation of the delta table schema defined for the wrapped
    /// table.
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.arrow_schema_ref.clone()
    }

    /// Returns the arrow schema representation of the partitioned files written to table
    pub fn partition_arrow_schema(&self) -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(
            self.arrow_schema_ref
                .fields()
                .iter()
                .filter(|f| !self.partition_columns.contains(f.name()))
                .map(|f| f.to_owned())
                .collect::<Vec<_>>(),
        ))
    }
}

#[async_trait::async_trait]
impl DeltaWriter<RecordBatch> for RecordBatchWriter {
    /// Divides a single record batch into into multiple according to table partitioning.
    /// Values are written to arrow buffers, to collect data until it should be written to disk.
    async fn write(&mut self, values: RecordBatch) -> Result<(), DeltaWriterError> {
        let arrow_schema = self.partition_arrow_schema();
        for result in self.divide_by_partition_values(&values)? {
            let partition_key =
                get_partition_key(&self.partition_columns, &result.partition_values)?;
            match self.arrow_writers.get_mut(&partition_key) {
                Some(writer) => {
                    writer.write(&result.record_batch)?;
                }
                None => {
                    let mut writer = PartitionWriter::new(
                        arrow_schema.clone(),
                        result.partition_values,
                        self.writer_properties.clone(),
                    )?;
                    writer.write(&result.record_batch)?;
                    let _ = self.arrow_writers.insert(partition_key, writer);
                }
            }
        }

        Ok(())
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaWriterError> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, mut writer) in writers {
            let metadata = writer.arrow_writer.close()?;

            let path = next_data_path(&self.partition_columns, &writer.partition_values, None)?;

            let obj_bytes = writer.cursor.data();
            let file_size = obj_bytes.len() as i64;
            let storage_path = self
                .storage
                .join_path(self.table_uri.as_str(), path.as_str());

            //
            // TODO: Wrap in retry loop to handle temporary network errors
            //

            self.storage
                .put_obj(&storage_path, obj_bytes.as_slice())
                .await?;

            // Replace self null_counts with an empty map. Use the other for stats.
            let null_counts = std::mem::take(&mut writer.null_counts);

            actions.push(create_add(
                &writer.partition_values,
                null_counts,
                path,
                file_size,
                &metadata,
            )?);
        }
        Ok(actions)
    }
}

/// Helper container for partitioned record batches
pub struct PartitionResult {
    /// values found in partition columns
    pub partition_values: HashMap<String, Option<String>>,
    /// remaining dataset with partition column values removed
    pub record_batch: RecordBatch,
}

pub(crate) struct PartitionWriter {
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    pub(super) cursor: InMemoryWriteableCursor,
    pub(super) arrow_writer: ArrowWriter<InMemoryWriteableCursor>,
    pub(super) partition_values: HashMap<String, Option<String>>,
    pub(super) null_counts: NullCounts,
    pub(super) buffered_record_batch_count: usize,
}

impl PartitionWriter {
    pub fn new(
        arrow_schema: Arc<ArrowSchema>,
        partition_values: HashMap<String, Option<String>>,
        writer_properties: WriterProperties,
    ) -> Result<Self, ParquetError> {
        let cursor = InMemoryWriteableCursor::default();
        let arrow_writer = ArrowWriter::try_new(
            cursor.clone(),
            arrow_schema.clone(),
            Some(writer_properties.clone()),
        )?;

        let null_counts = NullCounts::new();
        let buffered_record_batch_count = 0;

        Ok(Self {
            arrow_schema,
            writer_properties,
            cursor,
            arrow_writer,
            partition_values,
            null_counts,
            buffered_record_batch_count,
        })
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many
    /// record batches and flushed after the appropriate number of bytes has been written.
    pub fn write(&mut self, record_batch: &RecordBatch) -> Result<(), DeltaWriterError> {
        if record_batch.schema() != self.arrow_schema {
            return Err(DeltaWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: self.arrow_schema.clone(),
            });
        }

        // Copy current cursor bytes so we can recover from failures
        let current_cursor_bytes = self.cursor.data();
        match self.arrow_writer.write(record_batch) {
            Ok(_) => {
                self.buffered_record_batch_count += 1;
                apply_null_counts(&record_batch.clone().into(), &mut self.null_counts, 0);
                Ok(())
            }
            // If a write fails we need to reset the state of the PartitionWriter
            Err(e) => {
                let new_cursor = cursor_from_bytes(current_cursor_bytes.as_slice())?;
                let _ = std::mem::replace(&mut self.cursor, new_cursor.clone());
                let arrow_writer = ArrowWriter::try_new(
                    new_cursor,
                    self.arrow_schema.clone(),
                    Some(self.writer_properties.clone()),
                )?;
                let _ = std::mem::replace(&mut self.arrow_writer, arrow_writer);
                Err(e.into())
            }
        }
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.cursor.len()
    }
}

/// Partition a RecordBatch along partition columns
pub fn divide_by_partition_values(
    arrow_schema: ArrowSchemaRef,
    partition_columns: Vec<String>,
    values: &RecordBatch,
) -> Result<Vec<PartitionResult>, DeltaWriterError> {
    let mut partitions = Vec::new();

    if partition_columns.is_empty() {
        partitions.push(PartitionResult {
            partition_values: HashMap::new(),
            record_batch: values.clone(),
        });
        return Ok(partitions);
    }

    let schema = values.schema();

    // collect all columns in order relevant for partitioning
    let sort_columns = partition_columns
        .clone()
        .into_iter()
        .map(|col| {
            Ok(SortColumn {
                values: values.column(schema.index_of(&col)?).clone(),
                options: None,
            })
        })
        .collect::<Result<Vec<_>, DeltaWriterError>>()?;

    let indices = lexsort_to_indices(sort_columns.as_slice(), None)?;
    let sorted_partition_columns = sort_columns
        .iter()
        .map(|c| {
            Ok(SortColumn {
                values: take(c.values.as_ref(), &indices, None)?,
                options: None,
            })
        })
        .collect::<Result<Vec<_>, DeltaWriterError>>()?;

    let partition_ranges = lexicographical_partition_ranges(sorted_partition_columns.as_slice())?;

    for range in partition_ranges {
        // get row indices for current partition
        let idx: UInt32Array = (range.start..range.end)
            .map(|i| Some(indices.value(i)))
            .into_iter()
            .collect();

        let partition_key_iter = sorted_partition_columns.iter().map(|c| {
            stringified_partition_value(&c.values.slice(range.start, range.end - range.start))
        });

        let mut partition_values = HashMap::new();
        for (key, value) in partition_columns.clone().iter().zip(partition_key_iter) {
            partition_values.insert(key.clone(), value?);
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{
        test_utils::{create_initialized_table, get_record_batch},
        utils::get_partition_key,
    };
    use std::collections::HashMap;
    use std::path::Path;

    #[tokio::test]
    async fn test_divide_record_batch_no_partition() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table, HashMap::new()).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].record_batch, batch)
    }

    #[tokio::test]
    async fn test_divide_record_batch_single_partition() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table, HashMap::new()).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];
        validate_partition_map(partitions, &partition_cols, expected_keys)
    }

    #[tokio::test]
    async fn test_divide_record_batch_multiple_partitions() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table, HashMap::new()).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01/id=A"),
            String::from("modified=2021-02-01/id=B"),
            String::from("modified=2021-02-02/id=A"),
            String::from("modified=2021-02-02/id=B"),
        ];
        validate_partition_map(partitions, &partition_cols.clone(), expected_keys)
    }

    #[tokio::test]
    async fn test_write_no_partitions() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table, HashMap::new()).unwrap();

        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 1);
    }

    #[tokio::test]
    async fn test_write_multiple_partitions() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table, HashMap::new()).unwrap();

        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 4);

        let expected_keys = vec![
            String::from("modified=2021-02-01/id=A"),
            String::from("modified=2021-02-01/id=B"),
            String::from("modified=2021-02-02/id=A"),
            String::from("modified=2021-02-02/id=B"),
        ];
        let table_dir = Path::new(&table.table_uri);
        for key in expected_keys {
            let partition_dir = table_dir.join(key);
            assert!(partition_dir.exists())
        }
    }

    fn validate_partition_map(
        partitions: Vec<PartitionResult>,
        partition_cols: &[String],
        expected_keys: Vec<String>,
    ) {
        assert_eq!(partitions.len(), expected_keys.len());
        for result in partitions {
            let partition_key =
                get_partition_key(partition_cols, &result.partition_values).unwrap();
            assert!(expected_keys.contains(&partition_key));
            let ref_batch = get_record_batch(Some(partition_key.clone()), false);
            assert_eq!(ref_batch, result.record_batch);
        }
    }
}
