//! Main writer API to write record batches to Delta Table
//!
//! Writes Arrow record batches to a Delta Table, handling partitioning and file statistics.
//! Each Parquet file is buffered in-memory and only written once `flush()` is called on
//! the writer. Once written, add actions are returned by the writer. It's the users responsibility
//! to create the transaction using those actions.

use std::{collections::HashMap, sync::Arc};

use arrow::array::{Array, UInt32Array};
use arrow::compute::{partition, take};
use arrow::record_batch::RecordBatch;
use arrow_array::ArrayRef;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use parquet::{basic::Compression, file::properties::WriterProperties};
use uuid::Uuid;

use super::stats::create_add;
use super::utils::{
    arrow_schema_without_partitions, next_data_path, record_batch_without_partitions,
    stringified_partition_value, PartitionPath, ShareableBuffer,
};
use super::{DeltaWriter, DeltaWriterError};
use crate::errors::DeltaTableError;
use crate::kernel::{Add, StructType};
use crate::table::builder::DeltaTableBuilder;
use crate::table::DeltaTableMetaData;
use crate::DeltaTable;

/// Writes messages to a delta lake table.
pub struct RecordBatchWriter {
    storage: Arc<dyn ObjectStore>,
    arrow_schema_ref: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, PartitionWriter>,
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
        let storage = DeltaTableBuilder::from_uri(table_uri)
            .with_storage_options(storage_options.unwrap_or_default())
            .build_storage()?
            .object_store();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage,
            arrow_schema_ref: schema,
            writer_properties,
            partition_columns: partition_columns.unwrap_or_default(),
            arrow_writers: HashMap::new(),
        })
    }

    /// Creates a [`RecordBatchWriter`] to write data to provided Delta Table
    pub fn for_table(table: &DeltaTable) -> Result<Self, DeltaTableError> {
        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.get_metadata()?;
        let arrow_schema =
            <ArrowSchema as TryFrom<&StructType>>::try_from(&metadata.schema.clone())?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns.clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage: table.object_store(),
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
    ) -> Result<bool, DeltaTableError> {
        let schema: ArrowSchema =
            <ArrowSchema as TryFrom<&StructType>>::try_from(&metadata.schema)?;

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

    ///Write a batch to the specified partition
    pub async fn write_partition(
        &mut self,
        record_batch: RecordBatch,
        partition_values: &HashMap<String, Option<String>>,
    ) -> Result<(), DeltaTableError> {
        let arrow_schema =
            arrow_schema_without_partitions(&self.arrow_schema_ref, &self.partition_columns);
        let partition_key =
            PartitionPath::from_hashmap(&self.partition_columns, partition_values)?.into();

        let record_batch = record_batch_without_partitions(&record_batch, &self.partition_columns)?;

        match self.arrow_writers.get_mut(&partition_key) {
            Some(writer) => {
                writer.write(&record_batch)?;
            }
            None => {
                let mut writer = PartitionWriter::new(
                    arrow_schema,
                    partition_values.clone(),
                    self.writer_properties.clone(),
                )?;
                writer.write(&record_batch)?;
                let _ = self.arrow_writers.insert(partition_key, writer);
            }
        }

        Ok(())
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
    /// Divides a single record batch into into multiple according to table partitioning.
    /// Values are written to arrow buffers, to collect data until it should be written to disk.
    async fn write(&mut self, values: RecordBatch) -> Result<(), DeltaTableError> {
        for result in self.divide_by_partition_values(&values)? {
            self.write_partition(result.record_batch, &result.partition_values)
                .await?;
        }
        Ok(())
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, writer) in writers {
            let metadata = writer.arrow_writer.close()?;
            let prefix =
                PartitionPath::from_hashmap(&self.partition_columns, &writer.partition_values)?;
            let prefix = Path::parse(prefix)?;
            let uuid = Uuid::new_v4();
            let path = next_data_path(&prefix, 0, &uuid, &writer.writer_properties);
            let obj_bytes = Bytes::from(writer.buffer.to_vec());
            let file_size = obj_bytes.len() as i64;
            self.storage.put(&path, obj_bytes).await?;

            actions.push(create_add(
                &writer.partition_values,
                path.to_string(),
                file_size,
                &metadata,
            )?);
        }
        Ok(actions)
    }
}

/// Helper container for partitioned record batches
#[derive(Clone, Debug)]
pub struct PartitionResult {
    /// values found in partition columns
    pub partition_values: HashMap<String, Option<String>>,
    /// remaining dataset with partition column values removed
    pub record_batch: RecordBatch,
}

struct PartitionWriter {
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    pub(super) buffer: ShareableBuffer,
    pub(super) arrow_writer: ArrowWriter<ShareableBuffer>,
    pub(super) partition_values: HashMap<String, Option<String>>,
    pub(super) buffered_record_batch_count: usize,
}

impl PartitionWriter {
    pub fn new(
        arrow_schema: Arc<ArrowSchema>,
        partition_values: HashMap<String, Option<String>>,
        writer_properties: WriterProperties,
    ) -> Result<Self, ParquetError> {
        let buffer = ShareableBuffer::default();
        let arrow_writer = ArrowWriter::try_new(
            buffer.clone(),
            arrow_schema.clone(),
            Some(writer_properties.clone()),
        )?;

        let buffered_record_batch_count = 0;

        Ok(Self {
            arrow_schema,
            writer_properties,
            buffer,
            arrow_writer,
            partition_values,
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
        let buffer_bytes = self.buffer.to_vec();

        match self.arrow_writer.write(record_batch) {
            Ok(_) => {
                self.buffered_record_batch_count += 1;
                Ok(())
            }
            // If a write fails we need to reset the state of the PartitionWriter
            Err(e) => {
                let new_buffer = ShareableBuffer::from_bytes(buffer_bytes.as_slice());
                let _ = std::mem::replace(&mut self.buffer, new_buffer.clone());
                let arrow_writer = ArrowWriter::try_new(
                    new_buffer,
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
        self.buffer.len() + self.arrow_writer.in_progress_size()
    }
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
            partition_values: HashMap::new(),
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

        let partition_key_iter = sorted_partition_columns.iter().map(|col| {
            stringified_partition_value(&col.slice(range.start, range.end - range.start))
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
    use super::*;
    use crate::writer::{
        test_utils::{create_initialized_table, get_record_batch},
        utils::PartitionPath,
    };
    use arrow::json::ReaderBuilder;
    use std::path::Path;

    #[tokio::test]
    async fn test_buffer_len_includes_unflushed_row_group() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        writer.write(batch).await.unwrap();

        assert!(writer.buffer_len() > 0);
    }

    #[tokio::test]
    async fn test_divide_record_batch_no_partition() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].record_batch, batch)
    }

    #[tokio::test]
    async fn test_divide_record_batch_single_partition() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = writer.divide_by_partition_values(&batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];
        validate_partition_map(partitions, &partition_cols, expected_keys)
    }

    /*
     * This test is a little messy but demonstrates a bug when
     * trying to write data to a Delta Table that has a map column and partition columns
     *
     * For readability the schema and data for the write are defined in JSON
     */
    #[tokio::test]
    async fn test_divide_record_batch_with_map_single_partition() {
        use crate::DeltaOps;

        let table = crate::writer::test_utils::create_bare_table();
        let partition_cols = vec!["modified".to_string()];
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

        let table = DeltaOps(table)
            .create()
            .with_partition_columns(partition_cols.to_vec())
            .with_columns(delta_schema.fields().clone())
            .await
            .unwrap();

        let buf = r#"
            {"id" : "0xdeadbeef", "value" : 42, "modified" : "2021-02-01",
                "metadata" : {"some-key" : "some-value"}}
            {"id" : "0xdeadcaf", "value" : 3, "modified" : "2021-02-02",
                "metadata" : {"some-key" : "some-value"}}"#
            .as_bytes();

        let schema: ArrowSchema =
            <ArrowSchema as TryFrom<&StructType>>::try_from(&delta_schema).unwrap();

        // Using a batch size of two since the buf above only has two records
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

        let expected_keys = vec![
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];

        assert_eq!(partitions.len(), expected_keys.len());
        for result in partitions {
            let partition_key =
                PartitionPath::from_hashmap(&partition_cols, &result.partition_values)
                    .unwrap()
                    .into();
            assert!(expected_keys.contains(&partition_key));
        }
    }

    #[tokio::test]
    async fn test_divide_record_batch_multiple_partitions() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(&partition_cols).await;
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

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
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 1);
    }

    #[tokio::test]
    async fn test_write_multiple_partitions() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(&partition_cols).await;
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
        let table_uri = table.table_uri();
        let table_dir = Path::new(&table_uri);
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
                PartitionPath::from_hashmap(partition_cols, &result.partition_values)
                    .unwrap()
                    .into();
            assert!(expected_keys.contains(&partition_key));
            let ref_batch = get_record_batch(Some(partition_key.clone()), false);
            assert_eq!(ref_batch, result.record_batch);
        }
    }
}
