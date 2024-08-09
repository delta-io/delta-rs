//! Main writer API to write json messages to delta table
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow::record_batch::*;
use bytes::Bytes;
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::{
    arrow::ArrowWriter, basic::Compression, errors::ParquetError,
    file::properties::WriterProperties,
};
use serde_json::Value;
use tracing::{info, warn};
use uuid::Uuid;

use super::stats::create_add;
use super::utils::{
    arrow_schema_without_partitions, next_data_path, record_batch_from_message,
    record_batch_without_partitions,
};
use super::{DeltaWriter, DeltaWriterError, WriteMode};
use crate::errors::DeltaTableError;
use crate::kernel::{scalars::ScalarExt, Add, PartitionsExt, StructType};
use crate::storage::ObjectStoreRetryExt;
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::DEFAULT_NUM_INDEX_COLS;
use crate::writer::utils::ShareableBuffer;
use crate::DeltaTable;

type BadValue = (Value, ParquetError);

/// Writes messages to a delta lake table.
pub struct JsonWriter {
    storage: Arc<dyn ObjectStore>,
    arrow_schema_ref: Arc<arrow_schema::Schema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, DataArrowWriter>,
}

/// Writes messages to an underlying arrow buffer.
pub(crate) struct DataArrowWriter {
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    buffer: ShareableBuffer,
    arrow_writer: ArrowWriter<ShareableBuffer>,
    partition_values: IndexMap<String, Scalar>,
    buffered_record_batch_count: usize,
}

impl DataArrowWriter {
    /// Writes the given JSON buffer and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many json buffers and flushed after the appropriate number of bytes has been written.
    async fn write_values(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        json_buffer: Vec<Value>,
    ) -> Result<(), DeltaWriterError> {
        let record_batch = record_batch_from_message(arrow_schema.clone(), json_buffer.as_slice())?;

        if record_batch.schema() != arrow_schema {
            return Err(DeltaWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: arrow_schema,
            });
        }

        let result = self
            .write_record_batch(partition_columns, record_batch)
            .await;

        if let Err(DeltaWriterError::Parquet { source }) = result {
            self.write_partial(partition_columns, arrow_schema, json_buffer, source)
                .await
        } else {
            result
        }
    }

    async fn write_partial(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        json_buffer: Vec<Value>,
        parquet_error: ParquetError,
    ) -> Result<(), DeltaWriterError> {
        warn!("Failed with parquet error while writing record batch. Attempting quarantine of bad records.");
        let (good, bad) = quarantine_failed_parquet_rows(arrow_schema.clone(), json_buffer)?;
        let record_batch = record_batch_from_message(arrow_schema, good.as_slice())?;
        self.write_record_batch(partition_columns, record_batch)
            .await?;
        info!(
            "Wrote {} good records to record batch and quarantined {} bad records.",
            good.len(),
            bad.len()
        );
        Err(DeltaWriterError::PartialParquetWrite {
            skipped_values: bad,
            sample_error: parquet_error,
        })
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many record batches and flushed after the appropriate number of bytes has been written.
    async fn write_record_batch(
        &mut self,
        partition_columns: &[String],
        record_batch: RecordBatch,
    ) -> Result<(), DeltaWriterError> {
        if self.partition_values.is_empty() {
            let partition_values = extract_partition_values(partition_columns, &record_batch)?;
            self.partition_values = partition_values;
        }

        // Copy current buffered bytes so we can recover from failures
        let buffer_bytes = self.buffer.to_vec();

        let record_batch = record_batch_without_partitions(&record_batch, partition_columns)?;
        let result = self.arrow_writer.write(&record_batch);

        match result {
            Ok(_) => {
                self.buffered_record_batch_count += 1;
                Ok(())
            }
            // If a write fails we need to reset the state of the DeltaArrowWriter
            Err(e) => {
                let new_buffer = ShareableBuffer::from_bytes(buffer_bytes.as_slice());
                let _ = std::mem::replace(&mut self.buffer, new_buffer.clone());
                let arrow_writer = Self::new_underlying_writer(
                    new_buffer,
                    self.arrow_schema.clone(),
                    self.writer_properties.clone(),
                )?;
                let _ = std::mem::replace(&mut self.arrow_writer, arrow_writer);
                self.partition_values.clear();

                Err(e.into())
            }
        }
    }

    fn new(
        arrow_schema: Arc<ArrowSchema>,
        writer_properties: WriterProperties,
    ) -> Result<Self, ParquetError> {
        let buffer = ShareableBuffer::default();
        let arrow_writer = Self::new_underlying_writer(
            buffer.clone(),
            arrow_schema.clone(),
            writer_properties.clone(),
        )?;

        let partition_values = IndexMap::new();
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

    fn new_underlying_writer(
        buffer: ShareableBuffer,
        arrow_schema: Arc<ArrowSchema>,
        writer_properties: WriterProperties,
    ) -> Result<ArrowWriter<ShareableBuffer>, ParquetError> {
        ArrowWriter::try_new(buffer, arrow_schema, Some(writer_properties))
    }
}

impl JsonWriter {
    /// Create a new JsonWriter instance
    pub fn try_new(
        table_uri: String,
        schema: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self, DeltaTableError> {
        let storage = DeltaTableBuilder::from_uri(table_uri)
            .with_storage_options(storage_options.unwrap_or_default())
            .build_storage()?;

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage: storage.object_store(),
            arrow_schema_ref: schema,
            writer_properties,
            partition_columns: partition_columns.unwrap_or_default(),
            arrow_writers: HashMap::new(),
        })
    }

    /// Creates a JsonWriter to write to the given table
    pub fn for_table(table: &DeltaTable) -> Result<JsonWriter, DeltaTableError> {
        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.metadata()?;
        let arrow_schema = <ArrowSchema as TryFrom<&StructType>>::try_from(&metadata.schema()?)?;
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

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.arrow_writers.values().map(|w| w.buffer.len()).sum()
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
    pub fn arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.arrow_schema_ref.clone()
    }

    fn divide_by_partition_values(
        &self,
        records: Vec<Value>,
    ) -> Result<HashMap<String, Vec<Value>>, DeltaWriterError> {
        let mut partitioned_records: HashMap<String, Vec<Value>> = HashMap::new();

        for record in records {
            let partition_value = self.json_to_partition_values(&record)?;
            match partitioned_records.get_mut(&partition_value) {
                Some(vec) => vec.push(record),
                None => {
                    partitioned_records.insert(partition_value, vec![record]);
                }
            };
        }

        Ok(partitioned_records)
    }

    fn json_to_partition_values(&self, value: &Value) -> Result<String, DeltaWriterError> {
        if let Some(obj) = value.as_object() {
            let key: Vec<String> = self
                .partition_columns
                .iter()
                .map(|c| obj.get(c).unwrap_or(&Value::Null).to_string())
                .collect();
            return Ok(key.join("/"));
        }

        Err(DeltaWriterError::InvalidRecord(value.to_string()))
    }
}

#[async_trait::async_trait]
impl DeltaWriter<Vec<Value>> for JsonWriter {
    /// Write a chunk of values into the internal write buffers with the default write mode
    async fn write(&mut self, values: Vec<Value>) -> Result<(), DeltaTableError> {
        self.write_with_mode(values, WriteMode::Default).await
    }

    /// Writes the given values to internal parquet buffers for each represented partition.
    async fn write_with_mode(
        &mut self,
        values: Vec<Value>,
        mode: WriteMode,
    ) -> Result<(), DeltaTableError> {
        if mode != WriteMode::Default {
            warn!("The JsonWriter does not currently support non-default write modes, falling back to default mode");
        }
        let mut partial_writes: Vec<(Value, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();
        let divided = self.divide_by_partition_values(values)?;
        let partition_columns = self.partition_columns.clone();
        let writer_properties = self.writer_properties.clone();

        for (key, values) in divided {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => {
                    let result = writer
                        .write_values(&partition_columns, arrow_schema.clone(), values)
                        .await;
                    collect_partial_write_failure(&mut partial_writes, result)?;
                }
                None => {
                    let schema = arrow_schema_without_partitions(&arrow_schema, &partition_columns);
                    let mut writer = DataArrowWriter::new(schema, writer_properties.clone())?;
                    let result = writer
                        .write_values(&partition_columns, arrow_schema.clone(), values)
                        .await;
                    collect_partial_write_failure(&mut partial_writes, result)?;
                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        if !partial_writes.is_empty() {
            return Err(DeltaWriterError::PartialParquetWrite {
                sample_error: match &partial_writes[0].1 {
                    ParquetError::General(msg) => ParquetError::General(msg.to_owned()),
                    ParquetError::ArrowError(msg) => ParquetError::ArrowError(msg.to_owned()),
                    ParquetError::EOF(msg) => ParquetError::EOF(msg.to_owned()),
                    ParquetError::External(err) => ParquetError::General(err.to_string()),
                    ParquetError::IndexOutOfBound(u, v) => {
                        ParquetError::IndexOutOfBound(u.to_owned(), v.to_owned())
                    }
                    ParquetError::NYI(msg) => ParquetError::NYI(msg.to_owned()),
                },
                skipped_values: partial_writes,
            }
            .into());
        }

        Ok(())
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, writer) in writers {
            let metadata = writer.arrow_writer.close()?;
            let prefix = writer.partition_values.hive_partition_path();
            let prefix = Path::parse(prefix)?;
            let uuid = Uuid::new_v4();

            let path = next_data_path(&prefix, 0, &uuid, &writer.writer_properties);
            let obj_bytes = Bytes::from(writer.buffer.to_vec());
            let file_size = obj_bytes.len() as i64;
            self.storage
                .put_with_retries(&path, obj_bytes.into(), 15)
                .await?;

            actions.push(create_add(
                &writer.partition_values,
                path.to_string(),
                file_size,
                &metadata,
                DEFAULT_NUM_INDEX_COLS,
                &None,
            )?);
        }
        Ok(actions)
    }
}

fn collect_partial_write_failure(
    partial_writes: &mut Vec<(Value, ParquetError)>,
    writer_result: Result<(), DeltaWriterError>,
) -> Result<(), DeltaWriterError> {
    match writer_result {
        Err(DeltaWriterError::PartialParquetWrite { skipped_values, .. }) => {
            partial_writes.extend(skipped_values);
            Ok(())
        }
        _ => writer_result,
    }
}

fn quarantine_failed_parquet_rows(
    arrow_schema: Arc<ArrowSchema>,
    values: Vec<Value>,
) -> Result<(Vec<Value>, Vec<BadValue>), DeltaWriterError> {
    let mut good: Vec<Value> = Vec::new();
    let mut bad: Vec<BadValue> = Vec::new();

    for value in values {
        let record_batch = record_batch_from_message(arrow_schema.clone(), &[value.clone()])?;
        let buffer = ShareableBuffer::default();
        let mut writer = ArrowWriter::try_new(buffer.clone(), arrow_schema.clone(), None)?;

        match writer.write(&record_batch) {
            Ok(_) => good.push(value),
            Err(e) => bad.push((value, e)),
        }
    }

    Ok((good, bad))
}

fn extract_partition_values(
    partition_cols: &[String],
    record_batch: &RecordBatch,
) -> Result<IndexMap<String, Scalar>, DeltaWriterError> {
    let mut partition_values = IndexMap::new();

    for col_name in partition_cols.iter() {
        let arrow_schema = record_batch.schema();
        let i = arrow_schema.index_of(col_name)?;
        let col = record_batch.column(i);
        let value = Scalar::from_array(col.as_ref(), 0)
            .ok_or(DeltaWriterError::MissingPartitionColumn(col_name.clone()))?;

        partition_values.insert(col_name.clone(), value);
    }

    Ok(partition_values)
}

#[cfg(test)]
mod tests {
    use arrow_schema::ArrowError;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;
    use std::sync::Arc;

    use super::*;
    use crate::arrow::array::Int32Array;
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::kernel::DataType;
    use crate::writer::test_utils::get_delta_schema;
    use crate::writer::DeltaWriter;
    use crate::writer::JsonWriter;

    #[tokio::test]
    async fn test_partition_not_written_to_parquet() {
        let table_dir = tempfile::tempdir().unwrap();
        let schema = get_delta_schema();
        let path = table_dir.path().to_str().unwrap().to_string();

        let arrow_schema = <ArrowSchema as TryFrom<&StructType>>::try_from(&schema).unwrap();
        let mut writer = JsonWriter::try_new(
            path.clone(),
            Arc::new(arrow_schema),
            Some(vec!["modified".to_string()]),
            None,
        )
        .unwrap();

        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );

        writer.write(vec![data]).await.unwrap();
        let add_actions = writer.flush().await.unwrap();
        let add = &add_actions[0];
        let path = table_dir.path().join(&add.path);

        let file = File::open(path.as_path()).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();
        let schema_desc = metadata.file_metadata().schema_descr();

        let columns = schema_desc
            .columns()
            .iter()
            .map(|desc| desc.name().to_string())
            .collect::<Vec<String>>();
        assert_eq!(columns, vec!["id".to_string(), "value".to_string()]);
    }

    #[test]
    fn test_extract_partition_values() {
        let record_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("col1", ArrowDataType::Int32, false),
                ArrowField::new("col2", ArrowDataType::Int32, false),
                ArrowField::new("col3", ArrowDataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![2, 1])),
                Arc::new(Int32Array::from(vec![None, None])),
            ],
        )
        .unwrap();

        assert_eq!(
            extract_partition_values(
                &[
                    String::from("col1"),
                    String::from("col2"),
                    String::from("col3")
                ],
                &record_batch
            )
            .unwrap(),
            IndexMap::from([
                (String::from("col1"), Scalar::Integer(1)),
                (String::from("col2"), Scalar::Integer(2)),
                (String::from("col3"), Scalar::Null(DataType::INTEGER)),
            ])
        );
        assert_eq!(
            extract_partition_values(&[String::from("col1")], &record_batch).unwrap(),
            IndexMap::from([(String::from("col1"), Scalar::Integer(1)),])
        );
        assert!(extract_partition_values(&[String::from("col4")], &record_batch).is_err())
    }

    #[tokio::test]
    async fn test_parsing_error() {
        let table_dir = tempfile::tempdir().unwrap();
        let schema = get_delta_schema();
        let path = table_dir.path().to_str().unwrap().to_string();

        let arrow_schema = <ArrowSchema as TryFrom<&StructType>>::try_from(&schema).unwrap();
        let mut writer = JsonWriter::try_new(
            path.clone(),
            Arc::new(arrow_schema),
            Some(vec!["modified".to_string()]),
            None,
        )
        .unwrap();

        let data = serde_json::json!(
            {
                "id" : "A",
                "value": "abc",
                "modified": "2021-02-01"
            }
        );

        let res = writer.write(vec![data]).await;
        assert!(matches!(
            res,
            Err(DeltaTableError::Arrow {
                source: ArrowError::JsonError(_)
            })
        ));
    }

    // The following sets of tests are related to #1386 and mergeSchema support
    // <https://github.com/delta-io/delta-rs/issues/1386>
    mod schema_evolution {
        use super::*;

        #[tokio::test]
        async fn test_json_write_mismatched_values() {
            let table_dir = tempfile::tempdir().unwrap();
            let schema = get_delta_schema();
            let path = table_dir.path().to_str().unwrap().to_string();

            let arrow_schema = <ArrowSchema as TryFrom<&StructType>>::try_from(&schema).unwrap();
            let mut writer = JsonWriter::try_new(
                path.clone(),
                Arc::new(arrow_schema),
                Some(vec!["modified".to_string()]),
                None,
            )
            .unwrap();

            let data = serde_json::json!(
                {
                    "id" : "A",
                    "value": 42,
                    "modified": "2021-02-01"
                }
            );

            writer.write(vec![data]).await.unwrap();
            let add_actions = writer.flush().await.unwrap();
            assert_eq!(add_actions.len(), 1);

            let second_data = serde_json::json!(
                {
                    "id" : 1,
                    "name" : "Ion"
                }
            );

            if writer.write(vec![second_data]).await.is_ok() {
                panic!("Should not have successfully written");
            }
        }

        #[tokio::test]
        async fn test_json_write_mismatched_schema() {
            use crate::operations::create::CreateBuilder;
            let table_dir = tempfile::tempdir().unwrap();
            let schema = get_delta_schema();
            let path = table_dir.path().to_str().unwrap().to_string();

            let mut table = CreateBuilder::new()
                .with_location(&path)
                .with_table_name("test-table")
                .with_comment("A table for running tests")
                .with_columns(schema.fields().cloned())
                .await
                .unwrap();
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), 0);

            let arrow_schema = <ArrowSchema as TryFrom<&StructType>>::try_from(&schema).unwrap();
            let mut writer = JsonWriter::try_new(
                path.clone(),
                Arc::new(arrow_schema),
                Some(vec!["modified".to_string()]),
                None,
            )
            .unwrap();

            let data = serde_json::json!(
                {
                    "id" : "A",
                    "value": 42,
                    "modified": "2021-02-01"
                }
            );

            writer.write(vec![data]).await.unwrap();
            let add_actions = writer.flush().await.unwrap();
            assert_eq!(add_actions.len(), 1);

            let second_data = serde_json::json!(
                {
                    "postcode" : 1,
                    "name" : "Ion"
                }
            );

            // TODO This should fail because we haven't asked to evolve the schema
            writer.write(vec![second_data]).await.unwrap();
            writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(table.version(), 1);
        }
    }
}
