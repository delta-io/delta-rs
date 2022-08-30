//! Main writer API to write json messages to delta table
use super::{
    stats::{apply_null_counts, create_add, NullCounts},
    utils::{
        arrow_schema_without_partitions, next_data_path, record_batch_from_message,
        record_batch_without_partitions, stringified_partition_value,
    },
    DeltaWriter, DeltaWriterError,
};
use crate::builder::DeltaTableBuilder;
use crate::{action::Add, DeltaTable, DeltaTableMetaData, Schema};
use crate::{storage::DeltaObjectStore, writer::utils::ShareableBuffer};
use arrow::{
    datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    record_batch::*,
};
use bytes::Bytes;
use log::{info, warn};
use object_store::ObjectStore;
use parquet::{
    arrow::ArrowWriter, basic::Compression, errors::ParquetError,
    file::properties::WriterProperties,
};
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

type BadValue = (Value, ParquetError);

/// Writes messages to a delta lake table.
pub struct JsonWriter {
    storage: Arc<DeltaObjectStore>,
    arrow_schema_ref: Arc<arrow::datatypes::Schema>,
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
    partition_values: HashMap<String, Option<String>>,
    null_counts: NullCounts,
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
                apply_null_counts(&record_batch.into(), &mut self.null_counts, 0);
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

        let partition_values = HashMap::new();
        let null_counts = NullCounts::new();
        let buffered_record_batch_count = 0;

        Ok(Self {
            arrow_schema,
            writer_properties,
            buffer,
            arrow_writer,
            partition_values,
            null_counts,
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
    ) -> Result<Self, DeltaWriterError> {
        let storage = DeltaTableBuilder::from_uri(&table_uri)
            .with_storage_options(storage_options.unwrap_or_default())
            .build_storage()?;

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

    /// Creates a JsonWriter to write to the given table
    pub fn for_table(table: &DeltaTable) -> Result<JsonWriter, DeltaWriterError> {
        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.get_metadata()?;
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema)?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns.clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage: table.storage.clone(),
            arrow_schema_ref,
            writer_properties,
            partition_columns,
            arrow_writers: HashMap::new(),
        })
    }

    /// Retrieves the latest schema from table, compares to the current and updates if changed.
    /// When schema is updated then `true` is returned which signals the caller that parquet
    /// created file or arrow batch should be revisited.
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
    /// Writes the given values to internal parquet buffers for each represented partition.
    async fn write(&mut self, values: Vec<Value>) -> Result<(), DeltaWriterError> {
        let mut partial_writes: Vec<(Value, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, values) in self.divide_by_partition_values(values)? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => collect_partial_write_failure(
                    &mut partial_writes,
                    writer
                        .write_values(&self.partition_columns, arrow_schema.clone(), values)
                        .await,
                )?,
                None => {
                    let schema =
                        arrow_schema_without_partitions(&arrow_schema, &self.partition_columns);
                    let mut writer = DataArrowWriter::new(schema, self.writer_properties.clone())?;

                    collect_partial_write_failure(
                        &mut partial_writes,
                        writer
                            .write_values(&self.partition_columns, self.arrow_schema(), values)
                            .await,
                    )?;

                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        if !partial_writes.is_empty() {
            let sample = partial_writes.first().map(|t| t.to_owned());
            if let Some((_, e)) = sample {
                return Err(DeltaWriterError::PartialParquetWrite {
                    skipped_values: partial_writes,
                    sample_error: e,
                });
            } else {
                unreachable!()
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
            let obj_bytes = Bytes::from(writer.buffer.to_vec());
            let file_size = obj_bytes.len() as i64;
            self.storage.put(&path, obj_bytes).await?;

            // Replace self null_counts with an empty map. Use the other for stats.
            let null_counts = std::mem::take(&mut writer.null_counts);

            actions.push(create_add(
                &writer.partition_values,
                null_counts,
                path.to_string(),
                file_size,
                &metadata,
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
) -> Result<HashMap<String, Option<String>>, DeltaWriterError> {
    let mut partition_values = HashMap::new();

    for col_name in partition_cols.iter() {
        let arrow_schema = record_batch.schema();

        let i = arrow_schema.index_of(col_name)?;
        let col = record_batch.column(i);

        let partition_string = stringified_partition_value(col)?;

        partition_values.insert(col_name.clone(), partition_string);
    }

    Ok(partition_values)
}

#[cfg(test)]
mod tests {
    use crate::writer::test_utils::get_delta_schema;
    use crate::writer::DeltaWriter;
    use crate::writer::JsonWriter;
    use crate::Schema;
    use arrow::datatypes::Schema as ArrowSchema;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_partition_not_written_to_parquet() {
        let table_dir = tempfile::tempdir().unwrap();
        let schema = get_delta_schema();
        let path = table_dir.path().to_str().unwrap().to_string();

        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&schema).unwrap();
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
                "value": "test",
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
}
