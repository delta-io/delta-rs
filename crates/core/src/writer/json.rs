//! Main writer API to write json messages to delta table
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow::record_batch::*;
use bytes::Bytes;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use itertools::Itertools;
use object_store::path::Path;
use parquet::{
    arrow::ArrowWriter, basic::Compression, errors::ParquetError,
    file::properties::WriterProperties,
};
use serde_json::Value;
use tracing::*;
use url::Url;
use uuid::Uuid;

use super::stats::create_add;
use super::utils::{
    arrow_schema_without_partitions, next_data_path, record_batch_from_message,
    record_batch_without_partitions,
};
use super::{DeltaWriter, DeltaWriterError, WriteMode};
use crate::DeltaTable;
use crate::errors::DeltaTableError;
use crate::kernel::{Add, PartitionsExt, scalars::ScalarExt};
use crate::logstore::ObjectStoreRetryExt;
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::TablePropertiesExt as _;
use crate::writer::utils::ShareableBuffer;

type BadValue = (Value, ParquetError);

/// Writes messages to a delta lake table.
#[derive(Debug)]
pub struct JsonWriter {
    table: DeltaTable,
    /// Optional schema to use, otherwise try to rely on the schema from the [DeltaTable]
    schema_ref: Option<ArrowSchemaRef>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, DataArrowWriter>,
}

/// Writes messages to an underlying arrow buffer.
#[derive(Debug)]
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
        warn!(
            "Failed with parquet error while writing record batch. Attempting quarantine of bad records."
        );
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
    pub async fn try_new(
        table_url: Url,
        schema_ref: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self, DeltaTableError> {
        let table = DeltaTableBuilder::from_url(table_url)?
            .with_storage_options(storage_options.unwrap_or_default())
            .load()
            .await?;
        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            table,
            schema_ref: Some(schema_ref),
            writer_properties,
            partition_columns: partition_columns.unwrap_or_default(),
            arrow_writers: HashMap::new(),
        })
    }

    /// Creates a JsonWriter to write to the given table
    pub fn for_table(table: &DeltaTable) -> Result<JsonWriter, DeltaTableError> {
        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.snapshot()?.metadata();
        let partition_columns = metadata.partition_columns().clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            table: table.clone(),
            writer_properties,
            partition_columns,
            schema_ref: None,
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

    /// Returns the user-defined arrow schema representation or the schema defined for the wrapped
    /// table.
    ///
    pub fn arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        if let Some(schema_ref) = self.schema_ref.as_ref() {
            return schema_ref.clone();
        }
        let schema = self
            .table
            .snapshot()
            .expect("Failed to unwrap snapshot for table")
            .schema();
        Arc::new(
            schema
                .as_ref()
                .try_into_arrow()
                .expect("Failed to coerce delta schema to arrow"),
        )
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
            warn!(
                "The JsonWriter does not currently support non-default write modes, falling back to default mode"
            );
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
                    // ParquetError is non exhaustive, so have a fallback
                    e => ParquetError::General(e.to_string()),
                },
                skipped_values: partial_writes,
            }
            .into());
        }

        Ok(())
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another
    /// file.
    ///
    /// This function returns the [Add] actions which should be committed to the [DeltaTable] for
    /// the written data files
    #[instrument(skip(self), fields(writer_count = 0))]
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::with_capacity(writers.len());

        Span::current().record("writer_count", writers.len());

        for (_, writer) in writers {
            let metadata = writer.arrow_writer.close()?;
            let prefix = writer.partition_values.hive_partition_path();
            let prefix = Path::parse(prefix)?;
            let uuid = Uuid::new_v4();

            let path = next_data_path(&prefix, 0, &uuid, &writer.writer_properties);
            let obj_bytes = Bytes::from(writer.buffer.to_vec());
            let file_size = obj_bytes.len() as i64;

            debug!(path = %path, size = file_size, rows = metadata.file_metadata().num_rows(), "writing data file");

            self.table
                .object_store()
                .put_with_retries(&path, obj_bytes.into(), 15)
                .await?;

            let table_config = self.table.snapshot()?.table_config();

            actions.push(create_add(
                &writer.partition_values,
                path.to_string(),
                file_size,
                &metadata,
                table_config.num_indexed_cols(),
                &table_config
                    .data_skipping_stats_columns
                    .as_ref()
                    .map(|cols| cols.iter().map(|c| c.to_string()).collect_vec()),
            )?);
        }
        debug!(actions_count = actions.len(), "flush completed");
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
    let mut good: Vec<Value> = Vec::with_capacity(values.len());
    let mut bad: Vec<BadValue> = Vec::with_capacity(values.len());

    for value in values {
        let record_batch =
            record_batch_from_message(arrow_schema.clone(), std::slice::from_ref(&value))?;
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
    use super::*;

    use arrow_schema::ArrowError;
    #[cfg(feature = "datafusion")]
    use futures::TryStreamExt;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    use crate::arrow::array::Int32Array;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use crate::operations::create::CreateBuilder;
    use crate::writer::test_utils::get_delta_schema;

    /// Generate a simple test table which has been pre-created at version 0
    async fn get_test_table(table_dir: &tempfile::TempDir) -> DeltaTable {
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
        assert_eq!(table.version(), Some(0));
        table
    }

    #[tokio::test]
    async fn test_partition_not_written_to_parquet() {
        let table_dir = tempfile::tempdir().unwrap();
        let table = get_test_table(&table_dir).await;
        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
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
                &[String::from("col1"), String::from("col2"),],
                &record_batch
            )
            .unwrap(),
            IndexMap::from([
                (String::from("col1"), Scalar::Integer(1)),
                (String::from("col2"), Scalar::Integer(2)),
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
        let table = get_test_table(&table_dir).await;

        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
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
            let table = get_test_table(&table_dir).await;

            let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
            let mut writer = JsonWriter::try_new(
                Url::from_directory_path(table_dir.path()).unwrap(),
                arrow_schema,
                Some(vec!["modified".to_string()]),
                None,
            )
            .await
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

        #[cfg(feature = "datafusion")]
        #[tokio::test]
        async fn test_json_write_mismatched_schema() {
            let table_dir = tempfile::tempdir().unwrap();
            let mut table = get_test_table(&table_dir).await;

            let mut writer = JsonWriter::try_new(
                table.table_url().clone(),
                table.snapshot().unwrap().snapshot().arrow_schema(),
                Some(vec!["modified".to_string()]),
                None,
            )
            .await
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
            assert_eq!(table.version(), Some(1));
        }
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_json_write_checkpoint() {
        use std::fs;

        let table_dir = tempfile::tempdir().unwrap();
        let schema = get_delta_schema();
        let path = table_dir.path().to_str().unwrap().to_string();
        let config: HashMap<String, Option<String>> = vec![
            (
                "delta.checkpointInterval".to_string(),
                Some("5".to_string()),
            ),
            ("delta.checkpointPolicy".to_string(), Some("v2".to_string())),
        ]
        .into_iter()
        .collect();
        let mut table = CreateBuilder::new()
            .with_location(&path)
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(schema.fields().cloned())
            .with_configuration(config)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let mut writer = JsonWriter::for_table(&table).unwrap();
        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );
        for _ in 1..6 {
            writer.write(vec![data.clone()]).await.unwrap();
            writer.flush_and_commit(&mut table).await.unwrap();
        }
        let dir_path = path + "/_delta_log";

        let target_file = "00000000000000000004.checkpoint.parquet";
        let entries: Vec<_> = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().into_string().unwrap() == target_file)
            .collect();
        assert_eq!(entries.len(), 1);
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_json_write_data_skipping_stats_columns() {
        let table_dir = tempfile::tempdir().unwrap();
        let path = table_dir.path().to_str().unwrap().to_string();
        let config: HashMap<String, Option<String>> = vec![(
            "delta.dataSkippingStatsColumns".to_string(),
            Some("id,value".to_string()),
        )]
        .into_iter()
        .collect();

        let schema = get_delta_schema();
        let mut table = CreateBuilder::new()
            .with_location(&path)
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(schema.fields().cloned())
            .with_configuration(config)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
        .unwrap();
        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );

        writer.write(vec![data]).await.unwrap();
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
        let expected_stats = "{\"numRecords\":1,\"minValues\":{\"id\":\"A\",\"value\":42},\"maxValues\":{\"id\":\"A\",\"value\":42},\"nullCount\":{\"id\":0,\"value\":0}}";
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

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_json_write_data_skipping_num_indexed_cols() {
        let table_dir = tempfile::tempdir().unwrap();
        let path = table_dir.path().to_str().unwrap().to_string();
        let config: HashMap<String, Option<String>> = vec![(
            "delta.dataSkippingNumIndexedCols".to_string(),
            Some("1".to_string()),
        )]
        .into_iter()
        .collect();

        let schema = get_delta_schema();
        let mut table = CreateBuilder::new()
            .with_location(&path)
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(schema.fields().cloned())
            .with_configuration(config)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
        .unwrap();
        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );

        writer.write(vec![data]).await.unwrap();
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
        let expected_stats = "{\"numRecords\":1,\"minValues\":{\"id\":\"A\"},\"maxValues\":{\"id\":\"A\"},\"nullCount\":{\"id\":0}}";
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
