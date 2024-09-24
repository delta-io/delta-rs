//! Main writer API to write record batches to Delta Table
//!
//! Writes Arrow record batches to a Delta Table, handling partitioning and file statistics.
//! Each Parquet file is buffered in-memory and only written once `flush()` is called on
//! the writer. Once written, add actions are returned by the writer. It's the users responsibility
//! to create the transaction using those actions.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{new_null_array, Array, ArrayRef, RecordBatch, UInt32Array};
use arrow_ord::partition::partition;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow_select::take::take;
use bytes::Bytes;
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use object_store::{path::Path, ObjectStore};
use parquet::{arrow::ArrowWriter, errors::ParquetError};
use parquet::{basic::Compression, file::properties::WriterProperties};
use tracing::log::*;
use uuid::Uuid;

use super::stats::create_add;
use super::utils::{
    arrow_schema_without_partitions, next_data_path, record_batch_without_partitions,
    ShareableBuffer,
};
use super::{DeltaWriter, DeltaWriterError, WriteMode};
use crate::errors::DeltaTableError;
use crate::kernel::{scalars::ScalarExt, Action, Add, PartitionsExt, StructType};
use crate::operations::cast::merge_schema::merge_arrow_schema;
use crate::storage::ObjectStoreRetryExt;
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::DEFAULT_NUM_INDEX_COLS;
use crate::DeltaTable;

/// Writes messages to a delta lake table.
pub struct RecordBatchWriter {
    storage: Arc<dyn ObjectStore>,
    arrow_schema_ref: ArrowSchemaRef,
    original_schema_ref: ArrowSchemaRef,
    writer_properties: WriterProperties,
    should_evolve: bool,
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
            arrow_schema_ref: schema.clone(),
            original_schema_ref: schema,
            writer_properties,
            partition_columns: partition_columns.unwrap_or_default(),
            should_evolve: false,
            arrow_writers: HashMap::new(),
        })
    }

    /// Creates a [`RecordBatchWriter`] to write data to provided Delta Table
    pub fn for_table(table: &DeltaTable) -> Result<Self, DeltaTableError> {
        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.metadata()?;
        let arrow_schema =
            <ArrowSchema as TryFrom<&StructType>>::try_from(&metadata.schema()?.clone())?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns.clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage: table.object_store(),
            arrow_schema_ref: arrow_schema_ref.clone(),
            original_schema_ref: arrow_schema_ref.clone(),
            writer_properties,
            partition_columns,
            should_evolve: false,
            arrow_writers: HashMap::new(),
        })
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
        partition_values: &IndexMap<String, Scalar>,
        mode: WriteMode,
    ) -> Result<ArrowSchemaRef, DeltaTableError> {
        let arrow_schema =
            arrow_schema_without_partitions(&self.arrow_schema_ref, &self.partition_columns);
        let partition_key = partition_values.hive_partition_path();

        let record_batch = record_batch_without_partitions(&record_batch, &self.partition_columns)?;

        let written_schema = match self.arrow_writers.get_mut(&partition_key) {
            Some(writer) => writer.write(&record_batch, mode)?,
            None => {
                let mut writer = PartitionWriter::new(
                    arrow_schema,
                    partition_values.clone(),
                    self.writer_properties.clone(),
                )?;
                let schema = writer.write(&record_batch, mode)?;
                let _ = self.arrow_writers.insert(partition_key, writer);
                schema
            }
        };
        Ok(written_schema)
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
    /// Write a chunk of values into the internal write buffers with the default write mode
    async fn write(&mut self, values: RecordBatch) -> Result<(), DeltaTableError> {
        self.write_with_mode(values, WriteMode::Default).await
    }
    /// Divides a single record batch into into multiple according to table partitioning.
    /// Values are written to arrow buffers, to collect data until it should be written to disk.
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
        // Set the should_evolve flag for later in case the writer should perform schema evolution
        // on its flush_and_commit
        self.should_evolve = mode == WriteMode::MergeSchema;

        for result in self.divide_by_partition_values(&values)? {
            let schema = self
                .write_partition(result.record_batch, &result.partition_values, mode)
                .await?;
            self.arrow_schema_ref = schema;
        }
        Ok(())
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, writer) in writers {
            let metadata = writer.arrow_writer.close()?;
            let prefix = Path::parse(writer.partition_values.hive_partition_path())?;
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

    /// Flush the internal write buffers to files in the delta table folder structure.
    /// and commit the changes to the Delta log, creating a new table version.
    async fn flush_and_commit(&mut self, table: &mut DeltaTable) -> Result<i64, DeltaTableError> {
        use crate::kernel::{Metadata, StructType};
        let mut adds: Vec<Action> = self.flush().await?.drain(..).map(Action::Add).collect();

        if self.arrow_schema_ref != self.original_schema_ref && self.should_evolve {
            let schema: StructType = self.arrow_schema_ref.clone().try_into()?;
            if !self.partition_columns.is_empty() {
                return Err(DeltaTableError::Generic(
                    "Merging Schemas with partition columns present is currently unsupported"
                        .to_owned(),
                ));
            }
            let part_cols: Vec<String> = vec![];
            let metadata = Metadata::try_new(schema, part_cols, HashMap::new())?;
            adds.push(Action::Metadata(metadata));
        }
        super::flush_and_commit(adds, table).await
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

struct PartitionWriter {
    arrow_schema: ArrowSchemaRef,
    writer_properties: WriterProperties,
    pub(super) buffer: ShareableBuffer,
    pub(super) arrow_writer: ArrowWriter<ShareableBuffer>,
    pub(super) partition_values: IndexMap<String, Scalar>,
    pub(super) buffered_record_batch_count: usize,
}

impl PartitionWriter {
    pub fn new(
        arrow_schema: ArrowSchemaRef,
        partition_values: IndexMap<String, Scalar>,
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
    ///
    /// Returns the schema which was written by the write which can be used to understand if a
    /// schema evolution has happened
    pub fn write(
        &mut self,
        record_batch: &RecordBatch,
        mode: WriteMode,
    ) -> Result<ArrowSchemaRef, DeltaWriterError> {
        let merged_batch = if record_batch.schema() != self.arrow_schema {
            match mode {
                WriteMode::MergeSchema => {
                    debug!("The writer and record batch schemas do not match, merging");

                    let merged = merge_arrow_schema(
                        self.arrow_schema.clone(),
                        record_batch.schema().clone(),
                        true,
                    )?;
                    self.arrow_schema = merged;

                    let mut cols = vec![];
                    for field in self.arrow_schema.fields() {
                        if let Some(column) = record_batch.column_by_name(field.name()) {
                            cols.push(column.clone());
                        } else {
                            let null_column =
                                new_null_array(field.data_type(), record_batch.num_rows());
                            cols.push(null_column);
                        }
                    }
                    Some(RecordBatch::try_new(self.arrow_schema.clone(), cols)?)
                }
                WriteMode::Default => {
                    // If the schemas didn't match then an error should be pushed up
                    Err(DeltaWriterError::SchemaMismatch {
                        record_batch_schema: record_batch.schema(),
                        expected_schema: self.arrow_schema.clone(),
                    })?
                }
            }
        } else {
            None
        };

        // Copy current cursor bytes so we can recover from failures
        let buffer_bytes = self.buffer.to_vec();
        let record_batch = merged_batch.as_ref().unwrap_or(record_batch);

        match self.arrow_writer.write(record_batch) {
            Ok(_) => {
                self.buffered_record_batch_count += 1;
                Ok(self.arrow_schema.clone())
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
    use super::*;
    use crate::operations::create::CreateBuilder;
    use crate::writer::test_utils::*;
    use arrow::json::ReaderBuilder;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
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
        use crate::DeltaOps;

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

        let table = DeltaOps(table)
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

        let schema: ArrowSchema = (&delta_schema).try_into().unwrap();

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
        validate_partition_map(partitions, expected_keys)
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
        use crate::operations::create::CreateBuilder;
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
    mod schema_evolution {
        use itertools::Itertools;

        use super::*;

        #[tokio::test]
        async fn test_write_mismatched_schema() {
            let batch = get_record_batch(None, false);
            let partition_cols = vec![];
            let table = create_initialized_table(&partition_cols).await;
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
            assert_eq!(table.version(), 0);

            let batch = get_record_batch(None, false);
            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            writer.write(batch).await.unwrap();
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 1);
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), 1);

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
                "Failed to write with WriteMode::MergeSchema, {:?}",
                result
            );
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 2);
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), 2);

            let new_schema = table.metadata().unwrap().schema().unwrap();
            let expected_columns = vec!["id", "value", "modified", "vid", "name"];
            let found_columns: Vec<&String> = new_schema.fields().map(|f| f.name()).collect();
            assert_eq!(
                expected_columns, found_columns,
                "The new table schema does not contain all evolved columns as expected"
            );
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
            assert_eq!(table.version(), 0);

            let batch = get_record_batch(None, false);
            let mut writer = RecordBatchWriter::for_table(&table).unwrap();

            writer.write(batch).await.unwrap();
            let version = writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(version, 1);
            table.load().await.expect("Failed to load table");
            assert_eq!(table.version(), 1);

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
            let batch = get_record_batch(None, false);
            let partition_cols = vec![];
            let mut table = create_initialized_table(&partition_cols).await;

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
                "Did not expect to successfully add new writes with different column types: {:?}",
                result
            );
        }

        #[tokio::test]
        async fn test_schema_evolution_with_nonnullable_col() {
            use crate::kernel::{
                DataType as DeltaDataType, PrimitiveType, StructField, StructType,
            };

            let table_schema = StructType::new(vec![
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
            ]);
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
            assert_eq!(table.version(), 0);

            // Hand-crafting the first RecordBatch to ensure that a write with non-nullable columns
            // works properly before attepting the second write
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
                "Should not have been able to write with a missing non-nullable column: {:?}",
                result
            );
        }
    }
}
