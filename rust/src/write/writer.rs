//! Main writer API to write record batches to delta table
use super::arrow_buffer::{stringified_partition_value, DataArrowWriter};
use super::{stats::create_add, *};
use crate::{
    action::{Action, Add, DeltaOperation, Remove},
    get_backend_for_uri_with_options, DeltaDataTypeVersion, DeltaTable, DeltaTableMetaData, Schema,
    StorageBackend,
};
use arrow::{
    array::UInt32Array,
    compute::{lexicographical_partition_ranges, lexsort_to_indices, take, SortColumn},
    datatypes::Schema as ArrowSchema,
};
use parquet::{basic::Compression, errors::ParquetError, file::properties::WriterProperties};
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

/// Writes messages to a delta lake table.
pub struct DataWriter {
    storage: Box<dyn StorageBackend>,
    arrow_schema_ref: Arc<arrow::datatypes::Schema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, DataArrowWriter>,
}

impl DataWriter {
    /// Creates a DataWriter to write to the given table
    pub fn for_table(
        table: &DeltaTable,
        options: HashMap<String, String>,
    ) -> Result<DataWriter, DataWriterError> {
        let storage = get_backend_for_uri_with_options(&table.table_uri, options)?;

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
            storage,
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
    ) -> Result<bool, DataWriterError> {
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

    /// Writes the given values to internal parquet buffers for each represented partition.
    pub async fn write_record_batch(
        &mut self,
        values: &RecordBatch,
    ) -> Result<(), DataWriterError> {
        let mut partial_writes: Vec<(RecordBatch, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, batch) in self.divide_record_batch_by_partition_values(values)? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => DataWriter::collect_partial_write_failure(
                    &mut partial_writes,
                    writer
                        .write_values(&self.partition_columns, arrow_schema.clone(), &batch)
                        .await,
                )?,
                None => {
                    let mut writer =
                        DataArrowWriter::new(arrow_schema.clone(), self.writer_properties.clone())?;

                    DataWriter::collect_partial_write_failure(
                        &mut partial_writes,
                        writer
                            .write_values(&self.partition_columns, self.arrow_schema(), &batch)
                            .await,
                    )?;

                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        if !partial_writes.is_empty() {
            let sample = partial_writes.first().map(|t| t.to_owned());
            if let Some((_, e)) = sample {
                return Err(DataWriterError::PartialParquetWrite {
                    // TODO handle error with generic messages
                    skipped_values: vec![],
                    sample_error: e,
                });
            } else {
                unreachable!()
            }
        }

        Ok(())
    }

    /// Writes the given values to internal parquet buffers for each represented partition.
    pub async fn write(&mut self, values: Vec<RecordBatch>) -> Result<(), DataWriterError> {
        let mut partial_writes: Vec<(RecordBatch, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, partition_values) in self.divide_by_partition_values(values)? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => {
                    for batch in partition_values {
                        DataWriter::collect_partial_write_failure(
                            &mut partial_writes,
                            writer
                                .write_values(&self.partition_columns, arrow_schema.clone(), &batch)
                                .await,
                        )?
                    }
                }
                None => {
                    let mut writer =
                        DataArrowWriter::new(arrow_schema.clone(), self.writer_properties.clone())?;
                    for batch in partition_values {
                        DataWriter::collect_partial_write_failure(
                            &mut partial_writes,
                            writer
                                .write_values(&self.partition_columns, self.arrow_schema(), &batch)
                                .await,
                        )?;
                    }
                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        if !partial_writes.is_empty() {
            let sample = partial_writes.first().map(|t| t.to_owned());
            if let Some((_, e)) = sample {
                return Err(DataWriterError::PartialParquetWrite {
                    // TODO handle error with generic messages
                    skipped_values: vec![],
                    sample_error: e,
                });
            } else {
                unreachable!()
            }
        }

        Ok(())
    }

    /// Commit data written to buffers to delta table
    pub async fn commit(
        &mut self,
        table: &mut DeltaTable,
        operation: Option<DeltaOperation>,
        removals: Option<Vec<Remove>>,
    ) -> Result<DeltaDataTypeVersion, DataWriterError> {
        let mut adds = self.write_parquet_files(&table.table_uri).await?;
        let mut tx = table.create_transaction(None);
        tx.add_actions(adds.drain(..).map(Action::add).collect());
        if let Some(mut remove_actions) = removals {
            tx.add_actions(remove_actions.drain(..).map(Action::remove).collect());
        }
        let version = tx.commit(operation).await?;
        Ok(version)
    }

    /// Write and immediately commit data
    // TODO trying to pass the table and record batch stream leads to compiler errors.
    // There is a fix by simply wrapping the stream type, but it seems not worth it given the limited
    // gain in convenience...
    // https://github.com/rust-lang/rust/issues/63033#issuecomment-521234696
    // pub async fn write_and_commit(
    //     &mut self,
    //     table: &mut DeltaTable,
    //     values: SendableRecordBatchStream,
    //     // operation: Option<DeltaOperation>,
    // ) -> Result<DeltaDataTypeVersion, DataWriterError> {
    //     self.write(values).await?;
    //     // TODO find a way to handle operation, maybe set on writer?
    //     self.commit(table, None, None).await
    // }

    fn divide_by_partition_values(
        &mut self,
        values: Vec<RecordBatch>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, DataWriterError> {
        let mut partitions = HashMap::new();

        if self.partition_columns.is_empty() {
            partitions.insert(Value::Null.to_string(), values);
            return Ok(partitions);
        }

        for batch in values {
            let parts = self.divide_record_batch_by_partition_values(&batch)?;
            for (key, part_values) in parts {
                match partitions.get_mut(&key) {
                    Some(vec) => vec.push(part_values),
                    None => {
                        partitions.insert(key, vec![part_values]);
                    }
                }
            }
        }

        Ok(partitions)
    }

    fn divide_record_batch_by_partition_values(
        &mut self,
        values: &RecordBatch,
    ) -> Result<HashMap<String, RecordBatch>, DataWriterError> {
        let mut partitions = HashMap::new();

        if self.partition_columns.is_empty() {
            partitions.insert(Value::Null.to_string(), values.clone());
            return Ok(partitions);
        }

        let schema = values.schema();
        // let batch_schema = Arc::new(ArrowSchema::new(
        //     schema
        //         .fields()
        //         .iter()
        //         .filter(|f| !self.partition_columns.contains(f.name()))
        //         .map(|f| f.to_owned())
        //         .collect::<Vec<_>>(),
        // ));

        // collect all columns in order relevant for partitioning
        let sort_columns = self
            .partition_columns
            .clone()
            .into_iter()
            .map(|col| SortColumn {
                values: values.column(schema.index_of(&col).unwrap()).clone(),
                options: None,
            })
            .collect::<Vec<_>>();

        let indices = lexsort_to_indices(sort_columns.as_slice(), None).unwrap();
        let sorted_partition_columns = sort_columns
            .iter()
            .map(|c| SortColumn {
                values: take(c.values.as_ref(), &indices, None).unwrap(),
                options: None,
            })
            .collect::<Vec<_>>();

        let ranges = lexicographical_partition_ranges(sorted_partition_columns.as_slice())?;

        for range in ranges {
            // get row indices for current partition
            let idx: UInt32Array = (range.start..range.end)
                .map(|i| Some(indices.value(i)))
                .into_iter()
                .collect();

            let partition_key = sorted_partition_columns
                .iter()
                .map(|c| {
                    stringified_partition_value(
                        &c.values.slice(range.start, range.end - range.start),
                    )
                    .unwrap()
                })
                .zip(self.partition_columns.clone())
                .map(|tuple| format!("{}={}", tuple.1, tuple.0.unwrap_or(Value::Null.to_string())))
                .collect::<Vec<_>>()
                .join("/");

            let batch_data = schema
                .fields()
                .iter()
                .map(|f| values.column(schema.index_of(&f.name()).unwrap()).clone())
                .map(move |col| take(col.as_ref(), &idx, None).unwrap())
                .collect::<Vec<_>>();

            partitions.insert(
                partition_key,
                RecordBatch::try_new(schema.clone(), batch_data).unwrap(),
            );
        }

        Ok(partitions)
    }

    // TODO provide meaningful implementation
    fn collect_partial_write_failure(
        _partial_writes: &mut Vec<(RecordBatch, ParquetError)>,
        writer_result: Result<(), DataWriterError>,
    ) -> Result<(), DataWriterError> {
        match writer_result {
            Err(DataWriterError::PartialParquetWrite { .. }) => {
                // TODO handle templated type in Error definition
                // partial_writes.extend(skipped_values);
                Ok(())
            }
            other => other,
        }
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.arrow_writers.values().map(|w| w.cursor.len()).sum()
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    pub async fn write_parquet_files(
        &mut self,
        table_uri: &str,
    ) -> Result<Vec<Add>, DataWriterError> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, mut writer) in writers {
            let metadata = writer.arrow_writer.close()?;

            let path = self.next_data_path(&self.partition_columns, &writer.partition_values)?;

            let obj_bytes = writer.cursor.data();
            let file_size = obj_bytes.len() as i64;

            let storage_path = self.storage.join_path(table_uri, path.as_str());

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

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that
    // I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        partition_cols: &[String],
        partition_values: &HashMap<String, Option<String>>,
    ) -> Result<String, DataWriterError> {
        // TODO: what does 00000 mean?
        let first_part = "00000";
        let uuid_part = Uuid::new_v4();
        // TODO: what does c000 mean?
        let last_part = "c000";

        // NOTE: If we add a non-snappy option, file name must change
        let file_name = format!(
            "part-{}-{}-{}.snappy.parquet",
            first_part, uuid_part, last_part
        );

        let data_path = if !partition_cols.is_empty() {
            let mut path_parts = vec![];

            for k in partition_cols.iter() {
                let partition_value = partition_values
                    .get(k)
                    .ok_or_else(|| DataWriterError::MissingPartitionColumn(k.to_string()))?;

                let partition_value = partition_value
                    .as_deref()
                    .unwrap_or(NULL_PARTITION_VALUE_DATA_PATH);
                let part = format!("{}={}", k, partition_value);

                path_parts.push(part);
            }
            path_parts.push(file_name);
            path_parts.join("/")
        } else {
            file_name
        };

        Ok(data_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{action::Protocol, DeltaTableConfig, SchemaDataType, SchemaField};
    use arrow::array::{Int32Array, StringArray, UInt32Array};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn convert_schema_arrow_to_delta() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
        ]);
        let ref_schema = get_delta_schema();
        let schema = Schema::try_from(Arc::new(arrow_schema)).unwrap();
        assert_eq!(schema, ref_schema);
    }

    #[tokio::test]
    async fn test_divide_record_batch_no_partition() {
        let batch = get_record_batch(None);
        let table = create_initialized_table(vec![]).await;
        let mut writer = DataWriter::for_table(&table, HashMap::new()).unwrap();

        let partitions = writer
            .divide_record_batch_by_partition_values(&batch)
            .unwrap();

        let expected_keys = vec![Value::Null.to_string()];
        validate_partition_map(partitions, expected_keys)
    }

    #[tokio::test]
    async fn test_divide_record_batch_single_partition() {
        let batch = get_record_batch(None);
        let table = create_initialized_table(vec![String::from("modified")]).await;
        let mut writer = DataWriter::for_table(&table, HashMap::new()).unwrap();

        let partitions = writer
            .divide_record_batch_by_partition_values(&batch)
            .unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];
        validate_partition_map(partitions, expected_keys)
    }

    #[tokio::test]
    async fn test_divide_record_batch_multiple_partitions() {
        let batch = get_record_batch(None);
        let table = create_initialized_table(vec!["modified".to_string(), "id".to_string()]).await;
        let mut writer = DataWriter::for_table(&table, HashMap::new()).unwrap();

        let partitions = writer
            .divide_record_batch_by_partition_values(&batch)
            .unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01/id=A"),
            String::from("modified=2021-02-01/id=B"),
            String::from("modified=2021-02-02/id=A"),
            String::from("modified=2021-02-02/id=B"),
        ];
        validate_partition_map(partitions, expected_keys)
    }

    fn validate_partition_map(
        partitions: HashMap<String, RecordBatch>,
        expected_keys: Vec<String>,
    ) {
        assert_eq!(partitions.len(), expected_keys.len());
        for (key, values) in partitions {
            assert!(expected_keys.contains(&key));
            let ref_batch = get_record_batch(Some(key));
            assert_eq!(ref_batch, Arc::new(values));
        }
    }

    fn get_record_batch(part: Option<String>) -> Arc<RecordBatch> {
        let base_int = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        let base_str =
            StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);
        let base_mod = StringArray::from(vec![
            "2021-02-02",
            "2021-02-02",
            "2021-02-02",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
        ]);
        let indices = match &part {
            Some(key) if key == "modified=2021-02-01" => {
                UInt32Array::from(vec![3, 4, 5, 6, 7, 8, 9, 10])
            }
            Some(key) if key == "modified=2021-02-01/id=A" => {
                UInt32Array::from(vec![4, 5, 6, 9, 10])
            }
            Some(key) if key == "modified=2021-02-01/id=B" => UInt32Array::from(vec![3, 7, 8]),
            Some(key) if key == "modified=2021-02-02" => UInt32Array::from(vec![0, 1, 2]),
            Some(key) if key == "modified=2021-02-02/id=A" => UInt32Array::from(vec![0, 2]),
            Some(key) if key == "modified=2021-02-02/id=B" => UInt32Array::from(vec![1]),
            _ => UInt32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        };

        let int_values = take(&base_int, &indices, None).unwrap();
        let str_values = take(&base_str, &indices, None).unwrap();
        let mod_values = take(&base_mod, &indices, None).unwrap();

        // expected results from parsing json payload
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
        ]));
        Arc::new(RecordBatch::try_new(schema, vec![str_values, int_values, mod_values]).unwrap())
    }

    fn get_delta_schema() -> Schema {
        Schema::new(vec![
            SchemaField::new(
                "id".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "value".to_string(),
                SchemaDataType::primitive("integer".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "modified".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
        ])
    }

    fn create_bare_table() -> DeltaTable {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let backend = Box::new(crate::storage::file::FileStorageBackend::new(
            table_path.to_str().unwrap(),
        ));
        DeltaTable::new(
            table_path.to_str().unwrap(),
            backend,
            DeltaTableConfig::default(),
        )
        .unwrap()
    }

    async fn create_initialized_table(partition_cols: Vec<String>) -> DeltaTable {
        let mut table = create_bare_table();
        let table_schema = get_delta_schema();

        let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String("CREATE TABLE".to_string()),
        );
        commit_info.insert(
            "userName".to_string(),
            serde_json::Value::String("test user".to_string()),
        );

        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let metadata = DeltaTableMetaData::new(
            None,
            None,
            None,
            table_schema,
            partition_cols,
            HashMap::new(),
        );

        table
            .create(metadata, protocol, Some(commit_info))
            .await
            .unwrap();

        table
    }
}
