//! Main writer API to write record batches to delta table
use super::partition_writer::{stringified_partition_value, PartitionWriter};
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
pub struct DeltaWriter {
    storage: Box<dyn StorageBackend>,
    arrow_schema_ref: Arc<arrow::datatypes::Schema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, PartitionWriter>,
}

impl DeltaWriter {
    /// Creates a DeltaWriter to write to the given table
    pub fn for_table(
        table: &DeltaTable,
        options: HashMap<String, String>,
    ) -> Result<DeltaWriter, DeltaWriterError> {
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

    /// Divides a single record batch into into multiple according to table partitioning.
    /// Values are written to arrow buffers, to collect data until it should be written to disk.
    pub async fn write_record_batch(
        &mut self,
        values: &RecordBatch,
    ) -> Result<(), DeltaWriterError> {
        let mut partial_writes: Vec<(RecordBatch, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, batch) in self.divide_record_batch_by_partition_values(values)? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => DeltaWriter::collect_partial_write_failure(
                    &mut partial_writes,
                    writer
                        .write_record_batch(&self.partition_columns, &batch)
                        .await,
                )?,
                None => {
                    let mut writer = PartitionWriter::new(
                        arrow_schema.clone(),
                        self.writer_properties.clone(),
                    )?;

                    DeltaWriter::collect_partial_write_failure(
                        &mut partial_writes,
                        writer
                            .write_record_batch(&self.partition_columns, &batch)
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
    pub async fn write(&mut self, values: Vec<RecordBatch>) -> Result<(), DeltaWriterError> {
        let mut partial_writes: Vec<(RecordBatch, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, partition_values) in self.divide_by_partition_values(values.as_slice())? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => {
                    for batch in partition_values {
                        DeltaWriter::collect_partial_write_failure(
                            &mut partial_writes,
                            writer
                                .write_record_batch(&self.partition_columns, &batch)
                                .await,
                        )?
                    }
                }
                None => {
                    let mut writer = PartitionWriter::new(
                        arrow_schema.clone(),
                        self.writer_properties.clone(),
                    )?;
                    for batch in partition_values {
                        DeltaWriter::collect_partial_write_failure(
                            &mut partial_writes,
                            writer
                                .write_record_batch(&self.partition_columns, &batch)
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
                return Err(DeltaWriterError::PartialParquetWrite {
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
    ) -> Result<DeltaDataTypeVersion, DeltaWriterError> {
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
    pub async fn write_and_commit(
        &mut self,
        table: &mut DeltaTable,
        values: Vec<RecordBatch>,
        operation: Option<DeltaOperation>,
        removals: Option<Vec<Remove>>,
    ) -> Result<DeltaDataTypeVersion, DeltaWriterError> {
        self.write(values).await?;
        self.commit(table, operation, removals).await
    }

    fn divide_by_partition_values(
        &mut self,
        values: &[RecordBatch],
    ) -> Result<HashMap<String, Vec<RecordBatch>>, DeltaWriterError> {
        let mut partitions = HashMap::new();

        if self.partition_columns.is_empty() {
            partitions.insert(Value::Null.to_string(), values.to_vec());
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
    ) -> Result<HashMap<String, RecordBatch>, DeltaWriterError> {
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

        let partition_ranges =
            lexicographical_partition_ranges(sorted_partition_columns.as_slice())?;

        for range in partition_ranges {
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
                .map(|tuple| {
                    format!(
                        "{}={}",
                        tuple.1,
                        tuple
                            .0
                            .unwrap_or(NULL_PARTITION_VALUE_DATA_PATH.to_string())
                    )
                })
                .collect::<Vec<_>>()
                .join("/");

            // TODO We should probably only add the `batch_schema` columns here,
            // but somehow the stats test files then, investigating ...
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
        writer_result: Result<(), DeltaWriterError>,
    ) -> Result<(), DeltaWriterError> {
        match writer_result {
            Err(DeltaWriterError::PartialParquetWrite { .. }) => {
                // TODO handle templated type in Error definition
                // partial_writes.extend(skipped_values);
                Ok(())
            }
            other => other,
        }
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    pub async fn write_parquet_files(
        &mut self,
        table_uri: &str,
    ) -> Result<Vec<Add>, DeltaWriterError> {
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

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that
    // I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        partition_cols: &[String],
        partition_values: &HashMap<String, Option<String>>,
    ) -> Result<String, DeltaWriterError> {
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
                    .ok_or_else(|| DeltaWriterError::MissingPartitionColumn(k.to_string()))?;

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
    pub fn arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.arrow_schema_ref.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test_utils::{create_initialized_table, get_record_batch};
    use std::collections::HashMap;
    use std::path::Path;

    #[tokio::test]
    async fn test_divide_record_batch_no_partition() {
        let batch = get_record_batch(None);
        let table = create_initialized_table(vec![]).await;
        let mut writer = DeltaWriter::for_table(&table, HashMap::new()).unwrap();

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
        let mut writer = DeltaWriter::for_table(&table, HashMap::new()).unwrap();

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
        let mut writer = DeltaWriter::for_table(&table, HashMap::new()).unwrap();

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

    #[tokio::test]
    async fn test_write_no_partitions() {
        let batch = get_record_batch(None);
        let mut table = create_initialized_table(vec![]).await;
        let mut writer = DeltaWriter::for_table(&table, HashMap::new()).unwrap();

        writer.write(vec![batch]).await.unwrap();
        writer.commit(&mut table, None, None).await.unwrap();

        let files = table.get_file_uris();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_write_and_commit_no_partitions() {
        let batch = get_record_batch(None);
        let mut table = create_initialized_table(vec![]).await;
        let mut writer = DeltaWriter::for_table(&table, HashMap::new()).unwrap();

        writer
            .write_and_commit(&mut table, vec![batch], None, None)
            .await
            .unwrap();

        let files = table.get_file_uris();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_write_multiple_partitions() {
        let batch = get_record_batch(None);
        let mut table =
            create_initialized_table(vec!["modified".to_string(), "id".to_string()]).await;
        let mut writer = DeltaWriter::for_table(&table, HashMap::new()).unwrap();

        writer.write(vec![batch]).await.unwrap();
        writer.commit(&mut table, None, None).await.unwrap();

        let files = table.get_file_uris();
        assert_eq!(files.len(), 4);

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
        partitions: HashMap<String, RecordBatch>,
        expected_keys: Vec<String>,
    ) {
        assert_eq!(partitions.len(), expected_keys.len());
        for (key, values) in partitions {
            assert!(expected_keys.contains(&key));
            let ref_batch = get_record_batch(Some(key.clone()));
            assert_eq!(ref_batch, values);
        }
    }
}
