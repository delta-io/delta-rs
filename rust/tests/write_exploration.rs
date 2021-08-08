extern crate chrono;
extern crate deltalake;
extern crate utime;

use std::convert::TryFrom;

use arrow::{
    array::{as_primitive_array, Array},
    datatypes::Schema as ArrowSchema,
    // TODO: use for computing column stats
    // compute::kernels::aggregate,
    datatypes::*,
    error::ArrowError,
    json::reader::Decoder,
    record_batch::RecordBatch,
};
use deltalake::{
    action::{Action, Add, Remove, Stats},
    DeltaTableError, DeltaTableMetaData, Schema, StorageError, UriError,
};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    errors::ParquetError,
    file::{properties::WriterProperties, writer::InMemoryWriteableCursor},
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum DeltaWriterError {
    #[error("Partition column contains more than one value")]
    NonDistinctPartitionValue,

    #[error("Missing partition column: {col_name}")]
    MissingPartitionColumn { col_name: String },

    #[error("Invalid table path: {}", .source)]
    UriError {
        #[from]
        source: UriError,
    },

    #[error("Storage interaction failed: {source}")]
    Storage {
        #[from]
        source: StorageError,
    },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("Arrow interaction failed: {source}")]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error("Parquet write failed: {source}")]
    Parquet {
        #[from]
        source: ParquetError,
    },
}

pub struct DeltaWriter {
    table_path: String,
}

/// A writer that writes record batches in parquet format to a table location.
/// This should be used along side a DeltaTransaction wrapping the same DeltaTable instance.
impl DeltaWriter {
    pub async fn for_table_path(table_path: String) -> Result<DeltaWriter, DeltaWriterError> {
        Ok(Self { table_path })
    }

    // Ideally, we should separate the initialization of the cursor and the call to close to enable writing multiple record batches to the same file.
    // Keeping it simple for now and writing a single record batch to each file.
    pub async fn write_record_batch(
        &self,
        metadata: &DeltaTableMetaData,
        record_batch: &RecordBatch,
    ) -> Result<Add, DeltaWriterError> {
        let partition_values = extract_partition_values(metadata, record_batch)?;

        // TODO: lookup column stats
        // let column_stats = HashMap::new();

        let cursor = self
            .write_to_parquet_buffer(metadata, &record_batch)
            .await?;

        let path = self.next_data_path(metadata, &partition_values).unwrap();

        // TODO: handle error
        let obj_bytes = cursor.into_inner().unwrap();

        let storage_path = format!("{}/{}", self.table_path, path);

        // `storage.put_obj` is for log files
        if let Some(p) = PathBuf::from(&storage_path).parent() {
            fs::create_dir_all(p).unwrap();

            let mut f = File::create(&storage_path).unwrap();
            f.write_all(&obj_bytes).unwrap();
        }

        create_add(
            &partition_values,
            path,
            obj_bytes.len() as i64,
            &record_batch,
        )
    }

    // Ideally, we should separate the initialization of the cursor and the call to close to enable writing multiple record batches to the same file.
    // Keeping it simple for now and writing a single record batch to each file.
    async fn write_to_parquet_buffer(
        &self,
        metadata: &DeltaTableMetaData,
        batch: &RecordBatch,
    ) -> Result<InMemoryWriteableCursor, DeltaWriterError> {
        let schema = &metadata.schema;
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(schema).unwrap();
        let arrow_schema_ref = Arc::new(arrow_schema);

        let writer_properties = WriterProperties::builder()
            // TODO: Extract config/env for writer properties and set more than just compression
            .set_compression(Compression::SNAPPY)
            .build();
        let cursor = InMemoryWriteableCursor::default();
        let mut writer = ArrowWriter::try_new(
            cursor.clone(),
            arrow_schema_ref.clone(),
            Some(writer_properties),
        )
        .unwrap();

        writer.write(batch)?;
        writer.close()?;

        Ok(cursor)
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        metadata: &DeltaTableMetaData,
        partition_values: &HashMap<String, Option<String>>,
    ) -> Result<String, DeltaWriterError> {
        // TODO: what does 00000 mean?
        let first_part = "00000";
        let uuid_part = Uuid::new_v4();
        // TODO: what does c000 mean?
        let last_part = "c000";

        let file_name = format!("part-{}-{}-{}.parquet", first_part, uuid_part, last_part);

        let partition_cols = metadata.partition_columns.as_slice();

        let data_path = if partition_cols.len() > 0 {
            let mut path_part = String::with_capacity(20);

            // ugly string builder hack
            let mut first = true;

            for k in partition_cols.iter() {
                let partition_value = partition_values
                    .get(k)
                    .ok_or(DeltaWriterError::MissingPartitionColumn {
                        col_name: k.to_string(),
                    })?
                    .clone()
                    .unwrap_or("__HIVE_DEFAULT_PARTITION__".to_string());

                if first {
                    first = false;
                } else {
                    path_part.push_str("/");
                }

                path_part.push_str(k);
                path_part.push_str("=");
                path_part.push_str(&partition_value);
            }

            format!("{}/{}", path_part, file_name)
        } else {
            file_name
        };

        Ok(data_path)
    }
}

pub struct InMemValueIter<'a> {
    buffer: &'a [Value],
    current_index: usize,
}

impl<'a> InMemValueIter<'a> {
    fn from_vec(v: &'a [Value]) -> Self {
        Self {
            buffer: v,
            current_index: 0,
        }
    }
}

impl<'a> Iterator for InMemValueIter<'a> {
    type Item = Result<Value, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.buffer.get(self.current_index);

        self.current_index += 1;

        item.map(|v| Ok(v.to_owned()))
    }
}

pub fn record_batch_from_json_buffer(
    arrow_schema_ref: Arc<ArrowSchema>,
    json_buffer: &[Value],
) -> Result<RecordBatch, DeltaWriterError> {
    let row_count = json_buffer.len();
    let mut value_ter = InMemValueIter::from_vec(json_buffer);
    let decoder = Decoder::new(arrow_schema_ref.clone(), row_count, None);
    let batch = decoder.next_batch(&mut value_ter)?;

    // handle none
    let batch = batch.unwrap();

    Ok(batch)
}

pub fn extract_partition_values(
    metadata: &DeltaTableMetaData,
    record_batch: &RecordBatch,
) -> Result<HashMap<String, Option<String>>, DeltaWriterError> {
    let partition_cols = metadata.partition_columns.as_slice();

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

pub fn create_add(
    partition_values: &HashMap<String, Option<String>>,
    path: String,
    size: i64,
    record_batch: &RecordBatch,
) -> Result<Add, DeltaWriterError> {
    let stats = Stats {
        num_records: record_batch.num_rows() as i64,
        // TODO: calculate additional stats
        // look at https://github.com/apache/arrow/blob/master/rust/arrow/src/compute/kernels/aggregate.rs for pulling these stats
        min_values: HashMap::new(),
        max_values: HashMap::new(),
        null_count: HashMap::new(),
    };
    let stats_string = serde_json::to_string(&stats).unwrap();

    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    let add = Add {
        path,
        size,

        partition_values: partition_values.to_owned(),
        partition_values_parsed: None,

        modification_time: modification_time,
        data_change: true,

        // TODO: calculate additional stats
        stats: Some(stats_string),
        stats_parsed: None,
        // ?
        tags: None,
    };

    Ok(add)
}

pub fn create_remove(path: String) -> Remove {
    let deletion_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let deletion_timestamp = deletion_timestamp.as_millis() as i64;

    Remove {
        path,
        deletion_timestamp: deletion_timestamp,
        data_change: true,
        extended_file_metadata: Some(false),
        ..Default::default()
    }
}

// very naive implementation for plucking the partition value from the first element of a column array.
// ideally, we would do some validation to ensure the record batch containing the passed partition column contains only distinct values.
// if we calculate stats _first_, we can avoid the extra iteration by ensuring max and min match for the column.
// however, stats are optional and can be added later with `dataChange` false log entries, and it may be more appropriate to add stats _later_ to speed up the initial write.
// a happy middle-road might be to compute stats for partition columns only on the initial write since we should validate partition values anyway, and compute additional stats later (at checkpoint time perhaps?).
// also this does not currently support nested partition columns and many other data types.
fn stringified_partition_value(arr: &Arc<dyn Array>) -> Result<Option<String>, DeltaWriterError> {
    let data_type = arr.data_type();

    let s = match data_type {
        DataType::Int8 => as_primitive_array::<Int8Type>(arr).value(0).to_string(),
        DataType::Int16 => as_primitive_array::<Int16Type>(arr).value(0).to_string(),
        DataType::Int32 => as_primitive_array::<Int32Type>(arr).value(0).to_string(),
        DataType::Int64 => as_primitive_array::<Int64Type>(arr).value(0).to_string(),
        DataType::UInt8 => as_primitive_array::<UInt8Type>(arr).value(0).to_string(),
        DataType::UInt16 => as_primitive_array::<UInt16Type>(arr).value(0).to_string(),
        DataType::UInt32 => as_primitive_array::<UInt32Type>(arr).value(0).to_string(),
        DataType::UInt64 => as_primitive_array::<UInt64Type>(arr).value(0).to_string(),
        DataType::Utf8 => {
            let data = arrow::array::as_string_array(arr);

            data.value(0).to_string()
        }
        // TODO: handle more types
        _ => {
            unimplemented!("Unimplemented data type: {:?}", data_type);
        }
    };

    // according to delta spec: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    // empty string should convert to null
    let partition_value = if s.is_empty() { None } else { Some(s) };

    Ok(partition_value)
}

#[tokio::test]
async fn smoke_test() {
    cleanup_log_dir();

    // NOTE: Test table is partitioned by `modified`

    // initialize table and writer
    let mut delta_table = deltalake::open_table("./tests/data/write_exploration")
        .await
        .unwrap();
    let delta_writer = DeltaWriter::for_table_path(delta_table.table_uri.clone())
        .await
        .unwrap();

    //
    // ---
    //

    //
    // tx 1 - insert some data
    //

    // start a transaction
    let metadata = delta_table.get_metadata().unwrap().clone();
    let mut transaction = delta_table.create_transaction(None);

    // test data set #1 - inserts
    let json_rows = vec![
        json!({ "id": "A", "value": 42, "modified": "2021-02-01" }),
        json!({ "id": "B", "value": 44, "modified": "2021-02-01" }),
        json!({ "id": "C", "value": 46, "modified": "2021-02-01" }),
        json!({ "id": "D", "value": 48, "modified": "2021-02-01" }),
        json!({ "id": "E", "value": 50, "modified": "2021-02-01" }),
        json!({ "id": "F", "value": 52, "modified": "2021-02-01" }),
        json!({ "id": "G", "value": 54, "modified": "2021-02-01" }),
        json!({ "id": "H", "value": 56, "modified": "2021-02-01" }),
    ];

    let arrow_schema_ref =
        Arc::new(<ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema).unwrap());
    let record_batch =
        record_batch_from_json_buffer(arrow_schema_ref, json_rows.as_slice()).unwrap();

    // write data and collect add
    let add = delta_writer
        .write_record_batch(&metadata, &record_batch)
        .await
        .unwrap();

    // HACK: cloning the add path to remove later in test. Ultimately, an "UpdateCommmand" will need to handle this differently
    let remove_path = add.path.clone();

    transaction.add_action(Action::add(add));
    // commit the transaction
    transaction.commit(None).await.unwrap();

    //
    // ---
    //

    //
    // tx 2 - an update this time
    // NOTE: this is not a _real_ update since we don't rewrite the previous file, but it tests the transaction logic well enough for now.
    //

    // start a transaction
    let metadata = delta_table.get_metadata().unwrap().clone();
    let mut transaction = delta_table.create_transaction(None);

    // test data set #2 - updates
    let json_rows = vec![
        json!({ "id": "D", "value": 148, "modified": "2021-02-02" }),
        json!({ "id": "E", "value": 150, "modified": "2021-02-02" }),
        json!({ "id": "F", "value": 152, "modified": "2021-02-02" }),
    ];

    let arrow_schema_ref =
        Arc::new(<ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema).unwrap());
    let record_batch =
        record_batch_from_json_buffer(arrow_schema_ref, json_rows.as_slice()).unwrap();

    // TODO: resolve diffs by rewriting previous add and also creating a remove of the previous add
    // See "UpdateCommand.scala" in reference implementation
    // 1. add a `filter_files` fn to identify files matching our predicate - `WHERE id in ('D', 'E', 'F')`
    // 2. rewrite the new add buffer
    // 3. write the new add
    // 4. determine the remove files
    // 5. commit the new add and remove in the same tx

    // For now, leaving this super broken with a remove of the previous file which we know the path of since it is in context, and an add of just the updated ids.
    // This is "super broken" since we aren't actually re-writing the add file to include the unchanged records so unchanged records will be removed by this tx.
    // Utlimately, we should have an "update" command to encapsulate the re-write logic.
    // Its still an interesting scenario for API exploration so we can test the tx log and have fodder to envision the api.

    // write data and collect add
    // TODO: update adds should re-write the original add contents changing only the modified records
    let add = delta_writer
        .write_record_batch(&metadata, &record_batch)
        .await
        .unwrap();

    // TODO: removes should be calculated based on files containing a match for the update key.
    let remove = create_remove(remove_path);

    // HACK: cloning the add path to remove later in test. Ultimately, an "UpdateCommmand" will need to handle this differently
    let remove_path = add.path.clone();

    transaction.add_actions(vec![Action::add(add), Action::remove(remove)]);
    // commit the transaction
    transaction.commit(None).await.unwrap();

    //
    // tx 3 - an update with null and empty string as partition values
    // NOTE: this is not a _real_ update since we don't rewrite the previous file, but it tests the transaction logic well enough for now.
    //

    // start a transaction
    let metadata = delta_table.get_metadata().unwrap().clone();
    let mut transaction = delta_table.create_transaction(None);

    // test data set #3 - updates
    let json_rows = vec![
        json!({ "id": "G", "value": 154, "modified": null }),
        json!({ "id": "H", "value": 156, "modified": "" }),
    ];

    let arrow_schema_ref =
        Arc::new(<ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema).unwrap());
    let record_batch =
        record_batch_from_json_buffer(arrow_schema_ref, json_rows.as_slice()).unwrap();

    // write data and collect add
    // TODO: update adds should re-write the original add contents changing only the modified records
    let add = delta_writer
        .write_record_batch(&metadata, &record_batch)
        .await
        .unwrap();

    // TODO: removes should be calculated based on files containing a match for the update key.
    let remove = create_remove(remove_path);

    transaction.add_actions(vec![Action::add(add), Action::remove(remove)]);
    // commit the transaction
    transaction.commit(None).await.unwrap();

    let files = delta_table.get_files();
    assert_eq!(files.len(), 1);

    let partition_values = extract_partition_values(&metadata, &record_batch).unwrap();
    let mut expected_partition_values = HashMap::new();
    expected_partition_values.insert(String::from("modified"), None);
    assert_eq!(partition_values, expected_partition_values);

    // A notable thing to mention:
    // This implementation treats the DeltaTable instance as a single snapshot of the current log.
    // I think this is fine as long as only a single writer is using a DeltaTable instance at a time.
    // We won't be able to check conflicts correctly if multiple threads try to use a single DeltaTable instance to write different transactions at the same time.
}

fn cleanup_log_dir() {
    let log_dir = PathBuf::from("./tests/data/write_exploration/_delta_log");
    let paths = fs::read_dir(log_dir.as_path()).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let path = d.path();

                if let Some(extension) = path.extension() {
                    if extension == "json" && path.file_stem().unwrap() != "00000000000000000000" {
                        fs::remove_file(path).unwrap();
                    }
                }
            }
            _ => {}
        }
    }

    let data_dir = PathBuf::from("./tests/data/write_exploration");
    let paths = fs::read_dir(data_dir.as_path()).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let path = d.path();
                if path.is_dir() && path.to_str().unwrap().contains("=") {
                    fs::remove_dir_all(path).unwrap();
                } else if let Some(extension) = path.extension() {
                    if extension == "parquet" {
                        fs::remove_file(path).unwrap();
                    }
                }
            }
            _ => {}
        }
    }
}
