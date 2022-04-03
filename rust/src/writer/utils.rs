//! Handle JSON messages when writing to delta tables
use crate::writer::DeltaWriterError;
use arrow::{
    array::{as_primitive_array, Array},
    datatypes::{
        DataType, Int16Type, Int32Type, Int64Type, Int8Type, Schema as ArrowSchema, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
    json::reader::Decoder,
    record_batch::*,
};
use parquet::file::writer::InMemoryWriteableCursor;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use uuid::Uuid;

const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

pub(crate) fn get_partition_key(
    partition_columns: &[String],
    partition_values: &HashMap<String, Option<String>>,
) -> Result<String, DeltaWriterError> {
    let mut path_parts = vec![];
    for k in partition_columns.iter() {
        let partition_value = partition_values
            .get(k)
            .ok_or_else(|| DeltaWriterError::MissingPartitionColumn(k.to_string()))?;

        let partition_value = partition_value
            .as_deref()
            .unwrap_or(NULL_PARTITION_VALUE_DATA_PATH);
        let part = format!("{}={}", k, partition_value);

        path_parts.push(part);
    }

    Ok(path_parts.join("/"))
}

// TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that
// I have not been able to find documentation for yet.
pub(crate) fn next_data_path(
    partition_columns: &[String],
    partition_values: &HashMap<String, Option<String>>,
    part: Option<i32>,
) -> Result<String, DeltaWriterError> {
    // TODO: what does 00000 mean?
    // TODO (roeap): my understanding is, that the values are used as a counter - i.e. if a single batch of
    // data written to one partition needs to be split due to desired file size constraints.
    let first_part = match part {
        Some(count) => format!("{:0>5}", count),
        _ => "00000".to_string(),
    };
    let uuid_part = Uuid::new_v4();
    // TODO: what does c000 mean?
    let last_part = "c000";

    // NOTE: If we add a non-snappy option, file name must change
    let file_name = format!(
        "part-{}-{}-{}.snappy.parquet",
        first_part, uuid_part, last_part
    );

    if partition_columns.is_empty() {
        return Ok(file_name);
    }

    let partition_key = get_partition_key(partition_columns, partition_values)?;
    Ok(format!("{}/{}", partition_key, file_name))
}

/// Create a new cursor from existing bytes
pub(crate) fn cursor_from_bytes(bytes: &[u8]) -> Result<InMemoryWriteableCursor, std::io::Error> {
    let mut cursor = InMemoryWriteableCursor::default();
    cursor.write_all(bytes)?;
    Ok(cursor)
}

/// partition json values
pub fn divide_by_partition_values(
    partition_columns: &[String],
    records: Vec<Value>,
) -> Result<HashMap<String, Vec<Value>>, DeltaWriterError> {
    let mut partitioned_records: HashMap<String, Vec<Value>> = HashMap::new();

    for record in records {
        let partition_value = json_to_partition_values(partition_columns, &record)?;
        match partitioned_records.get_mut(&partition_value) {
            Some(vec) => vec.push(record),
            None => {
                partitioned_records.insert(partition_value, vec![record]);
            }
        };
    }

    Ok(partitioned_records)
}

fn json_to_partition_values(
    partition_columns: &[String],
    value: &Value,
) -> Result<String, DeltaWriterError> {
    if let Some(obj) = value.as_object() {
        let key: Vec<String> = partition_columns
            .iter()
            .map(|c| obj.get(c).unwrap_or(&Value::Null).to_string())
            .collect();
        return Ok(key.join("/"));
    }

    Err(DeltaWriterError::InvalidRecord(value.to_string()))
}

/// Convert a vector of json values to a RecordBatch
pub fn record_batch_from_message(
    arrow_schema: Arc<ArrowSchema>,
    message_buffer: &[Value],
) -> Result<RecordBatch, DeltaWriterError> {
    let row_count = message_buffer.len();
    let mut value_iter = message_buffer.iter().map(|j| Ok(j.to_owned()));
    let decoder = Decoder::new(arrow_schema, row_count, None);
    decoder
        .next_batch(&mut value_iter)?
        .ok_or(DeltaWriterError::EmptyRecordBatch)
}

// very naive implementation for plucking the partition value from the first element of a column array.
// ideally, we would do some validation to ensure the record batch containing the passed partition column contains only distinct values.
// if we calculate stats _first_, we can avoid the extra iteration by ensuring max and min match for the column.
// however, stats are optional and can be added later with `dataChange` false log entries, and it may be more appropriate to add stats _later_ to speed up the initial write.
// a happy middle-road might be to compute stats for partition columns only on the initial write since we should validate partition values anyway, and compute additional stats later (at checkpoint time perhaps?).
// also this does not currently support nested partition columns and many other data types.
// TODO is this comment still valid, since we should be sure now, that the arrays where this
// gets aplied have a single unique value
pub(crate) fn stringified_partition_value(
    arr: &Arc<dyn Array>,
) -> Result<Option<String>, DeltaWriterError> {
    let data_type = arr.data_type();

    if arr.is_null(0) {
        return Ok(None);
    }

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

    Ok(Some(s))
}
