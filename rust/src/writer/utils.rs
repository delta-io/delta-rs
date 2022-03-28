//! Handle JSON messages when writing to delta tables
use crate::writer::DeltaWriterError;
use arrow::{datatypes::Schema as ArrowSchema, json::reader::Decoder, record_batch::*};
use serde_json::Value;
use std::collections::HashMap;
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
