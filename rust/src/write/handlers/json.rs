//! Handle JSON messages when writing to delta tables
use crate::write::DataWriterError;
use arrow::{datatypes::Schema as ArrowSchema, json::reader::Decoder, record_batch::*};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// partition json values
pub fn divide_by_partition_values(
    partition_columns: &[String],
    records: Vec<Value>,
) -> Result<HashMap<String, Vec<Value>>, DataWriterError> {
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
) -> Result<String, DataWriterError> {
    if let Some(obj) = value.as_object() {
        let key: Vec<String> = partition_columns
            .iter()
            .map(|c| obj.get(c).unwrap_or(&Value::Null).to_string())
            .collect();
        return Ok(key.join("/"));
    }

    Err(DataWriterError::InvalidRecord(value.to_string()))
}

/// Convert a vector of json values to a RecordBatch
pub fn record_batch_from_message(
    arrow_schema: Arc<ArrowSchema>,
    message_buffer: &[Value],
) -> Result<RecordBatch, DataWriterError> {
    let row_count = message_buffer.len();
    let mut value_iter = message_buffer.iter().map(|j| Ok(j.to_owned()));
    let decoder = Decoder::new(arrow_schema, row_count, None);
    decoder
        .next_batch(&mut value_iter)?
        .ok_or(DataWriterError::EmptyRecordBatch)
}
