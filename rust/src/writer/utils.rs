//! Handle JSON messages when writing to delta tables
use crate::writer::DeltaWriterError;
use arrow::{
    array::{as_primitive_array, Array, ArrayRef, StructArray},
    datatypes::{
        DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema as ArrowSchema,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    json::reader::{Decoder, DecoderOptions},
    record_batch::*,
};
use parquet::file::writer::InMemoryWriteableCursor;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
use std::sync::Arc;
use uuid::Uuid;

const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct PartitionPath {
    path: String,
}

impl PartitionPath {
    pub fn from_hashmap(
        partition_columns: &[String],
        partition_values: &HashMap<String, Option<String>>,
    ) -> Result<Self, DeltaWriterError> {
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

        Ok(PartitionPath {
            path: path_parts.join("/"),
        })
    }
}

impl From<PartitionPath> for String {
    fn from(path: PartitionPath) -> String {
        path.path
    }
}

impl AsRef<str> for PartitionPath {
    fn as_ref(&self) -> &str {
        &self.path
    }
}

impl Display for PartitionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.path.fmt(f)
    }
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

    let partition_key = PartitionPath::from_hashmap(partition_columns, partition_values)?;
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
    let mut value_iter = message_buffer.iter().map(|j| Ok(j.to_owned()));
    let options = DecoderOptions::new().with_batch_size(message_buffer.len());
    let decoder = Decoder::new(arrow_schema, options);
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

fn backfill_field(
    array: ArrayRef,
    target_field: &Field,
    num_rows: usize,
) -> Result<ArrayRef, DeltaWriterError> {
    use arrow::datatypes::DataType::*;
    let source_type = array.data_type();

    match target_field.data_type() {
        Boolean | UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32
        | Float64 | Utf8 => Ok(array),
        Struct(target_fields) => {
            if let DataType::Struct(source_fields) = source_type {
                let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                let mut actual_source_fields = 0;
                let mut new_fields = Vec::new();

                for target_field in target_fields {
                    let mut new_field = true;
                    for (i, source_field) in source_fields.iter().enumerate() {
                        if target_field.name() == source_field.name() {
                            let f = backfill_field(
                                struct_array.column(i).to_owned(),
                                target_field,
                                num_rows,
                            )?;
                            new_fields.push((target_field.clone(), f));
                            actual_source_fields += 1;
                            new_field = false;
                            break;
                        }
                    }

                    if new_field {
                        if !target_field.is_nullable() {
                            //TODO: New Error Type
                            return Err(DeltaWriterError::InvalidRecord(
                                "New column is not nullable".to_string(),
                            ));
                        }
                        new_fields.push((
                            target_field.clone(),
                            arrow::array::new_null_array(target_field.data_type(), num_rows),
                        ));
                    }
                }

                if actual_source_fields != source_fields.len() {
                    return Err(DeltaWriterError::InvalidRecord(
                        "Source has columns that do not exist in target".to_string(),
                    ));
                }
                Ok(Arc::new(StructArray::from(new_fields)))
            } else {
                Err(DeltaWriterError::InvalidRecord(
                    "Schema Mismatch".to_owned(),
                ))
            }
        }
        //TODO: New Error type
        data_type => Err(DeltaWriterError::InvalidRecord(format!(
            "Datatype {:?} not handled",
            data_type
        ))),
    }
}

/// Columns that are defined in the target schema but are missing from the batch are back filled
/// TODO: Check if schema evolution for structs is supported. Currently on handles the first level of columns.
#[allow(dead_code)]
pub(crate) fn backfill_record_batch(
    batch: &RecordBatch,
    target: arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, DeltaWriterError> {
    let mut columns = Vec::new();
    let mut columns_from_source = 0;
    let source = batch.schema();
    let num_rows = batch.num_rows();

    let source_owned = (*source).to_owned();
    let target_owned = (*batch.schema()).to_owned();
    ArrowSchema::try_merge([source_owned, target_owned])?;

    for target_field in target.fields() {
        let mut new_field = true;

        for (i, source_field) in source.fields().iter().enumerate() {
            if target_field.name() == source_field.name() {
                let f = backfill_field(batch.column(i).to_owned(), target_field, num_rows)?;
                columns.push(f);
                columns_from_source += 1;
                new_field = false;
                break;
            }
        }

        if new_field {
            if !target_field.is_nullable() {
                //TODO: New Error Type
                return Err(DeltaWriterError::InvalidRecord(
                    "New column is not nullable".to_string(),
                ));
            }
            columns.push(arrow::array::new_null_array(
                target_field.data_type(),
                num_rows,
            ));
        }
    }

    if columns_from_source != batch.columns().len() {
        //TODO: New Error Type
        return Err(DeltaWriterError::InvalidRecord(
            "Source has columns that do not exist in target".to_string(),
        ));
    }

    Ok(RecordBatch::try_new(target, columns)?)
}

#[cfg(test)]
mod tests {
    use super::backfill_record_batch;
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow::record_batch::*;
    use std::sync::Arc;

    #[test]
    fn test_backfill_new_column() {
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));

        let source = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]);

        let id_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let a_arr = Arc::new(Int32Array::from(vec![10, 11, 12, 13]));

        let batch = RecordBatch::try_new(Arc::new(source), vec![id_arr, a_arr]).unwrap();

        let backfilled = backfill_record_batch(&batch, target.clone()).unwrap();

        assert_eq!(backfilled.columns().len(), 3);
        assert_eq!(backfilled.schema(), target);
        //TODO Check the arrays are in proper order
    }

    #[test]
    /// Cannot have any additonal columns that are not present in the target table's schema
    fn test_backfill_fail_extra_source_column() {
        let source = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]);

        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let id_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let a_arr = Arc::new(Int32Array::from(vec![10, 11, 12, 13]));

        let batch =
            RecordBatch::try_new(Arc::new(source), vec![id_arr, a_arr.clone(), a_arr.clone()])
                .unwrap();

        let backfilled = backfill_record_batch(&batch, target.clone());
        assert!(backfilled.is_err())
    }

    #[test]
    fn test_backfill_fail_not_nullable_column() {
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let source = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]);

        let id_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let a_arr = Arc::new(Int32Array::from(vec![10, 11, 12, 13]));

        let batch = RecordBatch::try_new(Arc::new(source), vec![id_arr, a_arr]).unwrap();

        let backfilled = backfill_record_batch(&batch, target.clone());
        assert!(backfilled.is_err())
    }

    #[test]
    /// Cannot have column data types that differ from the column datatypes in the target table
    fn test_backfill_fail_different_types() {
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let source = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Float32, false),
        ]);

        let id_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let a_arr = Arc::new(Float32Array::from(vec![10.0, 11.3, 12.12, 13.44]));

        let batch = RecordBatch::try_new(Arc::new(source), vec![id_arr, a_arr]).unwrap();

        let backfilled = backfill_record_batch(&batch, target.clone());
        assert!(backfilled.is_err())
    }

    #[test]
    ///Fields can also be added to nested structs
    fn test_backfill_nested() {
        let f = vec![
            Field::new("x", DataType::Float32, false),
            Field::new("y", DataType::Float32, true),
        ];

        let g = vec![Field::new("x", DataType::Float32, false)];

        let x_arr = Arc::new(Float32Array::from(vec![10.0, 11.3, 12.12, 13.44]));

        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("pos", DataType::Struct(f), false),
        ]));

        let source = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("pos", DataType::Struct(g), false),
        ]));

        let id_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let pos_arr = Arc::new(StructArray::from(vec![(
            Field::new("x", DataType::Float32, false),
            x_arr.clone() as ArrayRef,
        )]));

        let batch = RecordBatch::try_new(source, vec![id_arr, pos_arr]).unwrap();
        let _backfilled = backfill_record_batch(&batch, target.clone()).unwrap();
    }
}
