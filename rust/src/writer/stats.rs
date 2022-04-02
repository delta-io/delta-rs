use super::*;
use crate::{
    action::{Add, ColumnValueStat, Stats},
    time_utils::timestamp_to_delta_stats_string,
    DeltaDataTypeLong,
};
use arrow::{
    array::{
        as_boolean_array, as_primitive_array, as_struct_array, make_array, Array, ArrayData,
        StructArray,
    },
    buffer::MutableBuffer,
};
use parquet::{
    basic::TimestampType,
    errors::ParquetError,
    file::{metadata::RowGroupMetaData, statistics::Statistics},
    schema::types::{ColumnDescriptor, SchemaDescriptor},
};
use parquet_format::FileMetaData;
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub type NullCounts = HashMap<String, ColumnCountStat>;
pub type MinAndMaxValues = (
    HashMap<String, ColumnValueStat>,
    HashMap<String, ColumnValueStat>,
);

pub(crate) fn apply_null_counts(
    array: &StructArray,
    null_counts: &mut HashMap<String, ColumnCountStat>,
    nest_level: i32,
) {
    let fields = match array.data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!(),
    };

    array
        .columns()
        .into_iter()
        .zip(fields)
        .for_each(|(column, field)| {
            let key = field.name().to_owned();

            match column.data_type() {
                // Recursive case
                DataType::Struct(_) => {
                    let col_struct = null_counts
                        .entry(key)
                        .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));

                    match col_struct {
                        ColumnCountStat::Column(map) => {
                            apply_null_counts(as_struct_array(column), map, nest_level + 1);
                        }
                        _ => unreachable!(),
                    }
                }
                // Base case
                _ => {
                    let col_struct = null_counts
                        .entry(key.clone())
                        .or_insert_with(|| ColumnCountStat::Value(0));

                    match col_struct {
                        ColumnCountStat::Value(n) => {
                            let null_count = column.null_count() as DeltaDataTypeLong;
                            let n = null_count + *n;
                            null_counts.insert(key, ColumnCountStat::Value(n));
                        }
                        _ => unreachable!(),
                    }
                }
            }
        });
}

pub(crate) fn create_add(
    partition_values: &HashMap<String, Option<String>>,
    null_counts: NullCounts,
    path: String,
    size: i64,
    file_metadata: &FileMetaData,
) -> Result<Add, DeltaWriterError> {
    let (min_values, max_values) =
        min_max_values_from_file_metadata(partition_values, file_metadata)?;

    let stats = Stats {
        num_records: file_metadata.num_rows,
        min_values,
        max_values,
        null_count: null_counts,
    };

    let stats_string = serde_json::to_string(&stats)
        .or(Err(DeltaWriterError::StatsSerializationFailed { stats }))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    Ok(Add {
        path,
        size,
        partition_values: partition_values.to_owned(),
        partition_values_parsed: None,
        modification_time,
        data_change: true,
        stats: Some(stats_string),
        stats_parsed: None,
        tags: None,
    })
}

fn min_max_values_from_file_metadata(
    partition_values: &HashMap<String, Option<String>>,
    file_metadata: &FileMetaData,
) -> Result<MinAndMaxValues, DeltaWriterError> {
    let type_ptr = parquet::schema::types::from_thrift(file_metadata.schema.as_slice());
    let schema_descriptor = type_ptr.map(|type_| Arc::new(SchemaDescriptor::new(type_)))?;

    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();

    let row_group_metadata: Result<Vec<RowGroupMetaData>, ParquetError> = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()))
        .collect();
    let row_group_metadata = row_group_metadata?;

    for i in 0..schema_descriptor.num_columns() {
        let column_descr = schema_descriptor.column(i);

        // If max rep level is > 0, this is an array element or a struct element of an array or something downstream of an array.
        // delta/databricks only computes null counts for arrays - not min max.
        // null counts are tracked at the record batch level, so skip any column with max_rep_level
        // > 0
        if column_descr.max_rep_level() > 0 {
            continue;
        }

        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();

        // Do not include partition columns in statistics
        if partition_values.contains_key(&column_path_parts[0]) {
            continue;
        }

        let statistics: Vec<&Statistics> = row_group_metadata
            .iter()
            .filter_map(|g| g.column(i).statistics())
            .collect();

        let _ = apply_min_max_for_column(
            statistics.as_slice(),
            column_descr.clone(),
            column_path_parts,
            &mut min_values,
            &mut max_values,
        )?;
    }

    Ok((min_values, max_values))
}

fn apply_min_max_for_column(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
    column_path_parts: &[String],
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
) -> Result<(), DeltaWriterError> {
    match (column_path_parts.len(), column_path_parts.first()) {
        // Base case - we are at the leaf struct level in the path
        (1, _) => {
            let (min, max) = min_and_max_from_parquet_statistics(statistics, column_descr.clone())?;

            if let Some(min) = min {
                let min = ColumnValueStat::Value(min);
                min_values.insert(column_descr.name().to_string(), min);
            }

            if let Some(max) = max {
                let max = ColumnValueStat::Value(max);
                max_values.insert(column_descr.name().to_string(), max);
            }

            Ok(())
        }
        // Recurse to load value at the appropriate level of HashMap
        (_, Some(key)) => {
            let child_min_values = min_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
            let child_max_values = max_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));

            match (child_min_values, child_max_values) {
                (ColumnValueStat::Column(mins), ColumnValueStat::Column(maxes)) => {
                    let remaining_parts: Vec<String> = column_path_parts
                        .iter()
                        .skip(1)
                        .map(|s| s.to_string())
                        .collect();

                    apply_min_max_for_column(
                        statistics,
                        column_descr,
                        remaining_parts.as_slice(),
                        mins,
                        maxes,
                    )?;

                    Ok(())
                }
                _ => {
                    unreachable!();
                }
            }
        }
        // column path parts will always have at least one element.
        (_, None) => {
            unreachable!();
        }
    }
}

#[inline]
fn is_utf8(opt: Option<LogicalType>) -> bool {
    matches!(opt.as_ref(), Some(LogicalType::STRING(_)))
}

fn min_and_max_from_parquet_statistics(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
) -> Result<(Option<Value>, Option<Value>), DeltaWriterError> {
    let stats_with_min_max: Vec<&Statistics> = statistics
        .iter()
        .filter(|s| s.has_min_max_set())
        .copied()
        .collect();

    if stats_with_min_max.is_empty() {
        return Ok((None, None));
    }

    let (data_size, data_type) = match stats_with_min_max.first() {
        Some(Statistics::Boolean(_)) => (std::mem::size_of::<bool>(), DataType::Boolean),
        Some(Statistics::Int32(_)) => (std::mem::size_of::<i32>(), DataType::Int32),
        Some(Statistics::Int64(_)) => (std::mem::size_of::<i64>(), DataType::Int64),
        Some(Statistics::Float(_)) => (std::mem::size_of::<f32>(), DataType::Float32),
        Some(Statistics::Double(_)) => (std::mem::size_of::<f64>(), DataType::Float64),
        Some(Statistics::ByteArray(_)) if is_utf8(column_descr.logical_type()) => {
            (0, DataType::Utf8)
        }
        _ => {
            // NOTE: Skips
            // Statistics::Int96(_)
            // Statistics::ByteArray(_)
            // Statistics::FixedLenByteArray(_)

            return Ok((None, None));
        }
    };

    if data_type == DataType::Utf8 {
        return Ok(min_max_strings_from_stats(&stats_with_min_max));
    }

    let arrow_buffer_capacity = stats_with_min_max.len() * data_size;

    let min_array = arrow_array_from_bytes(
        data_type.clone(),
        arrow_buffer_capacity,
        stats_with_min_max.iter().map(|s| s.min_bytes()).collect(),
    )?;

    let max_array = arrow_array_from_bytes(
        data_type.clone(),
        arrow_buffer_capacity,
        stats_with_min_max.iter().map(|s| s.max_bytes()).collect(),
    )?;

    match data_type {
        DataType::Boolean => {
            let min = arrow::compute::min_boolean(as_boolean_array(&min_array));
            let min = min.map(Value::Bool);

            let max = arrow::compute::max_boolean(as_boolean_array(&max_array));
            let max = max.map(Value::Bool);

            Ok((min, max))
        }
        DataType::Int32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array = as_primitive_array::<arrow::datatypes::Int32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            Ok((min, max))
        }
        DataType::Int64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let max_array = as_primitive_array::<arrow::datatypes::Int64Type>(&max_array);
            let max = arrow::compute::max(max_array);

            match column_descr.logical_type().as_ref() {
                Some(LogicalType::TIMESTAMP(TimestampType { unit, .. })) => {
                    let min = min.map(|n| Value::String(timestamp_to_delta_stats_string(n, unit)));
                    let max = max.map(|n| Value::String(timestamp_to_delta_stats_string(n, unit)));

                    Ok((min, max))
                }
                _ => {
                    let min = min.map(|i| Value::Number(Number::from(i)));
                    let max = max.map(|i| Value::Number(Number::from(i)));

                    Ok((min, max))
                }
            }
        }
        DataType::Float32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.and_then(|f| Number::from_f64(f as f64).map(Value::Number));

            let max_array = as_primitive_array::<arrow::datatypes::Float32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.and_then(|f| Number::from_f64(f as f64).map(Value::Number));

            Ok((min, max))
        }
        DataType::Float64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.and_then(|f| Number::from_f64(f).map(Value::Number));

            let max_array = as_primitive_array::<arrow::datatypes::Float64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.and_then(|f| Number::from_f64(f).map(Value::Number));

            Ok((min, max))
        }
        _ => Ok((None, None)),
    }
}

fn min_max_strings_from_stats(
    stats_with_min_max: &[&Statistics],
) -> (Option<Value>, Option<Value>) {
    let min_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.min_bytes()).ok());

    let min_value = min_string_candidates
        .min()
        .map(|s| Value::String(s.to_string()));

    let max_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.max_bytes()).ok());

    let max_value = max_string_candidates
        .max()
        .map(|s| Value::String(s.to_string()));

    (min_value, max_value)
}

fn arrow_array_from_bytes(
    data_type: DataType,
    capacity: usize,
    byte_arrays: Vec<&[u8]>,
) -> Result<Arc<dyn Array>, DeltaWriterError> {
    let mut buffer = MutableBuffer::new(capacity);

    for arr in byte_arrays.iter() {
        buffer.extend_from_slice(arr);
    }

    let builder = ArrayData::builder(data_type)
        .len(byte_arrays.len())
        .add_buffer(buffer.into());

    let data = builder.build()?;

    Ok(make_array(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{test_utils::get_record_batch, utils::record_batch_from_message};
    use crate::{
        action::{ColumnCountStat, ColumnValueStat},
        DeltaTable, DeltaTableError,
    };
    use lazy_static::lazy_static;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::path::Path;

    #[test]
    fn test_apply_null_counts() {
        let record_batch = get_record_batch(None, true);
        let mut ref_null_counts = HashMap::new();
        ref_null_counts.insert("id".to_string(), ColumnCountStat::Value(3));
        ref_null_counts.insert("value".to_string(), ColumnCountStat::Value(1));
        ref_null_counts.insert("modified".to_string(), ColumnCountStat::Value(0));

        let mut null_counts = HashMap::new();
        apply_null_counts(&record_batch.clone().into(), &mut null_counts, 0);

        assert_eq!(null_counts, ref_null_counts)
    }

    #[tokio::test]
    async fn test_delta_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path();
        create_temp_table(table_path);

        let table = load_table(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap();

        let mut writer = RecordBatchWriter::for_table(&table, HashMap::new()).unwrap();

        let arrow_schema = writer.arrow_schema();
        let batch = record_batch_from_message(arrow_schema, JSON_ROWS.clone().as_ref()).unwrap();

        writer.write(batch).await.unwrap();
        let add = writer.flush().await.unwrap();
        assert_eq!(add.len(), 1);
        let stats = add[0].get_stats().unwrap().unwrap();

        let min_max_keys = vec!["meta", "some_int", "some_string", "some_bool"];
        let mut null_count_keys = vec!["some_list", "some_nested_list"];
        null_count_keys.extend_from_slice(min_max_keys.as_slice());

        assert_eq!(min_max_keys.len(), stats.min_values.len());
        assert_eq!(min_max_keys.len(), stats.max_values.len());
        assert_eq!(null_count_keys.len(), stats.null_count.len());

        // assert on min values
        for (k, v) in stats.min_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(302, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert_eq!(false, v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("GET", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                _ => assert!(false, "Key should not be present"),
            }
        }

        // assert on max values
        for (k, v) in stats.max_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(1, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(400, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert_eq!(true, v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("PUT", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                _ => assert!(false, "Key should not be present"),
            }
        }

        // assert on null count
        for (k, v) in stats.null_count.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnCountStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition);

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!(0, timestamp);
                }
                ("some_int", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_bool", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_string", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_list", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_nested_list", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                ("date", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                _ => assert!(false, "Key should not be present"),
            }
        }
    }

    async fn load_table(
        table_uri: &str,
        options: HashMap<String, String>,
    ) -> Result<DeltaTable, DeltaTableError> {
        let backend = crate::get_backend_for_uri_with_options(table_uri, options)?;
        let mut table = DeltaTable::new(
            table_uri,
            backend,
            crate::DeltaTableConfig {
                require_tombstones: true,
            },
        )?;
        table.load().await?;
        Ok(table)
    }

    fn create_temp_table(table_path: &Path) {
        let log_path = table_path.join("_delta_log");

        let _ = std::fs::create_dir(log_path.as_path()).unwrap();
        let _ = std::fs::write(
            log_path.join("00000000000000000000.json"),
            V0_COMMIT.as_str(),
        )
        .unwrap();
    }

    lazy_static! {
        static ref SCHEMA: Value = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "meta",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "kafka",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "topic",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "partition",
                                            "type": "integer",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "offset",
                                            "type": "long",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            },
                            {
                                "name": "producer",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "timestamp",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            }
                        ]
                    },
                    "nullable": true, "metadata": {}
                },
                { "name": "some_string", "type": "string", "nullable": true, "metadata": {} },
                { "name": "some_int", "type": "integer", "nullable": true, "metadata": {} },
                { "name": "some_bool", "type": "boolean", "nullable": true, "metadata": {} },
                {
                    "name": "some_list",
                    "type": {
                        "type": "array",
                        "elementType": "string",
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "some_nested_list",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "array",
                            "elementType": "integer",
                            "containsNull": true
                        },
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
               },
               { "name": "date", "type": "string", "nullable": true, "metadata": {} },
            ]
        });
        static ref V0_COMMIT: String = {
            let schema_string = serde_json::to_string(&SCHEMA.clone()).unwrap();
            let jsons = [
                json!({
                    "protocol":{"minReaderVersion":1,"minWriterVersion":2}
                }),
                json!({
                    "metaData": {
                        "id": "22ef18ba-191c-4c36-a606-3dad5cdf3830",
                        "format": {
                            "provider": "parquet", "options": {}
                        },
                        "schemaString": schema_string,
                        "partitionColumns": ["date"], "configuration": {}, "createdTime": 1564524294376i64
                    }
                }),
            ];

            jsons
                .iter()
                .map(|j| serde_json::to_string(j).unwrap())
                .collect::<Vec<String>>()
                .join("\n")
                .to_string()
        };
        static ref JSON_ROWS: Vec<Value> = {
            std::iter::repeat(json!({
                "meta": {
                    "kafka": {
                        "offset": 0,
                        "partition": 0,
                        "topic": "some_topic"
                    },
                    "producer": {
                        "timestamp": "2021-06-22"
                    },
                },
                "some_string": "GET",
                "some_int": 302,
                "some_bool": true,
                "some_list": ["a", "b", "c"],
                "some_nested_list": [[42], [84]],
                "date": "2021-06-22",
            }))
            .take(100)
            .chain(
                std::iter::repeat(json!({
                    "meta": {
                        "kafka": {
                            "offset": 100,
                            "partition": 1,
                            "topic": "another_topic"
                        },
                        "producer": {
                            "timestamp": "2021-06-22"
                        },
                    },
                    "some_string": "PUT",
                    "some_int": 400,
                    "some_bool": false,
                    "some_list": ["x", "y", "z"],
                    "some_nested_list": [[42], [84]],
                    "date": "2021-06-22",
                }))
                .take(100),
            )
            .chain(
                std::iter::repeat(json!({
                    "meta": {
                        "kafka": {
                            "offset": 0,
                            "partition": 0,
                            "topic": "some_topic"
                        },
                        "producer": {
                            "timestamp": "2021-06-22"
                        },
                    },
                    "some_nested_list": [[42], null],
                    "date": "2021-06-22",
                }))
                .take(100),
            )
            .collect()
        };
    }
}
