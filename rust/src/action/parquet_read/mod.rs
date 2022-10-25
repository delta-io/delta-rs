use chrono::{SecondsFormat, TimeZone, Utc};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use parquet::record::{Field, ListAccessor, MapAccessor, RowAccessor};
use serde_json::json;
use std::collections::HashMap;

use crate::action::{
    Action, ActionError, Add, ColumnCountStat, ColumnValueStat, MetaData, Protocol, Remove, Stats,
    Txn,
};

fn populate_hashmap_with_option_from_parquet_map(
    map: &mut HashMap<String, Option<String>>,
    pmap: &parquet::record::Map,
) -> Result<(), &'static str> {
    let keys = pmap.get_keys();
    let values = pmap.get_values();
    for j in 0..pmap.len() {
        map.entry(
            keys.get_string(j)
                .map_err(|_| "key for HashMap in parquet has to be a string")?
                .clone(),
        )
        .or_insert_with(|| values.get_string(j).ok().cloned());
    }

    Ok(())
}

fn gen_action_type_error(action: &str, field: &str, expected_type: &str) -> ActionError {
    ActionError::InvalidField(format!(
        "type for {} in {} action should be {}",
        field, action, expected_type
    ))
}

impl Add {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "path" => {
                    re.path = record
                        .get_string(i)
                        .map_err(|_| gen_action_type_error("add", "path", "string"))?
                        .clone();
                }
                "size" => {
                    re.size = record
                        .get_long(i)
                        .map_err(|_| gen_action_type_error("add", "size", "long"))?;
                }
                "modificationTime" => {
                    re.modification_time = record
                        .get_long(i)
                        .map_err(|_| gen_action_type_error("add", "modificationTime", "long"))?;
                }
                "dataChange" => {
                    re.data_change = record
                        .get_bool(i)
                        .map_err(|_| gen_action_type_error("add", "dataChange", "bool"))?;
                }
                "partitionValues" => {
                    let parquet_map = record
                        .get_map(i)
                        .map_err(|_| gen_action_type_error("add", "partitionValues", "map"))?;
                    populate_hashmap_with_option_from_parquet_map(
                        &mut re.partition_values,
                        parquet_map,
                    )
                    .map_err(|estr| {
                        ActionError::InvalidField(format!(
                            "Invalid partitionValues for add action: {}",
                            estr,
                        ))
                    })?;
                }
                "partitionValues_parsed" => {
                    re.partition_values_parsed = Some(
                        record
                            .get_group(i)
                            .map_err(|_| {
                                gen_action_type_error("add", "partitionValues_parsed", "struct")
                            })?
                            .clone(),
                    );
                }
                "tags" => match record.get_map(i) {
                    Ok(tags_map) => {
                        let mut tags = HashMap::new();
                        populate_hashmap_with_option_from_parquet_map(&mut tags, tags_map)
                            .map_err(|estr| {
                                ActionError::InvalidField(format!(
                                    "Invalid tags for add action: {}",
                                    estr,
                                ))
                            })?;
                        re.tags = Some(tags);
                    }
                    _ => {
                        re.tags = None;
                    }
                },
                "stats" => match record.get_string(i) {
                    Ok(stats) => {
                        re.stats = Some(stats.clone());
                    }
                    _ => {
                        re.stats = None;
                    }
                },
                "stats_parsed" => match record.get_group(i) {
                    Ok(stats_parsed) => {
                        re.stats_parsed = Some(stats_parsed.clone());
                    }
                    _ => {
                        re.stats_parsed = None;
                    }
                },
                _ => {
                    log::warn!(
                        "Unexpected field name `{}` for add action: {:?}",
                        name,
                        record
                    );
                }
            }
        }

        Ok(re)
    }

    /// Returns the composite HashMap representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    pub fn get_stats_parsed(&self) -> Result<Option<Stats>, ActionError> {
        self.stats_parsed.as_ref().map_or(Ok(None), |record| {
                let mut stats = Stats::default();

                for (i, (name, _)) in record.get_column_iter().enumerate() {
                    match name.as_str() {
                        "numRecords" => match record.get_long(i) {
                            Ok(v) => {
                                stats.num_records = v;
                            }
                            _ => {
                                log::error!("Expect type of stats_parsed field numRecords to be long, got: {}", record);
                            }
                        }
                        "minValues" => match record.get_group(i) {
                            Ok(row) => {
                                for (name, field) in row.get_column_iter() {
                                    match field {
                                        Field::Null => {},
                                        _ => {
                                            match field.try_into() {
                                                Ok(values) => {
                                                    stats.min_values.insert(name.clone(), values);
                                                }
                                                _ => {
                                                    log::warn!("Unexpected type of minValues field, got: {}", field);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                log::error!("Expect type of stats_parsed field minRecords to be struct, got: {}", record);
                            }
                        }
                        "maxValues" => match record.get_group(i) {
                            Ok(row) => {
                                for (name, field) in row.get_column_iter() {
                                    match field {
                                        Field::Null => {},
                                        _ => {
                                            match field.try_into() {
                                                Ok(values) => {
                                                    stats.max_values.insert(name.clone(), values);
                                                }
                                                _ => {
                                                    log::warn!("Unexpected type of maxValues field, got: {}", field);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                log::error!("Expect type of stats_parsed field maxRecords to be struct, got: {}", record);
                            }
                        }
                        "nullCount" => match record.get_group(i) {
                            Ok(row) => {
                                for (name, field) in row.get_column_iter() {
                                    match field {
                                        Field::Null => {}
                                        _ => {
                                            match field.try_into() {
                                                Ok(count) => {
                                                    stats.null_count.insert(name.clone(), count);
                                                },
                                                _ => {
                                                    log::warn!("Expect type of nullCount field to be struct or int64, got: {}", field);
                                                },
                                            };
                                        }
                                    }
                                }
                            }
                            _ => {
                                log::error!("Expect type of stats_parsed field nullCount to be struct, got: {}", record);
                            }
                        }
                        _ => {
                            log::warn!(
                                "Unexpected field name `{}` for stats_parsed: {:?}",
                                name,
                                record,
                            );
                        }
                    }
                }

                Ok(Some(stats))
            })
    }
}

impl TryFrom<&Field> for ColumnValueStat {
    type Error = &'static str;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        match field {
            Field::Group(group) => Ok(ColumnValueStat::Column(HashMap::from_iter(
                group
                    .get_column_iter()
                    .filter_map(|(field_name, field)| match field.try_into() {
                        Ok(count) => Some((field_name.clone(), count)),
                        _ => {
                            log::warn!(
                                "Unexpected type when parsing min/max values for {}. Found {}",
                                field_name,
                                field
                            );
                            None
                        }
                    }),
            ))),
            _ => primitive_parquet_field_to_json_value(field).map(ColumnValueStat::Value),
        }
    }
}

impl TryFrom<&Field> for ColumnCountStat {
    type Error = &'static str;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        match field {
            Field::Group(group) => Ok(ColumnCountStat::Column(HashMap::from_iter(
                group
                    .get_column_iter()
                    .filter_map(|(field_name, field)| match field.try_into() {
                        Ok(count) => Some((field_name.clone(), count)),
                        _ => {
                            log::warn!(
                                "Unexpected type when parsing nullCounts for {}. Found {}",
                                field_name,
                                field
                            );
                            None
                        }
                    }),
            ))),
            Field::Long(value) => Ok(ColumnCountStat::Value(*value)),
            _ => Err("Invalid type for nullCounts"),
        }
    }
}

fn primitive_parquet_field_to_json_value(field: &Field) -> Result<serde_json::Value, &'static str> {
    match field {
        Field::Bool(value) => Ok(json!(value)),
        Field::Byte(value) => Ok(json!(value)),
        Field::Short(value) => Ok(json!(value)),
        Field::Int(value) => Ok(json!(value)),
        Field::Long(value) => Ok(json!(value)),
        Field::Float(value) => Ok(json!(value)),
        Field::Double(value) => Ok(json!(value)),
        Field::Str(value) => Ok(json!(value)),
        Field::Decimal(decimal) => match BigInt::from_signed_bytes_be(decimal.data()).to_f64() {
            Some(int) => Ok(json!(
                int / (10_i64.pow((decimal.scale()).try_into().unwrap()) as f64)
            )),
            _ => Err("Invalid type for nullCounts"),
        },
        Field::TimestampMillis(timestamp) => Ok(serde_json::Value::String(
            convert_timestamp_millis_to_string(*timestamp),
        )),
        Field::Date(date) => Ok(serde_json::Value::String(convert_date_to_string(*date))),
        _ => Err("Invalid type for nullCounts"),
    }
}

fn convert_timestamp_millis_to_string(value: u64) -> String {
    let dt = Utc.timestamp((value / 1000) as i64, ((value % 1000) * 1000000) as u32);
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn convert_date_to_string(value: u32) -> String {
    static NUM_SECONDS_IN_DAY: i64 = 60 * 60 * 24;
    let dt = Utc.timestamp(value as i64 * NUM_SECONDS_IN_DAY, 0).date();
    format!("{}", dt.format("%Y-%m-%d"))
}

impl MetaData {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "id" => {
                    re.id = record
                        .get_string(i)
                        .map_err(|_| gen_action_type_error("metaData", "id", "string"))?
                        .clone();
                }
                "name" => match record.get_string(i) {
                    Ok(s) => re.name = Some(s.clone()),
                    _ => re.name = None,
                },
                "description" => match record.get_string(i) {
                    Ok(s) => re.description = Some(s.clone()),
                    _ => re.description = None,
                },
                "partitionColumns" => {
                    let columns_list = record.get_list(i).map_err(|_| {
                        gen_action_type_error("metaData", "partitionColumns", "list")
                    })?;
                    for j in 0..columns_list.len() {
                        re.partition_columns.push(
                            columns_list
                                .get_string(j)
                                .map_err(|_| {
                                    gen_action_type_error(
                                        "metaData",
                                        "partitionColumns.value",
                                        "string",
                                    )
                                })?
                                .clone(),
                        );
                    }
                }
                "schemaString" => {
                    re.schema_string = record
                        .get_string(i)
                        .map_err(|_| gen_action_type_error("metaData", "schemaString", "string"))?
                        .clone();
                }
                "createdTime" => {
                    re.created_time =
                        Some(record.get_long(i).map_err(|_| {
                            gen_action_type_error("metaData", "createdTime", "long")
                        })?);
                }
                "configuration" => {
                    let configuration_map = record
                        .get_map(i)
                        .map_err(|_| gen_action_type_error("metaData", "configuration", "map"))?;
                    populate_hashmap_with_option_from_parquet_map(
                        &mut re.configuration,
                        configuration_map,
                    )
                    .map_err(|estr| {
                        ActionError::InvalidField(format!(
                            "Invalid configuration for metaData action: {}",
                            estr,
                        ))
                    })?;
                }
                "format" => {
                    let format_record = record
                        .get_group(i)
                        .map_err(|_| gen_action_type_error("metaData", "format", "struct"))?;

                    re.format.provider = format_record
                        .get_string(0)
                        .map_err(|_| {
                            gen_action_type_error("metaData", "format.provider", "string")
                        })?
                        .clone();
                    match record.get_map(1) {
                        Ok(options_map) => {
                            let mut options = HashMap::new();
                            populate_hashmap_with_option_from_parquet_map(
                                &mut options,
                                options_map,
                            )
                            .map_err(|estr| {
                                ActionError::InvalidField(format!(
                                    "Invalid format.options for metaData action: {}",
                                    estr,
                                ))
                            })?;
                            re.format.options = options;
                        }
                        _ => {
                            re.format.options = HashMap::new();
                        }
                    }
                }
                _ => {
                    log::warn!(
                        "Unexpected field name `{}` for metaData action: {:?}",
                        name,
                        record
                    );
                }
            }
        }

        Ok(re)
    }
}

impl Remove {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
            data_change: true,
            extended_file_metadata: Some(false),
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "path" => {
                    re.path = record
                        .get_string(i)
                        .map_err(|_| gen_action_type_error("remove", "path", "string"))?
                        .clone();
                }
                "dataChange" => {
                    re.data_change = record
                        .get_bool(i)
                        .map_err(|_| gen_action_type_error("remove", "dataChange", "bool"))?;
                }
                "extendedFileMetadata" => {
                    re.extended_file_metadata = record.get_bool(i).map(Some).unwrap_or(None);
                }
                "deletionTimestamp" => {
                    re.deletion_timestamp = Some(record.get_long(i).map_err(|_| {
                        gen_action_type_error("remove", "deletionTimestamp", "long")
                    })?);
                }
                "partitionValues" => match record.get_map(i) {
                    Ok(_) => {
                        let parquet_map = record.get_map(i).map_err(|_| {
                            gen_action_type_error("remove", "partitionValues", "map")
                        })?;
                        let mut partition_values = HashMap::new();
                        populate_hashmap_with_option_from_parquet_map(
                            &mut partition_values,
                            parquet_map,
                        )
                        .map_err(|estr| {
                            ActionError::InvalidField(format!(
                                "Invalid partitionValues for remove action: {}",
                                estr,
                            ))
                        })?;
                        re.partition_values = Some(partition_values);
                    }
                    _ => re.partition_values = None,
                },
                "tags" => match record.get_map(i) {
                    Ok(tags_map) => {
                        let mut tags = HashMap::new();
                        populate_hashmap_with_option_from_parquet_map(&mut tags, tags_map)
                            .map_err(|estr| {
                                ActionError::InvalidField(format!(
                                    "Invalid tags for remove action: {}",
                                    estr,
                                ))
                            })?;
                        re.tags = Some(tags);
                    }
                    _ => {
                        re.tags = None;
                    }
                },
                "size" => {
                    re.size = record.get_long(i).map(Some).unwrap_or(None);
                }
                "numRecords" => {}
                _ => {
                    log::warn!(
                        "Unexpected field name `{}` for remove action: {:?}",
                        name,
                        record
                    );
                }
            }
        }

        Ok(re)
    }
}

impl Txn {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "appId" => {
                    re.app_id = record
                        .get_string(i)
                        .map_err(|_| gen_action_type_error("txn", "appId", "string"))?
                        .clone();
                }
                "version" => {
                    re.version = record
                        .get_long(i)
                        .map_err(|_| gen_action_type_error("txn", "version", "long"))?;
                }
                "lastUpdated" => {
                    re.last_updated = record.get_long(i).map(Some).unwrap_or(None);
                }
                _ => {
                    log::warn!(
                        "Unexpected field name `{}` for txn action: {:?}",
                        name,
                        record
                    );
                }
            }
        }

        Ok(re)
    }
}

impl Protocol {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "minReaderVersion" => {
                    re.min_reader_version = record.get_int(i).map_err(|_| {
                        gen_action_type_error("protocol", "minReaderVersion", "int")
                    })?;
                }
                "minWriterVersion" => {
                    re.min_writer_version = record.get_int(i).map_err(|_| {
                        gen_action_type_error("protocol", "minWriterVersion", "int")
                    })?;
                }
                _ => {
                    log::warn!(
                        "Unexpected field name `{}` for protocol action: {:?}",
                        name,
                        record
                    );
                }
            }
        }

        Ok(re)
    }
}

impl Action {
    /// Returns an action from the given parquet Row. Used when deserializing delta log parquet
    /// checkpoints.
    pub fn from_parquet_record(
        schema: &parquet::schema::types::Type,
        record: &parquet::record::Row,
    ) -> Result<Self, ActionError> {
        // find column that's not none
        let (col_idx, col_data) = {
            let mut col_idx = None;
            let mut col_data = None;
            for i in 0..record.len() {
                match record.get_group(i) {
                    Ok(group) => {
                        col_idx = Some(i);
                        col_data = Some(group);
                    }
                    _ => {
                        continue;
                    }
                }
            }

            match (col_idx, col_data) {
                (Some(idx), Some(group)) => (idx, group),
                _ => {
                    return Err(ActionError::InvalidRow(
                        "Parquet action row only contains null columns".to_string(),
                    ));
                }
            }
        };

        let fields = schema.get_fields();
        let field = &fields[col_idx];

        Ok(match field.get_basic_info().name() {
            "add" => Action::add(Add::from_parquet_record(col_data)?),
            "metaData" => Action::metaData(MetaData::from_parquet_record(col_data)?),
            "remove" => Action::remove(Remove::from_parquet_record(col_data)?),
            "txn" => Action::txn(Txn::from_parquet_record(col_data)?),
            "protocol" => Action::protocol(Protocol::from_parquet_record(col_data)?),
            name => {
                return Err(ActionError::InvalidField(format!(
                    "Unexpected action from checkpoint: {}",
                    name,
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_action_without_partition_values_and_stats() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use std::fs::File;

        let path = "./tests/data/delta-0.2.0/_delta_log/00000000000000000003.checkpoint.parquet";
        let preader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();

        let mut iter = preader.get_row_iter(None).unwrap();
        let record = iter.nth(9).unwrap();
        let add_record = record.get_group(1).unwrap();
        let add_action = Add::from_parquet_record(add_record).unwrap();

        assert_eq!(add_action.partition_values.len(), 0);
        assert_eq!(add_action.stats, None);
    }
}
