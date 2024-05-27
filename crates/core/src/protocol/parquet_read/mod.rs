use std::{collections::HashMap, str::FromStr};

use chrono::{SecondsFormat, TimeZone, Utc};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use parquet::record::{Field, ListAccessor, MapAccessor, RowAccessor};
use serde_json::json;
use tracing::{debug, error, warn};

use crate::kernel::models::actions::serde_path::decode_path;
use crate::kernel::{
    Action, Add, AddCDCFile, DeletionVectorDescriptor, Metadata, Protocol, Remove, StorageType,
    Transaction,
};
use crate::protocol::{ColumnCountStat, ColumnValueStat, ProtocolError, Stats};

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

fn gen_action_type_error(action: &str, field: &str, expected_type: &str) -> ProtocolError {
    ProtocolError::InvalidField(format!(
        "type for {field} in {action} action should be {expected_type}"
    ))
}

impl AddCDCFile {
    fn from_parquet_record(_record: &parquet::record::Row) -> Result<Self, ProtocolError> {
        let re = Self {
            ..Default::default()
        };
        Ok(re)
    }
}

impl DeletionVectorDescriptor {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ProtocolError> {
        let mut re = Self {
            cardinality: -1,
            offset: None,
            path_or_inline_dv: "".to_string(),
            size_in_bytes: -1,
            storage_type: StorageType::default(),
        };
        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "storageType" => {
                    re.storage_type =
                        StorageType::from_str(record.get_string(i).map_err(|_| {
                            gen_action_type_error("add", "deletionVector.storage_type", "string")
                        })?)?;
                }
                "pathOrInlineDv" => {
                    re.path_or_inline_dv = record
                        .get_string(i)
                        .map_err(|_| {
                            gen_action_type_error("add", "deletionVector.pathOrInlineDv", "string")
                        })?
                        .clone();
                }
                "offset" => {
                    re.offset = match record.get_int(i) {
                        Ok(x) => Some(x),
                        _ => None,
                    }
                }
                "sizeInBytes" => {
                    re.size_in_bytes = record.get_int(i).map_err(|_| {
                        gen_action_type_error("add", "deletionVector.sizeInBytes", "int")
                    })?;
                }
                "cardinality" => {
                    re.cardinality = record.get_long(i).map_err(|_| {
                        gen_action_type_error("add", "deletionVector.sizeInBytes", "long")
                    })?;
                }
                _ => {
                    debug!(
                        "Unexpected field name `{}` for deletion vector: {:?}",
                        name, record
                    );
                }
            }
        }
        Ok(re)
    }
}

impl Add {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ProtocolError> {
        let mut re = Self {
            path: "".to_string(),
            size: -1,
            modification_time: -1,
            data_change: true,
            partition_values: HashMap::new(),
            stats: None,
            stats_parsed: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            tags: None,
            clustering_provider: None,
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "path" => {
                    re.path = decode_path(
                        record
                            .get_string(i)
                            .map_err(|_| gen_action_type_error("add", "path", "string"))?
                            .clone()
                            .as_str(),
                    )?;
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
                        ProtocolError::InvalidField(format!(
                            "Invalid partitionValues for add action: {estr}",
                        ))
                    })?;
                }
                // "partitionValues_parsed" => {
                //     re.partition_values_parsed = Some(
                //         record
                //             .get_group(i)
                //             .map_err(|_| {
                //                 gen_action_type_error("add", "partitionValues_parsed", "struct")
                //             })?
                //             .clone(),
                //     );
                // }
                "tags" => match record.get_map(i) {
                    Ok(tags_map) => {
                        let mut tags = HashMap::new();
                        populate_hashmap_with_option_from_parquet_map(&mut tags, tags_map)
                            .map_err(|estr| {
                                ProtocolError::InvalidField(format!(
                                    "Invalid tags for add action: {estr}",
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
                "deletionVector" => match record.get_group(i) {
                    Ok(row) => {
                        re.deletion_vector =
                            Some(DeletionVectorDescriptor::from_parquet_record(row)?);
                    }
                    _ => {
                        re.deletion_vector = None;
                    }
                },
                _ => {
                    debug!(
                        "Unexpected field name `{}` for add action: {:?}",
                        name, record
                    );
                }
            }
        }

        Ok(re)
    }

    /// Returns the composite HashMap representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    pub fn get_stats_parsed(&self) -> Result<Option<Stats>, ProtocolError> {
        self.stats_parsed.as_ref().map_or(Ok(None), |record| {
                let mut stats = Stats::default();

                for (i, (name, _)) in record.get_column_iter().enumerate() {
                    match name.as_str() {
                        "numRecords" => if let Ok(v) = record.get_long(i) {
                                stats.num_records = v;
                            } else {
                                error!("Expect type of stats_parsed field numRecords to be long, got: {}", record);
                            }
                        "minValues" => if let Ok(row) = record.get_group(i) {
                            for (name, field) in row.get_column_iter() {
                                if !matches!(field, Field::Null) {
                                    if let Some(values) = field_to_value_stat(field, name) {
                                        stats.min_values.insert(name.clone(), values);
                                    }
                                }
                            }
                        } else {
                            error!("Expect type of stats_parsed field minRecords to be struct, got: {}", record);
                        }
                        "maxValues" => if let Ok(row) = record.get_group(i) {
                            for (name, field) in row.get_column_iter() {
                                if !matches!(field, Field::Null) {
                                    if let Some(values) = field_to_value_stat(field, name) {
                                        stats.max_values.insert(name.clone(), values);
                                    }
                                }
                            }
                        } else {
                            error!("Expect type of stats_parsed field maxRecords to be struct, got: {}", record);
                        }
                        "nullCount" => if let Ok(row) = record.get_group(i) {
                            for (name, field) in row.get_column_iter() {
                                if !matches!(field, Field::Null) {
                                    if let Some(count) = field_to_count_stat(field, name) {
                                        stats.null_count.insert(name.clone(), count);
                                    }
                                }
                            }
                        } else {
                            error!("Expect type of stats_parsed field nullCount to be struct, got: {}", record);
                        }
                        _ => {
                            debug!(
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

fn field_to_value_stat(field: &Field, field_name: &str) -> Option<ColumnValueStat> {
    match field {
        Field::Group(group) => {
            let values = group.get_column_iter().filter_map(|(name, sub_field)| {
                field_to_value_stat(sub_field, name).map(|val| (name.clone(), val))
            });
            Some(ColumnValueStat::Column(HashMap::from_iter(values)))
        }
        _ => {
            if let Ok(val) = primitive_parquet_field_to_json_value(field) {
                Some(ColumnValueStat::Value(val))
            } else {
                warn!(
                    "Unexpected type when parsing min/max values for {}. Found {}",
                    field_name, field
                );
                None
            }
        }
    }
}

fn field_to_count_stat(field: &Field, field_name: &str) -> Option<ColumnCountStat> {
    match field {
        Field::Group(group) => {
            let counts = group.get_column_iter().filter_map(|(name, sub_field)| {
                field_to_count_stat(sub_field, name).map(|count| (name.clone(), count))
            });
            Some(ColumnCountStat::Column(HashMap::from_iter(counts)))
        }
        Field::Long(value) => Some(ColumnCountStat::Value(*value)),
        _ => {
            warn!(
                "Unexpected type when parsing nullCounts for {}. Found {}",
                field_name, field
            );
            None
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
            _ => Err("Invalid type for min/max values."),
        },
        Field::TimestampMicros(timestamp) => Ok(serde_json::Value::String(
            convert_timestamp_micros_to_string(*timestamp)?,
        )),
        Field::TimestampMillis(timestamp) => Ok(serde_json::Value::String(
            convert_timestamp_millis_to_string(*timestamp)?,
        )),
        Field::Date(date) => Ok(serde_json::Value::String(convert_date_to_string(*date)?)),
        _ => Err("Invalid type for min/max values."),
    }
}

fn convert_timestamp_millis_to_string(value: i64) -> Result<String, &'static str> {
    let seconds = value / 1000;
    let milliseconds = (value % 1000) as u32;

    let dt = Utc
        .timestamp_opt(seconds, milliseconds * 1_000_000)
        .single()
        .ok_or("Value out of bounds")?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn convert_timestamp_micros_to_string(value: i64) -> Result<String, &'static str> {
    let seconds = value / 1_000_000;
    let microseconds = (value % 1_000_000) as u32;

    let dt: chrono::DateTime<Utc> = Utc
        .timestamp_opt(seconds, microseconds * 1_000)
        .single()
        .ok_or("Value out of bounds")?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

fn convert_date_to_string(value: i32) -> Result<String, &'static str> {
    static NUM_SECONDS_IN_DAY: i64 = 60 * 60 * 24;
    let dt = Utc
        .timestamp_opt(value as i64 * NUM_SECONDS_IN_DAY, 0)
        .single()
        .ok_or("Value out of bounds")?
        .date_naive();
    Ok(format!("{}", dt.format("%Y-%m-%d")))
}

impl Metadata {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ProtocolError> {
        let mut re = Self {
            id: "".to_string(),
            name: None,
            description: None,
            partition_columns: vec![],
            schema_string: "".to_string(),
            created_time: None,
            configuration: HashMap::new(),
            format: Default::default(),
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
                "createdTime" => match record.get_long(i) {
                    Ok(s) => re.created_time = Some(s),
                    _ => re.created_time = None,
                },
                "configuration" => {
                    let configuration_map = record
                        .get_map(i)
                        .map_err(|_| gen_action_type_error("metaData", "configuration", "map"))?;
                    populate_hashmap_with_option_from_parquet_map(
                        &mut re.configuration,
                        configuration_map,
                    )
                    .map_err(|estr| {
                        ProtocolError::InvalidField(format!(
                            "Invalid configuration for metaData action: {estr}",
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
                                ProtocolError::InvalidField(format!(
                                    "Invalid format.options for metaData action: {estr}",
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
                    debug!(
                        "Unexpected field name `{}` for metaData action: {:?}",
                        name, record
                    );
                }
            }
        }

        Ok(re)
    }
}

impl Remove {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ProtocolError> {
        let mut re = Self {
            data_change: true,
            extended_file_metadata: Some(false),
            deletion_timestamp: None,
            deletion_vector: None,
            partition_values: None,
            path: "".to_string(),
            size: None,
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "path" => {
                    re.path = decode_path(
                        record
                            .get_string(i)
                            .map_err(|_| gen_action_type_error("remove", "path", "string"))?
                            .clone()
                            .as_str(),
                    )?;
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
                            ProtocolError::InvalidField(format!(
                                "Invalid partitionValues for remove action: {estr}",
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
                                ProtocolError::InvalidField(format!(
                                    "Invalid tags for remove action: {estr}",
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
                    debug!(
                        "Unexpected field name `{}` for remove action: {:?}",
                        name, record
                    );
                }
            }
        }

        Ok(re)
    }
}

impl Transaction {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ProtocolError> {
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
                    debug!(
                        "Unexpected field name `{}` for txn action: {:?}",
                        name, record
                    );
                }
            }
        }

        Ok(re)
    }
}

impl Protocol {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ProtocolError> {
        let mut re = Self {
            min_reader_version: -1,
            min_writer_version: -1,
            reader_features: None,
            writer_features: None,
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
                "readerFeatures" => {
                    re.reader_features = record
                        .get_list(i)
                        .map(|l| l.elements().iter().map(From::from).collect())
                        .ok()
                }
                "writerFeatures" => {
                    re.writer_features = record
                        .get_list(i)
                        .map(|l| l.elements().iter().map(From::from).collect())
                        .ok()
                }
                _ => {
                    debug!(
                        "Unexpected field name `{}` for protocol action: {:?}",
                        name, record
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
    ) -> Result<Self, ProtocolError> {
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
                    return Err(ProtocolError::InvalidRow(
                        "Parquet action row only contains null columns".to_string(),
                    ));
                }
            }
        };

        let fields = schema.get_fields();
        let field = &fields[col_idx];

        Ok(match field.get_basic_info().name() {
            "add" => Action::Add(Add::from_parquet_record(col_data)?),
            "metaData" => Action::Metadata(Metadata::from_parquet_record(col_data)?),
            "remove" => Action::Remove(Remove::from_parquet_record(col_data)?),
            "txn" => Action::Txn(Transaction::from_parquet_record(col_data)?),
            "protocol" => Action::Protocol(Protocol::from_parquet_record(col_data)?),
            "cdc" => Action::Cdc(AddCDCFile::from_parquet_record(col_data)?),
            name => {
                return Err(ProtocolError::InvalidField(format!(
                    "Unexpected action from checkpoint: {name}",
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

        let path =
            "../test/tests/data/delta-0.2.0/_delta_log/00000000000000000003.checkpoint.parquet";
        let preader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();

        let mut iter = preader.get_row_iter(None).unwrap();
        let record = iter.nth(9).unwrap().unwrap();
        let add_record = record.get_group(1).unwrap();
        let add_action = Add::from_parquet_record(add_record).unwrap();

        assert_eq!(add_action.partition_values.len(), 0);
        assert_eq!(add_action.stats, None);
    }

    #[test]
    fn test_issue_1372_field_to_value_stat() {
        let now = Utc::now();
        let timestamp_milliseconds = Field::TimestampMillis(now.timestamp_millis());
        let ts_millis = field_to_value_stat(&timestamp_milliseconds, "timestamp_millis");
        assert!(ts_millis.is_some());
        assert_eq!(
            ColumnValueStat::Value(json!(now.to_rfc3339_opts(SecondsFormat::Millis, true))),
            ts_millis.unwrap()
        );

        let timestamp_milliseconds = Field::TimestampMicros(now.timestamp_micros());
        let ts_micros = field_to_value_stat(&timestamp_milliseconds, "timestamp_micros");
        assert!(ts_micros.is_some());
        assert_eq!(
            ColumnValueStat::Value(json!(now.to_rfc3339_opts(SecondsFormat::Micros, true))),
            ts_micros.unwrap()
        );
    }
}
