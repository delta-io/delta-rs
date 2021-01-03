#![allow(non_snake_case, non_camel_case_types)]

use std::collections::HashMap;

use parquet::record::{ListAccessor, MapAccessor, RowAccessor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::schema::*;

#[derive(thiserror::Error, Debug)]
pub enum ActionError {
    #[error("Invalid action field: {0}")]
    InvalidField(String),
    #[error("Invalid action in parquet row: {0}")]
    InvalidRow(String),
    #[error("Generic action error: {0}")]
    Generic(String),
}

fn populate_hashmap_from_parquet_map(
    map: &mut HashMap<String, String>,
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
        .or_insert(
            values
                .get_string(j)
                .map_err(|_| "value for HashMap in parquet has to be a string")?
                .clone(),
        );
    }

    Ok(())
}

fn gen_action_type_error(action: &str, field: &str, expected_type: &str) -> ActionError {
    ActionError::InvalidField(format!(
        "type for {} in {} action should be {}",
        field, action, expected_type
    ))
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnValueStat {
    Column(HashMap<String, ColumnValueStat>),
    Value(serde_json::Value),
}

impl ColumnValueStat {
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnValueStat>> {
        match self {
            ColumnValueStat::Column(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<&serde_json::Value> {
        match self {
            ColumnValueStat::Value(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnCountStat {
    Column(HashMap<String, ColumnCountStat>),
    Value(DeltaDataTypeLong),
}

impl ColumnCountStat {
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnCountStat>> {
        match self {
            ColumnCountStat::Column(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<DeltaDataTypeLong> {
        match self {
            ColumnCountStat::Value(v) => Some(*v),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Stats {
    // number of records in this file
    pub numRecords: DeltaDataTypeLong,

    // start of per column stats

    // A value samller than all values present in the file for all columns
    pub minValues: HashMap<String, ColumnValueStat>,
    // A value larger than all values present in the file for all columns
    pub maxValues: HashMap<String, ColumnValueStat>,
    // The number of null values for all column
    pub nullCount: HashMap<String, ColumnCountStat>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Add {
    // A relative path, from the root of the table, to a file that should be added to the table
    pub path: String,
    // The size of this file in bytes
    pub size: DeltaDataTypeLong,
    // A map from partition column to value for this file
    pub partitionValues: HashMap<String, String>,
    // The time this file was created, as milliseconds since the epoch
    pub modificationTime: DeltaDataTypeTimestamp,
    // When false the file must already be present in the table or the records in the added file
    // must be contained in one or more remove actions in the same version
    //
    // streaming queries that are tailing the transaction log can use this flag to skip actions
    // that would not affect the final results.
    pub dataChange: bool,
    // Contains statistics (e.g., count, min/max values for columns) about the data in this file
    pub stats: Option<String>,
    // Map containing metadata about this file
    pub tags: Option<HashMap<String, String>>,
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
                    re.modificationTime = record
                        .get_long(i)
                        .map_err(|_| gen_action_type_error("add", "modificationTime", "long"))?;
                }
                "dataChange" => {
                    re.dataChange = record
                        .get_bool(i)
                        .map_err(|_| gen_action_type_error("add", "dataChange", "bool"))?;
                }
                "partitionValues" => {
                    let parquetMap = record
                        .get_map(i)
                        .map_err(|_| gen_action_type_error("add", "partitionValues", "map"))?;
                    for i in 0..parquetMap.len() {
                        let key = parquetMap
                            .get_keys()
                            .get_string(i)
                            .map_err(|_| {
                                gen_action_type_error("add", "partitionValues.key", "string")
                            })?
                            .clone();
                        let value = parquetMap
                            .get_values()
                            .get_string(i)
                            .map_err(|_| {
                                gen_action_type_error("add", "partitionValues.value", "string")
                            })?
                            .clone();
                        re.partitionValues.entry(key).or_insert(value);
                    }
                }
                "tags" => match record.get_map(i) {
                    Ok(tags_map) => {
                        let mut tags = HashMap::new();
                        populate_hashmap_from_parquet_map(&mut tags, tags_map).map_err(|estr| {
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
                _ => {
                    return Err(ActionError::InvalidField(format!(
                        "Unexpected field name for add action: {}",
                        name,
                    )));
                }
            }
        }

        Ok(re)
    }

    pub fn get_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        self.stats
            .as_ref()
            .map_or(Ok(None), |s| Ok(serde_json::from_str(s)?))
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Format {
    // Name of the encoding for files in this table
    provider: String,
    // A map containing configuration options for the format
    options: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MetaData {
    // Unique identifier for this table
    pub id: GUID,
    // User-provided identifier for this table
    pub name: Option<String>,
    // User-provided description for this table
    pub description: Option<String>,
    // Specification of the encoding for the files stored in the table
    pub format: Format,
    // Schema of the table
    pub schemaString: String,
    // An array containing the names of columns by which the data should be partitioned
    pub partitionColumns: Vec<String>,
    // NOTE: this field is undocumented
    pub configuration: HashMap<String, String>,
    // NOTE: this field is undocumented
    pub createdTime: DeltaDataTypeTimestamp,
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
                        re.partitionColumns.push(
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
                    re.schemaString = record
                        .get_string(i)
                        .map_err(|_| gen_action_type_error("metaData", "schemaString", "string"))?
                        .clone();
                }
                "createdTime" => {
                    re.createdTime = record
                        .get_long(i)
                        .map_err(|_| gen_action_type_error("metaData", "createdTime", "long"))?;
                }
                "configuration" => {
                    let configuration_map = record
                        .get_map(i)
                        .map_err(|_| gen_action_type_error("metaData", "configuration", "map"))?;
                    populate_hashmap_from_parquet_map(&mut re.configuration, configuration_map)
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
                            populate_hashmap_from_parquet_map(&mut options, options_map).map_err(
                                |estr| {
                                    ActionError::InvalidField(format!(
                                        "Invalid format.options for metaData action: {}",
                                        estr,
                                    ))
                                },
                            )?;
                            re.format.options = Some(options);
                        }
                        _ => {
                            re.format.options = None;
                        }
                    }
                }
                _ => {
                    return Err(ActionError::InvalidField(format!(
                        "Unexpected field name for metaData action: {}",
                        name,
                    )));
                }
            }
        }

        Ok(re)
    }

    pub fn get_schema(&self) -> Result<Schema, serde_json::error::Error> {
        serde_json::from_str(&self.schemaString)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Remove {
    pub path: String,
    pub deletionTimestamp: DeltaDataTypeTimestamp,
    pub dataChange: bool,
}

impl Remove {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
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
                    re.dataChange = record
                        .get_bool(i)
                        .map_err(|_| gen_action_type_error("remove", "dataChange", "bool"))?;
                }
                "deletionTimestamp" => {
                    re.deletionTimestamp = record.get_long(i).map_err(|_| {
                        gen_action_type_error("remove", "deletionTimestamp", "long")
                    })?;
                }
                _ => {
                    return Err(ActionError::InvalidField(format!(
                        "Unexpected field name for remove action: {}",
                        name,
                    )));
                }
            }
        }

        Ok(re)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Txn {
    // A unique identifier for the application performing the transaction
    pub appId: String,
    // An application-specific numeric identifier for this transaction
    pub version: DeltaDataTypeVersion,
    // NOTE: undocumented field
    pub lastUpdated: DeltaDataTypeTimestamp,
}

impl Txn {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "appId" => {
                    re.appId = record
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
                    re.lastUpdated = record
                        .get_long(i)
                        .map_err(|_| gen_action_type_error("txn", "lastUpdated", "long"))?;
                }
                _ => {
                    return Err(ActionError::InvalidField(format!(
                        "Unexpected field name for txn action: {}",
                        name,
                    )));
                }
            }
        }

        Ok(re)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Protocol {
    pub minReaderVersion: DeltaDataTypeInt,
    pub minWriterVersion: DeltaDataTypeInt,
}

impl Protocol {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, ActionError> {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "minReaderVersion" => {
                    re.minReaderVersion = record.get_int(i).map_err(|_| {
                        gen_action_type_error("protocol", "minReaderVersion", "int")
                    })?;
                }
                "minWriterVersion" => {
                    re.minWriterVersion = record.get_int(i).map_err(|_| {
                        gen_action_type_error("protocol", "minWriterVersion", "int")
                    })?;
                }
                _ => {
                    return Err(ActionError::InvalidField(format!(
                        "Unexpected field name for protocol action: {}",
                        name,
                    )));
                }
            }
        }

        Ok(re)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Action {
    metaData(MetaData),
    add(Add),
    remove(Remove),
    txn(Txn),
    protocol(Protocol),
    commitInfo(Value),
}

impl Action {
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
            "commitInfo" => {
                unimplemented!("FIXME: support commitInfo");
            }
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
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    #[test]
    fn test_add_action_without_partition_values_and_stats() {
        let path = "./tests/data/delta-0.2.0/_delta_log/00000000000000000003.checkpoint.parquet";
        let preader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();

        let mut iter = preader.get_row_iter(None).unwrap();
        let record = iter.nth(9).unwrap();
        let add_record = record.get_group(1).unwrap();
        let add_action = Add::from_parquet_record(&add_record).unwrap();

        assert_eq!(add_action.partitionValues.len(), 0);
        assert_eq!(add_action.stats, None);
    }

    #[test]
    fn test_load_table_stats() {
        let action = Add {
            stats: Some(
                serde_json::json!({
                    "numRecords": 22,
                    "minValues": {"a": 1, "nested": {"b": 2, "c": "a"}},
                    "maxValues": {"a": 10, "nested": {"b": 20, "c": "z"}},
                    "nullCount": {"a": 1, "nested": {"b": 0, "c": 1}},
                })
                .to_string(),
            ),
            ..Default::default()
        };

        let stats = action.get_stats().unwrap().unwrap();

        assert_eq!(stats.numRecords, 22);

        assert_eq!(
            stats.minValues["a"].as_value().unwrap(),
            &serde_json::json!(1)
        );
        assert_eq!(
            stats.minValues["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(2)
        );
        assert_eq!(
            stats.minValues["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("a")
        );

        assert_eq!(
            stats.maxValues["a"].as_value().unwrap(),
            &serde_json::json!(10)
        );
        assert_eq!(
            stats.maxValues["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(20)
        );
        assert_eq!(
            stats.maxValues["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("z")
        );

        assert_eq!(stats.nullCount["a"].as_value().unwrap(), 1);
        assert_eq!(
            stats.nullCount["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            0
        );
        assert_eq!(
            stats.nullCount["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            1
        );
    }
}
