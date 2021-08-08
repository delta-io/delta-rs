//! Actions included in Delta table transaction logs

#![allow(non_snake_case, non_camel_case_types)]

use std::collections::HashMap;

use parquet::record::{ListAccessor, MapAccessor, RowAccessor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::schema::*;

/// Error returned when an invalid Delta log action is encountered.
#[derive(thiserror::Error, Debug)]
pub enum ActionError {
    /// The action contains an invalid field.
    #[error("Invalid action field: {0}")]
    InvalidField(String),
    /// A parquet log checkpoint file contains an invalid action.
    #[error("Invalid action in parquet row: {0}")]
    InvalidRow(String),
    /// A generic action error. The wrapped error string describes the details.
    #[error("Generic action error: {0}")]
    Generic(String),
}

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
        .or_insert(Some(
            values
                .get_string(j)
                .map_err(|_| "value for HashMap in parquet has to be a string")?
                .clone(),
        ));
    }

    Ok(())
}

fn gen_action_type_error(action: &str, field: &str, expected_type: &str) -> ActionError {
    ActionError::InvalidField(format!(
        "type for {} in {} action should be {}",
        field, action, expected_type
    ))
}

/// Struct used to represent minValues and maxValues in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnValueStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnValueStat>),
    /// Json representation of statistics.
    Value(serde_json::Value),
}

impl ColumnValueStat {
    /// Returns the HashMap representation of the ColumnValueStat.
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnValueStat>> {
        match self {
            ColumnValueStat::Column(m) => Some(m),
            _ => None,
        }
    }

    /// Returns the serde_json representation of the ColumnValueStat.
    pub fn as_value(&self) -> Option<&serde_json::Value> {
        match self {
            ColumnValueStat::Value(v) => Some(v),
            _ => None,
        }
    }
}

/// Struct used to represent nullCount in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnCountStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnCountStat>),
    /// Json representation of statistics.
    Value(DeltaDataTypeLong),
}

impl ColumnCountStat {
    /// Returns the HashMap representation of the ColumnCountStat.
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnCountStat>> {
        match self {
            ColumnCountStat::Column(m) => Some(m),
            _ => None,
        }
    }

    /// Returns the serde_json representation of the ColumnCountStat.
    pub fn as_value(&self) -> Option<DeltaDataTypeLong> {
        match self {
            ColumnCountStat::Value(v) => Some(*v),
            _ => None,
        }
    }
}

/// Statistics associated with Add actions contained in the Delta log.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// Number of records in the file associated with the log action.
    pub num_records: DeltaDataTypeLong,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: HashMap<String, ColumnValueStat>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, ColumnValueStat>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, ColumnCountStat>,
}

/// File stats parsed from raw parquet format.
#[derive(Debug, Default)]
pub struct StatsParsed {
    /// Number of records in the file associated with the log action.
    pub num_records: DeltaDataTypeLong,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: HashMap<String, parquet::record::Field>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, parquet::record::Field>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, DeltaDataTypeLong>,
}

/// Delta log action that describes a parquet data file that is part of the table.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    /// A relative path, from the root of the table, to a file that should be added to the table
    pub path: String,
    /// The size of this file in bytes
    pub size: DeltaDataTypeLong,
    /// A map from partition column to value for this file
    pub partition_values: HashMap<String, Option<String>>,
    /// Partition values stored in raw parquet struct format. In this struct, the column names
    /// correspond to the partition columns and the values are stored in their corresponding data
    /// type. This is a required field when the table is partitioned and the table property
    /// delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
    /// column can be omitted.
    ///
    /// This field is only available in add action records read from checkpoints
    #[serde(skip_serializing, skip_deserializing)]
    pub partition_values_parsed: Option<parquet::record::Row>,
    /// The time this file was created, as milliseconds since the epoch
    pub modification_time: DeltaDataTypeTimestamp,
    /// When false the file must already be present in the table or the records in the added file
    /// must be contained in one or more remove actions in the same version
    ///
    /// streaming queries that are tailing the transaction log can use this flag to skip actions
    /// that would not affect the final results.
    pub data_change: bool,
    /// Contains statistics (e.g., count, min/max values for columns) about the data in this file
    pub stats: Option<String>,
    /// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
    /// raw parquet format. This field needs to be written when statistics are available and the
    /// table property: delta.checkpoint.writeStatsAsStruct is set to true.
    ///
    /// This field is only available in add action records read from checkpoints
    #[serde(skip_serializing, skip_deserializing)]
    pub stats_parsed: Option<parquet::record::Row>,
    /// Map containing metadata about this file
    pub tags: Option<HashMap<String, Option<String>>>,
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
                    let parquetMap = record
                        .get_map(i)
                        .map_err(|_| gen_action_type_error("add", "partitionValues", "map"))?;
                    populate_hashmap_with_option_from_parquet_map(
                        &mut re.partition_values,
                        parquetMap,
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

    /// Returns the serde_json representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    pub fn get_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        self.stats
            .as_ref()
            .map_or(Ok(None), |s| serde_json::from_str(s))
    }

    /// Returns the composite HashMap representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    pub fn get_stats_parsed(&self) -> Result<Option<StatsParsed>, parquet::errors::ParquetError> {
        self.stats_parsed.as_ref().map_or(Ok(None), |record| {
            let mut stats = StatsParsed::default();

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
                                stats.min_values.insert(name.clone(), field.clone());
                            }
                        }
                        _ => {
                            log::error!("Expect type of stats_parsed field minRecords to be struct, got: {}", record);
                        }
                    }
                    "maxValues" => match record.get_group(i) {
                        Ok(row) => {
                            for (name, field) in row.get_column_iter() {
                                stats.max_values.insert(name.clone(), field.clone());
                            }
                        }
                        _ => {
                            log::error!("Expect type of stats_parsed field maxRecords to be struct, got: {}", record);
                        }
                    }
                    "nullCount" => match record.get_group(i) {
                        Ok(row) => {
                            for (i, (name, _)) in row.get_column_iter().enumerate() {
                                match row.get_long(i) {
                                    Ok(v) => {
                                        stats.null_count.insert(name.clone(), v);
                                    }
                                    _ => {
                                        log::error!("Expect type of stats_parsed.nullRecords value to be struct, got: {}", row);
                                    }
                                }
                            }
                        }
                        _ => {
                            log::error!("Expect type of stats_parsed field maxRecords to be struct, got: {}", record);
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

/// Describes the data format of files in the table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Format {
    /// Name of the encoding for files in this table.
    provider: String,
    /// A map containing configuration options for the format.
    options: Option<HashMap<String, Option<String>>>,
}

impl Format {
    /// Allows creation of a new action::Format
    pub fn new(provider: String, options: Option<HashMap<String, Option<String>>>) -> Self {
        Self { provider, options }
    }

    /// Return the Format provider
    pub fn get_provider(self) -> String {
        self.provider
    }
}

// Assuming this is a more appropriate default than derived Default
impl Default for Format {
    fn default() -> Self {
        Self {
            provider: "parquet".to_string(),
            options: Default::default(),
        }
    }
}

/// Action that describes the metadata of the table.
/// This is a top-level action in Delta log entries.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MetaData {
    /// Unique identifier for this table
    pub id: Guid,
    /// User-provided identifier for this table
    pub name: Option<String>,
    /// User-provided description for this table
    pub description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub format: Format,
    /// Schema of the table
    pub schema_string: String,
    /// An array containing the names of columns by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: DeltaDataTypeTimestamp,
    /// A map containing configuration options for the table
    pub configuration: HashMap<String, Option<String>>,
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
                    re.created_time = record
                        .get_long(i)
                        .map_err(|_| gen_action_type_error("metaData", "createdTime", "long"))?;
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
                            re.format.options = Some(options);
                        }
                        _ => {
                            re.format.options = None;
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

    /// Returns the table schema from the embedded schema string contained within the metadata
    /// action.
    pub fn get_schema(&self) -> Result<Schema, serde_json::error::Error> {
        serde_json::from_str(&self.schema_string)
    }
}

/// Represents a tombstone (deleted file) in the Delta log.
/// This is a top-level action in Delta log entries.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// The path of the file that is removed from the table.
    pub path: String,
    /// The timestamp when the remove was added to table state.
    pub deletion_timestamp: DeltaDataTypeTimestamp,
    /// Whether data is changed by the remove. A table optimize will report this as false for
    /// example, since it adds and removes files by combining many files into one.
    pub data_change: bool,
    /// When true the fields partitionValues, size, and tags are present
    pub extended_file_metadata: Option<bool>,
    /// A map from partition column to value for this file.
    pub partition_values: Option<HashMap<String, Option<String>>>,
    /// Size of this file in bytes
    pub size: Option<DeltaDataTypeLong>,
    /// Map containing metadata about this file
    pub tags: Option<HashMap<String, Option<String>>>,
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
                    re.data_change = record
                        .get_bool(i)
                        .map_err(|_| gen_action_type_error("remove", "dataChange", "bool"))?;
                }
                "extendedFileMetadata" => {
                    re.extended_file_metadata = Some(record.get_bool(i).map_err(|_| {
                        gen_action_type_error("remove", "extendedFileMetadata", "bool")
                    })?);
                }
                "deletionTimestamp" => {
                    re.deletion_timestamp = record.get_long(i).map_err(|_| {
                        gen_action_type_error("remove", "deletionTimestamp", "long")
                    })?;
                }
                "partitionValues" => match record.get_map(i) {
                    Ok(_) => {
                        let parquetMap = record.get_map(i).map_err(|_| {
                            gen_action_type_error("remove", "partitionValues", "map")
                        })?;
                        let mut partitionValues = HashMap::new();
                        populate_hashmap_with_option_from_parquet_map(
                            &mut partitionValues,
                            parquetMap,
                        )
                        .map_err(|estr| {
                            ActionError::InvalidField(format!(
                                "Invalid partitionValues for remove action: {}",
                                estr,
                            ))
                        })?;
                        re.partition_values = Some(partitionValues);
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
                    re.size = Some(
                        record
                            .get_long(i)
                            .map_err(|_| gen_action_type_error("remove", "size", "long"))?,
                    );
                }
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

/// Action used by streaming systems to track progress using application-specific versions to
/// enable idempotency.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Txn {
    /// A unique identifier for the application performing the transaction.
    pub app_id: String,
    /// An application-specific numeric identifier for this transaction.
    pub version: DeltaDataTypeVersion,
    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    pub last_updated: Option<DeltaDataTypeTimestamp>,
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
                    re.last_updated = Some(
                        record
                            .get_long(i)
                            .map_err(|_| gen_action_type_error("txn", "lastUpdated", "long"))?,
                    );
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

/// Action used to increase the version of the Delta protocol required to read or write to the
/// table.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// Minimum version of the Delta read protocol a client must implement to correctly read the
    /// table.
    pub min_reader_version: DeltaDataTypeInt,
    /// Minimum version of the Delta write protocol a client must implement to correctly read the
    /// table.
    pub min_writer_version: DeltaDataTypeInt,
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

/// Represents an action in the Delta log. The Delta log is an aggregate of all actions performed
/// on the table, so the full list of actions is required to properly read a table.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    /// Changes the current metadata of the table. Must be present in the first version of a table.
    /// Subsequent `metaData` actions completely overwrite previous metadata.
    metaData(MetaData),
    /// Adds a file to the table state.
    add(Add),
    /// Removes a file from the table state.
    remove(Remove),
    /// Used by streaming systems to track progress externally with application specific version
    /// identifiers.
    txn(Txn),
    /// Describes the minimum reader and writer versions required to read or write to the table.
    protocol(Protocol),
    /// Describes commit provenance information for the table.
    commitInfo(Value),
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

/// Operation performed when creating a new log entry with one or more actions.
/// This is a key element of the `CommitInfo` action.
#[derive(Serialize, Deserialize, Debug)]
pub enum DeltaOperation {
    /// Represents a Delta `Write` operation.
    /// Write operations will typically only include `Add` actions.
    Write {
        /// The save mode used during the write.
        mode: SaveMode,
        /// The columns the write is partitioned by.
        partitionBy: Option<Vec<String>>,
        /// The predicate used during the write.
        predicate: Option<String>,
    },
    /// Represents a Delta `StreamingUpdate` operation.
    StreamingUpdate {
        /// The output mode the streaming writer is using.
        outputMode: OutputMode,
        /// The query id of the streaming writer.
        queryId: String,
        /// The epoch id of the written micro-batch.
        epochId: i64,
    },
    // TODO: Add more operations
}

/// The SaveMode used when performing a DeltaOperation
#[derive(Serialize, Deserialize, Debug)]
pub enum SaveMode {
    /// Files will be appended to the target location.
    Append,
    /// The target location will be overwritten.
    Overwrite,
    /// If files exist for the target, the operation must fail.
    ErrorIfExists,
    /// If files exist for the target, the operation must not proceed or change any data.
    Ignore,
}

/// The OutputMode used in streaming operations.
#[derive(Serialize, Deserialize, Debug)]
pub enum OutputMode {
    /// Only new rows will be written when new data is available.
    Append,
    /// The full output (all rows) will be written whenever new data is available.
    Complete,
    /// Only rows with updates will be written when new or changed data is available.
    Update,
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

        assert_eq!(add_action.partition_values.len(), 0);
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

        assert_eq!(stats.num_records, 22);

        assert_eq!(
            stats.min_values["a"].as_value().unwrap(),
            &serde_json::json!(1)
        );
        assert_eq!(
            stats.min_values["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(2)
        );
        assert_eq!(
            stats.min_values["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("a")
        );

        assert_eq!(
            stats.max_values["a"].as_value().unwrap(),
            &serde_json::json!(10)
        );
        assert_eq!(
            stats.max_values["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(20)
        );
        assert_eq!(
            stats.max_values["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("z")
        );

        assert_eq!(stats.null_count["a"].as_value().unwrap(), 1);
        assert_eq!(
            stats.null_count["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            0
        );
        assert_eq!(
            stats.null_count["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            1
        );
    }
}
