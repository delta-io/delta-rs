// Reference: https://github.com/delta-io/delta/blob/master/PROTOCOL.md

#![allow(non_snake_case, non_camel_case_types)]

use std::collections::HashMap;
use std::fmt;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;

use chrono;
use chrono::{DateTime, FixedOffset, Utc};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{ListAccessor, MapAccessor, RowAccessor};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror;

use super::storage;
use super::storage::{StorageBackend, StorageError, UriError};

pub type GUID = String;
pub type DeltaDataTypeLong = i64;
pub type DeltaDataTypeVersion = DeltaDataTypeLong;
pub type DeltaDataTypeTimestamp = DeltaDataTypeLong;
pub type DeltaDataTypeInt = i32;

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    version: DeltaDataTypeVersion, // 20 digits decimals
    size: DeltaDataTypeLong,
    parts: Option<u32>, // 10 digits decimals
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Format {
    // Name of the encoding for files in this table
    provider: String,
    // A map containing configuration options for the format
    options: Option<HashMap<String, String>>,
}

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SchemaTypeStruct {
    // type field is alwsy the string "struct", so we are ignoring it here
    r#type: String,
    fields: Vec<SchemaField>,
}

impl SchemaTypeStruct {
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeltaTableError {
    #[error("Failed to apply transaction log: {}", .source)]
    ApplyLog {
        #[from]
        source: ApplyLogError,
    },
    #[error("Failed to load checkpoint: {}", .source)]
    LoadCheckpoint {
        #[from]
        source: LoadCheckpointError,
    },
    #[error("Failed to read delta log object: {}", .source)]
    StorageError {
        #[from]
        source: StorageError,
    },
    #[error("Failed to read checkpoint: {}", .source)]
    ParquetError {
        #[from]
        source: ParquetError,
    },
    #[error("Invalid table path: {}", .source)]
    UriError {
        #[from]
        source: UriError,
    },
    #[error("Invalid JSON in log record: {}", .source)]
    InvalidJSON {
        #[from]
        source: serde_json::error::Error,
    },
    #[error("Invalid table version: {0}")]
    InvalidVersion(DeltaDataTypeVersion),
    #[error("Corrupted table, cannot read data file {}: {}", .path, .source)]
    MissingDataFile {
        source: std::io::Error,
        path: String,
    },
    #[error("Invalid action record found in log: {0}")]
    InvalidAction(String),
    #[error("Invalid datetime string: {}", .source)]
    InvalidDateTimeSTring {
        #[from]
        source: chrono::ParseError,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaField {
    // Name of this (possibly nested) column
    name: String,
    // String containing the name of a primitive type, a struct definition, an array definition or
    // a map definition
    r#type: SchemaDataType,
    // Boolean denoting whether this field can be null
    nullable: bool,
    // A JSON map containing information about this column. Keys prefixed with Delta are reserved
    // for the implementation.
    metadata: HashMap<String, String>,
}

impl SchemaField {
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type(&self) -> &SchemaDataType {
        &self.r#type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn get_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaTypeArray {
    // type field is alwsy the string "array", so we are ignoring it here
    r#type: String,
    // The type of element stored in this array represented as a string containing the name of a
    // primitive type, a struct definition, an array definition or a map definition
    elementType: Box<SchemaDataType>,
    // Boolean denoting whether this array can contain one or more null values
    containsNull: bool,
}

impl SchemaTypeArray {
    pub fn get_element_type(&self) -> &SchemaDataType {
        &self.elementType
    }

    pub fn does_contain_null(&self) -> bool {
        self.containsNull
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaTypeMap {
    r#type: String,
    // The type of element used for the key of this map, represented as a string containing the
    // name of a primitive type, a struct definition, an array definition or a map definition
    keyType: Box<SchemaDataType>,
    // The type of element used for the key of this map, represented as a string containing the
    // name of a primitive type, a struct definition, an array definition or a map definition
    valueType: Box<SchemaDataType>,
}

impl SchemaTypeMap {
    pub fn get_key_type(&self) -> &SchemaDataType {
        &self.keyType
    }

    pub fn get_value_type(&self) -> &SchemaDataType {
        &self.valueType
    }
}

/*
 * List of primitive types:
 *   string: utf8
 *   long  // undocumented, i64?
 *   integer: i32
 *   short: i16
 *   byte: i8
 *   float: f32
 *   double: f64
 *   boolean: bool
 *   binary: a sequence of binary data
 *   date: A calendar date, represented as a year-month-day triple without a timezone
 *   timestamp: Microsecond precision timestamp without a timezone
 */
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SchemaDataType {
    primitive(String),
    r#struct(SchemaTypeStruct),
    array(SchemaTypeArray),
    map(SchemaTypeMap),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    r#type: String,
    fields: Vec<SchemaField>,
}

impl Schema {
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }
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

fn gen_action_type_error(action: &str, field: &str, expected_type: &str) -> DeltaTableError {
    DeltaTableError::InvalidAction(format!(
        "type for {} in {} action should be {}",
        field, action, expected_type
    ))
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ActionAdd {
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

impl ActionAdd {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, DeltaTableError> {
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
                    let key = parquetMap
                        .get_keys()
                        .get_string(0)
                        .map_err(|_| gen_action_type_error("add", "partitionValues.key", "string"))?
                        .clone();
                    let value = parquetMap
                        .get_values()
                        .get_string(0)
                        .map_err(|_| {
                            gen_action_type_error("add", "partitionValues.value", "string")
                        })?
                        .clone();
                    re.partitionValues.entry(key).or_insert(value);
                }
                "tags" => match record.get_map(i) {
                    Ok(tags_map) => {
                        let mut tags = HashMap::new();
                        populate_hashmap_from_parquet_map(&mut tags, tags_map).map_err(|estr| {
                            DeltaTableError::InvalidAction(format!(
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
                "stats" => {
                    re.stats = Some(
                        record
                            .get_string(i)
                            .map_err(|_| gen_action_type_error("add", "stats", "string"))?
                            .clone(),
                    );
                }
                _ => {
                    return Err(DeltaTableError::InvalidAction(format!(
                        "Unexpected field name for add action: {}",
                        name,
                    )));
                }
            }
        }

        return Ok(re);
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ActionMetaData {
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

impl ActionMetaData {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, DeltaTableError> {
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
                            DeltaTableError::InvalidAction(format!(
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
                                    DeltaTableError::InvalidAction(format!(
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
                    return Err(DeltaTableError::InvalidAction(format!(
                        "Unexpected field name for metaData action: {}",
                        name,
                    )));
                }
            }
        }

        return Ok(re);
    }

    fn get_schema(&self) -> Result<Schema, serde_json::error::Error> {
        serde_json::from_str(&self.schemaString)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct ActionRemove {
    pub path: String,
    pub deletionTimestamp: DeltaDataTypeTimestamp,
    pub dataChange: bool,
}

impl ActionRemove {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, DeltaTableError> {
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
                    return Err(DeltaTableError::InvalidAction(format!(
                        "Unexpected field name for remove action: {}",
                        name,
                    )));
                }
            }
        }

        return Ok(re);
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ActionTxn {
    // A unique identifier for the application performing the transaction
    pub appId: String,
    // An application-specific numeric identifier for this transaction
    pub version: DeltaDataTypeVersion,
    // NOTE: undocumented field
    pub lastUpdated: DeltaDataTypeTimestamp,
}

impl ActionTxn {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, DeltaTableError> {
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
                    return Err(DeltaTableError::InvalidAction(format!(
                        "Unexpected field name for txn action: {}",
                        name,
                    )));
                }
            }
        }

        return Ok(re);
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ActionProtocol {
    pub minReaderVersion: DeltaDataTypeInt,
    pub minWriterVersion: DeltaDataTypeInt,
}

impl ActionProtocol {
    fn from_parquet_record(record: &parquet::record::Row) -> Result<Self, DeltaTableError> {
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
                    return Err(DeltaTableError::InvalidAction(format!(
                        "Unexpected field name for protocol action: {}",
                        name,
                    )));
                }
            }
        }

        return Ok(re);
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Action {
    metaData(ActionMetaData),
    add(ActionAdd),
    remove(ActionRemove),
    txn(ActionTxn),
    protocol(ActionProtocol),
    commitInfo(Value),
}

impl Action {
    fn from_parquet_record(
        schema: &parquet::schema::types::Type,
        record: &parquet::record::Row,
    ) -> Result<Self, DeltaTableError> {
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
                    return Err(DeltaTableError::InvalidAction(
                        "Parquet action row only contains null columns".to_string(),
                    ));
                }
            }
        };

        let fields = schema.get_fields();
        let field = &fields[col_idx];

        return Ok(match field.get_basic_info().name() {
            "add" => Action::add(ActionAdd::from_parquet_record(col_data)?),
            "metaData" => Action::metaData(ActionMetaData::from_parquet_record(col_data)?),
            "remove" => Action::remove(ActionRemove::from_parquet_record(col_data)?),
            "txn" => Action::txn(ActionTxn::from_parquet_record(col_data)?),
            "protocol" => Action::protocol(ActionProtocol::from_parquet_record(col_data)?),
            "commitInfo" => {
                unimplemented!("FIXME: support commitInfo");
            }
            name @ _ => {
                return Err(DeltaTableError::InvalidAction(format!(
                    "Unexpected action from checkpoint: {}",
                    name,
                )));
            }
        });
    }
}

pub struct DeltaTableMetaData {
    // Unique identifier for this table
    pub id: GUID,
    // User-provided identifier for this table
    pub name: Option<String>,
    // User-provided description for this table
    pub description: Option<String>,
    // Specification of the encoding for the files stored in the table
    pub format: Format,
    // Schema of the table
    pub schema: Schema,
    // An array containing the names of columns by which the data should be partitioned
    pub partitionColumns: Vec<String>,
    // NOTE: this field is undocumented
    pub configuration: HashMap<String, String>,
}

impl fmt::Display for DeltaTableMetaData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GUID={}, name={:?}, description={:?}, partitionColumns={:?}, configuration={:?}",
            self.id, self.name, self.description, self.partitionColumns, self.configuration
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ApplyLogError {
    #[error("End of transaction log")]
    EndOfLog,
    #[error("Invalid JSON in log record")]
    InvalidJSON {
        #[from]
        source: serde_json::error::Error,
    },
    #[error("Failed to read log content")]
    Storage { source: StorageError },
    #[error("Failed to read line from log record")]
    IO {
        #[from]
        source: std::io::Error,
    },
}

impl From<StorageError> for ApplyLogError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => ApplyLogError::EndOfLog,
            _ => ApplyLogError::Storage { source: error },
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LoadCheckpointError {
    #[error("Checkpoint file not found")]
    NotFound,
    #[error("Invalid JSON in checkpoint")]
    InvalidJSON {
        #[from]
        source: serde_json::error::Error,
    },
    #[error("Failed to read checkpoint content")]
    Storage { source: StorageError },
}

impl From<StorageError> for LoadCheckpointError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => LoadCheckpointError::NotFound,
            _ => LoadCheckpointError::Storage { source: error },
        }
    }
}

pub struct DeltaTable {
    pub version: DeltaDataTypeVersion,
    // A remove action should remain in the state of the table as a tombstone until it has expired
    // vacuum operation is responsible for providing the retention threshold
    pub tombstones: Vec<ActionRemove>,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub table_path: String,

    // metadata
    // application_transactions
    storage: Box<dyn StorageBackend>,

    files: Vec<String>,
    app_transaction_version: HashMap<String, DeltaDataTypeVersion>,
    commit_infos: Vec<Value>,
    current_metadata: Option<DeltaTableMetaData>,
    last_check_point: Option<CheckPoint>,
    log_path: String,
    version_timestamp: HashMap<DeltaDataTypeVersion, i64>,
}

impl DeltaTable {
    fn version_to_log_path(&self, version: DeltaDataTypeVersion) -> String {
        return format!("{}/{:020}.json", self.log_path, version);
    }

    fn get_checkpoint_data_paths(&self, check_point: &CheckPoint) -> Vec<String> {
        let checkpoint_prefix = format!("{}/{:020}", self.log_path, check_point.version);
        let mut checkpoint_data_paths = Vec::new();

        match check_point.parts {
            None => {
                checkpoint_data_paths.push(format!("{}.checkpoint.parquet", checkpoint_prefix));
            }
            Some(parts) => {
                for i in 0..parts {
                    checkpoint_data_paths.push(format!(
                        "{}.checkpoint.{:010}.{:010}.parquet",
                        checkpoint_prefix,
                        i + 1,
                        parts
                    ));
                }
            }
        }

        return checkpoint_data_paths;
    }

    fn get_last_checkpoint(&self) -> Result<CheckPoint, LoadCheckpointError> {
        let last_checkpoint_path = format!("{}/_last_checkpoint", self.log_path);
        let data = self.storage.get_obj(&last_checkpoint_path)?;
        return Ok(serde_json::from_slice(&data)?);
    }

    fn process_action(&mut self, action: &Action) -> Result<(), serde_json::error::Error> {
        match action {
            Action::add(v) => {
                self.files.push(v.path.clone());
            }
            Action::remove(v) => {
                self.files.retain(|e| *e != v.path);
                self.tombstones.push(v.clone());
            }
            Action::protocol(v) => {
                self.min_reader_version = v.minReaderVersion;
                self.min_writer_version = v.minWriterVersion;
            }
            Action::metaData(v) => {
                self.current_metadata = Some(DeltaTableMetaData {
                    id: v.id.clone(),
                    name: v.name.clone(),
                    description: v.description.clone(),
                    format: v.format.clone(),
                    schema: v.get_schema()?,
                    partitionColumns: v.partitionColumns.clone(),
                    configuration: v.configuration.clone(),
                });
            }
            Action::txn(v) => {
                self.app_transaction_version
                    .entry(v.appId.clone())
                    .or_insert(v.version);
            }
            Action::commitInfo(v) => {
                self.commit_infos.push(v.clone());
            }
        }

        Ok(())
    }

    fn find_check_point_before_version(
        &self,
        version: DeltaDataTypeVersion,
    ) -> Result<Option<CheckPoint>, DeltaTableError> {
        let mut cp: Option<CheckPoint> = None;
        let re_checkpoint = Regex::new(r"^*/_delta_log/(\d{20})\.checkpoint\.parquet$").unwrap();
        let re_checkpoint_parts =
            Regex::new(r"^*/_delta_log/(\d{20})\.checkpoint\.\d{10}\.(\d{10})\.parquet$").unwrap();

        for obj_meta in self.storage.list_objs(&self.log_path)? {
            match re_checkpoint.captures(&obj_meta.path) {
                Some(captures) => {
                    let curr_ver_str = captures.get(1).unwrap().as_str();
                    let curr_ver: DeltaDataTypeVersion = curr_ver_str.parse().unwrap();
                    if curr_ver > version {
                        // skip checkpoints newer than max version
                        continue;
                    }
                    if cp.is_none() || curr_ver > cp.unwrap().version {
                        cp = Some(CheckPoint {
                            version: curr_ver,
                            size: 0,
                            parts: None,
                        });
                    }
                    continue;
                }
                None => {}
            }

            match re_checkpoint_parts.captures(&obj_meta.path) {
                Some(captures) => {
                    let curr_ver_str = captures.get(1).unwrap().as_str();
                    let curr_ver: DeltaDataTypeVersion = curr_ver_str.parse().unwrap();
                    if curr_ver > version {
                        // skip checkpoints newer than max version
                        continue;
                    }
                    if cp.is_none() || curr_ver > cp.unwrap().version {
                        let parts_str = captures.get(2).unwrap().as_str();
                        let parts = parts_str.parse().unwrap();
                        cp = Some(CheckPoint {
                            version: curr_ver,
                            size: 0,
                            parts: Some(parts),
                        });
                    }
                    continue;
                }
                None => {}
            }
        }

        return Ok(cp);
    }

    fn apply_log_from_bufread<R: BufRead>(
        &mut self,
        reader: BufReader<R>,
    ) -> Result<(), ApplyLogError> {
        for line in reader.lines() {
            let action: Action = serde_json::from_str(line?.as_str())?;
            self.process_action(&action)?;
        }
        return Ok(());
    }

    fn apply_log(&mut self, version: DeltaDataTypeVersion) -> Result<(), ApplyLogError> {
        let log_path = self.version_to_log_path(version);
        let commit_log_bytes = self.storage.get_obj(&log_path)?;
        let reader = BufReader::new(Cursor::new(commit_log_bytes));
        return self.apply_log_from_bufread(reader);
    }

    fn restore_checkpoint(&mut self, check_point: CheckPoint) -> Result<(), DeltaTableError> {
        let checkpoint_data_paths = self.get_checkpoint_data_paths(&check_point);
        // process actions from checkpoint
        for f in &checkpoint_data_paths {
            let obj = self.storage.get_obj(&f)?;
            let preader = SerializedFileReader::new(Cursor::new(obj))?;
            let schema = preader.metadata().file_metadata().schema();
            if !schema.is_group() {
                return Err(DeltaTableError::InvalidAction(format!(
                    "Action record in checkpoint should be a struct"
                )));
            }
            let mut iter = preader.get_row_iter(None)?;
            while let Some(record) = iter.next() {
                self.process_action(&Action::from_parquet_record(&schema, &record)?)?;
            }
        }

        Ok(())
    }

    fn get_latest_version(&mut self) -> Result<DeltaDataTypeVersion, DeltaTableError> {
        let mut version = match self.get_last_checkpoint() {
            Ok(last_check_point) => last_check_point.version,
            Err(LoadCheckpointError::NotFound) => {
                // no checkpoint, start with version 0
                0
            }
            Err(e) => {
                return Err(DeltaTableError::LoadCheckpoint { source: e });
            }
        };

        // scan logs after checkpoint
        loop {
            match self.storage.head_obj(&self.version_to_log_path(version)) {
                Ok(meta) => {
                    // also cache timestamp for version
                    self.version_timestamp
                        .insert(version, meta.modified.timestamp());
                    version += 1;
                }
                Err(e) => {
                    match e {
                        StorageError::NotFound => {
                            version -= 1;
                        }
                        _ => return Err(DeltaTableError::from(e)),
                    }
                    break;
                }
            }
        }

        Ok(version)
    }

    pub fn load(&mut self) -> Result<(), DeltaTableError> {
        match self.get_last_checkpoint() {
            Ok(last_check_point) => {
                self.last_check_point = Some(last_check_point);
                self.restore_checkpoint(last_check_point)?;
                self.version = last_check_point.version;
            }
            Err(LoadCheckpointError::NotFound) => {
                // no checkpoint, start with version 0
                self.version = 0;
            }
            Err(e) => {
                return Err(DeltaTableError::LoadCheckpoint { source: e });
            }
        }

        // replay logs after checkpoint
        loop {
            match self.apply_log(self.version) {
                Ok(_) => {
                    self.version += 1;
                }
                Err(e) => {
                    match e {
                        ApplyLogError::EndOfLog => {
                            self.version -= 1;
                        }
                        _ => return Err(DeltaTableError::from(e)),
                    }
                    break;
                }
            }
        }

        return Ok(());
    }

    pub fn load_version(&mut self, version: DeltaDataTypeVersion) -> Result<(), DeltaTableError> {
        let last_log_reader;
        let log_path = self.version_to_log_path(version);
        // check if version is valid
        match self.storage.get_obj(&log_path) {
            Ok(commit_log_bytes) => {
                last_log_reader = BufReader::new(Cursor::new(commit_log_bytes));
            }
            Err(StorageError::NotFound) => {
                return Err(DeltaTableError::InvalidVersion(version));
            }
            Err(e) => {
                return Err(DeltaTableError::from(e));
            }
        }
        self.version = version;

        if version > 0 {
            let mut next_version;
            let max_version = version - 1;
            // 1. find latest checkpoint below version
            match self.find_check_point_before_version(version)? {
                Some(check_point) => {
                    self.restore_checkpoint(check_point)?;
                    next_version = check_point.version;
                }
                None => {
                    next_version = 0;
                }
            }

            // 2. apply all logs starting from checkpoint
            while next_version <= max_version {
                self.apply_log(next_version)?;
                next_version += 1;
            }
        }

        self.apply_log_from_bufread(last_log_reader)?;
        return Ok(());
    }

    fn get_version_timestamp(
        &mut self,
        version: DeltaDataTypeVersion,
    ) -> Result<i64, DeltaTableError> {
        match self.version_timestamp.get(&version) {
            Some(ts) => Ok(ts.clone()),
            None => {
                let meta = self.storage.head_obj(&self.version_to_log_path(version))?;
                let ts = meta.modified.timestamp();
                // also cache timestamp for version
                self.version_timestamp.insert(version, ts);

                Ok(ts)
            }
        }
    }

    pub fn get_files(&self) -> &Vec<String> {
        &self.files
    }

    pub fn get_file_paths(&self) -> Vec<String> {
        self.files
            .iter()
            .map(|fname| {
                let table_path = Path::new(&self.table_path);
                table_path
                    .join(fname)
                    .into_os_string()
                    .into_string()
                    .unwrap()
            })
            .collect()
    }

    pub fn get_tombstones(&self) -> &Vec<ActionRemove> {
        &self.tombstones
    }

    pub fn get_app_transaction_version(&self) -> &HashMap<String, DeltaDataTypeVersion> {
        &self.app_transaction_version
    }

    pub fn schema(&self) -> Option<&Schema> {
        match &self.current_metadata {
            Some(meta) => Some(&meta.schema),
            None => None,
        }
    }

    pub fn new(
        table_path: &str,
        storage_backend: Box<dyn StorageBackend>,
    ) -> Result<Self, DeltaTableError> {
        Ok(Self {
            version: 0,
            files: Vec::new(),
            storage: storage_backend,
            tombstones: Vec::new(),
            table_path: table_path.to_string(),
            min_reader_version: 0,
            min_writer_version: 0,
            current_metadata: None,
            commit_infos: Vec::new(),
            app_transaction_version: HashMap::new(),
            last_check_point: None,
            log_path: format!("{}/_delta_log", table_path),
            version_timestamp: HashMap::new(),
        })
    }

    pub fn load_with_datetime(&mut self, datetime: DateTime<Utc>) -> Result<(), DeltaTableError> {
        let mut min_version = 0;
        let mut max_version = self.get_latest_version()?;
        let mut version = min_version;
        let target_ts = datetime.timestamp();

        // binary search
        while min_version <= max_version {
            let pivot = (max_version + min_version) / 2;
            version = pivot;
            let pts = self.get_version_timestamp(pivot)?;

            if pts == target_ts {
                break;
            } else if pts < target_ts {
                min_version = pivot + 1;
            } else {
                max_version = pivot - 1;
                version = max_version
            }
        }

        if version < 0 {
            version = 0;
        }

        self.load_version(version)
    }
}

impl fmt::Display for DeltaTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaTable({})\n", self.table_path)?;
        write!(f, "\tversion: {}\n", self.version)?;
        match self.current_metadata.as_ref() {
            Some(metadata) => {
                write!(f, "\tmetadata: {}\n", metadata)?;
            }
            None => {
                write!(f, "\tmetadata: None\n")?;
            }
        }
        write!(
            f,
            "\tmin_version: read={}, write={}\n",
            self.min_reader_version, self.min_writer_version
        )?;
        write!(f, "\tfiles count: {}\n", self.files.len())
    }
}

pub fn open_table(table_path: &str) -> Result<DeltaTable, DeltaTableError> {
    let storage_backend = storage::get_backend_for_uri(table_path)?;
    let mut table = DeltaTable::new(table_path, storage_backend)?;
    table.load()?;

    Ok(table)
}

pub fn open_table_with_version(
    table_path: &str,
    version: DeltaDataTypeVersion,
) -> Result<DeltaTable, DeltaTableError> {
    let storage_backend = storage::get_backend_for_uri(table_path)?;
    let mut table = DeltaTable::new(table_path, storage_backend)?;
    table.load_version(version)?;

    Ok(table)
}

pub fn open_table_with_ds(table_path: &str, ds: &str) -> Result<DeltaTable, DeltaTableError> {
    let datetime = DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(ds)?);
    let storage_backend = storage::get_backend_for_uri(table_path)?;
    let mut table = DeltaTable::new(table_path, storage_backend)?;
    table.load_with_datetime(datetime)?;

    Ok(table)
}
