// Reference: https://github.com/delta-io/delta/blob/master/PROTOCOL.md

#![allow(non_snake_case, non_camel_case_types)]

extern crate parquet;

use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{ListAccessor, MapAccessor, RowAccessor};

use regex::Regex;
use std::collections::HashMap;
use std::fmt;
use std::io::{BufRead, BufReader, Cursor};

use thiserror;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::storage;
use super::storage::{StorageBackend, StorageError, UriError};

pub type GUID = String;
pub type DeltaDataTypeLong = i64;
pub type DeltaVersionType = DeltaDataTypeLong;
pub type DeltaDataTypeInt = i32;

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    version: DeltaVersionType, // 20 digits decimals
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
    fields: Vec<SchemaTypeStructField>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaTypeStructField {
    // Name of this (possibly nested) column
    name: String,
    // String containing the name of a primitive type, a struct definition, an array definition or
    // a map definition
    r#type: SchemaType,
    // Boolean denoting whether this field can be null
    nullable: bool,
    // A JSON map containing information about this column. Keys prefixed with Delta are reserved
    // for the implementation.
    metadata: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaTypeArray {
    // type field is alwsy the string "array", so we are ignoring it here
    r#type: String,
    // The type of element stored in this array represented as a string containing the name of a
    // primitive type, a struct definition, an array definition or a map definition
    elementType: Box<SchemaType>,
    // Boolean denoting whether this array can contain one or more null values
    containsNull: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaTypeMap {
    r#type: String,
    // The type of element used for the key of this map, represented as a string containing the
    // name of a primitive type, a struct definition, an array definition or a map definition
    keyType: Box<SchemaType>,
    // The type of element used for the key of this map, represented as a string containing the
    // name of a primitive type, a struct definition, an array definition or a map definition
    valueType: Box<SchemaType>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SchemaType {
    primitive(String),
    r#struct(SchemaTypeStruct),
    array(SchemaTypeArray),
    map(SchemaTypeMap),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    r#type: String,
    fields: Vec<SchemaTypeStructField>,
}

fn populate_hashmap_from_parquet_map(
    map: &mut HashMap<String, String>,
    pmap: &parquet::record::Map,
) {
    let keys = pmap.get_keys();
    let values = pmap.get_values();
    for j in 0..pmap.len() {
        map.entry(keys.get_string(j).unwrap().clone())
            .or_insert(values.get_string(j).unwrap().clone());
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ActionAdd {
    // A relative path, from the root of the table, to a file that should be added to the table
    path: String,
    // The size of this file in bytes
    size: DeltaDataTypeLong,
    // A map from partition column to value for this file
    partitionValues: HashMap<String, String>,
    // The time this file was created, as milliseconds since the epoch
    modificationTime: DeltaDataTypeLong,
    // When false the file must already be present in the table or the records in the added file
    // must be contained in one or more remove actions in the same version
    dataChange: bool,
    // Contains statistics (e.g., count, min/max values for columns) about the data in this file
    stats: Option<String>,
    // Map containing metadata about this file
    tags: Option<HashMap<String, String>>,
}

impl ActionAdd {
    fn from_parquet_record(record: &parquet::record::Row) -> Self {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "path" => {
                    re.path = record.get_string(i).unwrap().clone();
                }
                "size" => {
                    re.size = record.get_long(i).unwrap();
                }
                "modificationTime" => {
                    re.modificationTime = record.get_long(i).unwrap();
                }
                "dataChange" => {
                    re.dataChange = record.get_bool(i).unwrap();
                }
                "partitionValues" => {
                    let parquetMap = record.get_map(i).unwrap();
                    let key = parquetMap.get_keys().get_string(0).unwrap().clone();
                    let value = parquetMap.get_values().get_string(0).unwrap().clone();
                    re.partitionValues.entry(key).or_insert(value);
                }
                "tags" => match record.get_map(i) {
                    Ok(tags_map) => {
                        let mut tags = HashMap::new();
                        populate_hashmap_from_parquet_map(&mut tags, tags_map);
                        re.tags = Some(tags);
                    }
                    _ => {
                        re.tags = None;
                    }
                },
                "stats" => {
                    re.stats = Some(record.get_string(i).unwrap().clone());
                }
                _ => {
                    panic!("invalid add record field: {}", name);
                }
            }
        }

        return re;
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ActionMetaData {
    // Unique identifier for this table
    id: GUID,
    // User-provided identifier for this table
    name: Option<String>,
    // User-provided description for this table
    description: Option<String>,
    // Specification of the encoding for the files stored in the table
    format: Format,
    // Schema of the table
    schemaString: String,
    // An array containing the names of columns by which the data should be partitioned
    partitionColumns: Vec<String>,
    // NOTE: this field is undocumented
    configuration: HashMap<String, String>,
    // NOTE: this field is undocumented
    createdTime: DeltaDataTypeLong,
}

impl ActionMetaData {
    fn from_parquet_record(record: &parquet::record::Row) -> Self {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "id" => {
                    re.id = record.get_string(i).unwrap().clone();
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
                    let columns_list = record.get_list(i).unwrap();
                    for j in 0..columns_list.len() {
                        re.partitionColumns
                            .push(columns_list.get_string(j).unwrap().clone());
                    }
                }
                "schemaString" => {
                    re.schemaString = record.get_string(i).unwrap().clone();
                }
                "createdTime" => {
                    re.createdTime = record.get_long(i).unwrap();
                }
                "configuration" => {
                    let configuration_map = record.get_map(i).unwrap();
                    populate_hashmap_from_parquet_map(&mut re.configuration, configuration_map);
                }
                "format" => {
                    let format_record = record.get_group(i).unwrap();
                    re.format.provider = format_record.get_string(0).unwrap().clone();
                    match record.get_map(1) {
                        Ok(options_map) => {
                            let mut options = HashMap::new();
                            populate_hashmap_from_parquet_map(&mut options, options_map);
                            re.format.options = Some(options);
                        }
                        _ => {
                            re.format.options = None;
                        }
                    }
                }
                _ => {
                    panic!("invalid protocol record field: {}", name);
                }
            }
        }

        return re;
    }

    fn get_schema(&self) -> Result<Schema, serde_json::error::Error> {
        serde_json::from_str(&self.schemaString)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ActionRemove {
    path: String,
    deletionTimestamp: DeltaDataTypeLong,
    dataChange: bool,
}

impl ActionRemove {
    fn from_parquet_record(record: &parquet::record::Row) -> Self {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "path" => {
                    re.path = record.get_string(i).unwrap().clone();
                }
                "dataChange" => {
                    re.dataChange = record.get_bool(i).unwrap();
                }
                "deletionTimestamp" => {
                    re.deletionTimestamp = record.get_long(i).unwrap();
                }
                _ => {
                    panic!("invalid remove record field: {}", name);
                }
            }
        }

        return re;
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ActionTxn {
    appId: String,
    version: DeltaVersionType,
    // NOTE: undocumented field
    lastUpdated: DeltaDataTypeLong,
}

impl ActionTxn {
    fn from_parquet_record(record: &parquet::record::Row) -> Self {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "appId" => {
                    re.appId = record.get_string(i).unwrap().clone();
                }
                "version" => {
                    re.version = record.get_long(i).unwrap();
                }
                "lastUpdated" => {
                    re.lastUpdated = record.get_long(i).unwrap();
                }
                _ => {
                    panic!("invalid txn record field: {}", name);
                }
            }
        }

        return re;
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ActionProtocol {
    minReaderVersion: DeltaDataTypeInt,
    minWriterVersion: DeltaDataTypeInt,
}

impl ActionProtocol {
    fn from_parquet_record(record: &parquet::record::Row) -> Self {
        let mut re = Self {
            ..Default::default()
        };

        for (i, (name, _)) in record.get_column_iter().enumerate() {
            match name.as_str() {
                "minReaderVersion" => {
                    re.minReaderVersion = record.get_int(i).unwrap();
                }
                "minWriterVersion" => {
                    re.minWriterVersion = record.get_int(i).unwrap();
                }
                _ => {
                    panic!("invalid protocol record");
                }
            }
        }

        return re;
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
    ) -> Self {
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
            match col_data {
                Some(group) => (col_idx.unwrap(), group),
                None => {
                    panic!("FIXME: invalid record");
                }
            }
        };

        let fields = schema.get_fields();
        let field = &fields[col_idx];
        match field.get_basic_info().name() {
            "add" => {
                return Action::add(ActionAdd::from_parquet_record(col_data));
            }
            "metaData" => {
                return Action::metaData(ActionMetaData::from_parquet_record(col_data));
            }
            "remove" => {
                return Action::remove(ActionRemove::from_parquet_record(col_data));
            }
            "txn" => {
                return Action::txn(ActionTxn::from_parquet_record(col_data));
            }
            "protocol" => {
                return Action::protocol(ActionProtocol::from_parquet_record(col_data));
            }
            "commitInfo" => {
                panic!("FIXME: implement commitInfo");
            }
            _ => {
                panic!("FIXME: invalid action: {:#?}", field);
            }
        }
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
    InvalidVersion(DeltaVersionType),
    #[error("Corrupted table, cannot read data file {}: {}", .path, .source)]
    MissingDataFile {
        source: std::io::Error,
        path: String,
    },
}

pub struct DeltaTable {
    pub version: DeltaVersionType,
    pub tombstones: Vec<String>, // files that were recently deleted
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub table_path: String,

    // metadata
    // application_transactions
    storage: Box<dyn StorageBackend>,

    files: Vec<String>,
    app_transaction_version: HashMap<String, DeltaVersionType>,
    commit_infos: Vec<Value>,
    current_metadata: Option<DeltaTableMetaData>,
    last_check_point: Option<CheckPoint>,
    log_path: String,
}

impl DeltaTable {
    fn version_to_log_path(&self, version: DeltaVersionType) -> String {
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
        // FIXME: support dataChange field
        match action {
            Action::add(v) => {
                self.files.push(v.path.clone());
            }
            Action::remove(v) => {
                self.files.retain(|e| *e != v.path);
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
        version: DeltaVersionType,
    ) -> Result<Option<CheckPoint>, DeltaTableError> {
        let mut cp: Option<CheckPoint> = None;
        let re_checkpoint = Regex::new(r"^*/_delta_log/(\d{20})\.checkpoint\.parquet$").unwrap();
        let re_checkpoint_parts =
            Regex::new(r"^*/_delta_log/(\d{20})\.checkpoint\.\d{10}\.(\d{10})\.parquet$").unwrap();

        for key in self.storage.list_objs(&self.log_path)? {
            match re_checkpoint.captures(&key) {
                Some(captures) => {
                    let curr_ver_str = captures.get(1).unwrap().as_str();
                    let curr_ver: DeltaVersionType = curr_ver_str.parse().unwrap();
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

            match re_checkpoint_parts.captures(&key) {
                Some(captures) => {
                    let curr_ver_str = captures.get(1).unwrap().as_str();
                    let curr_ver: DeltaVersionType = curr_ver_str.parse().unwrap();
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

    fn apply_log(&mut self, version: DeltaVersionType) -> Result<(), ApplyLogError> {
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
                panic!("invalid checkpoint data file");
            }
            let mut iter = preader.get_row_iter(None)?;
            while let Some(record) = iter.next() {
                self.process_action(&Action::from_parquet_record(&schema, &record))?;
            }
        }

        Ok(())
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
                        _ => {
                            panic!("Apply error: {:#?}", e);
                        }
                    }
                    break;
                }
            }
        }

        return Ok(());
    }

    pub fn load_version(&mut self, version: DeltaVersionType) -> Result<(), DeltaTableError> {
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

    pub fn get_files(&self) -> &Vec<String> {
        &self.files
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
        })
    }
}

impl fmt::Display for DeltaTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaTable({})\n", self.table_path)?;
        write!(f, "\tversion: {}\n", self.version)?;
        write!(
            f,
            "\tmetadata: {}\n",
            self.current_metadata.as_ref().unwrap()
        )?;
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
    version: DeltaVersionType,
) -> Result<DeltaTable, DeltaTableError> {
    let storage_backend = storage::get_backend_for_uri(table_path)?;
    let mut table = DeltaTable::new(table_path, storage_backend)?;
    table.load_version(version)?;

    Ok(table)
}
