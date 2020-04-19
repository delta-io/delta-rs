#![allow(non_snake_case, non_camel_case_types)]

extern crate parquet;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{ListAccessor, MapAccessor, RowAccessor};

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::{prelude::*, BufReader, Cursor};

use thiserror::Error;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::storage::{
    parse_uri, FileStorageBackend, S3StorageBackend, StorageBackend, StorageError, Uri,
};

type GUID = String;
type DeltaDataTypeLong = i64;
type DeltaVersionType = DeltaDataTypeLong;
type DeltaDataTypeInt = i32;

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct LastCheckPoint {
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

#[derive(Debug)]
pub enum ApplyLogError {
    EndOfLog,
    InvalidJSON(String),
    Unknown(String),
}

impl Error for ApplyLogError {}

impl fmt::Display for ApplyLogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ApplyLogError::EndOfLog => write!(f, "End of transaction log"),
            ApplyLogError::InvalidJSON(s) => write!(f, "{}", s),
            ApplyLogError::Unknown(s) => write!(f, "{}", s),
        }
    }
}

impl From<StorageError> for ApplyLogError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => ApplyLogError::EndOfLog,
            StorageError::Unknown(s) => ApplyLogError::Unknown(format!("Storage error: {}", s)),
        }
    }
}

impl From<serde_json::error::Error> for ApplyLogError {
    fn from(error: serde_json::error::Error) -> Self {
        ApplyLogError::InvalidJSON(format!("Invalid json log record: {}", error))
    }
}

impl From<std::io::Error> for ApplyLogError {
    fn from(error: std::io::Error) -> Self {
        ApplyLogError::Unknown(format!("failed to read line from log record: {:#?}", error))
    }
}

#[derive(Debug)]
pub enum LoadCheckpointError {
    NotFound,
    InvalidJSON(String),
    Unknown(String),
}

impl Error for LoadCheckpointError {}

impl fmt::Display for LoadCheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LoadCheckpointError::NotFound => write!(f, "no checkpoint found"),
            LoadCheckpointError::InvalidJSON(s) => write!(f, "{}", s),
            LoadCheckpointError::Unknown(s) => write!(f, "{}", s),
        }
    }
}

impl From<StorageError> for LoadCheckpointError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => LoadCheckpointError::NotFound,
            StorageError::Unknown(s) => {
                LoadCheckpointError::Unknown(format!("unknown storage error: {}", s))
            }
        }
    }
}

impl From<serde_json::error::Error> for LoadCheckpointError {
    fn from(error: serde_json::error::Error) -> Self {
        LoadCheckpointError::InvalidJSON(format!("Invalid checkpoint: {}", error))
    }
}

#[derive(Error, Debug)]
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
}

pub struct DeltaTable {
    pub files: Vec<String>,
    pub version: DeltaVersionType,
    pub tombstones: Vec<String>, // files that were recently deleted
    pub min_reader_version: i32,
    pub min_writer_version: i32,

    // metadata
    // application_transactions
    table_path: String,
    storage: Box<dyn StorageBackend>,

    app_transaction_version: HashMap<String, DeltaVersionType>,
    commit_infos: Vec<Value>,
    current_metadata: Option<DeltaTableMetaData>,
    last_check_point: Option<LastCheckPoint>,
}

impl DeltaTable {
    fn version_to_log_path(&self, version: DeltaVersionType) -> String {
        return format!("{}/_delta_log/{:020}.json", self.table_path, version);
    }

    fn get_checkpoint_data_paths(
        checkpoint_prefix: &str,
        checkpoint_parts: Option<u32>,
    ) -> Vec<String> {
        let mut checkpoint_data_paths = Vec::new();

        match checkpoint_parts {
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

    fn load_last_checkpoint(
        &self,
        delta_log_dir: &str,
    ) -> Result<LastCheckPoint, LoadCheckpointError> {
        let last_checkpoint_path = format!("{}/_last_checkpoint", delta_log_dir);
        let data = self.storage.get_obj(&last_checkpoint_path)?;
        return Ok(serde_json::from_slice(&data)?);
    }

    fn process_action(&mut self, action: &Action) {
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
                    schema: serde_json::from_str(&v.schemaString).unwrap(),
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
    }

    fn apply_log(&mut self, version: DeltaVersionType) -> Result<(), ApplyLogError> {
        let log_path = self.version_to_log_path(version);
        let commit_log_bytes = self.storage.get_obj(&log_path)?;
        let reader = BufReader::new(Cursor::new(commit_log_bytes));
        for line in reader.lines() {
            let action: Action = serde_json::from_str(line?.as_str())?;
            self.process_action(&action);
        }
        return Ok(());
    }

    pub fn load(&mut self) -> Result<(), DeltaTableError> {
        let delta_log_dir = format!("{}/_delta_log", self.table_path);
        match self.load_last_checkpoint(&delta_log_dir) {
            Ok(last_check_point) => {
                self.last_check_point = Some(last_check_point);
                let checkpoint_data_paths = Self::get_checkpoint_data_paths(
                    &format!("{}/{:020}", delta_log_dir, last_check_point.version),
                    last_check_point.parts,
                );

                // process acttions from checkpoint
                for f in &checkpoint_data_paths {
                    let obj = self.storage.get_obj(&f).unwrap();
                    let preader = SerializedFileReader::new(Cursor::new(obj)).unwrap();
                    let schema = preader.metadata().file_metadata().schema();
                    if !schema.is_group() {
                        panic!("invalid checkpoint data file");
                    }
                    let mut iter = preader.get_row_iter(None).unwrap();
                    while let Some(record) = iter.next() {
                        self.process_action(&Action::from_parquet_record(&schema, &record));
                    }
                }

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

    pub fn new(
        table_path: &str,
        storage_backend: Box<dyn StorageBackend>,
    ) -> Result<Self, DeltaTableError> {
        let mut table = Self {
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
        };
        table.load()?;
        Ok(table)
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
    let storage_backend: Box<dyn StorageBackend>;
    let uri = parse_uri(table_path).unwrap();
    match uri {
        Uri::LocalPath(_) => storage_backend = Box::new(FileStorageBackend::new()),
        Uri::S3Object(_) => storage_backend = Box::new(S3StorageBackend::new()),
    }

    DeltaTable::new(table_path, storage_backend)
}
