// Reference: https://github.com/delta-io/delta/blob/master/PROTOCOL.md

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;

use chrono;
use chrono::{DateTime, FixedOffset, Utc};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror;

use super::action;
use super::action::Action;
use super::schema::*;
use super::storage;
use super::storage::{StorageBackend, StorageError, UriError};

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    version: DeltaDataTypeVersion, // 20 digits decimals
    size: DeltaDataTypeLong,
    parts: Option<u32>, // 10 digits decimals
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
    #[error("Invalid datetime string: {}", .source)]
    InvalidDateTimeSTring {
        #[from]
        source: chrono::ParseError,
    },
    #[error("Invalid action record found in log: {}", .source)]
    InvalidAction {
        #[from]
        source: action::ActionError,
    },
}

pub struct DeltaTableMetaData {
    // Unique identifier for this table
    pub id: GUID,
    // User-provided identifier for this table
    pub name: Option<String>,
    // User-provided description for this table
    pub description: Option<String>,
    // Specification of the encoding for the files stored in the table
    pub format: action::Format,
    // Schema of the table
    pub schema: Schema,
    // An array containing the names of columns by which the data should be partitioned
    pub partition_columns: Vec<String>,
    // NOTE: this field is undocumented
    pub configuration: HashMap<String, String>,
}

impl fmt::Display for DeltaTableMetaData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GUID={}, name={:?}, description={:?}, partitionColumns={:?}, configuration={:?}",
            self.id, self.name, self.description, self.partition_columns, self.configuration
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
    pub tombstones: Vec<action::Remove>,
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

        checkpoint_data_paths
    }

    fn get_last_checkpoint(&self) -> Result<CheckPoint, LoadCheckpointError> {
        let last_checkpoint_path = format!("{}/_last_checkpoint", self.log_path);
        let data = self.storage.get_obj(&last_checkpoint_path)?;

        Ok(serde_json::from_slice(&data)?)
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
                    partition_columns: v.partitionColumns.clone(),
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

    fn find_latest_check_point_for_version(
        &self,
        version: DeltaDataTypeVersion,
    ) -> Result<Option<CheckPoint>, DeltaTableError> {
        let mut cp: Option<CheckPoint> = None;
        let re_checkpoint = Regex::new(r"^*/_delta_log/(\d{20})\.checkpoint\.parquet$").unwrap();
        let re_checkpoint_parts =
            Regex::new(r"^*/_delta_log/(\d{20})\.checkpoint\.\d{10}\.(\d{10})\.parquet$").unwrap();

        for obj_meta in self.storage.list_objs(&self.log_path)? {
            if let Some(captures) = re_checkpoint.captures(&obj_meta.path) {
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

            if let Some(captures) = re_checkpoint_parts.captures(&obj_meta.path) {
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
        }

        Ok(cp)
    }

    fn apply_log_from_bufread<R: BufRead>(
        &mut self,
        reader: BufReader<R>,
    ) -> Result<(), ApplyLogError> {
        for line in reader.lines() {
            let action: Action = serde_json::from_str(line?.as_str())?;
            self.process_action(&action)?;
        }

        Ok(())
    }

    fn apply_log(&mut self, version: DeltaDataTypeVersion) -> Result<(), ApplyLogError> {
        let log_path = self.version_to_log_path(version);
        let commit_log_bytes = self.storage.get_obj(&log_path)?;
        let reader = BufReader::new(Cursor::new(commit_log_bytes));

        self.apply_log_from_bufread(reader)
    }

    fn restore_checkpoint(&mut self, check_point: CheckPoint) -> Result<(), DeltaTableError> {
        let checkpoint_data_paths = self.get_checkpoint_data_paths(&check_point);
        // process actions from checkpoint
        for f in &checkpoint_data_paths {
            let obj = self.storage.get_obj(&f)?;
            let preader = SerializedFileReader::new(Cursor::new(obj))?;
            let schema = preader.metadata().file_metadata().schema();
            if !schema.is_group() {
                return Err(DeltaTableError::from(action::ActionError::Generic(
                    "Action record in checkpoint should be a struct".to_string(),
                )));
            }
            for record in preader.get_row_iter(None)? {
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
                self.version = last_check_point.version + 1;
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

        Ok(())
    }

    pub fn load_version(&mut self, version: DeltaDataTypeVersion) -> Result<(), DeltaTableError> {
        // check if version is valid
        let log_path = self.version_to_log_path(version);
        match self.storage.head_obj(&log_path) {
            Ok(_) => {}
            Err(StorageError::NotFound) => {
                return Err(DeltaTableError::InvalidVersion(version));
            }
            Err(e) => {
                return Err(DeltaTableError::from(e));
            }
        }
        self.version = version;

        let mut next_version;
        // 1. find latest checkpoint below version
        match self.find_latest_check_point_for_version(version)? {
            Some(check_point) => {
                self.restore_checkpoint(check_point)?;
                next_version = check_point.version + 1;
            }
            None => {
                // no checkpoint found, start from the beginning
                next_version = 0;
            }
        }

        // 2. apply all logs starting from checkpoint
        while next_version <= self.version {
            self.apply_log(next_version)?;
            next_version += 1;
        }

        Ok(())
    }

    fn get_version_timestamp(
        &mut self,
        version: DeltaDataTypeVersion,
    ) -> Result<i64, DeltaTableError> {
        match self.version_timestamp.get(&version) {
            Some(ts) => Ok(*ts),
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

    pub fn get_tombstones(&self) -> &Vec<action::Remove> {
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

            match pts.cmp(&target_ts) {
                Ordering::Equal => {
                    break;
                }
                Ordering::Less => {
                    min_version = pivot + 1;
                }
                Ordering::Greater => {
                    max_version = pivot - 1;
                    version = max_version
                }
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
        writeln!(f, "DeltaTable({})", self.table_path)?;
        writeln!(f, "\tversion: {}", self.version)?;
        match self.current_metadata.as_ref() {
            Some(metadata) => {
                writeln!(f, "\tmetadata: {}", metadata)?;
            }
            None => {
                writeln!(f, "\tmetadata: None")?;
            }
        }
        writeln!(
            f,
            "\tmin_version: read={}, write={}",
            self.min_reader_version, self.min_writer_version
        )?;
        writeln!(f, "\tfiles count: {}", self.files.len())
    }
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DeltaTable <{}>", self.table_path)
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
