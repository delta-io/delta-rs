//! Delta Table read and write implementation

// Reference: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
//

use arrow::error::ArrowError;
use chrono::{DateTime, FixedOffset, Utc};
use futures::StreamExt;
use lazy_static::lazy_static;
use log::*;
use parquet::errors::ParquetError;
use parquet::file::{
    reader::{FileReader, SerializedFileReader},
    serialized_reader::SliceableCursor,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::io::{BufRead, BufReader, Cursor};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{cmp::Ordering, collections::HashSet};
use uuid::Uuid;

use crate::action::Stats;

use super::action;
use super::action::{Action, DeltaOperation};
use super::partitions::{DeltaTablePartition, PartitionFilter};
use super::schema::*;
use super::storage;
use super::storage::{parse_uri, StorageBackend, StorageError, UriError};

/// Metadata for a checkpoint file
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    /// Delta table version
    version: DeltaDataTypeVersion, // 20 digits decimals
    size: DeltaDataTypeLong,
    parts: Option<u32>, // 10 digits decimals
}

impl CheckPoint {
    /// Creates a new checkpoint from the given parameters.
    pub(crate) fn new(
        version: DeltaDataTypeVersion,
        size: DeltaDataTypeLong,
        parts: Option<u32>,
    ) -> Self {
        Self {
            version,
            size,
            parts,
        }
    }
}

impl PartialEq for CheckPoint {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl Eq for CheckPoint {}

/// Delta Table specific error
#[derive(thiserror::Error, Debug)]
pub enum DeltaTableError {
    /// Error returned when applying transaction log failed.
    #[error("Failed to apply transaction log: {}", .source)]
    ApplyLog {
        /// Apply error details returned when applying transaction log failed.
        #[from]
        source: ApplyLogError,
    },
    /// Error returned when loading checkpoint failed.
    #[error("Failed to load checkpoint: {}", .source)]
    LoadCheckpoint {
        /// Load checkpoint error details returned when loading checkpoint failed.
        #[from]
        source: LoadCheckpointError,
    },
    /// Error returned when reading the delta log object failed.
    #[error("Failed to read delta log object: {}", .source)]
    StorageError {
        /// Storage error details when reading the delta log object failed.
        #[from]
        source: StorageError,
    },
    /// Error returned when reading the checkpoint failed.
    #[error("Failed to read checkpoint: {}", .source)]
    ParquetError {
        /// Parquet error details returned when reading the checkpoint failed.
        #[from]
        source: ParquetError,
    },
    /// Error returned when converting the schema in Arrow format failed.
    #[error("Failed to convert into Arrow schema: {}", .source)]
    ArrowError {
        /// Arrow error details returned when converting the schema in Arrow format failed
        #[from]
        source: ArrowError,
    },
    /// Error returned when the table has an invalid path.
    #[error("Invalid table path: {}", .source)]
    UriError {
        /// Uri error details returned when the table has an invalid path.
        #[from]
        source: UriError,
    },
    /// Error returned when the log record has an invalid JSON.
    #[error("Invalid JSON in log record: {}", .source)]
    InvalidJson {
        /// JSON error details returned when the log record has an invalid JSON.
        #[from]
        source: serde_json::error::Error,
    },
    /// Error returned when the DeltaTable has an invalid version.
    #[error("Invalid table version: {0}")]
    InvalidVersion(DeltaDataTypeVersion),
    /// Error returned when the DeltaTable has no data files.
    #[error("Corrupted table, cannot read data file {}: {}", .path, .source)]
    MissingDataFile {
        /// Source error details returned when the DeltaTable has no data files.
        source: std::io::Error,
        /// The Path used of the DeltaTable
        path: String,
    },
    /// Error returned when the datetime string is invalid for a conversion.
    #[error("Invalid datetime string: {}", .source)]
    InvalidDateTimeString {
        /// Parse error details returned of the datetime string parse error.
        #[from]
        source: chrono::ParseError,
    },
    /// Error returned when the action record is invalid in log.
    #[error("Invalid action record found in log: {}", .source)]
    InvalidAction {
        /// Action error details returned of the invalid action.
        #[from]
        source: action::ActionError,
    },
    /// Error returned when it is not a DeltaTable.
    #[error("Not a Delta table: {0}")]
    NotATable(String),

    /// Error returned when no metadata was found in the DeltaTable.
    #[error("No metadata found, please make sure table is loaded.")]
    NoMetadata,
    /// Error returned when no schema was found in the DeltaTable.
    #[error("No schema found, please make sure table is loaded.")]
    NoSchema,
    /// Error returned when no partition was found in the DeltaTable.
    #[error("No partitions found, please make sure table is partitioned.")]
    LoadPartitions,

    /// Error returned when writes are attempted with data that doesn't match the schema of the
    /// table
    #[error("Data does not match the schema or partitions of the table: {}", msg)]
    SchemaMismatch {
        /// Information about the mismatch
        msg: String,
    },

    /// Error returned when a partition is not formatted as a Hive Partition.
    #[error("This partition is not formatted with key=value: {}", .partition)]
    PartitionError {
        /// The malformed partition used.
        partition: String,
    },
    /// Error returned when a invalid partition filter was found.
    #[error("Invalid partition filter found: {}.", .partition_filter)]
    InvalidPartitionFilter {
        /// The invalid partition filter used.
        partition_filter: String,
    },
    /// Error returned when Vacuum retention period is below the safe threshold
    #[error(
        "Invalid retention period, retention for Vacuum must be greater than 1 week (168 hours)"
    )]
    InvalidVacuumRetentionPeriod,
    /// Generic Delta Table error
    #[error("Generic DeltaTable error: {0}")]
    Generic(String),
}

/// Delta table metadata
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaTableMetaData {
    /// Unique identifier for this table
    pub id: Guid,
    /// User-provided identifier for this table
    pub name: Option<String>,
    /// User-provided description for this table
    pub description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub format: action::Format,
    /// Schema of the table
    pub schema: Schema,
    /// An array containing the names of columns by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: DeltaDataTypeTimestamp,
    /// table properties
    pub configuration: HashMap<String, Option<String>>,
}

impl DeltaTableMetaData {
    /// Create metadata for a DeltaTable from scratch
    pub fn new(
        name: Option<String>,
        description: Option<String>,
        format: Option<action::Format>,
        schema: Schema,
        partition_columns: Vec<String>,
        configuration: HashMap<String, Option<String>>,
    ) -> Self {
        // Reference implementation uses uuid v4 to create GUID:
        // https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L350
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            description,
            format: format.unwrap_or_default(),
            schema,
            partition_columns,
            created_time: Utc::now().timestamp_millis(),
            configuration,
        }
    }

    /// Return the configurations of the DeltaTableMetaData; could be empty
    pub fn get_configuration(&self) -> &HashMap<String, Option<String>> {
        &self.configuration
    }
}

impl fmt::Display for DeltaTableMetaData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GUID={}, name={:?}, description={:?}, partitionColumns={:?}, createdTime={:?}, configuration={:?}",
            self.id, self.name, self.description, self.partition_columns, self.created_time, self.configuration
        )
    }
}

impl TryFrom<action::MetaData> for DeltaTableMetaData {
    type Error = serde_json::error::Error;

    fn try_from(action_metadata: action::MetaData) -> Result<Self, Self::Error> {
        let schema = action_metadata.get_schema()?;
        Ok(Self {
            id: action_metadata.id,
            name: action_metadata.name,
            description: action_metadata.description,
            format: action_metadata.format,
            schema,
            partition_columns: action_metadata.partition_columns,
            created_time: action_metadata.created_time,
            configuration: action_metadata.configuration,
        })
    }
}

impl TryFrom<DeltaTableMetaData> for action::MetaData {
    type Error = serde_json::error::Error;

    fn try_from(metadata: DeltaTableMetaData) -> Result<Self, Self::Error> {
        let schema_string = serde_json::to_string(&metadata.schema)?;
        Ok(Self {
            id: metadata.id,
            name: metadata.name,
            description: metadata.description,
            format: metadata.format,
            schema_string,
            partition_columns: metadata.partition_columns,
            created_time: metadata.created_time,
            configuration: metadata.configuration,
        })
    }
}

/// Error related to Delta log application
#[derive(thiserror::Error, Debug)]
pub enum ApplyLogError {
    /// Error returned when the end of transaction log is reached.
    #[error("End of transaction log")]
    EndOfLog,
    /// Error returned when the JSON of the log record is invalid.
    #[error("Invalid JSON in log record")]
    InvalidJson {
        /// JSON error details returned when reading the JSON log record.
        #[from]
        source: serde_json::error::Error,
    },
    /// Error returned when the storage failed to read the log content.
    #[error("Failed to read log content")]
    Storage {
        /// Storage error details returned while reading the log content.
        source: StorageError,
    },
    /// Error returned when a line from log record is invalid.
    #[error("Failed to read line from log record")]
    Io {
        /// Source error details returned while reading the log record.
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

/// Error related to checkpoint loading
#[derive(thiserror::Error, Debug)]
pub enum LoadCheckpointError {
    /// Error returned when the JSON checkpoint is not found.
    #[error("Checkpoint file not found")]
    NotFound,
    /// Error returned when the JSON checkpoint is invalid.
    #[error("Invalid JSON in checkpoint: {source}")]
    InvalidJson {
        /// Error details returned while reading the JSON.
        #[from]
        source: serde_json::error::Error,
    },
    /// Error returned when it failed to read the checkpoint content.
    #[error("Failed to read checkpoint content: {source}")]
    Storage {
        /// Storage error details returned while reading the checkpoint content.
        source: StorageError,
    },
}

impl From<StorageError> for LoadCheckpointError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => LoadCheckpointError::NotFound,
            _ => LoadCheckpointError::Storage { source: error },
        }
    }
}

/// State snapshot currently held by the Delta Table instance.
#[derive(Default, Debug, Clone)]
pub struct DeltaTableState {
    // A remove action should remain in the state of the table as a tombstone until it has expired.
    // A tombstone expires when the creation timestamp of the delta file exceeds the expiration
    tombstones: Vec<action::Remove>,
    files: Vec<action::Add>,
    commit_infos: Vec<Value>,
    app_transaction_version: HashMap<String, DeltaDataTypeVersion>,
    min_reader_version: i32,
    min_writer_version: i32,
    current_metadata: Option<DeltaTableMetaData>,
}

impl DeltaTableState {
    /// Full list of tombstones (remove actions) representing files removed from table state).
    pub fn tombstones(&self) -> &Vec<action::Remove> {
        self.tombstones.as_ref()
    }

    /// Full list of add actions representing all parquet files that are part of the current
    /// delta table state.
    pub fn files(&self) -> &Vec<action::Add> {
        self.files.as_ref()
    }

    /// HashMap containing the last txn version stored for every app id writing txn
    /// actions.
    pub fn app_transaction_version(&self) -> &HashMap<String, DeltaDataTypeVersion> {
        &self.app_transaction_version
    }

    /// The min reader version required by the protocol.
    pub fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    /// The min writer version required by the protocol.
    pub fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    /// The most recent metadata of the table.
    pub fn current_metadata(&self) -> Option<&DeltaTableMetaData> {
        self.current_metadata.as_ref()
    }
}

#[inline]
/// Return path relative to parent_path
fn extract_rel_path<'a, 'b>(
    parent_path: &'b str,
    path: &'a str,
) -> Result<&'a str, DeltaTableError> {
    if path.starts_with(&parent_path) {
        // plus one to account for path separator
        Ok(&path[parent_path.len() + 1..])
    } else {
        Err(DeltaTableError::Generic(format!(
            "Parent path `{}` is not a prefix of path `{}`",
            parent_path, path
        )))
    }
}

/// In memory representation of a Delta Table
pub struct DeltaTable {
    /// The version of the table as of the most recent loaded Delta log entry.
    pub version: DeltaDataTypeVersion,
    /// The URI the DeltaTable was loaded from.
    pub table_uri: String,

    state: DeltaTableState,

    // metadata
    // application_transactions
    storage: Box<dyn StorageBackend>,

    last_check_point: Option<CheckPoint>,
    log_uri: String,
    version_timestamp: HashMap<DeltaDataTypeVersion, i64>,
}

impl DeltaTable {
    fn commit_uri_from_version(&self, version: DeltaDataTypeVersion) -> String {
        let version = format!("{:020}.json", version);
        self.storage.join_path(&self.log_uri, &version)
    }

    fn get_checkpoint_data_paths(&self, check_point: &CheckPoint) -> Vec<String> {
        let checkpoint_prefix_pattern = format!("{:020}", check_point.version);
        let checkpoint_prefix = self
            .storage
            .join_path(&self.log_uri, &checkpoint_prefix_pattern);
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

    async fn get_last_checkpoint(&self) -> Result<CheckPoint, LoadCheckpointError> {
        let last_checkpoint_path = self.storage.join_path(&self.log_uri, "_last_checkpoint");
        let data = self.storage.get_obj(&last_checkpoint_path).await?;

        Ok(serde_json::from_slice(&data)?)
    }

    async fn find_latest_check_point_for_version(
        &self,
        version: DeltaDataTypeVersion,
    ) -> Result<Option<CheckPoint>, DeltaTableError> {
        lazy_static! {
            static ref CHECKPOINT_REGEX: Regex =
                Regex::new(r#"^*[/\\]_delta_log[/\\](\d{20})\.checkpoint\.parquet$"#).unwrap();
            static ref CHECKPOINT_PARTS_REGEX: Regex = Regex::new(
                r#"^*[/\\]_delta_log[/\\](\d{20})\.checkpoint\.\d{10}\.(\d{10})\.parquet$"#
            )
            .unwrap();
        }

        let mut cp: Option<CheckPoint> = None;
        let mut stream = self.storage.list_objs(&self.log_uri).await?;

        while let Some(obj_meta) = stream.next().await {
            // Exit early if any objects can't be listed.
            let obj_meta = obj_meta?;
            if let Some(captures) = CHECKPOINT_REGEX.captures(&obj_meta.path) {
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

            if let Some(captures) = CHECKPOINT_PARTS_REGEX.captures(&obj_meta.path) {
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
            process_action(&mut self.state, action)?;
        }

        Ok(())
    }

    async fn apply_log(&mut self, version: DeltaDataTypeVersion) -> Result<(), ApplyLogError> {
        let commit_uri = self.commit_uri_from_version(version);
        let commit_log_bytes = self.storage.get_obj(&commit_uri).await?;
        let reader = BufReader::new(Cursor::new(commit_log_bytes));

        self.apply_log_from_bufread(reader)
    }

    async fn restore_checkpoint(&mut self, check_point: CheckPoint) -> Result<(), DeltaTableError> {
        let checkpoint_data_paths = self.get_checkpoint_data_paths(&check_point);
        // process actions from checkpoint
        self.state = DeltaTableState::default();
        for f in &checkpoint_data_paths {
            let obj = self.storage.get_obj(f).await?;
            let preader = SerializedFileReader::new(SliceableCursor::new(obj))?;
            let schema = preader.metadata().file_metadata().schema();
            if !schema.is_group() {
                return Err(DeltaTableError::from(action::ActionError::Generic(
                    "Action record in checkpoint should be a struct".to_string(),
                )));
            }
            for record in preader.get_row_iter(None)? {
                process_action(
                    &mut self.state,
                    Action::from_parquet_record(schema, &record)?,
                )?;
            }
        }

        Ok(())
    }

    async fn get_latest_version(&mut self) -> Result<DeltaDataTypeVersion, DeltaTableError> {
        let mut version = match self.get_last_checkpoint().await {
            Ok(last_check_point) => last_check_point.version + 1,
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
            match self
                .storage
                .head_obj(&self.commit_uri_from_version(version))
                .await
            {
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
                            if version < 0 {
                                let err = format!(
                                    "No snapshot or version 0 found, perhaps {} is an empty dir?",
                                    self.table_uri
                                );
                                return Err(DeltaTableError::NotATable(err));
                            }
                        }
                        _ => return Err(DeltaTableError::from(e)),
                    }
                    break;
                }
            }
        }

        Ok(version)
    }

    /// Load DeltaTable with data from latest checkpoint
    pub async fn load(&mut self) -> Result<(), DeltaTableError> {
        match self.get_last_checkpoint().await {
            Ok(last_check_point) => {
                self.last_check_point = Some(last_check_point);
                self.restore_checkpoint(last_check_point).await?;
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

        self.apply_logs_from_current_version().await?;

        Ok(())
    }

    /// Updates the DeltaTable to the most recent state committed to the transaction log by
    /// loading the last checkpoint and incrementally applying each version since.
    pub async fn update(&mut self) -> Result<(), DeltaTableError> {
        match self.get_last_checkpoint().await {
            Ok(last_check_point) => {
                if self.last_check_point != Some(last_check_point) {
                    self.last_check_point = Some(last_check_point);
                    self.restore_checkpoint(last_check_point).await?;
                    self.version = last_check_point.version + 1;
                }
            }
            Err(LoadCheckpointError::NotFound) => {
                self.version += 1;
            }
            Err(e) => {
                return Err(DeltaTableError::LoadCheckpoint { source: e });
            }
        }

        self.apply_logs_from_current_version().await?;

        Ok(())
    }

    /// Updates the DeltaTable to the latest version by incrementally applying newer versions.
    pub async fn update_incremental(&mut self) -> Result<(), DeltaTableError> {
        self.version += 1;
        self.apply_logs_from_current_version().await
    }

    async fn apply_logs_from_current_version(&mut self) -> Result<(), DeltaTableError> {
        // replay logs after checkpoint
        loop {
            match self.apply_log(self.version).await {
                Ok(_) => {
                    self.version += 1;
                }
                Err(e) => {
                    match e {
                        ApplyLogError::EndOfLog => {
                            self.version -= 1;
                            if self.version == -1 {
                                let err = format!(
                                    "No snapshot or version 0 found, perhaps {} is an empty dir?",
                                    self.table_uri
                                );
                                return Err(DeltaTableError::NotATable(err));
                            }
                        }
                        _ => {
                            return Err(DeltaTableError::from(e));
                        }
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Loads the DeltaTable state for the given version.
    pub async fn load_version(
        &mut self,
        version: DeltaDataTypeVersion,
    ) -> Result<(), DeltaTableError> {
        // check if version is valid
        let commit_uri = self.commit_uri_from_version(version);
        match self.storage.head_obj(&commit_uri).await {
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
        match self.find_latest_check_point_for_version(version).await? {
            Some(check_point) => {
                self.restore_checkpoint(check_point).await?;
                next_version = check_point.version + 1;
            }
            None => {
                // no checkpoint found, clear table state and start from the beginning
                self.state = DeltaTableState::default();
                next_version = 0;
            }
        }

        // 2. apply all logs starting from checkpoint
        while next_version <= self.version {
            self.apply_log(next_version).await?;
            next_version += 1;
        }

        Ok(())
    }

    async fn get_version_timestamp(
        &mut self,
        version: DeltaDataTypeVersion,
    ) -> Result<i64, DeltaTableError> {
        match self.version_timestamp.get(&version) {
            Some(ts) => Ok(*ts),
            None => {
                let meta = self
                    .storage
                    .head_obj(&self.commit_uri_from_version(version))
                    .await?;
                let ts = meta.modified.timestamp();
                // also cache timestamp for version
                self.version_timestamp.insert(version, ts);

                Ok(ts)
            }
        }
    }

    /// Returns the file list tracked in current table state filtered by provided
    /// `PartitionFilter`s.
    pub fn get_files_by_partitions(
        &self,
        filters: &[PartitionFilter<&str>],
    ) -> Result<Vec<String>, DeltaTableError> {
        let files = self
            .state
            .files
            .iter()
            .filter(|add| {
                let partitions = add
                    .partition_values
                    .iter()
                    .map(|p| DeltaTablePartition::from_partition_value(p, ""))
                    .collect::<Vec<DeltaTablePartition>>();
                filters
                    .iter()
                    .all(|filter| filter.match_partitions(&partitions))
            })
            .map(|add| add.path.clone())
            .collect();

        Ok(files)
    }

    /// Return the file uris as strings for the partition(s)
    #[deprecated(
        since = "0.4.0",
        note = "Please use the get_file_uris_by_partitions function instead"
    )]
    pub fn get_file_paths_by_partitions(
        &self,
        filters: &[PartitionFilter<&str>],
    ) -> Result<Vec<String>, DeltaTableError> {
        self.get_file_uris_by_partitions(filters)
    }

    /// Return the file uris as strings for the partition(s)
    pub fn get_file_uris_by_partitions(
        &self,
        filters: &[PartitionFilter<&str>],
    ) -> Result<Vec<String>, DeltaTableError> {
        let files = self.get_files_by_partitions(filters)?;
        Ok(files
            .iter()
            .map(|fname| self.storage.join_path(&self.table_uri, fname))
            .collect())
    }

    /// Return a refernece to all active "add" actions present in the loaded state
    pub fn get_active_add_actions(&self) -> &Vec<action::Add> {
        &self.state.files
    }

    /// Returns an iterator of file names present in the loaded state
    #[inline]
    pub fn get_files_iter(&self) -> impl Iterator<Item = &str> {
        self.state.files.iter().map(|add| add.path.as_str())
    }

    /// Returns a collection of file names present in the loaded state
    #[inline]
    pub fn get_files(&self) -> Vec<&str> {
        self.get_files_iter().collect()
    }

    /// Returns file names present in the loaded state in HashSet
    pub fn get_file_set(&self) -> HashSet<&str> {
        self.state
            .files
            .iter()
            .map(|add| add.path.as_str())
            .collect()
    }

    /// Returns a URIs for all active files present in the current table version.
    #[deprecated(
        since = "0.4.0",
        note = "Please use the get_file_uris function instead"
    )]
    pub fn get_file_paths(&self) -> Vec<String> {
        self.get_file_uris()
    }

    /// Returns a URIs for all active files present in the current table version.
    pub fn get_file_uris(&self) -> Vec<String> {
        self.state
            .files
            .iter()
            .map(|add| self.storage.join_path(&self.table_uri, &add.path))
            .collect()
    }

    /// Returns statistics for files, in order
    pub fn get_stats(&self) -> Vec<Result<Option<Stats>, DeltaTableError>> {
        self.state
            .files
            .iter()
            .map(|add| add.get_stats().map_err(DeltaTableError::from))
            .collect()
    }

    /// Returns the currently loaded state snapshot.
    pub fn get_state(&self) -> &DeltaTableState {
        &self.state
    }

    /// Returns the metadata associated with the loaded state.
    pub fn get_metadata(&self) -> Result<&DeltaTableMetaData, DeltaTableError> {
        self.state
            .current_metadata
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)
    }

    /// Returns a vector of tombstones (i.e. `Remove` actions present in the current delta log.
    pub fn get_tombstones(&self) -> &Vec<action::Remove> {
        &self.state.tombstones
    }

    /// Returns the current version of the DeltaTable based on the loaded metadata.
    pub fn get_app_transaction_version(&self) -> &HashMap<String, DeltaDataTypeVersion> {
        &self.state.app_transaction_version
    }

    /// Returns the minimum reader version supported by the DeltaTable based on the loaded
    /// metadata.
    pub fn get_min_reader_version(&self) -> i32 {
        self.state.min_reader_version
    }

    /// Returns the minimum writer version supported by the DeltaTable based on the loaded
    /// metadata.
    pub fn get_min_writer_version(&self) -> i32 {
        self.state.min_writer_version
    }

    /// List files no longer referenced by a Delta table and are older than the retention threshold.
    fn get_stale_files(&self, retention_hours: u64) -> Result<HashSet<&str>, DeltaTableError> {
        if retention_hours < 168 {
            return Err(DeltaTableError::InvalidVacuumRetentionPeriod);
        }
        let before_duration = (SystemTime::now() - Duration::from_secs(3600 * retention_hours))
            .duration_since(UNIX_EPOCH);
        let delete_before_timestamp = match before_duration {
            Ok(duration) => duration.as_millis() as i64,
            Err(_) => return Err(DeltaTableError::InvalidVacuumRetentionPeriod),
        };

        Ok(self
            .get_tombstones()
            .iter()
            .filter(|tombstone| tombstone.deletion_timestamp < delete_before_timestamp)
            .map(|tombstone| tombstone.path.as_str())
            .collect::<HashSet<_>>())
    }

    /// Whether a path should be hidden for delta-related file operations, such as Vacuum.
    /// Names of the form partitionCol=[value] are partition directories, and should be
    /// deleted even if they'd normally be hidden. The _db_index directory contains (bloom filter)
    /// indexes and these must be deleted when the data they are tied to is deleted.
    fn is_hidden_directory(&self, path_name: &str) -> Result<bool, DeltaTableError> {
        Ok((path_name.starts_with('.') || path_name.starts_with('_'))
            && !path_name.starts_with("_delta_index")
            && !path_name.starts_with("_change_data")
            && !self
                .state
                .current_metadata
                .as_ref()
                .ok_or(DeltaTableError::NoMetadata)?
                .partition_columns
                .iter()
                .any(|partition_column| path_name.starts_with(partition_column)))
    }

    /// Run the Vacuum command on the Delta Table: delete files no longer referenced by a Delta table and are older than the retention threshold.
    /// We do not recommend that you set a retention interval shorter than 7 days, because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table. If vacuum cleans up active files, concurrent readers can fail or, worse, tables can be corrupted when vacuum deletes files that have not yet been committed.
    pub async fn vacuum(
        &mut self,
        retention_hours: u64,
        dry_run: bool,
    ) -> Result<Vec<String>, DeltaTableError> {
        let expired_tombstones = self.get_stale_files(retention_hours)?;
        let valid_files = self.get_file_set();

        let mut files_to_delete = vec![];
        let mut all_files = self.storage.list_objs(&self.table_uri).await?;

        // TODO: table_path is currently only used in vacuum, consider precalcualte it during table
        // struct initialization if it ends up being used in other hot paths
        let table_path = parse_uri(&self.table_uri)?.path();

        while let Some(obj_meta) = all_files.next().await {
            let obj_meta = obj_meta?;
            // We can't use self.table_uri as the prefix to extract relative path because
            // obj_meta.path is not a URI. For example, for S3 objects, obj_meta.path is just the
            // object key without `s3://` and bucket name.
            let rel_path = extract_rel_path(&table_path, &obj_meta.path)?;

            if valid_files.contains(rel_path) // file is still being tracked in table
                || !expired_tombstones.contains(rel_path) // file is not an expired tombstone
                || self.is_hidden_directory(rel_path)?
            {
                continue;
            }

            files_to_delete.push(obj_meta.path);
        }

        if dry_run {
            return Ok(files_to_delete);
        }

        for rel_path in &files_to_delete {
            match self
                .storage
                .delete_obj(&self.storage.join_path(&self.table_uri, rel_path))
                .await
            {
                Ok(_) => continue,
                Err(StorageError::NotFound) => continue,
                Err(err) => return Err(DeltaTableError::StorageError { source: err }),
            }
        }

        Ok(files_to_delete)
    }

    /// Return table schema parsed from transaction log. Return None if table hasn't been loaded or
    /// no metadata was found in the log.
    pub fn schema(&self) -> Option<&Schema> {
        self.state.current_metadata.as_ref().map(|m| &m.schema)
    }

    /// Return table schema parsed from transaction log. Return `DeltaTableError` if table hasn't
    /// been loaded or no metadata was found in the log.
    pub fn get_schema(&self) -> Result<&Schema, DeltaTableError> {
        self.schema().ok_or(DeltaTableError::NoSchema)
    }

    /// Return the tables configurations that are encapsulated in the DeltaTableStates currentMetaData field
    pub fn get_configurations(&self) -> Result<&HashMap<String, Option<String>>, DeltaTableError> {
        Ok(self
            .state
            .current_metadata
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)?
            .get_configuration())
    }

    /// Creates a new DeltaTransaction for the DeltaTable.
    /// The transaction holds a mutable reference to the DeltaTable, preventing other references
    /// until the transaction is dropped.
    pub fn create_transaction(
        &mut self,
        options: Option<DeltaTransactionOptions>,
    ) -> DeltaTransaction {
        DeltaTransaction::new(self, options)
    }

    /// Tries to commit a prepared commit file. Returns `DeltaTransactionError::VersionAlreadyExists`
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    pub async fn try_commit_transaction(
        &mut self,
        commit: &PreparedCommit,
        version: DeltaDataTypeVersion,
    ) -> Result<DeltaDataTypeVersion, DeltaTransactionError> {
        // move temporary commit file to delta log directory
        // rely on storage to fail if the file already exists -
        self.storage
            .rename_obj(&commit.uri, &self.commit_uri_from_version(version))
            .await?;

        // NOTE: since we have the log entry in memory already,
        // we could optimize this further by merging the log entry instead of updating from storage.
        self.update_incremental().await?;

        Ok(version)
    }

    /// Create a new Delta Table struct without loading any data from backing storage.
    ///
    /// NOTE: This is for advanced users. If you don't know why you need to use this method, please
    /// call one of the `open_table` helper methods instead.
    pub fn new(
        table_uri: &str,
        storage_backend: Box<dyn StorageBackend>,
    ) -> Result<Self, DeltaTableError> {
        let table_uri = storage_backend.trim_path(table_uri);
        let log_uri_normalized = storage_backend.join_path(&table_uri, "_delta_log");
        Ok(Self {
            version: 0,
            state: DeltaTableState::default(),
            storage: storage_backend,
            table_uri,
            last_check_point: None,
            log_uri: log_uri_normalized,
            version_timestamp: HashMap::new(),
        })
    }

    /// Create a DeltaTable with version 0 given the provided MetaData and Protocol
    pub async fn create(
        &mut self,
        metadata: DeltaTableMetaData,
        protocol: action::Protocol,
    ) -> Result<(), DeltaTransactionError> {
        let meta = action::MetaData::try_from(metadata)?;

        // TODO add commit info action
        let actions = vec![Action::protocol(protocol), Action::metaData(meta)];
        let mut transaction = self.create_transaction(None);
        transaction.add_actions(actions.clone());

        let prepared_commit = transaction.prepare_commit(None).await?;
        self.try_commit_transaction(&prepared_commit, 0).await?;

        // Mutate the DeltaTable's state using process_action()
        // in order to get most up-to-date state based on the commit above
        for action in actions {
            let _ = process_action(&mut self.state, action)?;
        }

        Ok(())
    }

    /// Time travel Delta table to latest version that's created at or before provided `datetime`
    /// argument.
    ///
    /// Internally, this methods performs a binary search on all Delta transaction logs.
    pub async fn load_with_datetime(
        &mut self,
        datetime: DateTime<Utc>,
    ) -> Result<(), DeltaTableError> {
        let mut min_version = 0;
        let mut max_version = self.get_latest_version().await?;
        let mut version = min_version;
        let target_ts = datetime.timestamp();

        // binary search
        while min_version <= max_version {
            let pivot = (max_version + min_version) / 2;
            version = pivot;
            let pts = self.get_version_timestamp(pivot).await?;

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

        self.load_version(version).await
    }
}

impl fmt::Display for DeltaTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DeltaTable({})", self.table_uri)?;
        writeln!(f, "\tversion: {}", self.version)?;
        match self.state.current_metadata.as_ref() {
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
            self.state.min_reader_version, self.state.min_writer_version
        )?;
        writeln!(f, "\tfiles count: {}", self.state.files.len())
    }
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DeltaTable <{}>", self.table_uri)
    }
}

/// Error returned by the DeltaTransaction struct
#[derive(thiserror::Error, Debug)]
pub enum DeltaTransactionError {
    /// Error that indicates a Delta version conflict. i.e. a writer tried to write version _N_ but
    /// version _N_ already exists in the delta log.
    #[error("Version already existed when writing transaction. Last error: {source}")]
    VersionAlreadyExists {
        /// The wrapped TransactionCommitAttemptError.
        source: StorageError,
    },

    /// Error that indicates the record batch is missing a partition column required by the Delta
    /// schema.
    #[error("RecordBatch is missing partition column in Delta schema.")]
    MissingPartitionColumn,

    /// Error that indicates the transaction failed due to an underlying storage error.
    /// Specific details of the error are described by the wrapped storage error.
    #[error("Storage interaction failed: {source}")]
    Storage {
        /// The wrapped StorageError.
        source: StorageError,
    },

    /// Error that wraps an underlying DeltaTable error.
    /// The wrapped error describes the specific cause.
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// The wrapped DeltaTable error.
        #[from]
        source: DeltaTableError,
    },

    /// Error caused by a problem while using serde_json to serialize an action.
    #[error("Action serialization failed: {source}")]
    ActionSerializationFailed {
        /// The wrapped serde_json Error.
        #[from]
        source: serde_json::Error,
    },
}

impl From<StorageError> for DeltaTransactionError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::AlreadyExists(_) => {
                DeltaTransactionError::VersionAlreadyExists { source: error }
            }
            _ => DeltaTransactionError::Storage { source: error },
        }
    }
}

const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;

/// Options for customizing behavior of a `DeltaTransaction`
#[derive(Debug)]
pub struct DeltaTransactionOptions {
    /// number of retry attempts allowed when committing a transaction
    max_retry_commit_attempts: u32,
}

impl DeltaTransactionOptions {
    /// Creates a new `DeltaTransactionOptions`
    pub fn new(max_retry_commit_attempts: u32) -> Self {
        Self {
            max_retry_commit_attempts,
        }
    }
}

impl Default for DeltaTransactionOptions {
    fn default() -> Self {
        Self {
            max_retry_commit_attempts: DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS,
        }
    }
}

/// Object representing a delta transaction.
/// Clients that do not need to mutate action content in case a transaction conflict is encountered
/// may use the `commit` method and rely on optimistic concurrency to determine the
/// appropriate Delta version number for a commit. A good example of this type of client is an
/// append only client that does not need to maintain transaction state with external systems.
/// Clients that may need to do conflict resolution if the Delta version changes should use
/// the `prepare_commit` and `try_commit_transaction` methods and manage the Delta version
/// themselves so that they can resolve data conflicts that may occur between Delta versions.
///
/// Please not that in case of non-retryable error the temporary commit file such as
/// `_delta_log/_commit_<uuid>.json` will orphaned in storage.
#[derive(Debug)]
pub struct DeltaTransaction<'a> {
    delta_table: &'a mut DeltaTable,
    actions: Vec<Action>,
    options: DeltaTransactionOptions,
}

impl<'a> DeltaTransaction<'a> {
    /// Creates a new delta transaction.
    /// Holds a mutable reference to the delta table to prevent outside mutation while a transaction commit is in progress.
    /// Transaction behavior may be customized by passing an instance of `DeltaTransactionOptions`.
    pub fn new(delta_table: &'a mut DeltaTable, options: Option<DeltaTransactionOptions>) -> Self {
        DeltaTransaction {
            delta_table,
            actions: vec![],
            options: options.unwrap_or_else(DeltaTransactionOptions::default),
        }
    }

    /// Add an arbitrary "action" to the actions associated with this transaction
    pub fn add_action(&mut self, action: action::Action) {
        self.actions.push(action);
    }

    /// Add an arbitrary number of actions to the actions associated with this transaction
    pub fn add_actions(&mut self, actions: Vec<action::Action>) {
        for action in actions.into_iter() {
            self.actions.push(action);
        }
    }

    /// Create a new add action and write the given bytes to the storage backend as a fully formed
    /// Parquet file
    ///
    /// add_file accepts two optional parameters:
    ///
    /// partitions: an ordered vec of WritablePartitionValues for the file to be added
    /// actions: an ordered list of Actions to be inserted into the log file _ahead_ of the Add
    ///     action for the file added. This should typically be used for txn type actions
    pub async fn add_file(
        &mut self,
        bytes: &[u8],
        partitions: Option<Vec<(String, String)>>,
    ) -> Result<(), DeltaTransactionError> {
        let mut partition_values = HashMap::new();
        if let Some(partitions) = &partitions {
            for (key, value) in partitions {
                partition_values.insert(key.clone(), Some(value.clone()));
            }
        }

        let path = self.generate_parquet_filename(partitions);
        let parquet_uri = self
            .delta_table
            .storage
            .join_path(&self.delta_table.table_uri, &path);

        debug!("Writing a parquet file to {}", &parquet_uri);
        self.delta_table
            .storage
            .put_obj(&parquet_uri, bytes)
            .await
            .map_err(|source| DeltaTransactionError::Storage { source })?;

        // Determine the modification timestamp to include in the add action - milliseconds since epoch
        // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
        let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let modification_time = modification_time.as_millis() as i64;

        self.actions.push(Action::add(action::Add {
            path,
            partition_values,
            modification_time,
            size: bytes.len() as i64,
            partition_values_parsed: None,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }));

        Ok(())
    }

    fn generate_parquet_filename(&self, partitions: Option<Vec<(String, String)>>) -> String {
        /*
         * The specific file naming for parquet is not well documented including the preceding five
         * zeros and the trailing c000 string
         *
         */
        let mut path_parts = vec![];

        if let Some(partitions) = partitions {
            for partition in partitions {
                path_parts.push(format!("{}={}", partition.0, partition.1));
            }
        }

        path_parts.push(format!("part-00000-{}-c000.snappy.parquet", Uuid::new_v4()));

        self.delta_table
            .storage
            .join_paths(&path_parts.iter().map(|s| s.as_str()).collect::<Vec<&str>>())
    }

    /// Commits the given actions to the delta log.
    /// This method will retry the transaction commit based on the value of `max_retry_commit_attempts` set in `DeltaTransactionOptions`.
    pub async fn commit(
        &mut self,
        _operation: Option<DeltaOperation>,
    ) -> Result<DeltaDataTypeVersion, DeltaTransactionError> {
        // TODO: stubbing `operation` parameter (which will be necessary for writing the CommitInfo action), but leaving it unused for now.
        // `CommitInfo` is a fairly dynamic data structure so we should work out the data structure approach separately.

        // TODO: calculate isolation level to use when checking for conflicts.
        // Leaving conflict checking unimplemented for now to get the "single writer" implementation off the ground.
        // Leaving some commmented code in place as a guidepost for the future.

        // let no_data_changed = actions.iter().all(|a| match a {
        //     Action::add(x) => !x.dataChange,
        //     Action::remove(x) => !x.dataChange,
        //     _ => false,
        // });
        // let isolation_level = if no_data_changed {
        //     IsolationLevel::SnapshotIsolation
        // } else {
        //     IsolationLevel::Serializable
        // };

        let prepared_commit = self.prepare_commit(_operation).await?;

        // try to commit in a loop in case other writers write the next version first
        let version = self.try_commit_loop(&prepared_commit).await?;

        Ok(version)
    }

    /// Low-level transaction API. Creates a temporary commit file. Once created,
    /// the transaction object could be dropped and the actual commit could be executed
    /// with `DeltaTable.try_commit_transaction`.
    pub async fn prepare_commit(
        &self,
        _operation: Option<DeltaOperation>,
    ) -> Result<PreparedCommit, DeltaTransactionError> {
        let token = Uuid::new_v4().to_string();

        // TODO: create a CommitInfo action and prepend it to actions.
        // Serialize all actions that are part of this log entry.
        let log_entry = log_entry_from_actions(&self.actions)?;

        let file_name = format!("_commit_{}.json", token);
        let uri = self
            .delta_table
            .storage
            .join_path(&self.delta_table.log_uri, &file_name);

        self.delta_table
            .storage
            .put_obj(&uri, log_entry.as_bytes())
            .await?;

        Ok(PreparedCommit { uri })
    }

    async fn try_commit_loop(
        &mut self,
        commit: &PreparedCommit,
    ) -> Result<DeltaDataTypeVersion, DeltaTransactionError> {
        let mut attempt_number: u32 = 0;
        loop {
            self.delta_table.update_incremental().await?;

            let version = self.delta_table.version + 1;

            match self
                .delta_table
                .try_commit_transaction(commit, version)
                .await
            {
                Ok(v) => {
                    return Ok(v);
                }
                Err(e) => {
                    match e {
                        DeltaTransactionError::VersionAlreadyExists { .. }
                            if attempt_number > self.options.max_retry_commit_attempts + 1 =>
                        {
                            debug!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing.", self.options.max_retry_commit_attempts);
                            return Err(e);
                        }
                        DeltaTransactionError::VersionAlreadyExists { .. } => {
                            attempt_number += 1;
                            debug!("Transaction attempt failed. Incrementing attempt number to {} and retrying.", attempt_number);
                        }
                        // NOTE: Add other retryable errors as needed here
                        _ => {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }
}

/// Holds the uri to prepared commit temporary file created with `DeltaTransaction.prepare_commit`.
/// Once created, the actual commit could be executed with `DeltaTransaction.try_commit`.
#[derive(Debug)]
pub struct PreparedCommit {
    uri: String,
}

fn log_entry_from_actions(actions: &[Action]) -> Result<String, serde_json::Error> {
    let mut jsons = Vec::<String>::new();

    for action in actions {
        let json = serde_json::to_string(action)?;
        jsons.push(json);
    }

    Ok(jsons.join("\n"))
}

fn process_action(
    state: &mut DeltaTableState,
    action: Action,
) -> Result<(), serde_json::error::Error> {
    match action {
        Action::add(v) => {
            state.files.push(v);
        }
        Action::remove(v) => {
            state.files.retain(|a| *a.path != v.path);
            state.tombstones.push(v);
        }
        Action::protocol(v) => {
            state.min_reader_version = v.min_reader_version;
            state.min_writer_version = v.min_writer_version;
        }
        Action::metaData(v) => {
            state.current_metadata = Some(DeltaTableMetaData::try_from(v)?);
        }
        Action::txn(v) => {
            *state
                .app_transaction_version
                .entry(v.app_id)
                .or_insert(v.version) = v.version;
        }
        Action::commitInfo(v) => {
            state.commit_infos.push(v);
        }
    }

    Ok(())
}

/// Creates and loads a DeltaTable from the given path with current metadata.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table(table_uri: &str) -> Result<DeltaTable, DeltaTableError> {
    let storage_backend = storage::get_backend_for_uri(table_uri)?;
    let mut table = DeltaTable::new(table_uri, storage_backend)?;
    table.load().await?;

    Ok(table)
}

/// Creates a DeltaTable from the given path and loads it with the metadata from the given version.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table_with_version(
    table_uri: &str,
    version: DeltaDataTypeVersion,
) -> Result<DeltaTable, DeltaTableError> {
    let storage_backend = storage::get_backend_for_uri(table_uri)?;
    let mut table = DeltaTable::new(table_uri, storage_backend)?;
    table.load_version(version).await?;

    Ok(table)
}

/// Creates a DeltaTable from the given path.
/// Loads metadata from the version appropriate based on the given ISO-8601/RFC-3339 timestamp.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table_with_ds(table_uri: &str, ds: &str) -> Result<DeltaTable, DeltaTableError> {
    let datetime = DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(ds)?);
    let storage_backend = storage::get_backend_for_uri(table_uri)?;
    let mut table = DeltaTable::new(table_uri, storage_backend)?;
    table.load_with_datetime(datetime).await?;

    Ok(table)
}

/// Returns rust crate version, can be use used in language bindings to expose Rust core version
pub fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::{collections::HashMap, fs::File, path::Path};

    #[test]
    fn state_records_new_txn_version() {
        let mut app_transaction_version = HashMap::new();
        app_transaction_version.insert("abc".to_string(), 1);
        app_transaction_version.insert("xyz".to_string(), 1);

        let mut state = DeltaTableState {
            files: vec![],
            commit_infos: vec![],
            tombstones: vec![],
            current_metadata: None,
            min_reader_version: 1,
            min_writer_version: 2,
            app_transaction_version,
        };

        let txn_action = Action::txn(action::Txn {
            app_id: "abc".to_string(),
            version: 2,
            last_updated: Some(0),
        });

        let _ = process_action(&mut state, txn_action).unwrap();

        assert_eq!(2, *state.app_transaction_version.get("abc").unwrap());
        assert_eq!(1, *state.app_transaction_version.get("xyz").unwrap());
    }

    #[cfg(feature = "s3")]
    #[test]
    fn normalize_table_uri() {
        for table_uri in [
            "s3://tests/data/delta-0.8.0/",
            "s3://tests/data/delta-0.8.0//",
            "s3://tests/data/delta-0.8.0",
        ]
        .iter()
        {
            let be = storage::get_backend_for_uri(table_uri).unwrap();
            let table = DeltaTable::new(table_uri, be).unwrap();
            assert_eq!(table.table_uri, "s3://tests/data/delta-0.8.0");
        }
    }

    #[test]
    fn rel_path() {
        assert!(matches!(
            extract_rel_path("data/delta-0.8.0", "data/delta-0.8.0/abc/123"),
            Ok("abc/123"),
        ));

        assert!(matches!(
            extract_rel_path("data/delta-0.8.0", "data/delta-0.8.0/abc.json"),
            Ok("abc.json"),
        ));

        assert!(matches!(
            extract_rel_path("data/delta-0.8.0", "tests/abc.json"),
            Err(DeltaTableError::Generic(_)),
        ));
    }

    #[tokio::test]
    async fn parquet_filename() {
        let mut table = open_table("./tests/data/simple_table").await.unwrap();

        let txn = DeltaTransaction {
            delta_table: &mut table,
            actions: vec![],
            options: DeltaTransactionOptions::default(),
        };

        let partitions = vec![
            (String::from("col1"), String::from("a")),
            (String::from("col2"), String::from("b")),
        ];
        let parquet_filename = txn.generate_parquet_filename(Some(partitions));
        if cfg!(windows) {
            assert!(parquet_filename.contains("col1=a\\col2=b\\part-00000-"));
        } else {
            assert!(parquet_filename.contains("col1=a/col2=b/part-00000-"));
        }
    }

    #[tokio::test]
    async fn test_create_delta_table() {
        // Setup
        let test_schema = Schema::new(
            "test".to_string(),
            vec![
                SchemaField::new(
                    "Id".to_string(),
                    SchemaDataType::primitive("integer".to_string()),
                    true,
                    HashMap::new(),
                ),
                SchemaField::new(
                    "Name".to_string(),
                    SchemaDataType::primitive("string".to_string()),
                    true,
                    HashMap::new(),
                ),
            ],
        );

        let delta_md = DeltaTableMetaData::new(
            Some("Test Table Create".to_string()),
            Some("This table is made to test the create function for a DeltaTable".to_string()),
            None,
            test_schema,
            vec![],
            HashMap::new(),
        );

        let protocol = action::Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let tmp_dir = tempdir::TempDir::new("create_table_test").unwrap();
        let table_dir = tmp_dir.path().join("test_create");

        let path = table_dir.to_str().unwrap();
        let backend = Box::new(storage::file::FileStorageBackend::new(
            tmp_dir.path().to_str().unwrap(),
        ));
        let mut dt = DeltaTable::new(path, backend).unwrap();

        // Action
        dt.create(delta_md.clone(), protocol.clone()).await.unwrap();

        // Validation
        // assert DeltaTable version is now 0 and no data files have been added
        assert_eq!(dt.version, 0);
        assert_eq!(dt.state.files.len(), 0);

        // assert new _delta_log file created in tempDir
        let table_path = Path::new(&dt.table_uri);
        assert!(table_path.exists());

        let delta_log = table_path.join("_delta_log");
        assert!(delta_log.exists());

        let version_file = delta_log.join("00000000000000000000.json");
        assert!(version_file.exists());

        // Checking the data written to delta table is the same when read back
        let version_data = File::open(version_file).unwrap();
        let lines = BufReader::new(version_data).lines();

        for line in lines {
            let action: Action = serde_json::from_str(line.unwrap().as_str()).unwrap();
            match action {
                Action::protocol(action) => {
                    assert_eq!(action, protocol);
                }
                Action::metaData(action) => {
                    assert_eq!(DeltaTableMetaData::try_from(action).unwrap(), delta_md);
                }
                _ => (),
            }
        }

        // assert DeltaTableState metadata matches fields in above DeltaTableMetaData
        // assert metadata name
        let current_metadata = dt.get_metadata().unwrap();
        assert!(current_metadata.partition_columns.is_empty());
        assert!(current_metadata.configuration.is_empty());
    }
}
