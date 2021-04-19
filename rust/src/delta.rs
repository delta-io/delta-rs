// Reference: https://github.com/delta-io/delta/blob/master/PROTOCOL.md

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::io::{BufRead, BufReader, Cursor};

use arrow::error::ArrowError;
use chrono::{DateTime, FixedOffset, Utc};
use futures::StreamExt;
use lazy_static::lazy_static;
use log::debug;
use parquet::errors::ParquetError;
use parquet::file::{
    reader::{FileReader, SerializedFileReader},
    serialized_reader::SliceableCursor,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::convert::TryFrom;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::action;
use super::action::{Action, DeltaOperation};
use super::partitions::{DeltaTablePartition, PartitionFilter};
use super::schema::*;
use super::storage;
use super::storage::{StorageBackend, StorageError, UriError};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    version: DeltaDataTypeVersion, // 20 digits decimals
    size: DeltaDataTypeLong,
    parts: Option<u32>, // 10 digits decimals
}

impl PartialEq for CheckPoint {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl Eq for CheckPoint {}

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
    #[error("Failed to convert into Arrow schema: {}", .source)]
    ArrowError {
        #[from]
        source: ArrowError,
    },
    #[error("Invalid table path: {}", .source)]
    UriError {
        #[from]
        source: UriError,
    },
    #[error("Invalid JSON in log record: {}", .source)]
    InvalidJson {
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
    #[error("Not a Delta table")]
    NotATable,
    #[error("No metadata found, please make sure table is loaded.")]
    NoMetadata,
    #[error("No schema found, please make sure table is loaded.")]
    NoSchema,
    #[error("No partitions found, please make sure table is partitioned.")]
    LoadPartitions,
    #[error("This partition is not formatted with key=value: {}", .partition)]
    PartitionError { partition: String },
    #[error("Invalid partition filter found: {}.", .partition_filter)]
    InvalidPartitionFilter { partition_filter: String },
    #[error(
        "Invalid retention period, retention for Vacuum must be greater than 1 week (168 hours)"
    )]
    InvalidVacuumRetentionPeriod,
}

#[derive(Clone)]
pub struct DeltaTableMetaData {
    // Unique identifier for this table
    pub id: Guid,
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
    // The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: DeltaDataTypeTimestamp,
    // table properties
    pub configuration: HashMap<String, String>,
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

#[derive(thiserror::Error, Debug)]
pub enum ApplyLogError {
    #[error("End of transaction log")]
    EndOfLog,
    #[error("Invalid JSON in log record")]
    InvalidJson {
        #[from]
        source: serde_json::error::Error,
    },
    #[error("Failed to read log content")]
    Storage { source: StorageError },
    #[error("Failed to read line from log record")]
    Io {
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
    #[error("Invalid JSON in checkpoint: {source}")]
    InvalidJson {
        #[from]
        source: serde_json::error::Error,
    },
    #[error("Failed to read checkpoint content: {source}")]
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

#[derive(Default)]
struct DeltaTableState {
    // A remove action should remain in the state of the table as a tombstone until it has expired
    // vacuum operation is responsible for providing the retention threshold
    tombstones: Vec<action::Remove>,
    files: Vec<String>,
    commit_infos: Vec<Value>,
    app_transaction_version: HashMap<String, DeltaDataTypeVersion>,
    min_reader_version: i32,
    min_writer_version: i32,
    current_metadata: Option<DeltaTableMetaData>,
}

pub struct DeltaTable {
    pub version: DeltaDataTypeVersion,
    pub table_path: String,

    state: DeltaTableState,

    // metadata
    // application_transactions
    storage: Box<dyn StorageBackend>,

    last_check_point: Option<CheckPoint>,
    log_path: String,
    version_timestamp: HashMap<DeltaDataTypeVersion, i64>,
}

impl DeltaTable {
    fn version_to_log_path(&self, version: DeltaDataTypeVersion) -> String {
        let version = format!("{:020}.json", version);
        self.storage.join_path(&self.log_path, &version)
    }

    fn tmp_commit_log_path(&self, token: &str) -> String {
        let path = format!("_commit_{}.json", token);
        self.storage.join_path(&self.log_path, &path)
    }

    fn get_checkpoint_data_paths(&self, check_point: &CheckPoint) -> Vec<String> {
        let checkpoint_prefix_pattern = format!("{:020}", check_point.version);
        let checkpoint_prefix = self
            .storage
            .join_path(&self.log_path, &checkpoint_prefix_pattern);
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
        let last_checkpoint_path = self.storage.join_path(&self.log_path, "_last_checkpoint");
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
        let mut stream = self.storage.list_objs(&self.log_path).await?;

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
            process_action(&mut self.state, &action)?;
        }

        Ok(())
    }

    async fn apply_log(&mut self, version: DeltaDataTypeVersion) -> Result<(), ApplyLogError> {
        let log_path = self.version_to_log_path(version);
        let commit_log_bytes = self.storage.get_obj(&log_path).await?;
        let reader = BufReader::new(Cursor::new(commit_log_bytes));

        self.apply_log_from_bufread(reader)
    }

    async fn restore_checkpoint(&mut self, check_point: CheckPoint) -> Result<(), DeltaTableError> {
        let checkpoint_data_paths = self.get_checkpoint_data_paths(&check_point);
        // process actions from checkpoint
        self.state = DeltaTableState::default();
        for f in &checkpoint_data_paths {
            let obj = self.storage.get_obj(&f).await?;
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
                    &Action::from_parquet_record(&schema, &record)?,
                )?;
            }
        }

        Ok(())
    }

    async fn get_latest_version(&mut self) -> Result<DeltaDataTypeVersion, DeltaTableError> {
        let mut version = match self.get_last_checkpoint().await {
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
            match self
                .storage
                .head_obj(&self.version_to_log_path(version))
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
                        }
                        _ => return Err(DeltaTableError::from(e)),
                    }
                    break;
                }
            }
        }

        Ok(version)
    }

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

        self.apply_logs_after_current_version().await?;

        Ok(())
    }

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

        self.apply_logs_after_current_version().await?;

        Ok(())
    }

    async fn apply_logs_after_current_version(&mut self) -> Result<(), DeltaTableError> {
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
                                // no snapshot found, no 0 version found.  this is not a delta
                                // table, possibly an empty directroy.
                                return Err(DeltaTableError::NotATable);
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

    pub async fn load_version(
        &mut self,
        version: DeltaDataTypeVersion,
    ) -> Result<(), DeltaTableError> {
        // check if version is valid
        let log_path = self.version_to_log_path(version);
        match self.storage.head_obj(&log_path).await {
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
                // no checkpoint found, start from the beginning
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
                    .head_obj(&self.version_to_log_path(version))
                    .await?;
                let ts = meta.modified.timestamp();
                // also cache timestamp for version
                self.version_timestamp.insert(version, ts);

                Ok(ts)
            }
        }
    }

    pub fn get_files_by_partitions(
        &self,
        filters: &[PartitionFilter<&str>],
    ) -> Result<Vec<String>, DeltaTableError> {
        let partitions_number = match &self
            .state
            .current_metadata
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)?
            .partition_columns
        {
            partitions if !partitions.is_empty() => partitions.len(),
            _ => return Err(DeltaTableError::LoadPartitions),
        };
        let separator = "/";
        let files = self
            .state
            .files
            .iter()
            .filter(|f| {
                let partitions = f
                    .splitn(partitions_number + 1, separator)
                    .filter_map(|p: &str| DeltaTablePartition::try_from(p).ok())
                    .collect::<Vec<DeltaTablePartition>>();
                filters
                    .iter()
                    .all(|filter| filter.match_partitions(&partitions))
            })
            .cloned()
            .collect();

        Ok(files)
    }

    pub fn get_files(&self) -> &Vec<String> {
        &self.state.files
    }

    pub fn get_file_paths(&self) -> Vec<String> {
        self.state
            .files
            .iter()
            .map(|fname| self.storage.join_path(&self.table_path, fname))
            .collect()
    }

    pub fn get_metadata(&self) -> Result<&DeltaTableMetaData, DeltaTableError> {
        self.state
            .current_metadata
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)
    }

    pub fn get_tombstones(&self) -> &Vec<action::Remove> {
        &self.state.tombstones
    }

    pub fn get_app_transaction_version(&self) -> &HashMap<String, DeltaDataTypeVersion> {
        &self.state.app_transaction_version
    }

    pub fn get_min_reader_version(&self) -> i32 {
        self.state.min_reader_version
    }

    pub fn get_min_writer_version(&self) -> i32 {
        self.state.min_writer_version
    }

    /// Run the dry run of the Vacuum command on the Delta Table: list files no longer referenced by a Delta table and are older than the retention threshold.
    /// We do not recommend that you set a retention interval shorter than 7 days, because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table. If vacuum cleans up active files, concurrent readers can fail or, worse, tables can be corrupted when vacuum deletes files that have not yet been committed.
    pub fn vacuum_dry_run(&self, retention_hours: u64) -> Result<Vec<String>, DeltaTableError> {
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
            .filter(|tombstone| tombstone.deletionTimestamp < delete_before_timestamp)
            .map(|tombstone| self.storage.join_path(&self.table_path, &tombstone.path))
            .collect::<Vec<String>>())
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.state.current_metadata.as_ref().map(|m| &m.schema)
    }

    pub fn get_schema(&self) -> Result<&Schema, DeltaTableError> {
        self.schema().ok_or(DeltaTableError::NoSchema)
    }

    pub fn create_transaction(
        &mut self,
        options: Option<DeltaTransactionOptions>,
    ) -> DeltaTransaction {
        DeltaTransaction::new(self, options)
    }

    pub fn new(
        table_path: &str,
        storage_backend: Box<dyn StorageBackend>,
    ) -> Result<Self, DeltaTableError> {
        let log_path_normalized = storage_backend.join_path(table_path, "_delta_log");
        Ok(Self {
            version: 0,
            state: DeltaTableState::default(),
            storage: storage_backend,
            table_path: table_path.to_string(),
            last_check_point: None,
            log_path: log_path_normalized,
            version_timestamp: HashMap::new(),
        })
    }

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
        writeln!(f, "DeltaTable({})", self.table_path)?;
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
        write!(f, "DeltaTable <{}>", self.table_path)
    }
}

/// Error returned by the DeltaTransaction struct
#[derive(thiserror::Error, Debug)]
pub enum DeltaTransactionError {
    #[error("Transaction commit exceeded max retries. Last error: {inner}")]
    CommitRetriesExceeded {
        #[from]
        inner: TransactionCommitAttemptError,
    },

    #[error("RecordBatch is missing partition column in Delta schema.")]
    MissingPartitionColumn,

    #[error("Storage interaction failed: {source}")]
    Storage { source: StorageError },

    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("Action serialization failed: {source}")]
    ActionSerializationFailed {
        #[from]
        source: serde_json::Error,
    },
}

/// Error that occurs when a single transaction commit attempt fails
#[derive(thiserror::Error, Debug)]
pub enum TransactionCommitAttemptError {
    // NOTE: it would be nice to add a `num_retries` prop to this error so we can identify how frequently we hit optimistic concurrency retries and look for optimization paths
    #[error("Version already exists: {source}")]
    VersionExists { source: StorageError },

    #[error("Commit Failed due to DeltaTable error: {source}")]
    DeltaTable {
        #[from]
        source: DeltaTableError,
    },

    #[error("Commit Failed due to StorageError: {source}")]
    Storage { source: StorageError },
}

impl From<StorageError> for TransactionCommitAttemptError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::AlreadyExists(_) => {
                TransactionCommitAttemptError::VersionExists { source: error }
            }
            _ => TransactionCommitAttemptError::Storage { source: error },
        }
    }
}

const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;

/// Options for customizing behavior of a `DeltaTransaction`
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

/// Object representing a delta transaction
pub struct DeltaTransaction<'a> {
    delta_table: &'a mut DeltaTable,
    options: DeltaTransactionOptions,
}

impl<'a> DeltaTransaction<'a> {
    /// Creates a new delta transaction.
    /// Holds a mutable reference to the delta table to prevent outside mutation while a transaction commit is in progress.
    /// Transaction behavior may be customized by passing an instance of `DeltaTransactionOptions`.
    pub fn new(delta_table: &'a mut DeltaTable, options: Option<DeltaTransactionOptions>) -> Self {
        DeltaTransaction {
            delta_table,
            options: options.unwrap_or_else(DeltaTransactionOptions::default),
        }
    }

    /// Commits the given actions to the delta log.
    /// This method will retry the transaction commit based on the value of `max_retry_commit_attempts` set in `DeltaTransactionOptions`.
    pub async fn commit_with(
        &mut self,
        additional_actions: &[Action],
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

        // TODO: create a CommitInfo action and prepend it to actions.

        // Serialize all actions that are part of this log entry.
        let mut jsons = Vec::<String>::new();

        for action in additional_actions {
            let json = serde_json::to_string(action)?;
            jsons.push(json);
        }

        let log_entry = jsons.join("\n");
        let log_entry = log_entry.as_bytes();

        // try to commit in a loop in case other writers write the next version first
        let version = self.try_commit_loop(log_entry).await?;

        // NOTE: since we have the log entry in memory already,
        // we could optimize this further by merging the log entry instead of updating from storage.
        self.delta_table.update().await?;

        Ok(version)
    }

    async fn try_commit_loop(
        &mut self,
        log_entry: &[u8],
    ) -> Result<DeltaDataTypeVersion, TransactionCommitAttemptError> {
        let mut attempt_number: u32 = 0;

        let tmp_log_path = self.prepare_commit(log_entry).await?;
        loop {
            let commit_result = self.try_commit(&tmp_log_path).await;

            match commit_result {
                Ok(v) => {
                    return Ok(v);
                }
                Err(e) => {
                    match e {
                        TransactionCommitAttemptError::VersionExists { .. }
                            if attempt_number > self.options.max_retry_commit_attempts + 1 =>
                        {
                            debug!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing.", self.options.max_retry_commit_attempts);
                            return Err(e);
                        }
                        TransactionCommitAttemptError::VersionExists { .. } => {
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

    async fn prepare_commit(
        &mut self,
        log_entry: &[u8],
    ) -> Result<String, TransactionCommitAttemptError> {
        let token = Uuid::new_v4().to_string();
        let tmp_log_path = self.delta_table.tmp_commit_log_path(&token);

        self.delta_table
            .storage
            .put_obj(&tmp_log_path, log_entry)
            .await?;

        Ok(tmp_log_path)
    }

    async fn try_commit(
        &mut self,
        tmp_log_path: &str,
    ) -> Result<DeltaDataTypeVersion, TransactionCommitAttemptError> {
        // get the next delta table version and the log path where it should be written
        let attempt_version = self.next_attempt_version().await?;
        let log_path = self.delta_table.version_to_log_path(attempt_version);

        // move temporary commit file to delta log directory
        // rely on storage to fail if the file already exists -
        self.delta_table
            .storage
            .rename_obj(tmp_log_path, &log_path)
            .await?;

        Ok(attempt_version)
    }

    async fn next_attempt_version(
        &mut self,
    ) -> Result<DeltaDataTypeVersion, TransactionCommitAttemptError> {
        self.delta_table.update().await?;
        Ok(self.delta_table.version + 1)
    }
}

fn process_action(
    state: &mut DeltaTableState,
    action: &Action,
) -> Result<(), serde_json::error::Error> {
    match action {
        Action::add(v) => {
            state.files.push(v.path.clone());
        }
        Action::remove(v) => {
            state.files.retain(|e| *e != v.path);
            state.tombstones.push(v.clone());
        }
        Action::protocol(v) => {
            state.min_reader_version = v.minReaderVersion;
            state.min_writer_version = v.minWriterVersion;
        }
        Action::metaData(v) => {
            state.current_metadata = Some(DeltaTableMetaData {
                id: v.id.clone(),
                name: v.name.clone(),
                description: v.description.clone(),
                format: v.format.clone(),
                schema: v.get_schema()?,
                partition_columns: v.partitionColumns.clone(),
                created_time: v.createdTime,
                configuration: v.configuration.clone(),
            });
        }
        Action::txn(v) => {
            *state
                .app_transaction_version
                .entry(v.appId.clone())
                .or_insert(v.version) = v.version;
        }
        Action::commitInfo(v) => {
            state.commit_infos.push(v.clone());
        }
    }

    Ok(())
}

pub async fn open_table(table_path: &str) -> Result<DeltaTable, DeltaTableError> {
    let storage_backend = storage::get_backend_for_uri(table_path)?;
    let mut table = DeltaTable::new(table_path, storage_backend)?;
    table.load().await?;

    Ok(table)
}

pub async fn open_table_with_version(
    table_path: &str,
    version: DeltaDataTypeVersion,
) -> Result<DeltaTable, DeltaTableError> {
    let storage_backend = storage::get_backend_for_uri(table_path)?;
    let mut table = DeltaTable::new(table_path, storage_backend)?;
    table.load_version(version).await?;

    Ok(table)
}

pub async fn open_table_with_ds(table_path: &str, ds: &str) -> Result<DeltaTable, DeltaTableError> {
    let datetime = DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(ds)?);
    let storage_backend = storage::get_backend_for_uri(table_path)?;
    let mut table = DeltaTable::new(table_path, storage_backend)?;
    table.load_with_datetime(datetime).await?;

    Ok(table)
}

/// Returns rust create version, can be use used in language bindings to expose Rust core version
pub fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::action;
    use super::action::Action;
    use super::{process_action, DeltaTableState};
    use std::collections::HashMap;

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
            appId: "abc".to_string(),
            version: 2,
            lastUpdated: 0,
        });

        let _ = process_action(&mut state, &txn_action).unwrap();

        assert_eq!(2, *state.app_transaction_version.get("abc").unwrap());
        assert_eq!(1, *state.app_transaction_version.get("xyz").unwrap());
    }
}
