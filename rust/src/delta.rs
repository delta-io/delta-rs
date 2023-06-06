//! Delta Table read and write implementation

// Reference: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
//
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Formatter;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;
use std::{cmp::max, cmp::Ordering, collections::HashSet};

use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use lazy_static::lazy_static;
use log::debug;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use regex::Regex;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::action;
use super::action::{
    find_latest_check_point_for_version, get_last_checkpoint, Action, DeltaOperation,
};
use super::partitions::PartitionFilter;
use super::schema::*;
use super::table_state::DeltaTableState;
use crate::action::{Add, ProtocolError, Stats};
use crate::errors::DeltaTableError;
use crate::operations::vacuum::VacuumBuilder;
use crate::storage::{commit_uri_from_version, ObjectStoreRef};

// TODO re-exports only for transition
pub use crate::builder::{DeltaTableBuilder, DeltaTableConfig, DeltaVersion};

/// Metadata for a checkpoint file
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    /// Delta table version
    pub(crate) version: i64, // 20 digits decimals
    pub(crate) size: i64,
    pub(crate) parts: Option<u32>, // 10 digits decimals
}

impl CheckPoint {
    /// Creates a new checkpoint from the given parameters.
    pub fn new(version: i64, size: i64, parts: Option<u32>) -> Self {
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

/// Delta table metadata
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
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
    pub created_time: Option<i64>,
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
            created_time: Some(Utc::now().timestamp_millis()),
            configuration,
        }
    }

    /// Return the configurations of the DeltaTableMetaData; could be empty
    pub fn get_configuration(&self) -> &HashMap<String, Option<String>> {
        &self.configuration
    }

    /// Return partition fields along with their data type from the current schema.
    pub fn get_partition_col_data_types(&self) -> Vec<(&str, &SchemaDataType)> {
        // JSON add actions contain a `partitionValues` field which is a map<string, string>.
        // When loading `partitionValues_parsed` we have to convert the stringified partition values back to the correct data type.
        self.schema
            .get_fields()
            .iter()
            .filter_map(|f| {
                if self
                    .partition_columns
                    .iter()
                    .any(|s| s.as_str() == f.get_name())
                {
                    Some((f.get_name(), f.get_type()))
                } else {
                    None
                }
            })
            .collect()
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
    type Error = ProtocolError;

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

/// The next commit that's available from underlying storage
/// TODO: Maybe remove this and replace it with Some/None and create a `Commit` struct to contain the next commit
///
#[derive(Debug)]
pub enum PeekCommit {
    /// The next commit version and associated actions
    New(i64, Vec<Action>),
    /// Provided DeltaVersion is up to date
    UpToDate,
}

/// In memory representation of a Delta Table
pub struct DeltaTable {
    /// The state of the table as of the most recent loaded Delta log entry.
    pub state: DeltaTableState,
    /// the load options used during load
    pub config: DeltaTableConfig,
    /// object store to access log and data files
    pub(crate) storage: ObjectStoreRef,
    /// file metadata for latest checkpoint
    last_check_point: Option<CheckPoint>,
    /// table versions associated with timestamps
    version_timestamp: HashMap<i64, i64>,
}

impl Serialize for DeltaTable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.state)?;
        seq.serialize_element(&self.config)?;
        seq.serialize_element(self.storage.as_ref())?;
        seq.serialize_element(&self.last_check_point)?;
        seq.serialize_element(&self.version_timestamp)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for DeltaTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DeltaTableVisitor {}

        impl<'de> Visitor<'de> for DeltaTableVisitor {
            type Value = DeltaTable;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                formatter.write_str("struct DeltaTable")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let state = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let config = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let storage = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let last_check_point = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let version_timestamp = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;

                let table = DeltaTable {
                    state,
                    config,
                    storage: Arc::new(storage),
                    last_check_point,
                    version_timestamp,
                };
                Ok(table)
            }
        }

        deserializer.deserialize_seq(DeltaTableVisitor {})
    }
}

impl DeltaTable {
    /// Create a new Delta Table struct without loading any data from backing storage.
    ///
    /// NOTE: This is for advanced users. If you don't know why you need to use this method, please
    /// call one of the `open_table` helper methods instead.
    pub fn new(storage: ObjectStoreRef, config: DeltaTableConfig) -> Self {
        Self {
            state: DeltaTableState::with_version(-1),
            storage,
            config,
            last_check_point: None,
            version_timestamp: HashMap::new(),
        }
    }

    /// Create a new [`DeltaTable`] from a [`DeltaTableState`] without loading any
    /// data from backing storage.
    ///
    /// NOTE: This is for advanced users. If you don't know why you need to use this method,
    /// please call one of the `open_table` helper methods instead.
    pub(crate) fn new_with_state(storage: ObjectStoreRef, state: DeltaTableState) -> Self {
        Self {
            state,
            storage,
            config: Default::default(),
            last_check_point: None,
            version_timestamp: HashMap::new(),
        }
    }

    /// get a shared reference to the delta object store
    pub fn object_store(&self) -> ObjectStoreRef {
        self.storage.clone()
    }

    /// The URI of the underlying data
    pub fn table_uri(&self) -> String {
        self.storage.root_uri()
    }

    /// Return the list of paths of given checkpoint.
    pub fn get_checkpoint_data_paths(&self, check_point: &CheckPoint) -> Vec<Path> {
        let checkpoint_prefix = format!("{:020}", check_point.version);
        let log_path = self.storage.log_path();
        let mut checkpoint_data_paths = Vec::new();

        match check_point.parts {
            None => {
                let path = log_path.child(&*format!("{checkpoint_prefix}.checkpoint.parquet"));
                checkpoint_data_paths.push(path);
            }
            Some(parts) => {
                for i in 0..parts {
                    let path = log_path.child(&*format!(
                        "{}.checkpoint.{:010}.{:010}.parquet",
                        checkpoint_prefix,
                        i + 1,
                        parts
                    ));
                    checkpoint_data_paths.push(path);
                }
            }
        }

        checkpoint_data_paths
    }

    /// This method scans delta logs to find the earliest delta log version
    async fn get_earliest_delta_log_version(&self) -> Result<i64, DeltaTableError> {
        // TODO check if regex matches against path
        lazy_static! {
            static ref DELTA_LOG_REGEX: Regex =
                Regex::new(r#"^_delta_log/(\d{20})\.(json|checkpoint)*$"#).unwrap();
        }

        let mut current_delta_log_ver = i64::MAX;

        // Get file objects from table.
        let mut stream = self.storage.list(Some(self.storage.log_path())).await?;
        while let Some(obj_meta) = stream.next().await {
            let obj_meta = obj_meta?;

            if let Some(captures) = DELTA_LOG_REGEX.captures(obj_meta.location.as_ref()) {
                let log_ver_str = captures.get(1).unwrap().as_str();
                let log_ver: i64 = log_ver_str.parse().unwrap();
                if log_ver < current_delta_log_ver {
                    current_delta_log_ver = log_ver;
                }
            }
        }
        Ok(current_delta_log_ver)
    }

    #[cfg(any(feature = "parquet", feature = "parquet2"))]
    async fn restore_checkpoint(&mut self, check_point: CheckPoint) -> Result<(), DeltaTableError> {
        self.state = DeltaTableState::from_checkpoint(self, &check_point).await?;

        Ok(())
    }

    async fn get_latest_version(&mut self) -> Result<i64, DeltaTableError> {
        let mut version = match get_last_checkpoint(&self.storage).await {
            Ok(last_check_point) => last_check_point.version + 1,
            Err(ProtocolError::CheckpointNotFound) => {
                // no checkpoint, start with version 0
                0
            }
            Err(e) => {
                return Err(DeltaTableError::from(e));
            }
        };

        debug!("start with latest checkpoint version: {version}");

        // scan logs after checkpoint
        loop {
            match self.storage.head(&commit_uri_from_version(version)).await {
                Ok(meta) => {
                    // also cache timestamp for version
                    self.version_timestamp
                        .insert(version, meta.last_modified.timestamp());
                    version += 1;
                }
                Err(e) => {
                    match e {
                        ObjectStoreError::NotFound { .. } => {
                            version -= 1;
                            if version < 0 {
                                return Err(DeltaTableError::not_a_table(self.table_uri()));
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

    /// Currently loaded version of the table
    pub fn version(&self) -> i64 {
        self.state.version()
    }

    /// Load DeltaTable with data from latest checkpoint
    pub async fn load(&mut self) -> Result<(), DeltaTableError> {
        self.last_check_point = None;
        self.state = DeltaTableState::with_version(-1);
        self.update().await
    }

    /// Get the list of actions for the next commit
    pub async fn peek_next_commit(
        &self,
        current_version: i64,
    ) -> Result<PeekCommit, DeltaTableError> {
        let next_version = current_version + 1;
        let commit_uri = commit_uri_from_version(next_version);
        let commit_log_bytes = self.storage.get(&commit_uri).await;
        let commit_log_bytes = match commit_log_bytes {
            Err(ObjectStoreError::NotFound { .. }) => return Ok(PeekCommit::UpToDate),
            Err(err) => Err(err),
            Ok(result) => result.bytes().await,
        }?;

        debug!("parsing commit with version {next_version}...");
        let reader = BufReader::new(Cursor::new(commit_log_bytes));

        let mut actions = Vec::new();
        for re_line in reader.lines() {
            let line = re_line?;
            let lstr = line.as_str();
            let action =
                serde_json::from_str(lstr).map_err(|e| DeltaTableError::InvalidJsonLog {
                    json_err: e,
                    version: next_version,
                    line,
                })?;
            actions.push(action);
        }
        Ok(PeekCommit::New(next_version, actions))
    }

    /// Updates the DeltaTable to the most recent state committed to the transaction log by
    /// loading the last checkpoint and incrementally applying each version since.
    #[cfg(any(feature = "parquet", feature = "parquet2"))]
    pub async fn update(&mut self) -> Result<(), DeltaTableError> {
        match get_last_checkpoint(&self.storage).await {
            Ok(last_check_point) => {
                debug!("update with latest checkpoint {last_check_point:?}");
                if Some(last_check_point) == self.last_check_point {
                    self.update_incremental(None).await
                } else {
                    self.last_check_point = Some(last_check_point);
                    self.restore_checkpoint(last_check_point).await?;
                    self.update_incremental(None).await
                }
            }
            Err(ProtocolError::CheckpointNotFound) => {
                debug!("update without checkpoint");
                self.update_incremental(None).await
            }
            Err(err) => Err(DeltaTableError::from(err)),
        }
    }

    /// Updates the DeltaTable to the most recent state committed to the transaction log.
    #[cfg(not(any(feature = "parquet", feature = "parquet2")))]
    pub async fn update(&mut self) -> Result<(), DeltaTableError> {
        self.update_incremental(None).await
    }

    /// Updates the DeltaTable to the latest version by incrementally applying newer versions.
    /// It assumes that the table is already updated to the current version `self.version`.
    pub async fn update_incremental(
        &mut self,
        max_version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        debug!(
            "incremental update with version({}) and max_version({max_version:?})",
            self.version(),
        );

        while let PeekCommit::New(new_version, actions) =
            self.peek_next_commit(self.version()).await?
        {
            debug!("merging table state with version: {new_version}");
            let s = DeltaTableState::from_actions(actions, new_version)?;
            self.state
                .merge(s, self.config.require_tombstones, self.config.require_files);
            if Some(self.version()) == max_version {
                return Ok(());
            }
        }

        if self.version() == -1 {
            return Err(DeltaTableError::not_a_table(self.table_uri()));
        }

        Ok(())
    }

    /// Loads the DeltaTable state for the given version.
    pub async fn load_version(&mut self, version: i64) -> Result<(), DeltaTableError> {
        // check if version is valid
        let commit_uri = commit_uri_from_version(version);
        match self.storage.head(&commit_uri).await {
            Ok(_) => {}
            Err(ObjectStoreError::NotFound { .. }) => {
                return Err(DeltaTableError::InvalidVersion(version));
            }
            Err(e) => {
                return Err(DeltaTableError::from(e));
            }
        }

        // 1. find latest checkpoint below version
        #[cfg(any(feature = "parquet", feature = "parquet2"))]
        match find_latest_check_point_for_version(&self.storage, version).await? {
            Some(check_point) => {
                self.restore_checkpoint(check_point).await?;
            }
            None => {
                // no checkpoint found, clear table state and start from the beginning
                self.state = DeltaTableState::with_version(-1);
            }
        }

        debug!("update incrementally from version {version}");
        // 2. apply all logs starting from checkpoint
        self.update_incremental(Some(version)).await?;

        Ok(())
    }

    pub(crate) async fn get_version_timestamp(
        &mut self,
        version: i64,
    ) -> Result<i64, DeltaTableError> {
        match self.version_timestamp.get(&version) {
            Some(ts) => Ok(*ts),
            None => {
                let meta = self.storage.head(&commit_uri_from_version(version)).await?;
                let ts = meta.last_modified.timestamp();
                // also cache timestamp for version
                self.version_timestamp.insert(version, ts);

                Ok(ts)
            }
        }
    }

    /// Returns provenance information, including the operation, user, and so on, for each write to a table.
    /// The table history retention is based on the `logRetentionDuration` property of the Delta Table, 30 days by default.
    /// If `limit` is given, this returns the information of the latest `limit` commits made to this table. Otherwise,
    /// it returns all commits from the earliest commit.
    pub async fn history(
        &mut self,
        limit: Option<usize>,
    ) -> Result<Vec<action::CommitInfo>, DeltaTableError> {
        let mut version = match limit {
            Some(l) => max(self.version() - l as i64 + 1, 0),
            None => self.get_earliest_delta_log_version().await?,
        };
        let mut commit_infos_list = vec![];
        let mut earliest_commit: Option<i64> = None;

        loop {
            match DeltaTableState::from_commit(self, version).await {
                Ok(state) => {
                    commit_infos_list.append(state.commit_infos().clone().as_mut());
                    version += 1;
                }
                Err(e) => {
                    match e {
                        ProtocolError::EndOfLog => {
                            if earliest_commit.is_none() {
                                earliest_commit =
                                    Some(self.get_earliest_delta_log_version().await?);
                            };
                            if let Some(earliest) = earliest_commit {
                                if version < earliest {
                                    version = earliest;
                                    continue;
                                }
                            } else {
                                version -= 1;
                                if version == -1 {
                                    return Err(DeltaTableError::not_a_table(self.table_uri()));
                                }
                            }
                        }
                        _ => {
                            return Err(DeltaTableError::from(e));
                        }
                    }
                    return Ok(commit_infos_list);
                }
            }
        }
    }

    /// Obtain Add actions for files that match the filter
    pub fn get_active_add_actions_by_partitions<'a>(
        &'a self,
        filters: &'a [PartitionFilter<'a, &'a str>],
    ) -> Result<impl Iterator<Item = &'a Add> + '_, DeltaTableError> {
        self.state.get_active_add_actions_by_partitions(filters)
    }

    /// Returns the file list tracked in current table state filtered by provided
    /// `PartitionFilter`s.
    pub fn get_files_by_partitions(
        &self,
        filters: &[PartitionFilter<&str>],
    ) -> Result<Vec<Path>, DeltaTableError> {
        Ok(self
            .get_active_add_actions_by_partitions(filters)?
            .map(|add| Path::from(add.path.as_ref()))
            .collect())
    }

    /// Return the file uris as strings for the partition(s)
    pub fn get_file_uris_by_partitions(
        &self,
        filters: &[PartitionFilter<&str>],
    ) -> Result<Vec<String>, DeltaTableError> {
        let files = self.get_files_by_partitions(filters)?;
        Ok(files
            .iter()
            .map(|fname| self.storage.to_uri(fname))
            .collect())
    }

    /// Returns an iterator of file names present in the loaded state
    #[inline]
    pub fn get_files_iter(&self) -> impl Iterator<Item = Path> + '_ {
        self.state.file_paths_iter()
    }

    /// Returns a collection of file names present in the loaded state
    #[inline]
    pub fn get_files(&self) -> Vec<Path> {
        self.state.file_paths_iter().collect()
    }

    /// Returns file names present in the loaded state in HashSet
    pub fn get_file_set(&self) -> HashSet<Path> {
        self.state.file_paths_iter().collect()
    }

    /// Returns a URIs for all active files present in the current table version.
    pub fn get_file_uris(&self) -> impl Iterator<Item = String> + '_ {
        self.state
            .file_paths_iter()
            .map(|path| self.storage.to_uri(&path))
    }

    /// Returns statistics for files, in order
    pub fn get_stats(&self) -> impl Iterator<Item = Result<Option<Stats>, DeltaTableError>> + '_ {
        self.state.files().iter().map(|add| {
            add.get_stats()
                .map_err(|e| DeltaTableError::InvalidStatsJson { json_err: e })
        })
    }

    /// Returns partition values for files, in order
    pub fn get_partition_values(
        &self,
    ) -> impl Iterator<Item = &HashMap<String, Option<String>>> + '_ {
        self.state.files().iter().map(|add| &add.partition_values)
    }

    /// Returns the currently loaded state snapshot.
    pub fn get_state(&self) -> &DeltaTableState {
        &self.state
    }

    /// Returns the metadata associated with the loaded state.
    pub fn get_metadata(&self) -> Result<&DeltaTableMetaData, DeltaTableError> {
        self.state
            .current_metadata()
            .ok_or(DeltaTableError::NoMetadata)
    }

    /// Returns a vector of active tombstones (i.e. `Remove` actions present in the current delta log).
    pub fn get_tombstones(&self) -> impl Iterator<Item = &action::Remove> {
        self.state.unexpired_tombstones()
    }

    /// Returns the current version of the DeltaTable based on the loaded metadata.
    pub fn get_app_transaction_version(&self) -> &HashMap<String, i64> {
        self.state.app_transaction_version()
    }

    /// Returns the minimum reader version supported by the DeltaTable based on the loaded
    /// metadata.
    pub fn get_min_reader_version(&self) -> i32 {
        self.state.min_reader_version()
    }

    /// Returns the minimum writer version supported by the DeltaTable based on the loaded
    /// metadata.
    pub fn get_min_writer_version(&self) -> i32 {
        self.state.min_writer_version()
    }

    /// Return table schema parsed from transaction log. Return None if table hasn't been loaded or
    /// no metadata was found in the log.
    pub fn schema(&self) -> Option<&Schema> {
        self.state.schema()
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
            .current_metadata()
            .ok_or(DeltaTableError::NoMetadata)?
            .get_configuration())
    }

    /// Vacuum the delta table. See [`VacuumBuilder`] for more information.
    #[deprecated(
        since = "0.10.0",
        note = "use DelaOps from operations module to create a Vacuum operation."
    )]
    pub async fn vacuum(
        &mut self,
        retention_hours: Option<u64>,
        dry_run: bool,
        enforce_retention_duration: bool,
    ) -> Result<Vec<String>, DeltaTableError> {
        let mut plan = VacuumBuilder::new(self.object_store(), self.state.clone())
            .with_dry_run(dry_run)
            .with_enforce_retention_duration(enforce_retention_duration);
        if let Some(hours) = retention_hours {
            plan = plan.with_retention_period(Duration::hours(hours as i64));
        }

        let (table, metrics) = plan.await?;
        self.state = table.state;
        Ok(metrics.files_deleted)
    }

    /// Creates a new DeltaTransaction for the DeltaTable.
    /// The transaction holds a mutable reference to the DeltaTable, preventing other references
    /// until the transaction is dropped.
    #[deprecated(
        since = "0.10.0",
        note = "use 'commit' function from operations module to commit to Delta table."
    )]
    #[allow(deprecated)]
    pub fn create_transaction(
        &mut self,
        options: Option<DeltaTransactionOptions>,
    ) -> DeltaTransaction {
        DeltaTransaction::new(self, options)
    }

    /// Tries to commit a prepared commit file. Returns `DeltaTableError::VersionAlreadyExists`
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    #[deprecated(
        since = "0.10.0",
        note = "use 'commit' function from operations module to commite to Delta table."
    )]
    #[allow(deprecated)]
    pub async fn try_commit_transaction(
        &mut self,
        commit: &PreparedCommit,
        version: i64,
    ) -> Result<i64, DeltaTableError> {
        // move temporary commit file to delta log directory
        // rely on storage to fail if the file already exists -
        self.storage
            .rename_if_not_exists(&commit.uri, &commit_uri_from_version(version))
            .await
            .map_err(|e| match e {
                ObjectStoreError::AlreadyExists { .. } => {
                    DeltaTableError::VersionAlreadyExists(version)
                }
                _ => DeltaTableError::from(e),
            })?;

        self.update().await?;

        Ok(version)
    }

    /// Create a DeltaTable with version 0 given the provided MetaData, Protocol, and CommitInfo
    #[deprecated(
        since = "0.10.0",
        note = "use DelaOps from operations module to create a Create operation."
    )]
    #[allow(deprecated)]
    pub async fn create(
        &mut self,
        metadata: DeltaTableMetaData,
        protocol: action::Protocol,
        commit_info: Option<Map<String, Value>>,
        add_actions: Option<Vec<action::Add>>,
    ) -> Result<(), DeltaTableError> {
        let meta = action::MetaData::try_from(metadata)?;

        // delta-rs commit info will include the delta-rs version and timestamp as of now
        let mut enriched_commit_info = commit_info.unwrap_or_default();
        enriched_commit_info.insert(
            "delta-rs".to_string(),
            Value::String(crate_version().to_string()),
        );
        enriched_commit_info.insert(
            "timestamp".to_string(),
            Value::Number(serde_json::Number::from(Utc::now().timestamp_millis())),
        );

        let mut actions = vec![
            Action::commit_info(enriched_commit_info),
            Action::protocol(protocol),
            Action::metaData(meta),
        ];
        if let Some(add_actions) = add_actions {
            for add_action in add_actions {
                actions.push(Action::add(add_action));
            }
        };

        let mut transaction = self.create_transaction(None);
        transaction.add_actions(actions.clone());

        let prepared_commit = transaction.prepare_commit(None, None).await?;
        let committed_version = self.try_commit_transaction(&prepared_commit, 0).await?;

        let new_state = DeltaTableState::from_commit(self, committed_version).await?;
        self.state.merge(
            new_state,
            self.config.require_tombstones,
            self.config.require_files,
        );

        Ok(())
    }

    /// Time travel Delta table to the latest version that's created at or before provided
    /// `datetime` argument.
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
        writeln!(f, "DeltaTable({})", self.table_uri())?;
        writeln!(f, "\tversion: {}", self.version())?;
        match self.state.current_metadata() {
            Some(metadata) => {
                writeln!(f, "\tmetadata: {metadata}")?;
            }
            None => {
                writeln!(f, "\tmetadata: None")?;
            }
        }
        writeln!(
            f,
            "\tmin_version: read={}, write={}",
            self.state.min_reader_version(),
            self.state.min_writer_version()
        )?;
        writeln!(f, "\tfiles count: {}", self.state.files().len())
    }
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DeltaTable <{}>", self.table_uri())
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
#[deprecated(
    since = "0.10.0",
    note = "use 'commit' function from operations module to commit to Delta table."
)]
pub struct DeltaTransaction<'a> {
    delta_table: &'a mut DeltaTable,
    actions: Vec<Action>,
    options: DeltaTransactionOptions,
}

#[allow(deprecated)]
impl<'a> DeltaTransaction<'a> {
    /// Creates a new delta transaction.
    /// Holds a mutable reference to the delta table to prevent outside mutation while a transaction commit is in progress.
    /// Transaction behavior may be customized by passing an instance of `DeltaTransactionOptions`.
    pub fn new(delta_table: &'a mut DeltaTable, options: Option<DeltaTransactionOptions>) -> Self {
        DeltaTransaction {
            delta_table,
            actions: vec![],
            options: options.unwrap_or_default(),
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

    /// Commits the given actions to the delta log.
    /// This method will retry the transaction commit based on the value of `max_retry_commit_attempts` set in `DeltaTransactionOptions`.
    pub async fn commit(
        &mut self,
        operation: Option<DeltaOperation>,
        app_metadata: Option<Map<String, Value>>,
    ) -> Result<i64, DeltaTableError> {
        // TODO: stubbing `operation` parameter (which will be necessary for writing the CommitInfo action),
        // but leaving it unused for now. `CommitInfo` is a fairly dynamic data structure so we should work
        // out the data structure approach separately.

        // TODO: calculate isolation level to use when checking for conflicts.
        // Leaving conflict checking unimplemented for now to get the "single writer" implementation off the ground.
        // Leaving some commented code in place as a guidepost for the future.

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
        let prepared_commit = self.prepare_commit(operation, app_metadata).await?;

        // try to commit in a loop in case other writers write the next version first
        let version = self.try_commit_loop(&prepared_commit).await?;

        Ok(version)
    }

    /// Low-level transaction API. Creates a temporary commit file. Once created,
    /// the transaction object could be dropped and the actual commit could be executed
    /// with `DeltaTable.try_commit_transaction`.
    pub async fn prepare_commit(
        &mut self,
        operation: Option<DeltaOperation>,
        app_metadata: Option<Map<String, Value>>,
    ) -> Result<PreparedCommit, DeltaTableError> {
        if !self
            .actions
            .iter()
            .any(|a| matches!(a, action::Action::commitInfo(..)))
        {
            let mut extra_info = Map::<String, Value>::new();
            let mut commit_info = operation.map(|op| op.get_commit_info()).unwrap_or_default();
            commit_info.timestamp = Some(Utc::now().timestamp_millis());
            extra_info.insert(
                "clientVersion".to_string(),
                Value::String(format!("delta-rs.{}", crate_version())),
            );
            if let Some(mut meta) = app_metadata {
                extra_info.append(&mut meta)
            }
            commit_info.info = extra_info;
            self.add_action(action::Action::commitInfo(commit_info));
        }

        // Serialize all actions that are part of this log entry.
        let log_entry = bytes::Bytes::from(
            log_entry_from_actions(&self.actions)
                .map_err(|e| DeltaTableError::SerializeLogJson { json_err: e })?,
        );

        // Write delta log entry as temporary file to storage. For the actual commit,
        // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
        let token = Uuid::new_v4().to_string();
        let file_name = format!("_commit_{token}.json.tmp");
        let path = Path::from_iter(["_delta_log", &file_name]);

        self.delta_table.storage.put(&path, log_entry).await?;

        Ok(PreparedCommit { uri: path })
    }

    #[allow(deprecated)]
    async fn try_commit_loop(&mut self, commit: &PreparedCommit) -> Result<i64, DeltaTableError> {
        let mut attempt_number: u32 = 0;
        loop {
            self.delta_table.update().await?;

            let version = self.delta_table.version() + 1;

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
                        DeltaTableError::VersionAlreadyExists(_) => {
                            if attempt_number > self.options.max_retry_commit_attempts + 1 {
                                debug!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing.", self.options.max_retry_commit_attempts);
                                return Err(e);
                            } else {
                                attempt_number += 1;
                                debug!("Transaction attempt failed. Incrementing attempt number to {} and retrying.", attempt_number);
                            }
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
#[deprecated(
    since = "0.10.0",
    note = "use 'commit' function from operations module to commit to Delta table."
)]
pub struct PreparedCommit {
    uri: Path,
}

fn log_entry_from_actions(actions: &[Action]) -> Result<String, serde_json::Error> {
    let mut jsons = Vec::<String>::new();

    for action in actions {
        let json = serde_json::to_string(action)?;
        jsons.push(json);
    }

    Ok(jsons.join("\n"))
}

/// Creates and loads a DeltaTable from the given path with current metadata.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table(table_uri: impl AsRef<str>) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri).load().await?;
    Ok(table)
}

/// Same as `open_table`, but also accepts storage options to aid in building the table for a deduced
/// `StorageService`.
pub async fn open_table_with_storage_options(
    table_uri: impl AsRef<str>,
    storage_options: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_storage_options(storage_options)
        .load()
        .await?;
    Ok(table)
}

/// Creates a DeltaTable from the given path and loads it with the metadata from the given version.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table_with_version(
    table_uri: impl AsRef<str>,
    version: i64,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_version(version)
        .load()
        .await?;
    Ok(table)
}

/// Creates a DeltaTable from the given path.
/// Loads metadata from the version appropriate based on the given ISO-8601/RFC-3339 timestamp.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table_with_ds(
    table_uri: impl AsRef<str>,
    ds: impl AsRef<str>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_datestring(ds)?
        .load()
        .await?;
    Ok(table)
}

/// Returns rust crate version, can be use used in language bindings to expose Rust core version
pub fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::Protocol;
    use crate::builder::DeltaTableBuilder;
    use pretty_assertions::assert_eq;
    use std::io::{BufRead, BufReader};
    use std::{collections::HashMap, fs::File, path::Path};
    use tempdir::TempDir;

    #[tokio::test]
    async fn table_round_trip() {
        let (_, _, dt, tmp_dir) = create_test_table().await;
        let bytes = serde_json::to_vec(&dt).unwrap();
        let actual: DeltaTable = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual.version(), dt.version());
        drop(tmp_dir);
    }

    #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
    #[test]
    fn normalize_table_uri_s3() {
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        for table_uri in [
            "s3://tests/data/delta-0.8.0/",
            "s3://tests/data/delta-0.8.0//",
            "s3://tests/data/delta-0.8.0",
        ]
        .iter()
        {
            let table = DeltaTableBuilder::from_uri(table_uri).build().unwrap();
            assert_eq!(table.table_uri(), "s3://tests/data/delta-0.8.0");
        }
    }

    async fn create_test_table() -> (DeltaTableMetaData, Protocol, DeltaTable, TempDir) {
        // Setup
        let test_schema = Schema::new(vec![
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
        ]);

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
            min_writer_version: 1,
        };

        let tmp_dir = TempDir::new("create_table_test").unwrap();
        let table_dir = tmp_dir.path().join("test_create");
        std::fs::create_dir(&table_dir).unwrap();

        let mut dt = DeltaTableBuilder::from_uri(table_dir.to_str().unwrap())
            .build()
            .unwrap();

        let mut commit_info = Map::<String, Value>::new();
        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String("CREATE TABLE".to_string()),
        );
        commit_info.insert(
            "userName".to_string(),
            serde_json::Value::String("test user".to_string()),
        );
        // Action
        #[allow(deprecated)]
        dt.create(delta_md.clone(), protocol.clone(), Some(commit_info), None)
            .await
            .unwrap();
        (delta_md, protocol, dt, tmp_dir)
    }

    #[tokio::test]
    async fn test_create_delta_table() {
        let (delta_md, protocol, dt, tmp_dir) = create_test_table().await;

        // Validation
        // assert DeltaTable version is now 0 and no data files have been added
        assert_eq!(dt.version(), 0);
        assert_eq!(dt.state.files().len(), 0);

        // assert new _delta_log file created in tempDir
        let table_uri = dt.table_uri();
        let table_path = Path::new(&table_uri);
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
                Action::commitInfo(action) => {
                    assert_eq!(action.operation, Some("CREATE TABLE".to_string()));
                }
                _ => (),
            }
        }

        // assert DeltaTableState metadata matches fields in above DeltaTableMetaData
        // assert metadata name
        let current_metadata = dt.get_metadata().unwrap();
        assert!(current_metadata.partition_columns.is_empty());
        assert!(current_metadata.configuration.is_empty());
        drop(tmp_dir);
    }
}
