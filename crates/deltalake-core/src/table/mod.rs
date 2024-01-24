//! Delta Table read and write implementation

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Formatter;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;
use std::{cmp::max, cmp::Ordering, collections::HashSet};

use chrono::{DateTime, Utc};
use futures::StreamExt;
use lazy_static::lazy_static;
use log::debug;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use regex::Regex;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use self::builder::DeltaTableConfig;
use self::state::DeltaTableState;
use crate::errors::DeltaTableError;
use crate::partitions::PartitionFilter;
use crate::protocol::{self, find_latest_check_point_for_version, get_last_checkpoint, Action};
use crate::protocol::{Add, ProtocolError, Stats};
use crate::schema::*;
use crate::storage::{commit_uri_from_version, ObjectStoreRef};

pub mod builder;
pub mod config;
pub mod state;
#[cfg(feature = "arrow")]
pub mod state_arrow;

/// Metadata for a checkpoint file
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    /// Delta table version
    pub(crate) version: i64, // 20 digits decimals
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    /// The number of fragments if the last checkpoint was written in multiple parts. This field is optional.
    pub(crate) parts: Option<u32>, // 10 digits decimals
    /// The number of bytes of the checkpoint. This field is optional.
    pub(crate) size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint. This field is optional.
    pub(crate) num_of_add_files: Option<i64>,
}

/// Builder for CheckPoint
pub struct CheckPointBuilder {
    /// Delta table version
    pub(crate) version: i64, // 20 digits decimals
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    /// The number of fragments if the last checkpoint was written in multiple parts. This field is optional.
    pub(crate) parts: Option<u32>, // 10 digits decimals
    /// The number of bytes of the checkpoint. This field is optional.
    pub(crate) size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint. This field is optional.
    pub(crate) num_of_add_files: Option<i64>,
}

impl CheckPointBuilder {
    /// Creates a new [`CheckPointBuilder`] instance with the provided `version` and `size`.
    /// Size is the total number of actions in the checkpoint. See size_in_bytes for total size in bytes.
    pub fn new(version: i64, size: i64) -> Self {
        CheckPointBuilder {
            version,
            size,
            parts: None,
            size_in_bytes: None,
            num_of_add_files: None,
        }
    }

    /// The number of fragments if the last checkpoint was written in multiple parts. This field is optional.
    pub fn with_parts(mut self, parts: u32) -> Self {
        self.parts = Some(parts);
        self
    }

    /// The number of bytes of the checkpoint. This field is optional.
    pub fn with_size_in_bytes(mut self, size_in_bytes: i64) -> Self {
        self.size_in_bytes = Some(size_in_bytes);
        self
    }

    /// The number of AddFile actions in the checkpoint. This field is optional.
    pub fn with_num_of_add_files(mut self, num_of_add_files: i64) -> Self {
        self.num_of_add_files = Some(num_of_add_files);
        self
    }

    /// Build the final [`CheckPoint`] struct.
    pub fn build(self) -> CheckPoint {
        CheckPoint {
            version: self.version,
            size: self.size,
            parts: self.parts,
            size_in_bytes: self.size_in_bytes,
            num_of_add_files: self.num_of_add_files,
        }
    }
}

impl CheckPoint {
    /// Creates a new checkpoint from the given parameters.
    pub fn new(version: i64, size: i64, parts: Option<u32>) -> Self {
        Self {
            version,
            size,
            parts,
            size_in_bytes: None,
            num_of_add_files: None,
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
    pub format: protocol::Format,
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
        format: Option<protocol::Format>,
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

impl TryFrom<protocol::MetaData> for DeltaTableMetaData {
    type Error = ProtocolError;

    fn try_from(action_metadata: protocol::MetaData) -> Result<Self, Self::Error> {
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
                Regex::new(r"^_delta_log/(\d{20})\.(json|checkpoint)*$").unwrap();
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
    /// returns the latest available version of the table
    pub async fn get_latest_version(&mut self) -> Result<i64, DeltaTableError> {
        let version_start = match get_last_checkpoint(&self.storage).await {
            Ok(last_check_point) => last_check_point.version,
            Err(ProtocolError::CheckpointNotFound) => {
                // no checkpoint
                -1
            }
            Err(e) => {
                return Err(DeltaTableError::from(e));
            }
        };

        debug!("latest checkpoint version: {version_start}");

        let version_start = max(self.version(), version_start);

        lazy_static! {
            static ref DELTA_LOG_REGEX: Regex =
                Regex::new(r"_delta_log/(\d{20})\.(json|checkpoint).*$").unwrap();
        }

        // list files to find max version
        let version = async {
            let mut max_version: i64 = version_start;
            let prefix = Some(self.storage.log_path());
            let offset_path = commit_uri_from_version(max_version);
            let mut files = self.storage.list_with_offset(prefix, &offset_path).await?;

            while let Some(obj_meta) = files.next().await {
                let obj_meta = obj_meta?;
                if let Some(captures) = DELTA_LOG_REGEX.captures(obj_meta.location.as_ref()) {
                    let log_version = captures.get(1).unwrap().as_str().parse().unwrap();
                    // listing may not be ordered
                    max_version = max(max_version, log_version);
                    // also cache timestamp for version, for faster time-travel
                    self.version_timestamp
                        .insert(log_version, obj_meta.last_modified.timestamp());
                }
            }

            if max_version < 0 {
                return Err(DeltaTableError::not_a_table(self.table_uri()));
            }

            Ok::<i64, DeltaTableError>(max_version)
        }
        .await?;

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

        let actions = Self::get_actions(next_version, commit_log_bytes).await;
        Ok(PeekCommit::New(next_version, actions.unwrap()))
    }

    /// Reads a commit and gets list of actions
    async fn get_actions(
        version: i64,
        commit_log_bytes: bytes::Bytes,
    ) -> Result<Vec<Action>, DeltaTableError> {
        debug!("parsing commit with version {version}...");
        let reader = BufReader::new(Cursor::new(commit_log_bytes));

        let mut actions = Vec::new();
        for re_line in reader.lines() {
            let line = re_line?;
            let lstr = line.as_str();
            let action =
                serde_json::from_str(lstr).map_err(|e| DeltaTableError::InvalidJsonLog {
                    json_err: e,
                    line,
                    version,
                })?;
            actions.push(action);
        }
        Ok(actions)
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

        // update to latest version if given max_version is not larger than current version
        let max_version = max_version.filter(|x| x > &self.version());
        let max_version: i64 = match max_version {
            Some(x) => x,
            None => self.get_latest_version().await?,
        };

        let buf_size = self.config.log_buffer_size;

        let store = self.storage.clone();
        let mut log_stream = futures::stream::iter(self.version() + 1..max_version + 1)
            .map(|version| {
                let store = store.clone();
                let loc = commit_uri_from_version(version);
                async move {
                    let data = store.get(&loc).await?.bytes().await?;
                    let actions = Self::get_actions(version, data).await?;
                    Ok((version, actions))
                }
            })
            .buffered(buf_size);

        while let Some(res) = log_stream.next().await {
            let (new_version, actions) = match res {
                Ok((version, actions)) => (version, actions),
                Err(DeltaTableError::ObjectStore {
                    source: ObjectStoreError::NotFound { .. },
                }) => break, // no more files in the log
                Err(err) => return Err(err),
            };

            debug!("merging table state with version: {new_version}");
            let s = DeltaTableState::from_actions(actions, new_version)?;
            self.state
                .merge(s, self.config.require_tombstones, self.config.require_files);
            if self.version() == max_version {
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
    ) -> Result<Vec<protocol::CommitInfo>, DeltaTableError> {
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
        filters: &'a [PartitionFilter],
    ) -> Result<impl Iterator<Item = &Add> + '_, DeltaTableError> {
        self.state.get_active_add_actions_by_partitions(filters)
    }

    /// Returns the file list tracked in current table state filtered by provided
    /// `PartitionFilter`s.
    pub fn get_files_by_partitions(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Vec<Path>, DeltaTableError> {
        Ok(self
            .get_active_add_actions_by_partitions(filters)?
            .map(|add| {
                // Try to preserve percent encoding if possible
                match Path::parse(&add.path) {
                    Ok(path) => path,
                    Err(_) => Path::from(add.path.as_ref()),
                }
            })
            .collect())
    }

    /// Return the file uris as strings for the partition(s)
    pub fn get_file_uris_by_partitions(
        &self,
        filters: &[PartitionFilter],
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
    pub fn get_tombstones(&self) -> impl Iterator<Item = &protocol::Remove> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::create::CreateBuilder;
    #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
    use crate::table::builder::DeltaTableBuilder;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use tempdir::TempDir;

    #[tokio::test]
    async fn table_round_trip() {
        let (dt, tmp_dir) = create_test_table().await;
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

    async fn create_test_table() -> (DeltaTable, TempDir) {
        let tmp_dir = TempDir::new("create_table_test").unwrap();
        let table_dir = tmp_dir.path().join("test_create");
        std::fs::create_dir(&table_dir).unwrap();

        let dt = CreateBuilder::new()
            .with_location(table_dir.to_str().unwrap())
            .with_table_name("Test Table Create")
            .with_comment("This table is made to test the create function for a DeltaTable")
            .with_columns(vec![
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
            ])
            .await
            .unwrap();
        (dt, tmp_dir)
    }
}
