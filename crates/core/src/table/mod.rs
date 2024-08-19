//! Delta Table read and write implementation

use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;

use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore};
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use self::builder::DeltaTableConfig;
use self::state::DeltaTableState;
use crate::kernel::{
    Action, CommitInfo, DataCheck, DataType, LogicalFile, Metadata, Protocol, StructType,
    Transaction,
};
use crate::logstore::{self, extract_version_from_filename, LogStoreConfig, LogStoreRef};
use crate::partitions::PartitionFilter;
use crate::storage::{commit_uri_from_version, ObjectStoreRef};
use crate::{DeltaResult, DeltaTableError};

pub mod builder;
pub mod config;
pub mod state;
pub mod state_arrow;

/// Metadata for a checkpoint file
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct CheckPoint {
    /// Delta table version
    pub(crate) version: i64, // 20 digits decimals
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The number of fragments if the last checkpoint was written in multiple parts. This field is optional.
    pub(crate) parts: Option<u32>, // 10 digits decimals
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The number of bytes of the checkpoint. This field is optional.
    pub(crate) size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The number of AddFile actions in the checkpoint. This field is optional.
    pub(crate) num_of_add_files: Option<i64>,
}

#[derive(Default)]
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
            parts: parts.or(None),
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

/// A constraint in a check constraint
#[derive(Eq, PartialEq, Debug, Default, Clone)]
pub struct Constraint {
    /// The full path to the field.
    pub name: String,
    /// The SQL string that must always evaluate to true.
    pub expr: String,
}

impl Constraint {
    /// Create a new invariant
    pub fn new(field_name: &str, invariant_sql: &str) -> Self {
        Self {
            name: field_name.to_string(),
            expr: invariant_sql.to_string(),
        }
    }
}

impl DataCheck for Constraint {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_expression(&self) -> &str {
        &self.expr
    }
}

/// Return partition fields along with their data type from the current schema.
pub(crate) fn get_partition_col_data_types<'a>(
    schema: &'a StructType,
    metadata: &'a Metadata,
) -> Vec<(&'a String, &'a DataType)> {
    // JSON add actions contain a `partitionValues` field which is a map<string, string>.
    // When loading `partitionValues_parsed` we have to convert the stringified partition values back to the correct data type.
    schema
        .fields()
        .filter_map(|f| {
            if metadata
                .partition_columns
                .iter()
                .any(|s| s.as_str() == f.name())
            {
                Some((f.name(), f.data_type()))
            } else {
                None
            }
        })
        .collect()
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
#[derive(Clone)]
pub struct DeltaTable {
    /// The state of the table as of the most recent loaded Delta log entry.
    pub state: Option<DeltaTableState>,
    /// the load options used during load
    pub config: DeltaTableConfig,
    /// log store
    pub(crate) log_store: LogStoreRef,
}

impl Serialize for DeltaTable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.state)?;
        seq.serialize_element(&self.config)?;
        seq.serialize_element(self.log_store.config())?;
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
                let storage_config: LogStoreConfig = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let log_store = crate::logstore::logstore_for(
                    storage_config.location,
                    storage_config.options,
                    None,
                )
                .map_err(|_| A::Error::custom("Failed deserializing LogStore"))?;

                let table = DeltaTable {
                    state,
                    config,
                    log_store,
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
    pub fn new(log_store: LogStoreRef, config: DeltaTableConfig) -> Self {
        Self {
            state: None,
            log_store,
            config,
        }
    }

    /// Create a new [`DeltaTable`] from a [`DeltaTableState`] without loading any
    /// data from backing storage.
    ///
    /// NOTE: This is for advanced users. If you don't know why you need to use this method,
    /// please call one of the `open_table` helper methods instead.
    pub(crate) fn new_with_state(log_store: LogStoreRef, state: DeltaTableState) -> Self {
        Self {
            state: Some(state),
            log_store,
            config: Default::default(),
        }
    }

    /// get a shared reference to the delta object store
    pub fn object_store(&self) -> ObjectStoreRef {
        self.log_store.object_store()
    }

    /// Check if the [`DeltaTable`] exists
    pub async fn verify_deltatable_existence(&self) -> DeltaResult<bool> {
        self.log_store.is_delta_table_location().await
    }

    /// The URI of the underlying data
    pub fn table_uri(&self) -> String {
        self.log_store.root_uri()
    }

    /// get a shared reference to the log store
    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }

    /// returns the latest available version of the table
    pub async fn get_latest_version(&self) -> Result<i64, DeltaTableError> {
        self.log_store.get_latest_version(self.version()).await
    }

    /// Currently loaded version of the table
    pub fn version(&self) -> i64 {
        self.state.as_ref().map(|s| s.version()).unwrap_or(-1)
    }

    /// Load DeltaTable with data from latest checkpoint
    pub async fn load(&mut self) -> Result<(), DeltaTableError> {
        self.update_incremental(None).await
    }

    /// Updates the DeltaTable to the most recent state committed to the transaction log by
    /// loading the last checkpoint and incrementally applying each version since.
    pub async fn update(&mut self) -> Result<(), DeltaTableError> {
        self.update_incremental(None).await
    }

    /// Get the list of actions for the next commit
    pub async fn peek_next_commit(
        &self,
        current_version: i64,
    ) -> Result<PeekCommit, DeltaTableError> {
        let next_version = current_version + 1;
        let commit_log_bytes = match self.log_store.read_commit_entry(next_version).await {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => return Ok(PeekCommit::UpToDate),
            Err(err) => Err(err),
        }?;

        let actions = logstore::get_actions(next_version, commit_log_bytes).await;
        Ok(PeekCommit::New(next_version, actions.unwrap()))
    }

    /// Updates the DeltaTable to the latest version by incrementally applying newer versions.
    /// It assumes that the table is already updated to the current version `self.version`.
    pub async fn update_incremental(
        &mut self,
        max_version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        match self.state.as_mut() {
            Some(state) => state.update(self.log_store.clone(), max_version).await,
            _ => {
                let state = DeltaTableState::try_new(
                    &Path::default(),
                    self.log_store.object_store(),
                    self.config.clone(),
                    max_version,
                )
                .await?;
                self.state = Some(state);
                Ok(())
            }
        }
    }

    /// Loads the DeltaTable state for the given version.
    pub async fn load_version(&mut self, version: i64) -> Result<(), DeltaTableError> {
        if let Some(snapshot) = &self.state {
            if snapshot.version() > version {
                self.state = None;
            }
        }
        self.update_incremental(Some(version)).await
    }

    pub(crate) async fn get_version_timestamp(&self, version: i64) -> Result<i64, DeltaTableError> {
        match self
            .state
            .as_ref()
            .and_then(|s| s.version_timestamp(version))
        {
            Some(ts) => Ok(ts),
            None => {
                let meta = self
                    .object_store()
                    .head(&commit_uri_from_version(version))
                    .await?;
                let ts = meta.last_modified.timestamp_millis();
                Ok(ts)
            }
        }
    }

    /// Returns provenance information, including the operation, user, and so on, for each write to a table.
    /// The table history retention is based on the `logRetentionDuration` property of the Delta Table, 30 days by default.
    /// If `limit` is given, this returns the information of the latest `limit` commits made to this table. Otherwise,
    /// it returns all commits from the earliest commit.
    pub async fn history(&self, limit: Option<usize>) -> Result<Vec<CommitInfo>, DeltaTableError> {
        let infos = self
            .snapshot()?
            .snapshot
            .snapshot()
            .commit_infos(self.object_store(), limit)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(infos.into_iter().flatten().collect())
    }

    /// Obtain Add actions for files that match the filter
    pub fn get_active_add_actions_by_partitions<'a>(
        &'a self,
        filters: &'a [PartitionFilter],
    ) -> Result<impl Iterator<Item = DeltaResult<LogicalFile<'_>>> + '_, DeltaTableError> {
        self.state
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)?
            .get_active_add_actions_by_partitions(filters)
    }

    /// Returns the file list tracked in current table state filtered by provided
    /// `PartitionFilter`s.
    pub fn get_files_by_partitions(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Vec<Path>, DeltaTableError> {
        Ok(self
            .get_active_add_actions_by_partitions(filters)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|add| add.object_store_path())
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
            .map(|fname| self.log_store.to_uri(fname))
            .collect())
    }

    /// Returns an iterator of file names present in the loaded state
    #[inline]
    pub fn get_files_iter(&self) -> DeltaResult<impl Iterator<Item = Path> + '_> {
        Ok(self
            .state
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)?
            .file_paths_iter())
    }

    /// Returns a URIs for all active files present in the current table version.
    pub fn get_file_uris(&self) -> DeltaResult<impl Iterator<Item = String> + '_> {
        Ok(self
            .state
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)?
            .file_paths_iter()
            .map(|path| self.log_store.to_uri(&path)))
    }

    /// Get the number of files in the table - returns 0 if no metadata is loaded
    pub fn get_files_count(&self) -> usize {
        self.state.as_ref().map(|s| s.files_count()).unwrap_or(0)
    }

    /// Returns the currently loaded state snapshot.
    pub fn snapshot(&self) -> DeltaResult<&DeltaTableState> {
        self.state.as_ref().ok_or(DeltaTableError::NotInitialized)
    }

    /// Returns current table protocol
    pub fn protocol(&self) -> DeltaResult<&Protocol> {
        Ok(self
            .state
            .as_ref()
            .ok_or(DeltaTableError::NoMetadata)?
            .protocol())
    }

    /// Returns the metadata associated with the loaded state.
    pub fn metadata(&self) -> Result<&Metadata, DeltaTableError> {
        Ok(self.snapshot()?.metadata())
    }

    /// Returns the current version of the DeltaTable based on the loaded metadata.
    pub fn get_app_transaction_version(&self) -> HashMap<String, Transaction> {
        self.state
            .as_ref()
            .and_then(|s| s.app_transaction_version().ok())
            .map(|it| it.map(|t| (t.app_id.clone(), t)).collect())
            .unwrap_or_default()
    }

    /// Return table schema parsed from transaction log. Return None if table hasn't been loaded or
    /// no metadata was found in the log.
    pub fn schema(&self) -> Option<&StructType> {
        Some(self.snapshot().ok()?.schema())
    }

    /// Return table schema parsed from transaction log. Return `DeltaTableError` if table hasn't
    /// been loaded or no metadata was found in the log.
    pub fn get_schema(&self) -> Result<&StructType, DeltaTableError> {
        Ok(self.snapshot()?.schema())
    }

    /// Time travel Delta table to the latest version that's created at or before provided
    /// `datetime` argument.
    ///
    /// Internally, this methods performs a binary search on all Delta transaction logs.
    pub async fn load_with_datetime(
        &mut self,
        datetime: DateTime<Utc>,
    ) -> Result<(), DeltaTableError> {
        let mut min_version: i64 = -1;
        let log_store = self.log_store();
        let prefix = Some(log_store.log_path());
        let offset_path = commit_uri_from_version(min_version);
        let object_store = log_store.object_store();
        let mut files = object_store.list_with_offset(prefix, &offset_path);

        while let Some(obj_meta) = files.next().await {
            let obj_meta = obj_meta?;
            if let Some(log_version) = extract_version_from_filename(obj_meta.location.as_ref()) {
                if min_version == -1 {
                    min_version = log_version
                } else {
                    min_version = min(min_version, log_version);
                }
            }
            if min_version == 0 {
                break;
            }
        }
        let mut max_version = self.get_latest_version().await?;
        let mut version = min_version;
        let lowest_table_version = min_version;
        let target_ts = datetime.timestamp_millis();

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

        if version < lowest_table_version {
            version = lowest_table_version;
        }

        self.load_version(version).await
    }
}

impl fmt::Display for DeltaTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DeltaTable({})", self.table_uri())?;
        writeln!(f, "\tversion: {}", self.version())
    }
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DeltaTable <{}>", self.table_uri())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::*;
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::operations::create::CreateBuilder;

    #[tokio::test]
    async fn table_round_trip() {
        let (dt, tmp_dir) = create_test_table().await;
        let bytes = serde_json::to_vec(&dt).unwrap();
        let actual: DeltaTable = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual.version(), dt.version());
        drop(tmp_dir);
    }

    async fn create_test_table() -> (DeltaTable, TempDir) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let table_dir = tmp_dir.path().join("test_create");
        std::fs::create_dir(&table_dir).unwrap();

        let dt = CreateBuilder::new()
            .with_location(table_dir.to_str().unwrap())
            .with_table_name("Test Table Create")
            .with_comment("This table is made to test the create function for a DeltaTable")
            .with_columns(vec![
                StructField::new(
                    "Id".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    true,
                ),
                StructField::new(
                    "Name".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .await
            .unwrap();
        (dt, tmp_dir)
    }
}
