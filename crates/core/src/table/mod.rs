//! Delta Table read and write implementation

use std::cmp::{min, Ordering};
use std::fmt;
use std::fmt::Formatter;

use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore};
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use self::builder::DeltaTableConfig;
use self::state::DeltaTableState;
use crate::kernel::{CommitInfo, DataCheck, LogicalFileView};
use crate::logstore::{
    commit_uri_from_version, extract_version_from_filename, LogStoreConfig, LogStoreExt,
    LogStoreRef, ObjectStoreRef,
};
use crate::partitions::PartitionFilter;
use crate::{DeltaResult, DeltaTableError};

// NOTE: this use can go away when peek_next_commit is removed off of [DeltaTable]
pub use crate::logstore::PeekCommit;

pub mod builder;
pub mod config;
pub mod state;

mod columns;

// Re-exposing for backwards compatibility
pub use columns::*;

/// In memory representation of a Delta Table
///
/// A DeltaTable is a purely logical concept that represents a dataset that can ewvolve over time.
/// To attain concrete information about a table a snapshot need to be loaded.
/// Most commonly this is the latest state of the tablem but may also loaded for a specific
/// version or point in time.
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
                let log_store =
                    crate::logstore::logstore_for(storage_config.location, storage_config.options)
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
        let config = state.load_config().clone();
        Self {
            state: Some(state),
            log_store,
            config,
        }
    }

    /// get a shared reference to the delta object store
    pub fn object_store(&self) -> ObjectStoreRef {
        self.log_store.object_store(None)
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
        self.log_store
            .get_latest_version(self.version().unwrap_or(0))
            .await
    }

    /// Currently loaded version of the table - if any.
    ///
    /// This will return the latest version of the table if it has been loaded.
    /// Returns `None` if the table has not been loaded.
    pub fn version(&self) -> Option<i64> {
        self.state.as_ref().map(|s| s.version())
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

    /// Updates the DeltaTable to the latest version by incrementally applying newer versions.
    /// It assumes that the table is already updated to the current version `self.version`.
    pub async fn update_incremental(
        &mut self,
        max_version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        match self.state.as_mut() {
            Some(state) => state.update(&self.log_store, max_version).await,
            _ => {
                let state =
                    DeltaTableState::try_new(&self.log_store, self.config.clone(), max_version)
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
    pub async fn history(
        &self,
        limit: Option<usize>,
    ) -> Result<impl Iterator<Item = CommitInfo>, DeltaTableError> {
        let infos = self
            .snapshot()?
            .snapshot()
            .snapshot()
            .commit_infos(&self.log_store(), limit)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(infos.into_iter().flatten())
    }

    #[cfg(test)]
    /// We have enough internal tests that just need to check the last commit of the table.
    ///
    /// This is a silly convenience function to reduce some copy-paste in tests
    pub(crate) async fn last_commit(&self) -> Result<CommitInfo, DeltaTableError> {
        let mut infos: Vec<_> = self.history(Some(1)).await?.collect();
        infos.pop().ok_or(DeltaTableError::Generic(
            "Somehow there is nothing in the history!".into(),
        ))
    }

    /// Stream all logical files matching the provided `PartitionFilter`s.
    pub fn get_active_add_actions_by_partitions(
        &self,
        filters: &[PartitionFilter],
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        let Some(state) = self.state.as_ref() else {
            return Box::pin(futures::stream::once(async {
                Err(DeltaTableError::NotInitialized)
            }));
        };
        state
            .snapshot()
            .file_views_by_partitions(&self.log_store, filters)
    }

    /// Returns the file list tracked in current table state filtered by provided
    /// `PartitionFilter`s.
    pub async fn get_files_by_partitions(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Vec<Path>, DeltaTableError> {
        Ok(self
            .get_active_add_actions_by_partitions(filters)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|add| add.object_store_path())
            .collect())
    }

    /// Return the file uris as strings for the partition(s)
    pub async fn get_file_uris_by_partitions(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Vec<String>, DeltaTableError> {
        let files = self.get_files_by_partitions(filters).await?;
        Ok(files
            .iter()
            .map(|fname| self.log_store.to_uri(fname))
            .collect())
    }

    /// Returns a URIs for all active files present in the current table version.
    pub fn get_file_uris(&self) -> DeltaResult<impl Iterator<Item = String> + '_> {
        Ok(self
            .state
            .as_ref()
            .ok_or(DeltaTableError::NotInitialized)?
            .log_data()
            .into_iter()
            .map(|add| add.object_store_path())
            .map(|path| self.log_store.to_uri(&path)))
    }

    /// Returns the currently loaded state snapshot.
    ///
    /// This method provides access to the currently loaded state of the Delta table.
    ///
    /// ## Returns
    ///
    /// A reference to the current state of the Delta table.
    ///
    /// ## Errors
    ///
    /// Returns [`NotInitialized`](DeltaTableError::NotInitialized) if the table has not been initialized.
    pub fn snapshot(&self) -> DeltaResult<&DeltaTableState> {
        self.state.as_ref().ok_or(DeltaTableError::NotInitialized)
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
        let prefix = log_store.log_path();
        let offset_path = commit_uri_from_version(min_version);
        let object_store = log_store.object_store(None);
        let mut files = object_store.list_with_offset(Some(prefix), &offset_path);

        while let Some(obj_meta) = files.next().await {
            let obj_meta = obj_meta?;
            let location_path: Path = obj_meta.location.clone();
            let part_count = location_path.prefix_match(prefix).unwrap().count();
            if part_count > 1 {
                // Per the spec, ignore any files in subdirectories.
                // Spark may create these as uncommitted transactions which we don't want
                //
                // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries
                // "Delta files are stored as JSON in a directory at the *root* of the table
                // named _delta_log, and ... make up the log of all changes that have occurred to a table."
                continue;
            }
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
        let mut max_version = match self
            .log_store
            .get_latest_version(self.version().unwrap_or(min_version))
            .await
        {
            Ok(version) => version,
            Err(DeltaTableError::InvalidVersion(_)) => {
                return Err(DeltaTableError::NotATable(
                    log_store.table_root_url().to_string(),
                ))
            }
            Err(e) => return Err(e),
        };
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
        writeln!(f, "\tversion: {:?}", self.version())
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
