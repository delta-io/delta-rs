//! Delta Table read and write implementation

use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use delta_kernel::expressions::PredicateRef;
use futures::TryStreamExt;
use futures::future::ready;
use futures::stream::{BoxStream, once};
use object_store::path::Path;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

use self::state::DeltaTableState;
use crate::kernel::{CommitInfo, DataCheck, LogicalFileView, Snapshot, Version};
use crate::logstore::{LogStoreConfig, LogStoreRef, ObjectStoreRef};
use crate::partitions::FilterLiteral;
use crate::{DeltaResult, DeltaTableBuilder, DeltaTableError};

mod blind;
pub mod builder;
pub mod config;
pub mod state;

mod columns;

pub use blind::BlindDeltaTable;

// Re-exposing for backwards compatibility
pub use columns::*;

/// In memory representation of a Delta Table
///
/// A DeltaTable is a purely logical concept that represents a dataset that can evolve over time.
/// To attain concrete information about a table a snapshot need to be loaded.
/// Most commonly this is the latest state of the table, but may also loaded for a specific
/// version or point in time.
#[derive(Clone)]
pub struct DeltaTable {
    /// The state of the table as of the most recent loaded Delta log entry.
    pub state: Option<DeltaTableState>,
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
                #[derive(Deserialize)]
                #[serde(untagged)]
                enum StorageOrLegacyConfig {
                    Storage(Box<LogStoreConfig>),
                    Legacy {
                        #[serde(rename = "requireFiles", alias = "require_files")]
                        _require_files: bool,
                    },
                }
                let storage_config = match seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(1, &self))?
                {
                    StorageOrLegacyConfig::Storage(config) => *config,
                    StorageOrLegacyConfig::Legacy { .. } => seq
                        .next_element()?
                        .ok_or_else(|| A::Error::invalid_length(2, &self))?,
                };
                let log_store = crate::logstore::logstore_for(
                    storage_config.location(),
                    storage_config.options().clone(),
                )
                .map_err(|_| A::Error::custom("Failed deserializing LogStore"))?;

                let table = DeltaTable { state, log_store };
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
    pub fn new(log_store: LogStoreRef) -> Self {
        Self {
            state: None,
            log_store,
        }
    }

    /// Create a new [`DeltaTable`] instance, backed by an un-initialized in memory table
    ///
    /// Using this will not persist any changes beyond the lifetime of the table object.
    /// The main purpose of in-memory tables is for use in testing.
    ///
    /// ```
    /// use deltalake_core::DeltaTable;
    /// let table = DeltaTable::new_in_memory();
    /// ```
    pub fn new_in_memory() -> Self {
        let url = Url::parse("memory:///").unwrap();
        DeltaTableBuilder::from_url(url).unwrap().build().unwrap()
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
    pub fn table_url(&self) -> &Url {
        self.log_store.root_url()
    }

    /// get a shared reference to the log store
    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }

    /// returns the latest available version of the table
    pub async fn get_latest_version(&self) -> Result<Version, DeltaTableError> {
        self.log_store
            .get_latest_version(self.version().unwrap_or(0))
            .await
    }

    /// Currently loaded version of the table - if any.
    ///
    /// This will return the latest version of the table if it has been loaded.
    /// Returns `None` if the table has not been loaded.
    pub fn version(&self) -> Option<Version> {
        self.state.as_ref().map(|s| s.version())
    }

    /// Load DeltaTable with data from latest checkpoint
    pub async fn load(&mut self) -> Result<(), DeltaTableError> {
        self.update_incremental(None).await
    }

    /// Updates the DeltaTable to the most recent state committed to the transaction log by
    /// loading the last checkpoint and incrementally applying each version since.
    pub async fn update_state(&mut self) -> Result<(), DeltaTableError> {
        self.update_incremental(None).await
    }

    /// Updates the DeltaTable by incrementally applying newer versions, optionally bounded by
    /// `max_version`.
    ///
    /// This API is forward-only. Use [`DeltaTable::load_version`] to load an older version.
    pub async fn update_incremental(
        &mut self,
        max_version: Option<Version>,
    ) -> Result<(), DeltaTableError> {
        let Some(state) = self.state.as_mut() else {
            self.state = Some(DeltaTableState::try_new(&self.log_store, max_version).await?);
            return Ok(());
        };

        let current_version = state.version();
        if let Some(requested_version) = max_version
            && requested_version < current_version
        {
            return Err(DeltaTableError::VersionDowngrade {
                current_version,
                requested_version,
            });
        }

        state.update(&self.log_store, max_version).await?;
        Ok(())
    }

    /// Loads the DeltaTable state for the given version.
    pub async fn load_version(&mut self, version: Version) -> Result<(), DeltaTableError> {
        if let Some(snapshot) = &self.state
            && snapshot.version() > version
        {
            self.state = None;
        }
        self.update_incremental(Some(version)).await
    }

    /// Streams provenance information for each write to the table, newest commit first.
    ///
    /// The table history retention is based on the `logRetentionDuration` property of the Delta Table, 30 days by default.
    /// If `limit` is given, this returns the information of the latest `limit` commits made to this table. Otherwise,
    /// it returns all commits from the earliest commit.
    pub fn history(&self, limit: Option<usize>) -> BoxStream<'_, DeltaResult<CommitInfo>> {
        let Some(state) = self.state.as_ref() else {
            return Box::pin(once(ready(Err(DeltaTableError::NotInitialized))));
        };
        let log_store = self.log_store();
        Box::pin(
            once(async move {
                state
                    .snapshot()
                    .snapshot()
                    .commit_infos(&log_store, limit)
                    .await
            })
            .try_flatten()
            .try_filter_map(|info| ready(Ok(info))),
        )
    }

    #[cfg(test)]
    /// We have enough internal tests that just need to check the last commit of the table.
    ///
    /// This is a silly convenience function to reduce some copy-paste in tests
    pub(crate) async fn last_commit(&self) -> Result<CommitInfo, DeltaTableError> {
        let mut infos: Vec<_> = self.history(Some(1)).try_collect().await?;
        infos.pop().ok_or(DeltaTableError::Generic(
            "Somehow there is nothing in the history!".into(),
        ))
    }

    /// Stream all logical files matching the provided kernel [`Predicate`](delta_kernel::expressions::Predicate).
    ///
    /// Predicates over partition columns select files exactly. Predicates over
    /// data columns are evaluated against file statistics and select a superset:
    /// every file that may contain a matching row, including files without
    /// statistics for the referenced columns.
    pub fn get_active_add_actions_by_predicate(
        &self,
        predicate: Option<PredicateRef>,
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        let Some(state) = self.state.as_ref() else {
            return Box::pin(futures::stream::once(async {
                Err(DeltaTableError::NotInitialized)
            }));
        };
        state.snapshot().file_views(&self.log_store, predicate)
    }

    /// Stream all logical files matching the provided conjunction (AND) of
    /// `(column, op, value)` partition filter literals.
    pub fn get_active_add_actions_by_partitions(
        &self,
        filters: &[FilterLiteral<'_>],
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        let Some(state) = self.state.as_ref() else {
            return Box::pin(futures::stream::once(async {
                Err(DeltaTableError::NotInitialized)
            }));
        };

        if filters.is_empty() {
            return state.snapshot().file_views(&self.log_store, None);
        }

        let predicate = match crate::conjunction_to_kernel_predicate(
            filters,
            state.snapshot().schema().as_ref(),
        ) {
            Ok(predicate) => Arc::new(predicate),
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };
        state
            .snapshot()
            .file_views(&self.log_store, Some(predicate))
    }

    /// Returns the file list tracked in current table state filtered by provided
    /// partition filter literals.
    pub async fn get_files_by_partitions(
        &self,
        filters: &[FilterLiteral<'_>],
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
        filters: &[FilterLiteral<'_>],
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
    /// Commits made while in-commit timestamps are enabled are compared by their
    /// `inCommitTimestamp`, earlier commits by the modification time of their log file.
    pub async fn load_with_datetime(
        &mut self,
        datetime: DateTime<Utc>,
    ) -> Result<(), DeltaTableError> {
        let latest = Snapshot::try_new(self.log_store.as_ref(), None).await?;
        let version = latest
            .version_at_timestamp(self.log_store.as_ref(), datetime.timestamp_millis())
            .await?;
        self.load_version(version).await
    }
}

impl fmt::Display for DeltaTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DeltaTable({})", self.table_url())?;
        writeln!(f, "\tversion: {:?}", self.version())
    }
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DeltaTable <{}>", self.table_url())
    }
}

/// Normalize a given [Url] to **always** contain a trailing slash. This is critically important
/// for assumptions about [Url] equivalency and more importantly for **joining** on a Url`.
///
/// This function will also remove redundant slashes in the ]Url] path which can cause other
/// equivalency failures
///
/// ```ignore
///  left.join("_delta_log"); // produces `s3://bucket/prefix/_delta_log`
///  right.join("_delta_log"); // produces `s3://bucket/_delta_log`
/// ```
pub fn normalize_table_url(url: &Url) -> Url {
    let mut new_segments = vec![];
    for segment in url.path().split('/') {
        if !segment.is_empty() {
            new_segments.push(segment);
        }
    }
    // Add a trailing slash segment
    new_segments.push("");

    let mut url = url.clone();
    url.set_path(&new_segments.join("/"));
    url
}

#[cfg(test)]
mod tests {
    use arrow_ipc::writer::FileWriter;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::operations::create::CreateBuilder;

    fn legacy_eager_snapshot_payload(snapshot: &crate::kernel::EagerSnapshot) -> serde_json::Value {
        let mut snapshot_value = serde_json::to_value(snapshot.snapshot()).unwrap();
        let snapshot_fields = snapshot_value
            .as_array_mut()
            .expect("snapshot serde should use a sequence");
        snapshot_fields.pop();

        let materialized_files = snapshot
            .snapshot()
            .materialized_files()
            .expect("expected materialized files for legacy eager snapshot payload");
        let bytes = if materialized_files.batches.is_empty() {
            Vec::new()
        } else {
            let mut buffer = vec![];
            let mut writer =
                FileWriter::try_new(&mut buffer, materialized_files.batches[0].schema().as_ref())
                    .unwrap();
            for batch in materialized_files.batches.iter() {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
            drop(writer);
            buffer
        };

        json!([snapshot_value, bytes])
    }

    #[test]
    fn test_normalize_table_url() {
        for (u, path) in [
            (Url::parse("s3://bucket/prefix/").unwrap(), "/prefix/"),
            (Url::parse("s3://bucket/prefix").unwrap(), "/prefix/"),
            (
                Url::parse("s3://bucket/prefix with space/").unwrap(),
                "/prefix%20with%20space/",
            ),
            (
                Url::parse("s3://bucket/special&chars/你好/😊").unwrap(),
                "/special&chars/%E4%BD%A0%E5%A5%BD/%F0%9F%98%8A/",
            ),
            (
                Url::parse("s3://bucket/prefix/with/redundant/slashes//").unwrap(),
                "/prefix/with/redundant/slashes/",
            ),
        ] {
            assert_eq!(
                normalize_table_url(&u).path(),
                path,
                "Failed to normalize: {}",
                u.as_str()
            );
        }
    }

    #[tokio::test]
    async fn table_round_trip() {
        let (dt, tmp_dir) = create_test_table().await;
        let bytes = serde_json::to_vec(&dt).unwrap();
        let actual: DeltaTable = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual.version(), dt.version());
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn table_round_trip_preserves_legacy_eager_snapshot_payload() {
        let (dt, tmp_dir) = create_test_table().await;
        let mut value = serde_json::to_value(&dt).unwrap();
        let table_fields = value.as_array_mut().unwrap();
        let state = table_fields[0].as_object_mut().unwrap();
        state.insert(
            "snapshot".to_string(),
            legacy_eager_snapshot_payload(dt.state.as_ref().unwrap().snapshot()),
        );

        let actual: DeltaTable = serde_json::from_value(value).unwrap();
        assert_eq!(
            actual.snapshot().unwrap().log_data().num_files(),
            dt.snapshot().unwrap().log_data().num_files()
        );
        drop(tmp_dir);
    }

    #[test]
    fn table_deserializes_legacy_config_slot() {
        let snapshot: crate::kernel::EagerSnapshot = serde_json::from_str(include_str!(
            "../../tests/serde/eager_snapshot_pre_identity.json"
        ))
        .unwrap();
        let mut table = DeltaTable::new_in_memory();
        table.state = Some(DeltaTableState::new(snapshot));
        let mut value = serde_json::to_value(&table).unwrap();
        let config = json!({
            "requireFiles": true,
            "logBufferSize": 4,
            "logBatchSize": 1024,
            "skipStats": false,
        });
        value[0]["snapshot"][0]
            .as_array_mut()
            .unwrap()
            .insert(9, config.clone());
        value.as_array_mut().unwrap().insert(1, config);
        let actual: DeltaTable = serde_json::from_value(value).unwrap();
        assert_eq!(actual.version(), Some(1));
        assert_eq!(actual.snapshot().unwrap().log_data().num_files(), 1);
        assert_eq!(actual.table_url(), table.table_url());
    }

    #[tokio::test]
    async fn table_without_files_does_not_panic_on_log_data() {
        let (dt, _tmp_dir) = create_test_table().await;
        let url = dt.table_url().clone();

        let table = DeltaTableBuilder::from_url(url)
            .unwrap()
            .load()
            .await
            .unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            table.snapshot().unwrap().log_data().num_files()
        }));

        assert!(result.is_ok());
    }

    /// Copying a table resets the modification times of its log files, so time travel by
    /// timestamp has to use the in-commit timestamps when the table has them enabled.
    #[tokio::test]
    async fn load_with_datetime_uses_in_commit_timestamps() -> DeltaResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        fs_extra::dir::copy(
            "../test/tests/data/table_with_ict",
            temp_dir.path(),
            &Default::default(),
        )
        .unwrap();
        let table_dir = temp_dir.path().join("table_with_ict");
        let copied_at = std::time::SystemTime::now();
        let in_commit_timestamps: Vec<i64> = (0..4)
            .map(|version| {
                let commit_path = table_dir.join(format!("_delta_log/{version:020}.json"));
                std::fs::File::options()
                    .write(true)
                    .open(&commit_path)
                    .unwrap()
                    .set_modified(copied_at)
                    .unwrap();
                let commit = std::fs::read_to_string(&commit_path).unwrap();
                let commit_info: serde_json::Value =
                    serde_json::from_str(commit.lines().next().unwrap()).unwrap();
                commit_info["commitInfo"]["inCommitTimestamp"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        let table_url = Url::from_directory_path(table_dir.canonicalize().unwrap()).unwrap();

        for (version, in_commit_timestamp) in in_commit_timestamps.into_iter().enumerate() {
            let mut table = DeltaTable::try_from_url(table_url.clone()).await?;
            table
                .load_with_datetime(DateTime::from_timestamp_millis(in_commit_timestamp).unwrap())
                .await?;
            assert_eq!(table.version(), Some(version as Version));
            table
                .load_with_datetime(
                    DateTime::from_timestamp_millis(in_commit_timestamp - 1).unwrap(),
                )
                .await?;
            assert_eq!(table.version(), Some(version.saturating_sub(1) as Version));
        }
        Ok(())
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
