//! The module for delta table state.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};

use super::config::TableConfig;
use super::{get_partition_col_data_types, DeltaTableConfig};
use crate::kernel::{
    Action, Add, DataType, EagerSnapshot, LogDataHandler, LogicalFile, Metadata, Protocol, Remove,
    StructType,
};
use crate::logstore::LogStore;
use crate::partitions::{DeltaTablePartition, PartitionFilter};
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTableError};

/// State snapshot currently held by the Delta Table instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableState {
    app_transaction_version: HashMap<String, i64>,
    pub(crate) snapshot: EagerSnapshot,
}

impl DeltaTableState {
    /// Create a new DeltaTableState
    pub async fn try_new(
        table_root: &Path,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = EagerSnapshot::try_new(table_root, store.clone(), config, version).await?;
        Ok(Self {
            snapshot,
            app_transaction_version: HashMap::new(),
        })
    }

    /// Return table version
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the timestamp when a version commit was created.
    /// This is the timestamp of the commit file.
    /// If the commit file is not present, None is returned.
    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        self.snapshot.version_timestamp(version)
    }

    /// Construct a delta table state object from a list of actions
    #[cfg(test)]
    pub fn from_actions(actions: Vec<Action>) -> DeltaResult<Self> {
        use crate::protocol::SaveMode;
        let metadata = actions
            .iter()
            .find_map(|a| match a {
                Action::Metadata(m) => Some(m.clone()),
                _ => None,
            })
            .ok_or(DeltaTableError::NotInitialized)?;
        let protocol = actions
            .iter()
            .find_map(|a| match a {
                Action::Protocol(p) => Some(p.clone()),
                _ => None,
            })
            .ok_or(DeltaTableError::NotInitialized)?;

        let commit_data = [(
            actions,
            DeltaOperation::Create {
                mode: SaveMode::Append,
                location: Path::default().to_string(),
                protocol: protocol.clone(),
                metadata: metadata.clone(),
            },
            None,
        )];
        let snapshot = EagerSnapshot::new_test(&commit_data).unwrap();
        Ok(Self {
            app_transaction_version: Default::default(),
            snapshot,
        })
    }

    /// Returns a semantic accessor to the currently loaded log data.
    pub fn log_data(&self) -> LogDataHandler<'_> {
        self.snapshot.log_data()
    }

    /// Full list of tombstones (remove actions) representing files removed from table state).
    pub async fn all_tombstones(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> DeltaResult<impl Iterator<Item = Remove>> {
        Ok(self
            .snapshot
            .snapshot()
            .tombstones(store)?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten())
    }

    /// List of unexpired tombstones (remove actions) representing files removed from table state.
    /// The retention period is set by `deletedFileRetentionDuration` with default value of 1 week.
    pub async fn unexpired_tombstones(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> DeltaResult<impl Iterator<Item = Remove>> {
        let retention_timestamp = Utc::now().timestamp_millis()
            - self
                .table_config()
                .deleted_file_retention_duration()
                .as_millis() as i64;
        let tombstones = self.all_tombstones(store).await?.collect::<Vec<_>>();
        Ok(tombstones
            .into_iter()
            .filter(move |t| t.deletion_timestamp.unwrap_or(0) > retention_timestamp))
    }

    /// Full list of add actions representing all parquet files that are part of the current
    /// delta table state.
    pub fn file_actions(&self) -> DeltaResult<Vec<Add>> {
        Ok(self.snapshot.file_actions()?.collect())
    }

    /// Get the number of files in the current table state
    pub fn files_count(&self) -> usize {
        self.snapshot.files_count()
    }

    /// Returns an iterator of file names present in the loaded state
    #[inline]
    pub fn file_paths_iter(&self) -> impl Iterator<Item = Path> + '_ {
        self.log_data()
            .into_iter()
            .map(|add| add.object_store_path())
    }

    /// HashMap containing the last txn version stored for every app id writing txn
    /// actions.
    pub fn app_transaction_version(&self) -> &HashMap<String, i64> {
        &self.app_transaction_version
    }

    /// The most recent protocol of the table.
    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    /// The most recent metadata of the table.
    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    /// The table schema
    pub fn schema(&self) -> &StructType {
        self.snapshot.schema()
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        self.snapshot.table_config()
    }

    /// Merges new state information into our state
    ///
    /// The DeltaTableState also carries the version information for the given state,
    /// as there is a one-to-one match between a table state and a version. In merge/update
    /// scenarios we cannot infer the intended / correct version number. By default this
    /// function will update the tracked version if the version on `new_state` is larger then the
    /// currently set version however it is up to the caller to update the `version` field according
    /// to the version the merged state represents.
    pub(crate) fn merge(
        &mut self,
        actions: Vec<Action>,
        operation: &DeltaOperation,
        version: i64,
    ) -> Result<(), DeltaTableError> {
        let commit_infos = vec![(actions, operation.clone(), None)];
        let new_version = self.snapshot.advance(&commit_infos)?;
        if new_version != version {
            return Err(DeltaTableError::Generic("Version mismatch".to_string()));
        }
        Ok(())
    }

    /// Update the state of the table to the given version.
    pub async fn update(
        &mut self,
        log_store: Arc<dyn LogStore>,
        version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        self.snapshot.update(log_store, version).await?;
        Ok(())
    }

    /// Obtain Add actions for files that match the filter
    pub fn get_active_add_actions_by_partitions<'a>(
        &'a self,
        filters: &'a [PartitionFilter],
    ) -> Result<impl Iterator<Item = DeltaResult<LogicalFile<'_>>> + '_, DeltaTableError> {
        let current_metadata = self.metadata();

        let nonpartitioned_columns: Vec<String> = filters
            .iter()
            .filter(|f| !current_metadata.partition_columns.contains(&f.key))
            .map(|f| f.key.to_string())
            .collect();

        if !nonpartitioned_columns.is_empty() {
            return Err(DeltaTableError::ColumnsNotPartitioned {
                nonpartitioned_columns: { nonpartitioned_columns },
            });
        }

        let partition_col_data_types: HashMap<&String, &DataType> =
            get_partition_col_data_types(self.schema(), current_metadata)
                .into_iter()
                .collect();

        Ok(self.log_data().into_iter().filter_map(move |add| {
            let partitions = add.partition_values();
            if partitions.is_err() {
                return Some(Err(DeltaTableError::Generic(
                    "Failed to parse partition values".to_string(),
                )));
            }
            let partitions = partitions
                .unwrap()
                .iter()
                .map(|(k, v)| DeltaTablePartition::from_partition_value((*k, v)))
                .collect::<Vec<_>>();
            let is_valid = filters
                .iter()
                .all(|filter| filter.match_partitions(&partitions, &partition_col_data_types));

            if is_valid {
                Some(Ok(add))
            } else {
                None
            }
        }))
    }
}
