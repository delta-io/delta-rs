//! The module for delta table state.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::{filter_record_batch, is_not_null};
use arrow_array::{Array, Int64Array, StringArray, StructArray};
use chrono::Utc;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};

use super::config::TableConfig;
use super::{get_partition_col_data_types, DeltaTableConfig};
use crate::kernel::arrow::extract as ex;
use crate::kernel::{
    Action, ActionType, Add, AddCDCFile, DataType, EagerSnapshot, LogDataHandler, LogicalFile,
    Metadata, Protocol, Remove, ReplayVisitor, StructType, Txn,
};

use crate::logstore::LogStore;
use crate::operations::transaction::CommitData;
use crate::partitions::{DeltaTablePartition, PartitionFilter};
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTableError};

pub(crate) struct AppTransactionVisitor {
    app_transaction_version: HashMap<String, Txn>,
}

impl AppTransactionVisitor {
    pub(crate) fn new() -> Self {
        Self {
            app_transaction_version: HashMap::new(),
        }
    }
}

impl AppTransactionVisitor {
    pub fn merge(self, map: &HashMap<String, Txn>) -> HashMap<String, Txn> {
        let mut clone = map.clone();
        for (key, value) in self.app_transaction_version {
            clone.insert(key, value);
        }

        return clone;
    }
}

impl ReplayVisitor for AppTransactionVisitor {
    fn visit_batch(&mut self, batch: &arrow_array::RecordBatch) -> DeltaResult<()> {
        if batch.column_by_name("txn").is_none() {
            return Ok(());
        }

        let txn_col = ex::extract_and_cast::<StructArray>(batch, "txn")?;
        let filter = is_not_null(txn_col)?;

        let filtered = filter_record_batch(batch, &filter)?;
        let arr = ex::extract_and_cast::<StructArray>(&filtered, "txn")?;

        let id = ex::extract_and_cast::<StringArray>(arr, "appId")?;
        let version = ex::extract_and_cast::<Int64Array>(arr, "version")?;
        let last_updated = ex::extract_and_cast_opt::<Int64Array>(arr, "lastUpdated");

        for idx in 0..id.len() {
            if id.is_valid(idx) {
                let app_id = ex::read_str(id, idx)?;
                if self.app_transaction_version.contains_key(app_id) {
                    continue;
                }
                self.app_transaction_version.insert(
                    app_id.to_owned(),
                    Txn {
                        app_id: app_id.into(),
                        version: ex::read_primitive(version, idx)?,
                        last_updated: last_updated.and_then(|arr| ex::read_primitive_opt(arr, idx)),
                    },
                );
            }
        }

        Ok(())
    }

    fn required_actions(&self) -> Vec<ActionType> {
        vec![ActionType::Txn]
    }
}

/// State snapshot currently held by the Delta Table instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableState {
    pub(crate) app_transaction_version: HashMap<String, Txn>,
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
        let mut app_visitor = AppTransactionVisitor::new();
        let visitors: Vec<&mut dyn ReplayVisitor> = vec![&mut app_visitor];
        let snapshot = EagerSnapshot::try_new_with_visitor(
            table_root,
            store.clone(),
            config,
            version,
            visitors,
        )
        .await?;
        Ok(Self {
            snapshot,
            app_transaction_version: app_visitor.app_transaction_version,
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

        let commit_data = [CommitData::new(
            actions,
            DeltaOperation::Create {
                mode: SaveMode::Append,
                location: Path::default().to_string(),
                protocol: protocol.clone(),
                metadata: metadata.clone(),
            },
            HashMap::new(),
            Vec::new(),
        )
        .unwrap()];

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

    /// Full list of all of the CDC files added as part of the changeDataFeed feature
    pub fn cdc_files(&self) -> DeltaResult<Vec<AddCDCFile>> {
        Ok(self.snapshot.cdc_files()?.collect())
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
    pub fn app_transaction_version(&self) -> &HashMap<String, Txn> {
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
        // TODO: Maybe change this interface to just use CommitData..
        let commit_data = CommitData {
            actions,
            operation: operation.clone(),
            app_metadata: HashMap::new(),
            app_transactions: Vec::new(),
        };

        let mut app_txn_visitor = AppTransactionVisitor::new();
        let new_version = self
            .snapshot
            .advance(&vec![commit_data], vec![&mut app_txn_visitor])?;

        self.app_transaction_version = app_txn_visitor.merge(&self.app_transaction_version);

        if new_version != version {
            return Err(DeltaTableError::Generic("Version mismatch".to_string()));
        }
        Ok(())
    }

    /// Obtain the Eager snapshot of the state
    pub fn snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    /// Update the state of the table to the given version.
    pub async fn update(
        &mut self,
        log_store: Arc<dyn LogStore>,
        version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        let mut app_txn_visitor = AppTransactionVisitor::new();
        self.snapshot
            .update(log_store, version, vec![&mut app_txn_visitor])
            .await?;
        self.app_transaction_version = app_txn_visitor.merge(&self.app_transaction_version);
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
