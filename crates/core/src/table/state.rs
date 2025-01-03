//! The module for delta table state.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use delta_kernel::schema::Schema;
use delta_kernel::Expression;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};

use super::{config::TableConfig, DeltaTableConfig};
use crate::kernel::{
    ActionType, Add, AddCDCFile, EagerSnapshot, LogDataHandler, LogDataView, Metadata, Protocol,
    Remove, StructType, Transaction,
};
use crate::logstore::LogStore;
use crate::partitions::PartitionFilter;
use crate::{DeltaResult, DeltaTableError};

#[cfg(test)]
use crate::kernel::Action;

/// State snapshot currently held by the Delta Table instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableState {
    pub(crate) snapshot: EagerSnapshot,
}

impl DeltaTableState {
    /// Create a new [`DeltaTableState`]
    pub async fn try_new(
        table_root: &Path,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = EagerSnapshot::try_new_with_visitor(
            table_root,
            store.clone(),
            config,
            version,
            HashSet::from([ActionType::Txn]),
        )
        .await?;
        Ok(Self { snapshot })
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
        use crate::operations::transaction::CommitData;
        use crate::protocol::{DeltaOperation, SaveMode};
        use std::collections::HashMap;

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
        )];

        let snapshot = EagerSnapshot::new_test(&commit_data).unwrap();
        Ok(Self { snapshot })
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

    /// Full list of add actions representing all parquet files that are part of the current
    /// delta table state.
    pub fn file_actions_iter(&self) -> DeltaResult<impl Iterator<Item = Add> + '_> {
        self.snapshot.file_actions()
    }

    /// Get the number of files in the current table state
    pub fn files_count(&self) -> usize {
        self.snapshot.files_count()
    }

    /// Full list of all of the CDC files added as part of the changeDataFeed feature
    pub fn cdc_files(&self) -> DeltaResult<impl Iterator<Item = AddCDCFile> + '_> {
        self.snapshot.cdc_files()
    }

    /// Returns an iterator of file names present in the loaded state
    #[inline]
    pub fn file_paths_iter(&self) -> impl Iterator<Item = Path> + '_ {
        self.log_data()
            .into_iter()
            .map(|add| add.object_store_path())
    }

    /// HashMap containing the last transaction stored for every application.
    pub fn app_transaction_version(&self) -> DeltaResult<impl Iterator<Item = Transaction> + '_> {
        self.snapshot.transactions()
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

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        self.snapshot.table_config()
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
        self.snapshot.update(log_store, version).await?;
        Ok(())
    }

    /// Obtain Add actions for files that match the filter
    pub fn get_active_add_actions_by_partitions(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<LogDataView, DeltaTableError> {
        // validate all referenced columns are part of the partition columns.
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

        let predicate = to_predicate(filters, self.schema())?;
        self.snapshot
            .log_data_new()?
            .with_partition_filter(Some(&predicate))
    }
}

fn to_predicate(filters: &[PartitionFilter], schema: &Schema) -> DeltaResult<Expression> {
    if filters.len() == 0 {
        return Ok(Expression::literal(true));
    }
    if filters.len() == 1 {
        return filters[0].to_expression(schema);
    }
    Ok(Expression::and_from(
        filters
            .iter()
            .map(|f| f.to_expression(schema))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::schema::{DataType, StructField, StructType};

    #[test]
    fn test_to_predicate() {
        let filters = vec![crate::PartitionFilter {
            key: "k".to_string(),
            value: crate::PartitionValue::Equal("".to_string()),
        }];
        let schema = StructType::new(vec![StructField::new("k", DataType::STRING, true)]);

        let expr = to_predicate(&filters, &schema).unwrap();
        let expected = Expression::column(["k"]).is_null();

        assert_eq!(expr, expected)
    }
}
