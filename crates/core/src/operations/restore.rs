//! Perform restore of delta table to a specified version or datetime
//!
//! Algorithm:
//! 1) Read the latest state snapshot of the table.
//! 2) Read table state for version or datetime to restore
//! 3) Compute files available in state for restoring (files were removed by some commit)
//!    but missed in the latest. Add these files into commit as AddFile action.
//! 4) Compute files available in the latest state snapshot (files were added after version to restore)
//!    but missed in the state to restore. Add these files into commit as RemoveFile action.
//! 5) If ignore_missing_files option is false (default value) check availability of AddFile
//!    in file system.
//! 6) Commit Protocol, all RemoveFile and AddFile actions
//!    into delta log using `LogStore::write_commit_entry` (commit will be failed in case of parallel transaction)
//!    TODO: comment is outdated
//! 7) If table was modified in parallel then ignore restore and raise exception.
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = RestoreBuilder::new(table.object_store(), table.state).with_version_to_restore(1).await?;
//! ````

use std::cmp::max;
use std::collections::HashSet;
use std::ops::BitXor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;
use uuid::Uuid;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, TransactionError};
use crate::kernel::{Action, Add, ProtocolExt as _, ProtocolInner, Remove};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableConfig, DeltaTableError, ObjectStoreError};

/// Errors that can occur during restore
#[derive(thiserror::Error, Debug)]
enum RestoreError {
    #[error("Either the version or datetime should be provided for restore")]
    InvalidRestoreParameter,

    #[error("Version to restore {0} should be less then last available version {1}.")]
    TooLargeRestoreVersion(i64, i64),

    #[error("Find missing file {0} when restore.")]
    MissingDataFile(String),
}

impl From<RestoreError> for DeltaTableError {
    fn from(err: RestoreError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}

/// Metrics from Restore
#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreMetrics {
    /// Number of files removed
    pub num_removed_file: usize,
    /// Number of files restored
    pub num_restored_file: usize,
}

/// Restore a Delta table with given version
/// See this module's documentation for more information
pub struct RestoreBuilder {
    /// A snapshot of the to-be-restored table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Version to restore
    version_to_restore: Option<i64>,
    /// Datetime to restore
    datetime_to_restore: Option<DateTime<Utc>>,
    /// Ignore missing files
    ignore_missing_files: bool,
    /// Protocol downgrade allowed
    protocol_downgrade_allowed: bool,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation<()> for RestoreBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl RestoreBuilder {
    /// Create a new [`RestoreBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            version_to_restore: None,
            datetime_to_restore: None,
            ignore_missing_files: false,
            protocol_downgrade_allowed: false,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Set the version to restore
    pub fn with_version_to_restore(mut self, version: i64) -> Self {
        self.version_to_restore = Some(version);
        self
    }

    /// Set the datetime to restore
    pub fn with_datetime_to_restore(mut self, datetime: DateTime<Utc>) -> Self {
        self.datetime_to_restore = Some(datetime);
        self
    }

    /// Set whether to ignore missing files which delete manually or by vacuum.
    /// If true, continue to run when encountering missing files.
    pub fn with_ignore_missing_files(mut self, ignore_missing_files: bool) -> Self {
        self.ignore_missing_files = ignore_missing_files;
        self
    }

    /// Set whether allow to downgrade protocol
    pub fn with_protocol_downgrade_allowed(mut self, protocol_downgrade_allowed: bool) -> Self {
        self.protocol_downgrade_allowed = protocol_downgrade_allowed;
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    log_store: LogStoreRef,
    snapshot: DeltaTableState,
    version_to_restore: Option<i64>,
    datetime_to_restore: Option<DateTime<Utc>>,
    ignore_missing_files: bool,
    protocol_downgrade_allowed: bool,
    mut commit_properties: CommitProperties,
    operation_id: Uuid,
) -> DeltaResult<RestoreMetrics> {
    if !(version_to_restore
        .is_none()
        .bitxor(datetime_to_restore.is_none()))
    {
        return Err(DeltaTableError::from(RestoreError::InvalidRestoreParameter));
    }
    let mut table = DeltaTable::new(log_store.clone(), DeltaTableConfig::default());

    let version = match datetime_to_restore {
        Some(datetime) => {
            table.load_with_datetime(datetime).await?;
            table
                .version()
                .ok_or_else(|| DeltaTableError::NotInitialized)?
        }
        None => {
            table.load_version(version_to_restore.unwrap()).await?;
            table
                .version()
                .ok_or_else(|| DeltaTableError::NotInitialized)?
        }
    };

    if version >= snapshot.version() {
        return Err(DeltaTableError::from(RestoreError::TooLargeRestoreVersion(
            version,
            snapshot.version(),
        )));
    }

    let metadata_restored_version = table.metadata()?;

    let state_to_restore_files = table.snapshot()?.file_actions()?;
    let latest_state_files = snapshot.file_actions()?;
    let state_to_restore_files_set =
        HashSet::<Add>::from_iter(state_to_restore_files.iter().cloned());
    let latest_state_files_set = HashSet::<Add>::from_iter(latest_state_files.iter().cloned());

    let files_to_add: Vec<Add> = state_to_restore_files
        .into_iter()
        .filter(|a: &Add| !latest_state_files_set.contains(a))
        .map(|mut a: Add| -> Add {
            a.data_change = true;
            a
        })
        .collect();

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let files_to_remove: Vec<Remove> = latest_state_files
        .into_iter()
        .filter(|a: &Add| !state_to_restore_files_set.contains(a))
        .map(|a: Add| -> Remove {
            Remove {
                path: a.path.clone(),
                deletion_timestamp: Some(deletion_timestamp),
                data_change: true,
                extended_file_metadata: Some(false),
                partition_values: Some(a.partition_values.clone()),
                size: Some(a.size),
                tags: a.tags,
                deletion_vector: a.deletion_vector,
                base_row_id: a.base_row_id,
                default_row_commit_version: a.default_row_commit_version,
            }
        })
        .collect();

    if !ignore_missing_files {
        check_files_available(log_store.object_store(None).as_ref(), &files_to_add).await?;
    }

    let metrics = RestoreMetrics {
        num_removed_file: files_to_remove.len(),
        num_restored_file: files_to_add.len(),
    };

    let mut actions = vec![];
    let protocol = if protocol_downgrade_allowed {
        ProtocolInner {
            min_reader_version: table.protocol()?.min_reader_version(),
            min_writer_version: table.protocol()?.min_writer_version(),
            writer_features: if snapshot.protocol().min_writer_version() < 7 {
                None
            } else {
                table.protocol()?.writer_features_set()
            },
            reader_features: if snapshot.protocol().min_reader_version() < 3 {
                None
            } else {
                table.protocol()?.reader_features_set()
            },
        }
    } else {
        ProtocolInner {
            min_reader_version: max(
                table.protocol()?.min_reader_version(),
                snapshot.protocol().min_reader_version(),
            ),
            min_writer_version: max(
                table.protocol()?.min_writer_version(),
                snapshot.protocol().min_writer_version(),
            ),
            writer_features: snapshot.protocol().writer_features_set(),
            reader_features: snapshot.protocol().reader_features_set(),
        }
    };
    commit_properties
        .app_metadata
        .insert("readVersion".to_owned(), snapshot.version().into());
    commit_properties.app_metadata.insert(
        "operationMetrics".to_owned(),
        serde_json::to_value(&metrics)?,
    );

    actions.push(Action::Protocol(protocol.as_kernel()));
    actions.extend(files_to_add.into_iter().map(Action::Add));
    actions.extend(files_to_remove.into_iter().map(Action::Remove));
    // Add the metadata from the restored version to undo e.g. constraint or field metadata changes
    actions.push(Action::Metadata(metadata_restored_version.clone()));

    let operation = DeltaOperation::Restore {
        version: version_to_restore,
        datetime: datetime_to_restore.map(|time| -> i64 { time.timestamp_millis() }),
    };

    let prepared_commit = CommitBuilder::from(commit_properties)
        .with_actions(actions)
        .build(Some(&snapshot), log_store.clone(), operation)
        .into_prepared_commit_future()
        .await?;

    let commit_version = snapshot.version() + 1;
    let commit_bytes = prepared_commit.commit_or_bytes();
    match log_store
        .write_commit_entry(commit_version, commit_bytes.clone(), operation_id)
        .await
    {
        Ok(_) => {}
        Err(err @ TransactionError::VersionAlreadyExists(_)) => {
            return Err(err.into());
        }
        Err(err) => {
            log_store
                .abort_commit_entry(commit_version, commit_bytes.clone(), operation_id)
                .await?;
            return Err(err.into());
        }
    }
    Ok(metrics)
}

async fn check_files_available(
    object_store: &dyn ObjectStore,
    files: &Vec<Add>,
) -> DeltaResult<()> {
    for file in files {
        let file_path = Path::parse(file.path.clone())?;
        match object_store.head(&file_path).await {
            Ok(_) => {}
            Err(ObjectStoreError::NotFound { .. }) => {
                return Err(DeltaTableError::from(RestoreError::MissingDataFile(
                    file.path.clone(),
                )))
            }
            Err(e) => return Err(DeltaTableError::from(e)),
        }
    }
    Ok(())
}

impl std::future::IntoFuture for RestoreBuilder {
    type Output = DeltaResult<(DeltaTable, RestoreMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let metrics = execute(
                this.log_store.clone(),
                this.snapshot.clone(),
                this.version_to_restore,
                this.datetime_to_restore,
                this.ignore_missing_files,
                this.protocol_downgrade_allowed,
                this.commit_properties.clone(),
                operation_id,
            )
            .await?;

            this.post_execute(operation_id).await?;

            let mut table = DeltaTable::new_with_state(this.log_store, this.snapshot);
            table.update().await?;
            Ok((table, metrics))
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::writer::test_utils::{create_bare_table, get_record_batch};
    use crate::{DeltaOps, DeltaResult};

    /// Verify that restore respects constraints that were added/removed in previous version_to_restore
    /// <https://github.com/delta-io/delta-rs/issues/3352>
    #[tokio::test]
    #[cfg(feature = "datafusion")]
    async fn test_simple_restore_constraints() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;
        let first_v = table.version().unwrap();

        let constraint = DeltaOps(table)
            .add_constraint()
            .with_constraint("my_custom_constraint", "value < 100")
            .await;
        let table = constraint.expect("Failed to add constraint to table");

        let constraints = table
            .state
            .as_ref()
            .unwrap()
            .table_config()
            .get_constraints();
        assert!(constraints.len() == 1);
        assert_eq!(constraints[0].name, "my_custom_constraint");

        let (table, _metrics) = DeltaOps(table)
            .restore()
            .with_version_to_restore(first_v)
            .await?;
        assert_ne!(table.version(), Some(first_v));

        let constraints = table.state.unwrap().table_config().get_constraints();
        assert!(constraints.is_empty());

        Ok(())
    }
}
