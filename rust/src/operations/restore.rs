//! Perform restore of delta table to a specified version or datetime
//!
//! Algorithm:
//! 1) Read the latest state snapshot of the table.
//! 2) Read table state for version or datetime to restore
//! 3) Compute files available in state for restoring (files were removed by some commit)
//! but missed in the latest. Add these files into commit as AddFile action.
//! 4) Compute files available in the latest state snapshot (files were added after version to restore)
//! but missed in the state to restore. Add these files into commit as RemoveFile action.
//! 5) If ignore_missing_files option is false (default value) check availability of AddFile
//! in file system.
//! 6) Commit Protocol, all RemoveFile and AddFile actions
//! into delta log using `try_commit_transaction` (commit will be failed in case of parallel transaction)
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
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;

use crate::operations::transaction::{prepare_commit, try_commit_transaction, TransactionError};
use crate::protocol::{Action, Add, DeltaOperation, Protocol, Remove};
use crate::storage::ObjectStoreRef;
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
    store: ObjectStoreRef,
    /// Version to restore
    version_to_restore: Option<i64>,
    /// Datetime to restore
    datetime_to_restore: Option<DateTime<Utc>>,
    /// Ignore missing files
    ignore_missing_files: bool,
    /// Protocol downgrade allowed
    protocol_downgrade_allowed: bool,
}

impl RestoreBuilder {
    /// Create a new [`RestoreBuilder`]
    pub fn new(store: ObjectStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            store,
            version_to_restore: None,
            datetime_to_restore: None,
            ignore_missing_files: false,
            protocol_downgrade_allowed: false,
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
}

async fn execute(
    object_store: ObjectStoreRef,
    snapshot: DeltaTableState,
    version_to_restore: Option<i64>,
    datetime_to_restore: Option<DateTime<Utc>>,
    ignore_missing_files: bool,
    protocol_downgrade_allowed: bool,
) -> DeltaResult<RestoreMetrics> {
    if !(version_to_restore
        .is_none()
        .bitxor(datetime_to_restore.is_none()))
    {
        return Err(DeltaTableError::from(RestoreError::InvalidRestoreParameter));
    }
    let mut table = DeltaTable::new(object_store.clone(), DeltaTableConfig::default());
    let version = match datetime_to_restore {
        Some(datetime) => {
            table.load_with_datetime(datetime).await?;
            table.version()
        }
        None => {
            table.load_version(version_to_restore.unwrap()).await?;
            table.version()
        }
    };

    if version >= snapshot.version() {
        return Err(DeltaTableError::from(RestoreError::TooLargeRestoreVersion(
            version,
            snapshot.version(),
        )));
    }
    let state_to_restore_files = table.get_state().files().clone();
    let latest_state_files = snapshot.files().clone();
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
            }
        })
        .collect();

    if !ignore_missing_files {
        check_files_available(object_store.as_ref(), &files_to_add).await?;
    }

    let metrics = RestoreMetrics {
        num_removed_file: files_to_remove.len(),
        num_restored_file: files_to_add.len(),
    };

    let mut actions = vec![];
    let protocol = if protocol_downgrade_allowed {
        Protocol {
            min_reader_version: table.get_min_reader_version(),
            min_writer_version: table.get_min_writer_version(),
        }
    } else {
        Protocol {
            min_reader_version: max(
                table.get_min_reader_version(),
                snapshot.min_reader_version(),
            ),
            min_writer_version: max(
                table.get_min_writer_version(),
                snapshot.min_writer_version(),
            ),
        }
    };
    actions.push(Action::protocol(protocol));
    actions.extend(files_to_add.into_iter().map(Action::add));
    actions.extend(files_to_remove.into_iter().map(Action::remove));

    let commit = prepare_commit(
        object_store.as_ref(),
        &DeltaOperation::Restore {
            version: version_to_restore,
            datetime: datetime_to_restore.map(|time| -> i64 { time.timestamp_millis() }),
        },
        &actions,
        None,
    )
    .await?;
    let commit_version = snapshot.version() + 1;
    match try_commit_transaction(object_store.as_ref(), &commit, commit_version).await {
        Ok(_) => {}
        Err(err @ TransactionError::VersionAlreadyExists(_)) => {
            return Err(err.into());
        }
        Err(err) => {
            object_store.delete(&commit).await?;
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
            let metrics = execute(
                this.store.clone(),
                this.snapshot.clone(),
                this.version_to_restore,
                this.datetime_to_restore,
                this.ignore_missing_files,
                this.protocol_downgrade_allowed,
            )
            .await?;
            let mut table = DeltaTable::new_with_state(this.store, this.snapshot);
            table.update().await?;
            Ok((table, metrics))
        })
    }
}
