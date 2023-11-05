//! Delta transactions
use std::collections::HashMap;

use chrono::Utc;
use conflict_checker::ConflictChecker;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde_json::Value;

use crate::crate_version;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Action, CommitInfo};
use crate::protocol::DeltaOperation;
use crate::storage::commit_uri_from_version;
use crate::table::state::DeltaTableState;

mod conflict_checker;
#[cfg(feature = "datafusion")]
mod state;
#[cfg(test)]
pub(crate) mod test_utils;

use self::conflict_checker::{CommitConflictError, TransactionInfo, WinningCommitSummary};

const DELTA_LOG_FOLDER: &str = "_delta_log";

/// Error raised while commititng transaction
#[derive(thiserror::Error, Debug)]
pub enum TransactionError {
    /// Version already exists
    #[error("Tried committing existing table version: {0}")]
    VersionAlreadyExists(i64),

    /// Error returned when reading the delta log object failed.
    #[error("Error serializing commit log to json: {json_err}")]
    SerializeLogJson {
        /// Commit log record JSON serialization error.
        json_err: serde_json::error::Error,
    },

    /// Error returned when reading the delta log object failed.
    #[error("Log storage error: {}", .source)]
    ObjectStore {
        /// Storage error details when reading the delta log object failed.
        #[from]
        source: ObjectStoreError,
    },
    /// Error returned when a commit conflict ocurred
    #[error("Failed to commit transaction: {0}")]
    CommitConflict(#[from] CommitConflictError),
    /// Error returned when maximum number of commit trioals is exceeded
    #[error("Failed to commit transaction: {0}")]
    MaxCommitAttempts(i32),
    /// The transaction includes Remove action with data change but Delta table is append-only
    #[error(
        "The transaction includes Remove action with data change but Delta table is append-only"
    )]
    DeltaTableAppendOnly,
}

impl From<TransactionError> for DeltaTableError {
    fn from(err: TransactionError) -> Self {
        match err {
            TransactionError::VersionAlreadyExists(version) => {
                DeltaTableError::VersionAlreadyExists(version)
            }
            TransactionError::SerializeLogJson { json_err } => {
                DeltaTableError::SerializeLogJson { json_err }
            }
            TransactionError::ObjectStore { source } => DeltaTableError::ObjectStore { source },
            other => DeltaTableError::Transaction { source: other },
        }
    }
}

// Convert actions to their json representation
fn log_entry_from_actions<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
    read_snapshot: &DeltaTableState,
) -> Result<String, TransactionError> {
    let append_only = read_snapshot.table_config().append_only();
    let mut jsons = Vec::<String>::new();
    for action in actions {
        if append_only {
            if let Action::Remove(remove) = action {
                if remove.data_change {
                    return Err(TransactionError::DeltaTableAppendOnly);
                }
            }
        }
        let json = serde_json::to_string(action)
            .map_err(|e| TransactionError::SerializeLogJson { json_err: e })?;
        jsons.push(json);
    }
    Ok(jsons.join("\n"))
}

pub(crate) fn get_commit_bytes(
    operation: &DeltaOperation,
    actions: &Vec<Action>,
    read_snapshot: &DeltaTableState,
    app_metadata: Option<HashMap<String, Value>>,
) -> Result<bytes::Bytes, TransactionError> {
    if !actions.iter().any(|a| matches!(a, Action::CommitInfo(..))) {
        let mut extra_info = HashMap::<String, Value>::new();
        let mut commit_info = operation.get_commit_info();
        commit_info.timestamp = Some(Utc::now().timestamp_millis());
        extra_info.insert(
            "clientVersion".to_string(),
            Value::String(format!("delta-rs.{}", crate_version())),
        );
        if let Some(meta) = app_metadata {
            extra_info.extend(meta)
        }
        commit_info.info = extra_info;
        Ok(bytes::Bytes::from(log_entry_from_actions(
            actions
                .iter()
                .chain(std::iter::once(&Action::CommitInfo(commit_info))),
            read_snapshot,
        )?))
    } else {
        Ok(bytes::Bytes::from(log_entry_from_actions(
            actions,
            read_snapshot,
        )?))
    }
}

/// Low-level transaction API. Creates a temporary commit file. Once created,
/// the transaction object could be dropped and the actual commit could be executed
/// with `DeltaTable.try_commit_transaction`.
pub(crate) async fn prepare_commit<'a>(
    storage: &dyn ObjectStore,
    operation: &DeltaOperation,
    actions: &Vec<Action>,
    read_snapshot: &DeltaTableState,
    app_metadata: Option<HashMap<String, Value>>,
) -> Result<Path, TransactionError> {
    // Serialize all actions that are part of this log entry.
    let log_entry = get_commit_bytes(operation, actions, read_snapshot, app_metadata)?;

    // Write delta log entry as temporary file to storage. For the actual commit,
    // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
    let token = uuid::Uuid::new_v4().to_string();
    let file_name = format!("_commit_{token}.json.tmp");
    let path = Path::from_iter([DELTA_LOG_FOLDER, &file_name]);
    storage.put(&path, log_entry).await?;

    Ok(path)
}

/// Tries to commit a prepared commit file. Returns [DeltaTableError::VersionAlreadyExists]
/// if the given `version` already exists. The caller should handle the retry logic itself.
/// This is low-level transaction API. If user does not want to maintain the commit loop then
/// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
/// with retry logic.
pub(crate) async fn try_commit_transaction(
    storage: &dyn ObjectStore,
    tmp_commit: &Path,
    version: i64,
) -> Result<i64, TransactionError> {
    // move temporary commit file to delta log directory
    // rely on storage to fail if the file already exists -
    storage
        .rename_if_not_exists(tmp_commit, &commit_uri_from_version(version))
        .await
        .map_err(|err| match err {
            ObjectStoreError::AlreadyExists { .. } => {
                TransactionError::VersionAlreadyExists(version)
            }
            _ => TransactionError::from(err),
        })?;
    Ok(version)
}

/// Commit a transaction, with up to 15 retries. This is higher-level transaction API.
///
/// Will error early if the a concurrent transaction has already been committed
/// and conflicts with this transaction.
pub async fn commit(
    storage: &dyn ObjectStore,
    actions: &Vec<Action>,
    operation: DeltaOperation,
    read_snapshot: &DeltaTableState,
    app_metadata: Option<HashMap<String, Value>>,
) -> DeltaResult<i64> {
    commit_with_retries(storage, actions, operation, read_snapshot, app_metadata, 15).await
}

/// Commit a transaction, with up configurable number of retries. This is higher-level transaction API.
///
/// The function will error early if the a concurrent transaction has already been committed
/// and conflicts with this transaction.
pub async fn commit_with_retries(
    storage: &dyn ObjectStore,
    actions: &Vec<Action>,
    operation: DeltaOperation,
    read_snapshot: &DeltaTableState,
    app_metadata: Option<HashMap<String, Value>>,
    max_retries: usize,
) -> DeltaResult<i64> {
    let tmp_commit =
        prepare_commit(storage, &operation, actions, read_snapshot, app_metadata).await?;

    let mut attempt_number = 1;

    while attempt_number <= max_retries {
        let version = read_snapshot.version() + attempt_number as i64;
        match try_commit_transaction(storage, &tmp_commit, version).await {
            Ok(version) => return Ok(version),
            Err(TransactionError::VersionAlreadyExists(version)) => {
                let summary = WinningCommitSummary::try_new(storage, version - 1, version).await?;
                let transaction_info = TransactionInfo::try_new(
                    read_snapshot,
                    operation.read_predicate(),
                    actions,
                    // TODO allow tainting whole table
                    false,
                )?;
                let conflict_checker =
                    ConflictChecker::new(transaction_info, summary, Some(&operation));
                match conflict_checker.check_conflicts() {
                    Ok(_) => {
                        attempt_number += 1;
                    }
                    Err(err) => {
                        storage.delete(&tmp_commit).await?;
                        return Err(TransactionError::CommitConflict(err).into());
                    }
                };
            }
            Err(err) => {
                storage.delete(&tmp_commit).await?;
                return Err(err.into());
            }
        }
    }

    Err(TransactionError::MaxCommitAttempts(max_retries as i32).into())
}

#[cfg(all(test, feature = "parquet"))]
mod tests {
    use self::test_utils::{create_remove_action, init_table_actions};
    use super::*;
    use crate::DeltaConfigKey;
    use object_store::memory::InMemory;
    use std::collections::HashMap;

    #[test]
    fn test_commit_uri_from_version() {
        let version = commit_uri_from_version(0);
        assert_eq!(version, Path::from("_delta_log/00000000000000000000.json"));
        let version = commit_uri_from_version(123);
        assert_eq!(version, Path::from("_delta_log/00000000000000000123.json"))
    }

    #[test]
    fn test_log_entry_from_actions() {
        let actions = init_table_actions(None);
        let state = DeltaTableState::from_actions(actions.clone(), 0).unwrap();
        let entry = log_entry_from_actions(&actions, &state).unwrap();
        let lines: Vec<_> = entry.lines().collect();
        // writes every action to a line
        assert_eq!(actions.len(), lines.len())
    }

    fn remove_action_exists_when_delta_table_is_append_only(
        data_change: bool,
    ) -> Result<String, TransactionError> {
        let remove = create_remove_action("test_append_only", data_change);
        let mut actions = init_table_actions(Some(HashMap::from([(
            DeltaConfigKey::AppendOnly.as_ref().to_string(),
            Some("true".to_string()),
        )])));
        actions.push(remove);
        let state =
            DeltaTableState::from_actions(actions.clone(), 0).expect("Failed to get table state");
        log_entry_from_actions(&actions, &state)
    }

    #[test]
    fn test_remove_action_exists_when_delta_table_is_append_only() {
        let _err = remove_action_exists_when_delta_table_is_append_only(true)
            .expect_err("Remove action is included when Delta table is append-only. Should error");
        let _actions = remove_action_exists_when_delta_table_is_append_only(false)
            .expect("Data is not changed by the Remove action. Should succeed");
    }

    #[tokio::test]
    async fn test_try_commit_transaction() {
        let store = InMemory::new();
        let tmp_path = Path::from("_delta_log/tmp");
        let version_path = Path::from("_delta_log/00000000000000000000.json");
        store.put(&tmp_path, bytes::Bytes::new()).await.unwrap();
        store.put(&version_path, bytes::Bytes::new()).await.unwrap();

        // fails if file version already exists
        let res = try_commit_transaction(&store, &tmp_path, 0).await;
        assert!(res.is_err());

        // succeeds for next version
        let res = try_commit_transaction(&store, &tmp_path, 1).await.unwrap();
        assert_eq!(res, 1);
    }
}
