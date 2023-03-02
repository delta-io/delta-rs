//! Delta transactions
use crate::action::{Action, CommitInfo, DeltaOperation};
use crate::storage::{commit_uri_from_version, DeltaObjectStore, ObjectStoreRef};
use crate::table_state::DeltaTableState;
use crate::{crate_version, DeltaDataTypeVersion, DeltaResult, DeltaTableError};
use chrono::Utc;
use conflict_checker::ConflictChecker;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde_json::{Map, Value};

mod conflict_checker;
#[cfg(feature = "datafusion")]
mod state;
#[cfg(test)]
pub(crate) mod test_utils;
mod types;

pub use types::*;

const DELTA_LOG_FOLDER: &str = "_delta_log";

#[derive(thiserror::Error, Debug)]
pub(crate) enum TransactionError {
    #[error("Tried committing existing table version: {0}")]
    VersionAlreadyExists(DeltaDataTypeVersion),

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
        }
    }
}

// Convert actions to their json representation
fn log_entry_from_actions(actions: &[Action]) -> Result<String, TransactionError> {
    let mut jsons = Vec::<String>::new();
    for action in actions {
        let json = serde_json::to_string(action)
            .map_err(|e| TransactionError::SerializeLogJson { json_err: e })?;
        jsons.push(json);
    }
    Ok(jsons.join("\n"))
}

pub(crate) fn get_commit_bytes(
    operation: &DeltaOperation,
    actions: &mut Vec<Action>,
    app_metadata: Option<Map<String, Value>>,
) -> Result<bytes::Bytes, TransactionError> {
    if !actions.iter().any(|a| matches!(a, Action::commitInfo(..))) {
        let mut extra_info = Map::<String, Value>::new();
        let mut commit_info = operation.get_commit_info();
        commit_info.timestamp = Some(Utc::now().timestamp_millis());
        extra_info.insert(
            "clientVersion".to_string(),
            Value::String(format!("delta-rs.{}", crate_version())),
        );
        if let Some(mut meta) = app_metadata {
            extra_info.append(&mut meta)
        }
        commit_info.info = extra_info;
        actions.push(Action::commitInfo(commit_info));
    }

    // Serialize all actions that are part of this log entry.
    Ok(bytes::Bytes::from(log_entry_from_actions(actions)?))
}

/// Low-level transaction API. Creates a temporary commit file. Once created,
/// the transaction object could be dropped and the actual commit could be executed
/// with `DeltaTable.try_commit_transaction`.
pub(crate) async fn prepare_commit(
    storage: &DeltaObjectStore,
    operation: &DeltaOperation,
    actions: &mut Vec<Action>,
    app_metadata: Option<Map<String, Value>>,
) -> Result<Path, TransactionError> {
    // Serialize all actions that are part of this log entry.
    let log_entry = get_commit_bytes(operation, actions, app_metadata)?;

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
async fn try_commit_transaction(
    storage: &DeltaObjectStore,
    tmp_commit: &Path,
    version: DeltaDataTypeVersion,
) -> Result<DeltaDataTypeVersion, TransactionError> {
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

pub(crate) async fn commit(
    storage: ObjectStoreRef,
    mut actions: Vec<Action>,
    operation: DeltaOperation,
    read_snapshot: &DeltaTableState,
    app_metadata: Option<Map<String, Value>>,
) -> DeltaResult<DeltaDataTypeVersion> {
    // readPredicates.nonEmpty || readFiles.nonEmpty
    // TODO revise logic if files are read
    let depends_on_files = match operation {
        DeltaOperation::Create { .. } | DeltaOperation::StreamingUpdate { .. } => false,
        DeltaOperation::Optimize { .. } => true,
        DeltaOperation::Write {
            predicate: Some(_), ..
        } => true,
        _ => false,
    };

    // TODO actually get the prop from commit infos...
    let only_add_files = false;
    let _is_blind_append = only_add_files && !depends_on_files;

    let tmp_commit =
        prepare_commit(storage.as_ref(), &operation, &mut actions, app_metadata).await?;

    let max_attempts = 5;
    let mut attempt_number = 1;

    while attempt_number <= max_attempts {
        let version = read_snapshot.version() + attempt_number;
        match try_commit_transaction(storage.as_ref(), &tmp_commit, version).await {
            Ok(version) => return Ok(version),
            Err(TransactionError::VersionAlreadyExists(version)) => {
                let conflict_checker = ConflictChecker::try_new(
                    read_snapshot,
                    storage.clone(),
                    version,
                    operation.clone(),
                    &actions,
                )
                .await?;
                match conflict_checker.check_conflicts() {
                    Ok(_) => {
                        attempt_number += 1;
                    }
                    Err(_err) => {
                        storage.delete(&tmp_commit).await?;
                        return Err(DeltaTableError::VersionAlreadyExists(version));
                    }
                };
            }
            Err(err) => {
                storage.delete(&tmp_commit).await?;
                return Err(err.into());
            }
        }
    }

    // TODO max attempts error
    Err(DeltaTableError::VersionAlreadyExists(-1))
}

#[cfg(all(test, feature = "parquet"))]
mod tests {
    use super::*;

    #[test]
    fn test_commit_version() {
        let version = commit_uri_from_version(0);
        assert_eq!(version, Path::from("_delta_log/00000000000000000000.json"));
        let version = commit_uri_from_version(123);
        assert_eq!(version, Path::from("_delta_log/00000000000000000123.json"))
    }
}
