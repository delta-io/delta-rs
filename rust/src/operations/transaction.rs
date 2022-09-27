//! Delta transactions
use std::sync::Arc;

use crate::action::{Action, DeltaOperation};
use crate::storage::DeltaObjectStore;
use crate::{crate_version, DeltaDataTypeVersion, DeltaResult, DeltaTableError};

use chrono::Utc;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde_json::{Map, Value};

const DELTA_LOG_FOLDER: &str = "_delta_log";

#[derive(thiserror::Error, Debug)]
enum TransactionError {
    #[error("Tried committing existing table version: {0}")]
    VersionAlreadyExists(DeltaDataTypeVersion),

    /// Error returned when reading the delta log object failed.
    #[error("Error serializing commit: {}", .source)]
    Serialize {
        /// Storage error details when reading the delta log object failed.
        #[from]
        source: serde_json::Error,
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
            TransactionError::Serialize { source } => DeltaTableError::InvalidJson { source },
            TransactionError::ObjectStore { source } => DeltaTableError::ObjectStore { source },
        }
    }
}

/// Return the uri of commit version.
fn commit_uri_from_version(version: DeltaDataTypeVersion) -> Path {
    let version = format!("{:020}.json", version);
    Path::from_iter([DELTA_LOG_FOLDER, &version])
}

// Convert actions to their json representation
fn log_entry_from_actions(actions: &[Action]) -> Result<String, TransactionError> {
    let mut jsons = Vec::<String>::new();
    for action in actions {
        let json = serde_json::to_string(action)?;
        jsons.push(json);
    }
    Ok(jsons.join("\n"))
}

/// Low-level transaction API. Creates a temporary commit file. Once created,
/// the transaction object could be dropped and the actual commit could be executed
/// with `DeltaTable.try_commit_transaction`.
async fn prepare_commit(
    storage: &Arc<DeltaObjectStore>,
    operation: DeltaOperation,
    mut actions: Vec<Action>,
    app_metadata: Option<Map<String, Value>>,
) -> Result<Path, TransactionError> {
    if !actions.iter().any(|a| matches!(a, Action::commitInfo(..))) {
        let mut commit_info = Map::<String, Value>::new();
        commit_info.insert(
            "timestamp".to_string(),
            Value::Number(serde_json::Number::from(Utc::now().timestamp_millis())),
        );
        commit_info.insert(
            "clientVersion".to_string(),
            Value::String(format!("delta-rs.{}", crate_version())),
        );
        commit_info.append(&mut operation.get_commit_info());
        if let Some(mut meta) = app_metadata {
            commit_info.append(&mut meta)
        }
        actions.push(Action::commitInfo(commit_info));
    }

    // Serialize all actions that are part of this log entry.
    let log_entry = bytes::Bytes::from(log_entry_from_actions(&actions)?);

    // Write delta log entry as temporary file to storage. For the actual commit,
    // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
    let token = uuid::Uuid::new_v4().to_string();
    let file_name = format!("_commit_{}.json.tmp", token);
    let path = Path::from_iter([DELTA_LOG_FOLDER, &file_name]);
    storage.put(&path, log_entry).await?;

    Ok(path)
}

/// Tries to commit a prepared commit file. Returns [`DeltaTableError::VersionAlreadyExists`]
/// if the given `version` already exists. The caller should handle the retry logic itself.
/// This is low-level transaction API. If user does not want to maintain the commit loop then
/// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
/// with retry logic.
async fn try_commit_transaction(
    storage: &Arc<DeltaObjectStore>,
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
    storage: &Arc<DeltaObjectStore>,
    version: DeltaDataTypeVersion,
    actions: Vec<Action>,
    operation: DeltaOperation,
    app_metadata: Option<Map<String, Value>>,
) -> DeltaResult<DeltaDataTypeVersion> {
    let tmp_commit = prepare_commit(storage, operation, actions, app_metadata).await?;
    match try_commit_transaction(storage, &tmp_commit, version).await {
        Ok(version) => Ok(version),
        Err(TransactionError::VersionAlreadyExists(version)) => {
            storage.delete(&tmp_commit).await?;
            Err(DeltaTableError::VersionAlreadyExists(version))
        }
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{DeltaOperation, Protocol, SaveMode};
    use crate::storage::utils::flatten_list_stream;
    use crate::writer::test_utils::get_delta_metadata;
    use crate::{DeltaTable, DeltaTableBuilder};

    #[test]
    fn test_commit_version() {
        let version = commit_uri_from_version(0);
        assert_eq!(version, Path::from("_delta_log/00000000000000000000.json"));
        let version = commit_uri_from_version(123);
        assert_eq!(version, Path::from("_delta_log/00000000000000000123.json"))
    }

    #[tokio::test]
    async fn test_commits_writes_file() {
        let metadata = get_delta_metadata(&vec![]);
        let operation = DeltaOperation::Create {
            mode: SaveMode::Append,
            location: "memory://".into(),
            protocol: Protocol {
                min_reader_version: 1,
                min_writer_version: 1,
            },
            metadata,
        };

        let commit_path = Path::from("_delta_log/00000000000000000000.json");
        let storage = DeltaTableBuilder::from_uri("memory://")
            .build_storage()
            .unwrap();

        // successfully write in clean location
        commit(&storage, 0, vec![], operation.clone(), None)
            .await
            .unwrap();
        let head = storage.head(&commit_path).await;
        assert!(head.is_ok());
        assert_eq!(head.as_ref().unwrap().location, commit_path);

        // fail on overwriting
        let failed_commit = commit(&storage, 0, vec![], operation, None).await;
        assert!(failed_commit.is_err());
        assert!(matches!(
            failed_commit.unwrap_err(),
            DeltaTableError::VersionAlreadyExists(_)
        ));

        // check we clean up after ourselves
        let objects = flatten_list_stream(storage.as_ref(), None).await.unwrap();
        assert_eq!(objects.len(), 1);

        // table can be loaded
        let mut table = DeltaTable::new(storage, Default::default());
        table.load().await.unwrap();
        assert_eq!(table.version(), 0)
    }
}
