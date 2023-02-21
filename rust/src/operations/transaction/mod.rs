//! Delta transactions
use crate::action::{Action, CommitInfo, DeltaOperation};
use crate::storage::{commit_uri_from_version, DeltaObjectStore};
use crate::{crate_version, DeltaDataTypeVersion, DeltaResult, DeltaTableError};
use chrono::Utc;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde_json::{Map, Value};

mod conflict_checker;
#[cfg(feature = "datafusion")]
mod state;
mod types;

pub use types::*;

const DELTA_LOG_FOLDER: &str = "_delta_log";

#[derive(thiserror::Error, Debug)]
enum TransactionError {
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

/// Low-level transaction API. Creates a temporary commit file. Once created,
/// the transaction object could be dropped and the actual commit could be executed
/// with `DeltaTable.try_commit_transaction`.
async fn prepare_commit(
    storage: &DeltaObjectStore,
    operation: DeltaOperation,
    mut actions: Vec<Action>,
    app_metadata: Option<Map<String, Value>>,
) -> Result<Path, TransactionError> {
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
    let log_entry = bytes::Bytes::from(log_entry_from_actions(&actions)?);

    // Write delta log entry as temporary file to storage. For the actual commit,
    // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
    let token = uuid::Uuid::new_v4().to_string();
    let file_name = format!("_commit_{token}.json.tmp");
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
    storage: &DeltaObjectStore,
    version: DeltaDataTypeVersion,
    actions: Vec<Action>,
    operation: DeltaOperation,
    read_version: Option<DeltaDataTypeVersion>,
    app_metadata: Option<Map<String, Value>>,
) -> DeltaResult<DeltaDataTypeVersion> {
    // TODO(roeap) in the reference implementation this logic is implemented, which seems somewhat strange,
    // as it seems we will never have "WriteSerializable" as level - probably need to check the table config ...
    // https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L964
    let isolation_level = if can_downgrade_to_snapshot_isolation(&actions, &operation) {
        IsolationLevel::SnapshotIsolation
    } else {
        IsolationLevel::default_level()
    };

    // TODO: calculate isolation level to use when checking for conflicts.
    // Leaving conflict checking unimplemented for now to get the "single writer" implementation off the ground.
    // Leaving some commented code in place as a guidepost for the future.

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
    let is_blind_append = only_add_files && !depends_on_files;

    let _commit_info = CommitInfo {
        version: None,
        timestamp: Some(chrono::Utc::now().timestamp()),
        read_version,
        isolation_level: Some(isolation_level),
        operation: Some(operation.name().to_string()),
        operation_parameters: Some(operation.operation_parameters()?.collect()),
        user_id: None,
        user_name: None,
        is_blind_append: Some(is_blind_append),
        engine_info: Some(format!("Delta-RS/{}", crate_version())),
        ..Default::default()
    };

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

fn can_downgrade_to_snapshot_isolation<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
    operation: &DeltaOperation,
) -> bool {
    let mut data_changed = false;
    let mut has_non_file_actions = false;
    for action in actions {
        match action {
            Action::add(act) if act.data_change => data_changed = true,
            Action::remove(rem) if rem.data_change => data_changed = true,
            _ => has_non_file_actions = true,
        }
    }

    if has_non_file_actions {
        // if Non-file-actions are present (e.g. METADATA etc.), then don't downgrade the isolation
        // level to SnapshotIsolation.
        return false;
    }

    let default_isolation_level = IsolationLevel::default_level();
    // Note-1: For no-data-change transactions such as OPTIMIZE/Auto Compaction/ZorderBY, we can
    // change the isolation level to SnapshotIsolation. SnapshotIsolation allows reduced conflict
    // detection by skipping the
    // [[ConflictChecker.checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn]] check i.e.
    // don't worry about concurrent appends.
    // Note-2:
    // We can also use SnapshotIsolation for empty transactions. e.g. consider a commit:
    // t0 - Initial state of table
    // t1 - Q1, Q2 starts
    // t2 - Q1 commits
    // t3 - Q2 is empty and wants to commit.
    // In this scenario, we can always allow Q2 to commit without worrying about new files
    // generated by Q1.
    // The final order which satisfies both Serializability and WriteSerializability is: Q2, Q1
    // Note that Metadata only update transactions shouldn't be considered empty. If Q2 above has
    // a Metadata update (say schema change/identity column high watermark update), then Q2 can't
    // be moved above Q1 in the final SERIALIZABLE order. This is because if Q2 is moved above Q1,
    // then Q1 should see the updates from Q2 - which actually didn't happen.

    match default_isolation_level {
        IsolationLevel::Serializable => !data_changed,
        IsolationLevel::WriteSerializable => !data_changed && !operation.changes_data(),
        _ => false, // This case should never happen
    }
}

#[cfg(all(test, feature = "parquet"))]
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
        let metadata = get_delta_metadata(&[]);
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
        commit(storage.as_ref(), 0, vec![], operation.clone(), None, None)
            .await
            .unwrap();
        let head = storage.head(&commit_path).await;
        assert!(head.is_ok());
        assert_eq!(head.as_ref().unwrap().location, commit_path);

        // fail on overwriting
        let failed_commit = commit(storage.as_ref(), 0, vec![], operation, None, None).await;
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
