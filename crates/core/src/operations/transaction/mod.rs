//! Delta transactions
use std::collections::HashMap;

use chrono::Utc;
use conflict_checker::ConflictChecker;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde_json::Value;

use self::conflict_checker::{CommitConflictError, TransactionInfo, WinningCommitSummary};
use crate::crate_version;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Action, CommitInfo, ReaderFeatures, WriterFeatures};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;

pub use self::protocol::INSTANCE as PROTOCOL;

mod conflict_checker;
mod protocol;
#[cfg(feature = "datafusion")]
mod state;
#[cfg(test)]
pub(crate) mod test_utils;

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

    /// Error returned when unsupported reader features are required
    #[error("Unsupported reader features required: {0:?}")]
    UnsupportedReaderFeatures(Vec<ReaderFeatures>),

    /// Error returned when unsupported writer features are required
    #[error("Unsupported writer features required: {0:?}")]
    UnsupportedWriterFeatures(Vec<WriterFeatures>),

    /// Error returned when writer features are required but not specified
    #[error("Writer features must be specified for writerversion >= 7")]
    WriterFeaturesRequired,

    /// Error returned when reader features are required but not specified
    #[error("Reader features must be specified for reader version >= 3")]
    ReaderFeaturesRequired,

    /// The transaction failed to commit due to an error in an implementation-specific layer.
    /// Currently used by DynamoDb-backed S3 log store when database operations fail.
    #[error("Transaction failed: {msg}")]
    LogStoreError {
        /// Detailed message for the commit failure.
        msg: String,
        /// underlying error in the log store transactional layer.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
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
            other => DeltaTableError::Transaction { source: other },
        }
    }
}

// Convert actions to their json representation
fn log_entry_from_actions<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
) -> Result<String, TransactionError> {
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
    actions: &[Action],
    app_metadata: &HashMap<String, Value>,
) -> Result<bytes::Bytes, TransactionError> {
    if !actions.iter().any(|a| matches!(a, Action::CommitInfo(..))) {
        let mut extra_info = HashMap::<String, Value>::new();
        let mut commit_info = operation.get_commit_info();
        commit_info.timestamp = Some(Utc::now().timestamp_millis());
        extra_info.insert(
            "clientVersion".to_string(),
            Value::String(format!("delta-rs.{}", crate_version())),
        );
        if !app_metadata.is_empty() {
            extra_info.extend(app_metadata.to_owned())
        }
        commit_info.info = extra_info;
        Ok(bytes::Bytes::from(log_entry_from_actions(
            actions
                .iter()
                .chain(std::iter::once(&Action::CommitInfo(commit_info))),
        )?))
    } else {
        Ok(bytes::Bytes::from(log_entry_from_actions(actions)?))
    }
}

/// Low-level transaction API. Creates a temporary commit file. Once created,
/// the transaction object could be dropped and the actual commit could be executed
/// with `DeltaTable.try_commit_transaction`.
/// TODO: comment is outdated now
pub async fn prepare_commit<'a>(
    storage: &dyn ObjectStore,
    operation: &DeltaOperation,
    actions: &[Action],
    app_metadata: &HashMap<String, Value>,
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

#[derive(Default, Clone)]
pub struct CommitProperties {
    pub(crate) app_metadata: HashMap<String, Value>,
    max_retries: usize,
}

impl<'a> From<CommitProperties> for CommitBuilder<'a> {
    fn from(value: CommitProperties) -> Self {
        CommitBuilder {
            max_retries: value.max_retries,
            app_metadata: value.app_metadata,
            ..Default::default()
        }
    }
}

/// TODO
pub struct CommitBuilder<'a> {
    actions: &'a [Action],
    read_snapshot: Option<&'a DeltaTableState>,
    app_metadata: HashMap<String, Value>,
    max_retries: usize,
}

impl<'a> Default for CommitBuilder<'a> {
    fn default() -> Self {
        CommitBuilder {
            max_retries: 15,
            ..Default::default()
        }
    }
}

impl<'a> CommitBuilder<'a> {
    pub fn with_actions(mut self, actions: &'a [Action]) -> Self {
        self.actions = actions;
        self
    }

    pub fn with_snapshot(mut self, snapshot: &'a DeltaTableState) -> Self {
        self.read_snapshot = Some(snapshot);
        self
    }

    pub fn with_maybe_snapshot(mut self, snapshot: Option<&'a DeltaTableState>) -> Self {
        self.read_snapshot = snapshot;
        self
    }

    pub fn with_app_metadata(mut self, app_metadata: HashMap<String, Value>) -> Self {
        self.app_metadata = app_metadata;
        self
    }

    pub fn with_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn build(self, log_store: LogStoreRef, operation: DeltaOperation) -> Commit<'a> {
        Commit {
            log_store,
            actions: self.actions,
            operation,
            app_metadata: self.app_metadata,
            read_snapshot: self.read_snapshot,
            max_retries: self.max_retries,
        }
    }
}

struct Commit<'a> {
    log_store: LogStoreRef,
    read_snapshot: Option<&'a DeltaTableState>,
    actions: &'a [Action],
    operation: DeltaOperation,
    app_metadata: HashMap<String, Value>,
    max_retries: usize,
}

impl<'a> Commit<'a> {
    /// Commit a transaction, with up configurable number of retries. This is higher-level transaction API.
    ///
    /// The function will error early if the a concurrent transaction has already been committed
    /// and conflicts with this transaction.
    pub async fn execute(&self) -> DeltaResult<i64> {
        if let Some(read_snapshot) = &self.read_snapshot {
            PROTOCOL.can_commit(read_snapshot, &self.actions)?;
        }

        let tmp_commit = prepare_commit(
            self.log_store.object_store().as_ref(),
            &self.operation,
            &self.actions,
            &self.app_metadata,
        )
        .await?;

        if self.read_snapshot.is_none() {
            self.log_store.write_commit_entry(0, &tmp_commit).await?;
            return Ok(0);
        }

        let read_snapshot = self.read_snapshot.as_ref().unwrap();

        let mut attempt_number = 1;
        while attempt_number <= self.max_retries {
            let version = read_snapshot.version() + attempt_number as i64;
            match self
                .log_store
                .write_commit_entry(version, &tmp_commit)
                .await
            {
                Ok(()) => return Ok(version),
                Err(TransactionError::VersionAlreadyExists(version)) => {
                    let summary = WinningCommitSummary::try_new(
                        self.log_store.as_ref(),
                        version - 1,
                        version,
                    )
                    .await?;
                    let transaction_info = TransactionInfo::try_new(
                        read_snapshot,
                        self.operation.read_predicate(),
                        &self.actions,
                        // TODO allow tainting whole table
                        false,
                    )?;
                    let conflict_checker =
                        ConflictChecker::new(transaction_info, summary, Some(&self.operation));
                    match conflict_checker.check_conflicts() {
                        Ok(_) => {
                            attempt_number += 1;
                        }
                        Err(err) => {
                            self.log_store.object_store().delete(&tmp_commit).await?;
                            return Err(TransactionError::CommitConflict(err).into());
                        }
                    };
                }
                Err(err) => {
                    self.log_store.object_store().delete(&tmp_commit).await?;
                    return Err(err.into());
                }
            }
        }

        Err(TransactionError::MaxCommitAttempts(self.max_retries as i32).into())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use self::test_utils::init_table_actions;
    use super::*;
    use crate::{logstore::default_logstore::DefaultLogStore, storage::commit_uri_from_version};
    use object_store::memory::InMemory;
    use url::Url;

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
        let entry = log_entry_from_actions(&actions).unwrap();
        let lines: Vec<_> = entry.lines().collect();
        // writes every action to a line
        assert_eq!(actions.len(), lines.len())
    }

    #[tokio::test]
    async fn test_try_commit_transaction() {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("mem://what/is/this").unwrap();
        let log_store = DefaultLogStore::new(
            store.clone(),
            crate::logstore::LogStoreConfig {
                location: url,
                options: HashMap::new().into(),
            },
        );
        let tmp_path = Path::from("_delta_log/tmp");
        let version_path = Path::from("_delta_log/00000000000000000000.json");
        store.put(&tmp_path, bytes::Bytes::new()).await.unwrap();
        store.put(&version_path, bytes::Bytes::new()).await.unwrap();

        let res = log_store.write_commit_entry(0, &tmp_path).await;
        // fails if file version already exists
        assert!(res.is_err());

        // succeeds for next version
        log_store.write_commit_entry(1, &tmp_path).await.unwrap();
    }
}
