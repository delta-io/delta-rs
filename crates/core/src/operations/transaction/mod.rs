//! Delta transactions
use std::collections::HashMap;

use chrono::Utc;
use conflict_checker::ConflictChecker;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde_json::Value;

use self::conflict_checker::{CommitConflictError, TransactionInfo, WinningCommitSummary};
use crate::crate_version;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Action, CommitInfo, EagerSnapshot, ReaderFeatures, WriterFeatures};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;

pub use self::protocol::INSTANCE as PROTOCOL;

mod conflict_checker;
mod protocol;
#[cfg(feature = "datafusion")]
mod state;
#[cfg(test)]
pub(crate) mod test_utils;

const DELTA_LOG_FOLDER: &str = "_delta_log";
const DEFAULT_RETRIES: usize = 15;

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

/// Error raised while commititng transaction
#[derive(thiserror::Error, Debug)]
pub enum CommitBuilderError {}

impl From<CommitBuilderError> for DeltaTableError {
    fn from(err: CommitBuilderError) -> Self {
        DeltaTableError::CommitValidation { source: err }
    }
}
/// Data that was actually written to the log store.
pub struct CommitData {
    /// The actions
    pub actions: Vec<Action>,
    /// The Operation
    pub operation: DeltaOperation,
    /// The Metadata
    pub app_metadata: HashMap<String, Value>,
}

impl CommitData {
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

    /// Obtain the byte representation of the commit.
    pub fn get_bytes(&self) -> Result<bytes::Bytes, TransactionError> {
        // Data MUST be read from the passed `CommitData`. Don't add data that is not sourced from there.
        let actions = &self.actions;
        Ok(bytes::Bytes::from(Self::log_entry_from_actions(actions)?))
    }
}

#[derive(Clone, Debug)]
/// End user facing interface to be used by operations on the table.
/// Enable controling commit behaviour and modifying metadata that is written during a commit.
pub struct CommitProperties {
    pub(crate) app_metadata: HashMap<String, Value>,
    max_retries: usize,
}

impl Default for CommitProperties {
    fn default() -> Self {
        Self {
            app_metadata: Default::default(),
            max_retries: DEFAULT_RETRIES,
        }
    }
}

impl CommitProperties {
    /// Specify metadata the be comitted
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.app_metadata = HashMap::from_iter(metadata);
        self
    }
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
    actions: Vec<Action>,
    read_snapshot: Option<&'a EagerSnapshot>,
    app_metadata: HashMap<String, Value>,
    max_retries: usize,
}

impl<'a> Default for CommitBuilder<'a> {
    fn default() -> Self {
        CommitBuilder {
            actions: Vec::new(),
            read_snapshot: None,
            app_metadata: HashMap::new(),
            max_retries: DEFAULT_RETRIES,
        }
    }
}

impl<'a> CommitBuilder<'a> {
    /// Actions to be included in the commit
    pub fn with_actions(mut self, actions: Vec<Action>) -> Self {
        self.actions = actions;
        self
    }

    /// The snapshot of the Delta table used to perform the commit
    pub fn with_snapshot(mut self, snapshot: &'a EagerSnapshot) -> Self {
        self.read_snapshot = Some(snapshot);
        self
    }

    /// An optional snapshot of the Delta table used to perform the commit
    pub fn with_maybe_snapshot(mut self, snapshot: Option<&'a EagerSnapshot>) -> Self {
        self.read_snapshot = snapshot;
        self
    }

    /// Metadata for the operation performed like metrics, user, and notebook
    pub fn with_app_metadata(mut self, app_metadata: HashMap<String, Value>) -> Self {
        self.app_metadata = app_metadata;
        self
    }

    /// Prepare a Commit operation using the configured builder
    pub fn build(
        self,
        log_store: LogStoreRef,
        operation: DeltaOperation,
    ) -> Result<PreCommit<'a>, CommitBuilderError> {
        Ok(PreCommit {
            log_store,
            actions: self.actions,
            operation,
            app_metadata: self.app_metadata,
            read_snapshot: self.read_snapshot,
            max_retries: self.max_retries,
        })
    }
}

/// Represents a commit that has not yet started but all details are finalized
pub struct PreCommit<'a> {
    log_store: LogStoreRef,
    read_snapshot: Option<&'a EagerSnapshot>,
    actions: Vec<Action>,
    operation: DeltaOperation,
    app_metadata: HashMap<String, Value>,
    max_retries: usize,
}

impl<'a> std::future::IntoFuture for PreCommit<'a> {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move { this.into_prepared_commit_future().await?.await })
    }
}

impl<'a> PreCommit<'a> {
    /// Prepare the commit but do not finalize it
    pub fn into_prepared_commit_future(self) -> BoxFuture<'a, DeltaResult<PreparedCommit<'a>>> {
        let this = self;

        Box::pin(async move {
            if let Some(read_snapshot) = &this.read_snapshot {
                PROTOCOL.can_commit(read_snapshot, &this.actions, &this.operation)?;
            }

            let mut data = CommitData {
                actions: this.actions,
                app_metadata: this.app_metadata,
                operation: this.operation,
            };

            if !data
                .actions
                .iter()
                .any(|a| matches!(a, Action::CommitInfo(..)))
            {
                let mut commit_info = data.operation.get_commit_info();
                commit_info.timestamp = Some(Utc::now().timestamp_millis());
                data.app_metadata.insert(
                    "clientVersion".to_string(),
                    Value::String(format!("delta-rs.{}", crate_version())),
                );
                data.app_metadata.extend(commit_info.info);
                commit_info.info = data.app_metadata.clone();
                data.actions.push(Action::CommitInfo(commit_info))
            }

            // Serialize all actions that are part of this log entry.
            let log_entry = data.get_bytes()?;

            // Write delta log entry as temporary file to storage. For the actual commit,
            // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
            let token = uuid::Uuid::new_v4().to_string();
            let file_name = format!("_commit_{token}.json.tmp");
            let path = Path::from_iter([DELTA_LOG_FOLDER, &file_name]);
            this.log_store.object_store().put(&path, log_entry).await?;

            Ok(PreparedCommit {
                path,
                log_store: this.log_store,
                read_snapshot: this.read_snapshot,
                max_retries: this.max_retries,
                data: data,
            })
        })
    }
}

/// Represents a inflight commit with a temporary commit marker on the log store
pub struct PreparedCommit<'a> {
    path: Path,
    log_store: LogStoreRef,
    data: CommitData,
    read_snapshot: Option<&'a EagerSnapshot>,
    max_retries: usize,
}

impl<'a> PreparedCommit<'a> {
    /// The temporary commit file created
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<'a> std::future::IntoFuture for PreparedCommit<'a> {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let tmp_commit = &this.path;

            if this.read_snapshot.is_none() {
                this.log_store.write_commit_entry(0, tmp_commit).await?;
                return Ok(FinalizedCommit {
                    version: 0,
                    data: this.data,
                });
            }

            let read_snapshot = this.read_snapshot.as_ref().unwrap();

            let mut attempt_number = 1;
            while attempt_number <= this.max_retries {
                let version = read_snapshot.version() + attempt_number as i64;
                match this.log_store.write_commit_entry(version, tmp_commit).await {
                    Ok(()) => {
                        return Ok(FinalizedCommit {
                            version,
                            data: this.data,
                        })
                    }
                    Err(TransactionError::VersionAlreadyExists(version)) => {
                        let summary = WinningCommitSummary::try_new(
                            this.log_store.as_ref(),
                            version - 1,
                            version,
                        )
                        .await?;
                        let transaction_info = TransactionInfo::try_new(
                            read_snapshot,
                            this.data.operation.read_predicate(),
                            &this.data.actions,
                            // TODO allow tainting whole table
                            false,
                        )?;
                        let conflict_checker = ConflictChecker::new(
                            transaction_info,
                            summary,
                            Some(&this.data.operation),
                        );
                        match conflict_checker.check_conflicts() {
                            Ok(_) => {
                                attempt_number += 1;
                            }
                            Err(err) => {
                                this.log_store.object_store().delete(tmp_commit).await?;
                                return Err(TransactionError::CommitConflict(err).into());
                            }
                        };
                    }
                    Err(err) => {
                        this.log_store.object_store().delete(tmp_commit).await?;
                        return Err(err.into());
                    }
                }
            }

            Err(TransactionError::MaxCommitAttempts(this.max_retries as i32).into())
        })
    }
}

/// A commit that successfully completed
pub struct FinalizedCommit {
    /// The winning version number of the commit
    pub version: i64,
    /// The data that was comitted to the log store
    pub data: CommitData,
}

impl FinalizedCommit {
    /// The materialized version of the commit
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Data used to write the commit
    pub fn data(&self) -> &CommitData {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use self::test_utils::init_table_actions;
    use super::*;
    use crate::{
        logstore::{default_logstore::DefaultLogStore, LogStore},
        storage::commit_uri_from_version,
    };
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
