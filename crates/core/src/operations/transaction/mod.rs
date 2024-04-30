//! Add a commit entry to the Delta Table.
//! This module provides a unified interface for modifying commit behavior and attributes
//!
//! [`CommitProperties`] provides an unified client interface for
//!  all Delta opeartions. Internally this is used to initialize a [`CommitBuilder`]
//!  
//!  For advanced use cases [`CommitBuilder`] can be used which allows
//!  finer control over the commit process. The builder can be converted
//! into a future the yield either a [`PreparedCommit`] or a [`FinalizedCommit`].
//!
//! A [`PreparedCommit`] represents a temporary commit marker written to storage.
//! To convert to a [`FinalizedCommit`] an atomic rename is attempted. If the rename fails
//! then conflict resolution is performed and the atomic rename is tried for the latest version.
//!
//!<pre>
//!                                          Client Interface
//!        ┌─────────────────────────────┐                    
//!        │      Commit Properties      │                    
//!        │                             │                    
//!        │ Public commit interface for │                    
//!        │     all Delta Operations    │                    
//!        │                             │                    
//!        └─────────────┬───────────────┘                    
//!                      │                                    
//! ─────────────────────┼────────────────────────────────────
//!                      │                                    
//!                      ▼                  Advanced Interface
//!        ┌─────────────────────────────┐                    
//!        │       Commit Builder        │                    
//!        │                             │                    
//!        │   Advanced entry point for  │                    
//!        │     creating a commit       │                    
//!        └─────────────┬───────────────┘                    
//!                      │                                    
//!                      ▼                                    
//!     ┌───────────────────────────────────┐                 
//!     │                                   │                 
//!     │ ┌───────────────────────────────┐ │                 
//!     │ │        Prepared Commit        │ │                 
//!     │ │                               │ │                 
//!     │ │     Represents a temporary    │ │                 
//!     │ │   commit marker written to    │ │                 
//!     │ │           storage             │ │                 
//!     │ └──────────────┬────────────────┘ │                 
//!     │                │                  │                 
//!     │                ▼                  │                 
//!     │ ┌───────────────────────────────┐ │                 
//!     │ │       Finalize Commit         │ │                 
//!     │ │                               │ │                 
//!     │ │   Convert the commit marker   │ │                 
//!     │ │   to a commit using atomic    │ │                 
//!     │ │         operations            │ │                 
//!     │ │                               │ │                 
//!     │ └───────────────────────────────┘ │                 
//!     │                                   │                 
//!     └────────────────┬──────────────────┘                 
//!                      │                                    
//!                      ▼                                    
//!       ┌───────────────────────────────┐                   
//!       │          Post Commit          │                   
//!       │                               │                   
//!       │ Commit that was materialized  │                   
//!       │ to storage with post commit   │                   
//!       │      hooks to be executed     │                   
//!       └──────────────┬────────────────┘                 
//!                      │                                    
//!                      ▼    
//!       ┌───────────────────────────────┐                   
//!       │        Finalized Commit       │                   
//!       │                               │                   
//!       │ Commit that was materialized  │                   
//!       │         to storage            │                   
//!       │                               │                   
//!       └───────────────────────────────┘           
//!</pre>

use chrono::Utc;
use conflict_checker::ConflictChecker;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use serde_json::Value;
use std::collections::HashMap;

use self::conflict_checker::{CommitConflictError, TransactionInfo, WinningCommitSummary};
use crate::checkpoints::create_checkpoint_for;
use crate::errors::DeltaTableError;
use crate::kernel::{
    Action, CommitInfo, EagerSnapshot, Metadata, Protocol, ReaderFeatures, WriterFeatures,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::config::TableConfig;
use crate::table::state::DeltaTableState;
use crate::{crate_version, DeltaResult};
use tracing::debug;

pub use self::protocol::INSTANCE as PROTOCOL;

mod conflict_checker;
mod protocol;
#[cfg(feature = "datafusion")]
mod state;
#[cfg(test)]
pub(crate) mod test_utils;

const DELTA_LOG_FOLDER: &str = "_delta_log";
pub(crate) const DEFAULT_RETRIES: usize = 15;

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
    #[error("Writer features must be specified for writerversion >= 7, please specify: {0:?}")]
    WriterFeaturesRequired(WriterFeatures),

    /// Error returned when reader features are required but not specified
    #[error("Reader features must be specified for reader version >= 3, please specify: {0:?}")]
    ReaderFeaturesRequired(ReaderFeatures),

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

/// Reference to some structure that contains mandatory attributes for performing a commit.
pub trait TableReference: Send + Sync {
    /// Well known table configuration
    fn config(&self) -> TableConfig;

    /// Get the table protocol of the snapshot
    fn protocol(&self) -> &Protocol;

    /// Get the table metadata of the snapshot
    fn metadata(&self) -> &Metadata;

    /// Try to cast this table reference to a `EagerSnapshot`
    fn eager_snapshot(&self) -> Option<&EagerSnapshot>;
}

impl TableReference for EagerSnapshot {
    fn protocol(&self) -> &Protocol {
        EagerSnapshot::protocol(self)
    }

    fn metadata(&self) -> &Metadata {
        EagerSnapshot::metadata(self)
    }

    fn config(&self) -> TableConfig {
        self.table_config()
    }

    fn eager_snapshot(&self) -> Option<&EagerSnapshot> {
        Some(self)
    }
}

impl TableReference for DeltaTableState {
    fn config(&self) -> TableConfig {
        self.snapshot.config()
    }

    fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    fn eager_snapshot(&self) -> Option<&EagerSnapshot> {
        Some(&self.snapshot)
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
    /// Create new data to be comitted
    pub fn new(
        mut actions: Vec<Action>,
        operation: DeltaOperation,
        mut app_metadata: HashMap<String, Value>,
    ) -> Result<Self, CommitBuilderError> {
        if !actions.iter().any(|a| matches!(a, Action::CommitInfo(..))) {
            let mut commit_info = operation.get_commit_info();
            commit_info.timestamp = Some(Utc::now().timestamp_millis());
            app_metadata.insert(
                "clientVersion".to_string(),
                Value::String(format!("delta-rs.{}", crate_version())),
            );
            app_metadata.extend(commit_info.info);
            commit_info.info = app_metadata.clone();
            actions.push(Action::CommitInfo(commit_info))
        }
        Ok(CommitData {
            actions,
            operation,
            app_metadata,
        })
    }

    /// Convert actions to their json representation
    pub fn log_entry_from_actions<'a>(
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

#[derive(Clone, Debug, Copy)]
/// Properties for post commit hook.
pub struct PostCommitHookProperties {
    create_checkpoint: bool,
}

#[derive(Clone, Debug)]
/// End user facing interface to be used by operations on the table.
/// Enable controling commit behaviour and modifying metadata that is written during a commit.
pub struct CommitProperties {
    pub(crate) app_metadata: HashMap<String, Value>,
    max_retries: usize,
    create_checkpoint: bool,
}

impl Default for CommitProperties {
    fn default() -> Self {
        Self {
            app_metadata: Default::default(),
            max_retries: DEFAULT_RETRIES,
            create_checkpoint: true,
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

    /// Specify if it should create a checkpoint when the commit interval condition is met
    pub fn with_create_checkpoint(mut self, create_checkpoint: bool) -> Self {
        self.create_checkpoint = create_checkpoint;
        self
    }
}

impl From<CommitProperties> for CommitBuilder {
    fn from(value: CommitProperties) -> Self {
        CommitBuilder {
            max_retries: value.max_retries,
            app_metadata: value.app_metadata,
            post_commit_hook: PostCommitHookProperties {
                create_checkpoint: value.create_checkpoint,
            }
            .into(),
            ..Default::default()
        }
    }
}

/// Prepare data to be committed to the Delta log and control how the commit is performed
pub struct CommitBuilder {
    actions: Vec<Action>,
    app_metadata: HashMap<String, Value>,
    max_retries: usize,
    post_commit_hook: Option<PostCommitHookProperties>,
}

impl Default for CommitBuilder {
    fn default() -> Self {
        CommitBuilder {
            actions: Vec::new(),
            app_metadata: HashMap::new(),
            max_retries: DEFAULT_RETRIES,
            post_commit_hook: None,
        }
    }
}

impl<'a> CommitBuilder {
    /// Actions to be included in the commit
    pub fn with_actions(mut self, actions: Vec<Action>) -> Self {
        self.actions = actions;
        self
    }

    /// Metadata for the operation performed like metrics, user, and notebook
    pub fn with_app_metadata(mut self, app_metadata: HashMap<String, Value>) -> Self {
        self.app_metadata = app_metadata;
        self
    }

    /// Maximum number of times to retry the transaction before failing to commit
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Specify all the post commit hook properties
    pub fn with_post_commit_hook(mut self, post_commit_hook: PostCommitHookProperties) -> Self {
        self.post_commit_hook = post_commit_hook.into();
        self
    }

    /// Prepare a Commit operation using the configured builder
    pub fn build(
        self,
        table_data: Option<&'a dyn TableReference>,
        log_store: LogStoreRef,
        operation: DeltaOperation,
    ) -> Result<PreCommit<'a>, CommitBuilderError> {
        let data = CommitData::new(self.actions, operation, self.app_metadata)?;
        Ok(PreCommit {
            log_store,
            table_data,
            max_retries: self.max_retries,
            data,
            post_commit_hook: self.post_commit_hook,
        })
    }
}

/// Represents a commit that has not yet started but all details are finalized
pub struct PreCommit<'a> {
    log_store: LogStoreRef,
    table_data: Option<&'a dyn TableReference>,
    data: CommitData,
    max_retries: usize,
    post_commit_hook: Option<PostCommitHookProperties>,
}

impl<'a> std::future::IntoFuture for PreCommit<'a> {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move { this.into_prepared_commit_future().await?.await?.await })
    }
}

impl<'a> PreCommit<'a> {
    /// Prepare the commit but do not finalize it
    pub fn into_prepared_commit_future(self) -> BoxFuture<'a, DeltaResult<PreparedCommit<'a>>> {
        let this = self;

        Box::pin(async move {
            if let Some(table_reference) = this.table_data {
                PROTOCOL.can_commit(table_reference, &this.data.actions, &this.data.operation)?;
            }

            // Serialize all actions that are part of this log entry.
            let log_entry = this.data.get_bytes()?;

            // Write delta log entry as temporary file to storage. For the actual commit,
            // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
            let token = uuid::Uuid::new_v4().to_string();
            let file_name = format!("_commit_{token}.json.tmp");
            let path = Path::from_iter([DELTA_LOG_FOLDER, &file_name]);
            this.log_store.object_store().put(&path, log_entry).await?;

            Ok(PreparedCommit {
                path,
                log_store: this.log_store,
                table_data: this.table_data,
                max_retries: this.max_retries,
                data: this.data,
                post_commit: this.post_commit_hook,
            })
        })
    }
}

/// Represents a inflight commit with a temporary commit marker on the log store
pub struct PreparedCommit<'a> {
    path: Path,
    log_store: LogStoreRef,
    data: CommitData,
    table_data: Option<&'a dyn TableReference>,
    max_retries: usize,
    post_commit: Option<PostCommitHookProperties>,
}

impl<'a> PreparedCommit<'a> {
    /// The temporary commit file created
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<'a> std::future::IntoFuture for PreparedCommit<'a> {
    type Output = DeltaResult<PostCommit<'a>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let tmp_commit = &this.path;

            if this.table_data.is_none() {
                debug!("** Invoking write_commit_entry for version 0 **");
                this.log_store.write_commit_entry(0, tmp_commit).await?;
                return Ok(PostCommit {
                    version: 0,
                    data: this.data,
                    create_checkpoint: false,
                    log_store: this.log_store,
                    table_data: this.table_data,
                });
            }

            // unwrap() is safe here due to the above check
            // TODO: refactor to only depend on TableReference Trait
            let read_snapshot =
                this.table_data
                    .unwrap()
                    .eager_snapshot()
                    .ok_or(DeltaTableError::Generic(
                        "Expected an instance of EagerSnapshot".to_owned(),
                    ))?;

            let mut attempt_number = 1;
            while attempt_number <= this.max_retries {
                let version = read_snapshot.version() + attempt_number as i64;
                debug!("** Invoking write_commit_entry for version ({version}) and tmp commit: ({tmp_commit}) **");
                match this.log_store.write_commit_entry(version, tmp_commit).await {
                    Ok(()) => {
                        return Ok(PostCommit {
                            version,
                            data: this.data,
                            create_checkpoint: this
                                .post_commit
                                .map(|v| v.create_checkpoint)
                                .unwrap_or_default(),
                            log_store: this.log_store,
                            table_data: this.table_data,
                        });
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
                            this.data.operation.read_whole_table(),
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
                                this.log_store
                                    .abort_commit_entry(version, tmp_commit)
                                    .await?;
                                return Err(TransactionError::CommitConflict(err).into());
                            }
                        };
                    }
                    Err(err) => {
                        this.log_store
                            .abort_commit_entry(version, tmp_commit)
                            .await?;
                        return Err(err.into());
                    }
                }
            }

            Err(TransactionError::MaxCommitAttempts(this.max_retries as i32).into())
        })
    }
}

/// Represents items for the post commit hook
pub struct PostCommit<'a> {
    /// The winning version number of the commit
    pub version: i64,
    /// The data that was comitted to the log store
    pub data: CommitData,
    create_checkpoint: bool,
    log_store: LogStoreRef,
    table_data: Option<&'a dyn TableReference>,
}

impl<'a> PostCommit<'a> {
    /// Runs the post commit activities
    async fn run_post_commit_hook(
        &self,
        version: i64,
        commit_data: &CommitData,
    ) -> DeltaResult<()> {
        if self.create_checkpoint {
            self.create_checkpoint(&self.table_data, &self.log_store, version, commit_data)
                .await?
        }
        Ok(())
    }
    async fn create_checkpoint(
        &self,
        table: &Option<&'a dyn TableReference>,
        log_store: &LogStoreRef,
        version: i64,
        commit_data: &CommitData,
    ) -> DeltaResult<()> {
        if let Some(table) = table {
            let checkpoint_interval = table.config().checkpoint_interval() as i64;
            if ((version + 1) % checkpoint_interval) == 0 {
                // We have to advance the snapshot otherwise we can't create a checkpoint
                let mut snapshot = table.eager_snapshot().unwrap().clone();
                snapshot.advance(vec![commit_data])?;
                let state = DeltaTableState {
                    app_transaction_version: HashMap::new(),
                    snapshot,
                };
                create_checkpoint_for(version, &state, log_store.as_ref()).await?
            }
        }
        Ok(())
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

impl<'a> std::future::IntoFuture for PostCommit<'a> {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            match this.run_post_commit_hook(this.version, &this.data).await {
                Ok(_) => {
                    return Ok(FinalizedCommit {
                        version: this.version,
                        data: this.data,
                    })
                }
                Err(err) => return Err(err),
            };
        })
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
        let entry = CommitData::log_entry_from_actions(&actions).unwrap();
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
