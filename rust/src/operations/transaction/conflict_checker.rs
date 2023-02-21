//! Helper module to check if a transaction can be committed in case of conflicting commits.
#![allow(unused)]
use super::{CommitInfo, IsolationLevel};
use crate::action::{Action, Add, DeltaOperation, MetaData, Protocol, Remove};
use crate::operations::transaction::TransactionError;
use crate::storage::{commit_uri_from_version, ObjectStoreRef};
use crate::{
    table_state::DeltaTableState, DeltaDataTypeVersion, DeltaTable, DeltaTableError,
    DeltaTableMetaData,
};
use object_store::ObjectStore;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Cursor};

#[cfg(feature = "datafusion")]
use super::state::AddContainer;
#[cfg(feature = "datafusion")]
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};

/// Exceptions raised during commit conflict resolution
#[derive(thiserror::Error, Debug)]
pub enum CommitConflictError {
    /// This exception occurs when a concurrent operation adds files in the same partition
    /// (or anywhere in an un-partitioned table) that your operation reads. The file additions
    /// can be caused by INSERT, DELETE, UPDATE, or MERGE operations.
    #[error("Concurrent append failed.")]
    ConcurrentAppend,

    /// This exception occurs when a concurrent operation deleted a file that your operation read.
    /// Common causes are a DELETE, UPDATE, or MERGE operation that rewrites files.
    #[error("Concurrent delete-read failed.")]
    ConcurrentDeleteRead,

    /// This exception occurs when a concurrent operation deleted a file that your operation also deletes.
    /// This could be caused by two concurrent compaction operations rewriting the same files.
    #[error("Concurrent delete-delete failed.")]
    ConcurrentDeleteDelete,

    /// This exception occurs when a concurrent transaction updates the metadata of a Delta table.
    /// Common causes are ALTER TABLE operations or writes to your Delta table that update the schema of the table.
    #[error("Metadata changed since last commit.")]
    MetadataChanged,

    /// If a streaming query using the same checkpoint location is started multiple times concurrently
    /// and tries to write to the Delta table at the same time. You should never have two streaming
    /// queries use the same checkpoint location and run at the same time.
    #[error("Concurrent transaction failed.")]
    ConcurrentTransaction,

    /// This exception can occur in the following cases:
    /// - When your Delta table is upgraded to a new version. For future operations to succeed
    ///   you may need to upgrade your Delta Lake version.
    /// - When multiple writers are creating or replacing a table at the same time.
    /// - When multiple writers are writing to an empty path at the same time.
    #[error("Protocol changed since last commit.")]
    ProtocolChanged,

    /// Error returned when the table requires an unsupported writer version
    #[error("Delta-rs does not support writer version {0}")]
    UnsupportedWriterVersion(i32),

    /// Error returned when the table requires an unsupported writer version
    #[error("Delta-rs does not support reader version {0}")]
    UnsupportedReaderVersion(i32),

    /// Error returned when the snapshot has missing or corrupted data
    #[error("Snapshot is corrupted: {source}")]
    CorruptedState {
        /// Source error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error returned when evaluating predicate
    #[error("Error evaluating predicate: {source}")]
    Predicate {
        /// Source error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

/// A struct representing different attributes of current transaction needed for conflict detection.
pub(crate) struct TransactionInfo<'a> {
    pub(crate) txn_id: String,
    /// partition predicates by which files have been queried by the transaction
    pub(crate) read_predicates: Vec<String>,
    /// files that have been seen by the transaction
    pub(crate) read_files: HashSet<Add>,
    /// whether the whole table was read during the transaction
    pub(crate) read_whole_table: bool,
    /// appIds that have been seen by the transaction
    pub(crate) read_app_ids: HashSet<String>,
    /// delta log actions that the transaction wants to commit
    pub(crate) actions: Vec<Action>,
    /// read [`DeltaTableState`] used for the transaction
    pub(crate) read_snapshot: &'a DeltaTableState,
    /// [`CommitInfo`] for the commit
    pub(crate) commit_info: Option<CommitInfo>,
}

impl<'a> TransactionInfo<'a> {
    pub fn try_new(
        snapshot: &'a DeltaTableState,
        operation: &DeltaOperation,
    ) -> Result<Self, DeltaTableError> {
        Ok(Self {
            txn_id: "".into(),
            read_predicates: vec![],
            read_files: Default::default(),
            read_whole_table: true,
            read_app_ids: Default::default(),
            actions: vec![],
            read_snapshot: snapshot,
            commit_info: Some(operation.get_commit_info()),
        })
    }

    pub fn metadata(&self) -> Option<&DeltaTableMetaData> {
        self.read_snapshot.current_metadata()
    }

    pub fn metadata_changed(&self) -> bool {
        todo!()
    }

    pub fn final_actions_to_commit(&self) -> Vec<Action> {
        todo!()
    }
}

/// Summary of the Winning commit against which we want to check the conflict
pub(crate) struct WinningCommitSummary {
    pub actions: Vec<Action>,
    pub commit_info: Option<CommitInfo>,
}

impl WinningCommitSummary {
    pub fn new(actions: Vec<Action>, version: DeltaDataTypeVersion) -> Self {
        let commit_info = actions
            .iter()
            .find(|action| matches!(action, Action::commitInfo(_)))
            .map(|action| match action {
                Action::commitInfo(info) => {
                    let mut updated = info.clone();
                    updated.version = Some(version);
                    updated
                }
                _ => unreachable!(),
            });
        Self {
            actions,
            commit_info,
        }
    }

    pub fn metadata_updates(&self) -> Vec<MetaData> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::metaData(metadata) => Some(metadata),
                _ => None,
            })
            .collect()
    }

    pub fn app_level_transactions(&self) -> HashSet<String> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::txn(txn) => Some(txn.app_id),
                _ => None,
            })
            .collect()
    }

    pub fn protocol(&self) -> Vec<Protocol> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::protocol(protocol) => Some(protocol),
                _ => None,
            })
            .collect()
    }

    pub fn removed_files(&self) -> Vec<Remove> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::remove(remove) => Some(remove),
                _ => None,
            })
            .collect()
    }

    pub fn added_files(&self) -> Vec<Add> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::add(add) => Some(add),
                _ => None,
            })
            .collect()
    }

    pub fn blind_append_added_files(&self) -> Vec<Add> {
        if self.is_blind_append().unwrap_or(false) {
            self.added_files()
        } else {
            vec![]
        }
    }

    pub fn changed_data_added_files(&self) -> Vec<Add> {
        if self.is_blind_append().unwrap_or(false) {
            vec![]
        } else {
            self.added_files()
        }
    }

    pub fn only_add_files(&self) -> bool {
        !self
            .actions
            .iter()
            .any(|action| matches!(action, Action::remove(_)))
    }

    pub fn is_blind_append(&self) -> Option<bool> {
        self.commit_info
            .as_ref()
            .map(|opt| opt.is_blind_append.unwrap_or(false))
    }
}

pub(crate) struct ConflictChecker<'a> {
    /// transaction information for current transaction at start of check
    transaction_info: TransactionInfo<'a>,
    /// Version number of commit, that has been committed ahead of the current transaction
    winning_commit_version: DeltaDataTypeVersion,
    /// Summary of the transaction, that has been committed ahead of the current transaction
    winning_commit_summary: WinningCommitSummary,
    /// isolation level for the current transaction
    isolation_level: IsolationLevel,
    /// The state of the delta table at the base version from the current (not winning) commit
    snapshot: &'a DeltaTableState,
    /// The state of the delta table at the base version from the current (not winning) commit
    operation: DeltaOperation,
}

impl<'a> ConflictChecker<'a> {
    pub async fn try_new(
        snapshot: &'a DeltaTableState,
        object_store: ObjectStoreRef,
        winning_commit_version: DeltaDataTypeVersion,
        operation: DeltaOperation,
    ) -> Result<ConflictChecker<'_>, DeltaTableError> {
        // TODO raise proper error here or should we collect up so some max versions?
        assert!(winning_commit_version == snapshot.version() + 1);

        // create winning commit summary
        let commit_uri = commit_uri_from_version(winning_commit_version);
        let commit_log_bytes = object_store.get(&commit_uri).await?.bytes().await?;
        let reader = BufReader::new(Cursor::new(commit_log_bytes));
        let mut commit_actions = Vec::new();

        for maybe_line in reader.lines() {
            let line = maybe_line?;
            commit_actions.push(serde_json::from_str::<Action>(line.as_str()).map_err(|e| {
                DeltaTableError::InvalidJsonLog {
                    json_err: e,
                    version: winning_commit_version,
                    line,
                }
            })?);
        }
        let winning_commit_summary =
            WinningCommitSummary::new(commit_actions, winning_commit_version);

        let transaction_info = TransactionInfo::try_new(snapshot, &operation)?;

        Ok(Self {
            transaction_info,
            winning_commit_summary,
            winning_commit_version,
            isolation_level: IsolationLevel::Serializable,
            snapshot,
            operation,
        })
    }

    fn current_transaction_info(&self) -> &TransactionInfo {
        // TODO figure out when we need to update this
        &self.transaction_info
    }

    /// This function checks conflict of the `initial_current_transaction_info` against the
    /// `winning_commit_version` and returns an updated [`TransactionInfo`] that represents
    /// the transaction as if it had started while reading the `winning_commit_version`.
    pub fn check_conflicts(&self) -> Result<(), CommitConflictError> {
        self.check_protocol_compatibility()?;
        self.check_no_metadata_updates()?;
        self.check_for_added_files_that_should_have_been_read_by_current_txn()?;
        self.check_for_deleted_files_against_current_txn_read_files()?;
        self.check_for_deleted_files_against_current_txn_deleted_files()?;
        self.check_for_updated_application_transaction_ids_that_current_txn_depends_on()?;
        Ok(())
    }

    /// Asserts that the client is up to date with the protocol and is allowed
    /// to read and write against the protocol set by the committed transaction.
    fn check_protocol_compatibility(&self) -> Result<(), CommitConflictError> {
        for p in self.winning_commit_summary.protocol() {
            if self.snapshot.min_reader_version() < p.min_reader_version
                || self.snapshot.min_writer_version() < p.min_writer_version
            {
                return Err(CommitConflictError::ProtocolChanged);
            };
        }
        if !self.winning_commit_summary.protocol().is_empty()
            && self
                .current_transaction_info()
                .actions
                .iter()
                .any(|a| matches!(a, Action::protocol(_)))
        {
            return Err(CommitConflictError::ProtocolChanged);
        };
        Ok(())
    }

    /// Check if the committed transaction has changed metadata.
    fn check_no_metadata_updates(&self) -> Result<(), CommitConflictError> {
        // Fail if the metadata is different than what the txn read.
        if !self.winning_commit_summary.metadata_updates().is_empty() {
            Err(CommitConflictError::MetadataChanged)
        } else {
            Ok(())
        }
    }

    /// Check if the new files added by the already committed transactions
    /// should have been read by the current transaction.
    fn check_for_added_files_that_should_have_been_read_by_current_txn(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Fail if new files have been added that the txn should have read.
        let added_files_to_check = match self.isolation_level {
            IsolationLevel::WriteSerializable
                if !self.current_transaction_info().metadata_changed() =>
            {
                // don't conflict with blind appends
                self.winning_commit_summary.changed_data_added_files()
            }
            IsolationLevel::Serializable | IsolationLevel::WriteSerializable => {
                let mut files = self.winning_commit_summary.changed_data_added_files();
                files.extend(self.winning_commit_summary.blind_append_added_files());
                files
            }
            IsolationLevel::SnapshotIsolation => vec![],
        };

        // Here we need to check if the current transaction would have read the
        // added files. for this we need to be able to evaluate predicates. Err on the safe side is
        // to assume all files match
        cfg_if::cfg_if! {
            if #[cfg(feature = "datafusion")] {
                let added_files_matching_predicates =
                    if let Some(predicate_str) = self.operation.read_predicate() {
                        let arrow_schema =
                            self.transaction_info
                                .read_snapshot
                                .arrow_schema()
                                .map_err(|err| CommitConflictError::CorruptedState {
                                    source: Box::new(err),
                                })?;
                        let predicate = self
                            .transaction_info
                            .read_snapshot
                            .parse_predicate_expression(predicate_str)
                            .map_err(|err| CommitConflictError::Predicate {
                                source: Box::new(err),
                            })?;
                        AddContainer::new(&added_files_to_check, arrow_schema)
                            .predicate_matches(&[predicate])
                            .map_err(|err| CommitConflictError::Predicate {
                                source: Box::new(err),
                            })?
                            .cloned()
                            .collect::<Vec<_>>()
                    } else {
                        added_files_to_check
                    };
            } else {
                let added_files_matching_predicates = added_files_to_check;
            }
        }

        if !added_files_matching_predicates.is_empty() {
            Err(CommitConflictError::ConcurrentAppend)
        } else {
            Ok(())
        }
    }

    /// Check if [Remove] actions added by already committed transactions
    /// conflicts with files read by the current transaction.
    fn check_for_deleted_files_against_current_txn_read_files(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Fail if files have been deleted that the txn read.
        let read_file_path: HashSet<String> = self
            .current_transaction_info()
            .read_files
            .iter()
            .map(|f| f.path.clone())
            .collect();
        let deleted_read_overlap = self
            .winning_commit_summary
            .removed_files()
            .iter()
            // TODO remove cloned
            .cloned()
            .find(|f| read_file_path.contains(&f.path));
        if deleted_read_overlap.is_some()
            || (!self.winning_commit_summary.removed_files().is_empty()
                && self.current_transaction_info().read_whole_table)
        {
            Err(CommitConflictError::ConcurrentDeleteRead)
        } else {
            Ok(())
        }
    }

    /// Check if [Remove] actions added by already committed transactions conflicts
    /// with [Remove] actions this transaction is trying to add.
    fn check_for_deleted_files_against_current_txn_deleted_files(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Fail if a file is deleted twice.
        let txn_deleted_files: HashSet<String> = self
            .current_transaction_info()
            .actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::remove(remove) => Some(remove.path),
                _ => None,
            })
            .collect();
        let winning_deleted_files: HashSet<String> = self
            .winning_commit_summary
            .removed_files()
            .iter()
            .cloned()
            .map(|r| r.path)
            .collect();
        let intersection: HashSet<&String> = txn_deleted_files
            .intersection(&winning_deleted_files)
            .collect();
        if !intersection.is_empty() {
            Err(CommitConflictError::ConcurrentDeleteDelete)
        } else {
            Ok(())
        }
    }

    /// Checks if the winning transaction corresponds to some AppId on which
    /// current transaction also depends.
    fn check_for_updated_application_transaction_ids_that_current_txn_depends_on(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Fail if the appIds seen by the current transaction has been updated by the winning
        // transaction i.e. the winning transaction have [Txn] corresponding to
        // some appId on which current transaction depends on. Example - This can happen when
        // multiple instances of the same streaming query are running at the same time.
        let winning_txns = self.winning_commit_summary.app_level_transactions();
        let txn_overlap: HashSet<&String> = winning_txns
            .intersection(&self.current_transaction_info().read_app_ids)
            .collect();
        if !txn_overlap.is_empty() {
            Err(CommitConflictError::ConcurrentTransaction)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_utils::{
        create_add_action, create_initialized_table, init_table_actions,
    };
    use super::*;
    use crate::action::{Action, SaveMode};
    use crate::operations::transaction::commit;

    #[tokio::test]
    async fn test_append_only_commits() {
        let table = create_initialized_table(&[], None).await;

        let commit_info = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: Default::default(),
            predicate: None,
        }
        .get_commit_info();

        let add = create_add_action("file-path", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":10},\"nullCount\":{\"value\":0}}"));
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: Default::default(),
            predicate: None,
        };

        commit(
            table.object_store().as_ref(),
            1,
            vec![Action::add(add)],
            operation.clone(),
            Some(0),
            None,
        )
        .await
        .unwrap();

        let checker = ConflictChecker::try_new(&table.state, table.object_store(), 1, operation)
            .await
            .unwrap();

        println!("actions: {:?}", checker.winning_commit_summary.actions);

        let result = checker.check_conflicts();
        println!("result: {:?}", result);
    }
}
