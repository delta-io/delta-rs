//! Helper module to check if a transaction can be committed in case of conflicting commits.
use crate::action::{Action, Add, MetaData, Protocol, Remove};
use crate::{
    table_state::DeltaTableState, DeltaDataTypeTimestamp, DeltaDataTypeVersion, DeltaTable,
    DeltaTableError, DeltaTableMetaData,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

/// The commitInfo is a fairly flexible action within the delta specification, where arbitrary data can be stored.
/// However the reference implementation as well as delta-rs store useful information that may for instance
/// allow us to be more permissive in commit conflict resolution.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommitInfo {
    pub version: Option<DeltaDataTypeVersion>,
    pub timestamp: DeltaDataTypeTimestamp,
    pub user_id: Option<String>,
    pub user_name: Option<String>,
    pub operation: String,
    pub operation_parameters: HashMap<String, String>,
    pub read_version: Option<i64>,
    pub isolation_level: Option<IsolationLevel>,
    pub is_blind_append: Option<bool>,
}

/// Exceptions raised during commit conflict resolution
#[derive(thiserror::Error, Debug)]
pub enum CommitConflictError {
    /// This exception occurs when a concurrent operation adds files in the same partition
    /// (or anywhere in an unpartitioned table) that your operation reads. The file additions
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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table’s history.
    Serializable,

    /// A weaker isolation level than Serializable. It ensures only that the write
    /// operations (that is, not reads) are serializable. However, this is still stronger
    /// than Snapshot isolation. WriteSerializable is the default isolation level because
    /// it provides great balance of data consistency and availability for most common operations.
    WriteSerializable,

    SnapshotIsolation,
}

/// A struct representing different attributes of current transaction needed for conflict detection.
pub(crate) struct CurrentTransactionInfo {
    txn_id: String,
    /// partition predicates by which files have been queried by the transaction
    read_predicates: Vec<String>,
    /// files that have been seen by the transaction
    read_files: HashSet<Add>,
    /// whether the whole table was read during the transaction
    read_whole_table: bool,
    /// appIds that have been seen by the transaction
    read_app_ids: HashSet<String>,
    /// table metadata for the transaction
    metadata: DeltaTableMetaData,
    /// delta log actions that the transaction wants to commit
    actions: Vec<Action>,
    /// read [[Snapshot]] used for the transaction
    // read_snapshot: Snapshot,
    /// [CommitInfo] for the commit
    commit_info: Option<Map<String, Value>>,
}

impl CurrentTransactionInfo {
    pub fn new() -> Self {
        todo!()
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
                    // TODO remove panic
                    let mut ci = serde_json::from_value::<CommitInfo>(serde_json::Value::Object(
                        info.clone(),
                    ))
                    .unwrap();
                    ci.version = Some(version);
                    ci
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
    delta_table: &'a mut DeltaTable,
    initial_current_transaction_info: CurrentTransactionInfo,
    winning_commit_version: DeltaDataTypeVersion,
    /// Summary of the transaction, that has been committed ahead of the current transaction
    winning_commit_summary: WinningCommitSummary,
    /// isolation level for the current transaction
    isolation_level: IsolationLevel,
    /// The state of the delta table at the base version from the current (not winning) commit
    state: DeltaTableState,
}

impl<'a> ConflictChecker<'a> {
    pub async fn try_new(
        table: &'a mut DeltaTable,
        winning_commit_version: DeltaDataTypeVersion,
    ) -> Result<ConflictChecker<'a>, DeltaTableError> {
        // TODO raise proper error here
        assert!(winning_commit_version == table.version() + 1);
        let next_state = DeltaTableState::from_commit(table, winning_commit_version).await?;
        let mut new_state = table.get_state().clone();
        new_state.merge(next_state, true, true);
        todo!()
        // Self {}
    }

    fn current_transaction_info(&self) -> &CurrentTransactionInfo {
        // TODO figure out when we need to update this
        &self.initial_current_transaction_info
    }
    /// This function checks conflict of the `initial_current_transaction_info` against the
    /// `winning_commit_version` and returns an updated [CurrentTransactionInfo] that represents
    /// the transaction as if it had started while reading the `winning_commit_version`.
    pub fn check_conflicts(&self) -> Result<CurrentTransactionInfo, CommitConflictError> {
        self.check_protocol_compatibility()?;
        self.check_no_metadata_updates()?;
        self.check_for_added_files_that_should_have_been_read_by_current_txn()?;
        self.check_for_deleted_files_against_current_txn_read_files()?;
        self.check_for_deleted_files_against_current_txn_deleted_files()?;
        self.check_for_updated_application_transaction_ids_that_current_txn_depends_on()?;
        todo!()
    }

    /// Asserts that the client is up to date with the protocol and is allowed
    /// to read and write against the protocol set by the committed transaction.
    fn check_protocol_compatibility(&self) -> Result<(), CommitConflictError> {
        for p in self.winning_commit_summary.protocol() {
            if self.state.min_reader_version() < p.min_reader_version
                || self.state.min_writer_version() < p.min_writer_version
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
        // TODO here we need to check if the current transaction would have read the
        // added files. for this we need to be able to evaluate predicates. Err on the safe side is
        // to assume all files match
        let added_files_matching_predicates = added_files_to_check;
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
mod tests {}
