//! Helper module to check if a transaction can be committed in case of conflicting commits.
use std::collections::HashSet;

use super::CommitInfo;
#[cfg(feature = "datafusion")]
use crate::delta_datafusion::DataFusionMixins;
use crate::errors::DeltaResult;
use crate::kernel::EagerSnapshot;
use crate::kernel::Transaction;
use crate::kernel::{Action, Add, Metadata, Protocol, Remove};
use crate::logstore::{get_actions, LogStore};
use crate::protocol::DeltaOperation;
use crate::table::config::IsolationLevel;
use crate::DeltaTableError;

#[cfg(feature = "datafusion")]
use super::state::AddContainer;
#[cfg(feature = "datafusion")]
use datafusion_expr::Expr;
#[cfg(feature = "datafusion")]
use itertools::Either;

/// Exceptions raised during commit conflict resolution
#[derive(thiserror::Error, Debug)]
pub enum CommitConflictError {
    /// This exception occurs when a concurrent operation adds files in the same partition
    /// (or anywhere in an un-partitioned table) that your operation reads. The file additions
    /// can be caused by INSERT, DELETE, UPDATE, or MERGE operations.
    #[error("Commit failed: a concurrent transactions added new data.\nHelp: This transaction's query must be rerun to include the new data. Also, if you don't care to require this check to pass in the future, the isolation level can be set to Snapshot Isolation.")]
    ConcurrentAppend,

    /// This exception occurs when a concurrent operation deleted a file that your operation read.
    /// Common causes are a DELETE, UPDATE, or MERGE operation that rewrites files.
    #[error("Commit failed: a concurrent transaction deleted data this operation read.\nHelp: This transaction's query must be rerun to exclude the removed data. Also, if you don't care to require this check to pass in the future, the isolation level can be set to Snapshot Isolation.")]
    ConcurrentDeleteRead,

    /// This exception occurs when a concurrent operation deleted a file that your operation also deletes.
    /// This could be caused by two concurrent compaction operations rewriting the same files.
    #[error("Commit failed: a concurrent transaction deleted the same data your transaction deletes.\nHelp: you should retry this write operation. If it was based on data contained in the table, you should rerun the query generating the data.")]
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
    #[error("Protocol changed since last commit: {0}")]
    ProtocolChanged(String),

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

    /// Error returned when no metadata was found in the DeltaTable.
    #[error("No metadata found, please make sure table is loaded.")]
    NoMetadata,
}

/// A struct representing different attributes of current transaction needed for conflict detection.
#[allow(unused)]
pub(crate) struct TransactionInfo<'a> {
    txn_id: String,
    /// partition predicates by which files have been queried by the transaction
    ///
    /// If any new data files or removed data files match this predicate, the
    /// transaction should fail.
    #[cfg(not(feature = "datafusion"))]
    read_predicates: Option<String>,
    /// partition predicates by which files have been queried by the transaction
    #[cfg(feature = "datafusion")]
    read_predicates: Option<Expr>,
    /// appIds that have been seen by the transaction
    pub(crate) read_app_ids: HashSet<String>,
    /// delta log actions that the transaction wants to commit
    actions: &'a [Action],
    /// read [`DeltaTableState`] used for the transaction
    pub(crate) read_snapshot: &'a EagerSnapshot,
    /// Whether the transaction tainted the whole table
    read_whole_table: bool,
}

impl<'a> TransactionInfo<'a> {
    #[cfg(feature = "datafusion")]
    pub fn try_new(
        read_snapshot: &'a EagerSnapshot,
        read_predicates: Option<String>,
        actions: &'a [Action],
        read_whole_table: bool,
    ) -> DeltaResult<Self> {
        use datafusion::prelude::SessionContext;

        let session = SessionContext::new();
        let read_predicates = read_predicates
            .map(|pred| read_snapshot.parse_predicate_expression(pred, &session.state()))
            .transpose()?;

        let mut read_app_ids = HashSet::<String>::new();
        for action in actions.iter() {
            if let Action::Txn(Transaction { app_id, .. }) = action {
                read_app_ids.insert(app_id.clone());
            }
        }

        Ok(Self {
            txn_id: "".into(),
            read_predicates,
            read_app_ids,
            actions,
            read_snapshot,
            read_whole_table,
        })
    }

    #[cfg(feature = "datafusion")]
    #[allow(unused)]
    pub fn new(
        read_snapshot: &'a EagerSnapshot,
        read_predicates: Option<Expr>,
        actions: &'a Vec<Action>,
        read_whole_table: bool,
    ) -> Self {
        let mut read_app_ids = HashSet::<String>::new();
        for action in actions.iter() {
            if let Action::Txn(Transaction { app_id, .. }) = action {
                read_app_ids.insert(app_id.clone());
            }
        }
        Self {
            txn_id: "".into(),
            read_predicates,
            read_app_ids,
            actions,
            read_snapshot,
            read_whole_table,
        }
    }

    #[cfg(not(feature = "datafusion"))]
    pub fn try_new(
        read_snapshot: &'a EagerSnapshot,
        read_predicates: Option<String>,
        actions: &'a Vec<Action>,
        read_whole_table: bool,
    ) -> DeltaResult<Self> {
        let mut read_app_ids = HashSet::<String>::new();
        for action in actions.iter() {
            if let Action::Txn(Transaction { app_id, .. }) = action {
                read_app_ids.insert(app_id.clone());
            }
        }
        Ok(Self {
            txn_id: "".into(),
            read_predicates,
            read_app_ids,
            actions,
            read_snapshot,
            read_whole_table,
        })
    }

    /// Whether the transaction changed the tables metadatas
    pub fn metadata_changed(&self) -> bool {
        self.actions
            .iter()
            .any(|a| matches!(a, Action::Metadata(_)))
    }

    #[cfg(feature = "datafusion")]
    /// Files read by the transaction
    pub fn read_files(&self) -> Result<impl Iterator<Item = Add> + '_, CommitConflictError> {
        use crate::delta_datafusion::files_matching_predicate;

        if let Some(predicate) = &self.read_predicates {
            Ok(Either::Left(
                files_matching_predicate(self.read_snapshot, &[predicate.clone()]).map_err(
                    |err| CommitConflictError::Predicate {
                        source: Box::new(err),
                    },
                )?,
            ))
        } else {
            Ok(Either::Right(std::iter::empty()))
        }
    }

    #[cfg(not(feature = "datafusion"))]
    /// Files read by the transaction
    pub fn read_files(&self) -> Result<impl Iterator<Item = Add> + '_, CommitConflictError> {
        Ok(self.read_snapshot.file_actions().unwrap())
    }

    /// Whether the whole table was read during the transaction
    pub fn read_whole_table(&self) -> bool {
        self.read_whole_table
    }
}

/// Summary of the Winning commit against which we want to check the conflict
#[derive(Debug)]
pub(crate) struct WinningCommitSummary {
    pub actions: Vec<Action>,
    pub commit_info: Option<CommitInfo>,
}

impl WinningCommitSummary {
    pub async fn try_new(
        log_store: &dyn LogStore,
        read_version: i64,
        winning_commit_version: i64,
    ) -> DeltaResult<Self> {
        // NOTE using assert, since a wrong version would right now mean a bug in our code.
        assert_eq!(winning_commit_version, read_version + 1);

        let commit_log_bytes = log_store.read_commit_entry(winning_commit_version).await?;
        match commit_log_bytes {
            Some(bytes) => {
                let actions = get_actions(winning_commit_version, bytes).await?;
                let commit_info = actions
                    .iter()
                    .find(|action| matches!(action, Action::CommitInfo(_)))
                    .map(|action| match action {
                        Action::CommitInfo(info) => info.clone(),
                        _ => unreachable!(),
                    });

                Ok(Self {
                    actions,
                    commit_info,
                })
            }
            None => Err(DeltaTableError::InvalidVersion(winning_commit_version)),
        }
    }

    pub fn metadata_updates(&self) -> Vec<Metadata> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Metadata(metadata) => Some(metadata),
                _ => None,
            })
            .collect()
    }

    pub fn app_level_transactions(&self) -> HashSet<String> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Txn(txn) => Some(txn.app_id),
                _ => None,
            })
            .collect()
    }

    pub fn protocol(&self) -> Vec<Protocol> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Protocol(protocol) => Some(protocol),
                _ => None,
            })
            .collect()
    }

    pub fn removed_files(&self) -> Vec<Remove> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Remove(remove) => Some(remove),
                _ => None,
            })
            .collect()
    }

    pub fn added_files(&self) -> Vec<Add> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Add(add) => Some(add),
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

    pub fn is_blind_append(&self) -> Option<bool> {
        self.commit_info
            .as_ref()
            .map(|opt| opt.is_blind_append.unwrap_or(false))
    }
}

/// Checks if a failed commit may be committed after a conflicting winning commit
pub(crate) struct ConflictChecker<'a> {
    /// transaction information for current transaction at start of check
    txn_info: TransactionInfo<'a>,
    /// Summary of the transaction, that has been committed ahead of the current transaction
    winning_commit_summary: WinningCommitSummary,
    /// Isolation level for the current transaction
    isolation_level: IsolationLevel,
}

impl<'a> ConflictChecker<'a> {
    pub fn new(
        transaction_info: TransactionInfo<'a>,
        winning_commit_summary: WinningCommitSummary,
        operation: Option<&DeltaOperation>,
    ) -> ConflictChecker<'a> {
        let isolation_level = operation
            .and_then(|op| {
                if can_downgrade_to_snapshot_isolation(
                    &winning_commit_summary.actions,
                    op,
                    &transaction_info
                        .read_snapshot
                        .table_config()
                        .isolation_level(),
                ) {
                    Some(IsolationLevel::SnapshotIsolation)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                transaction_info
                    .read_snapshot
                    .table_config()
                    .isolation_level()
            });

        Self {
            txn_info: transaction_info,
            winning_commit_summary,
            isolation_level,
        }
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
            let (win_read, curr_read) = (
                p.min_reader_version,
                self.txn_info.read_snapshot.protocol().min_reader_version,
            );
            let (win_write, curr_write) = (
                p.min_writer_version,
                self.txn_info.read_snapshot.protocol().min_writer_version,
            );
            if curr_read < win_read || win_write < curr_write {
                return Err(CommitConflictError::ProtocolChanged(
                    format!("reqired read/write {win_read}/{win_write}, current read/write {curr_read}/{curr_write}"),
                ));
            };
        }
        if !self.winning_commit_summary.protocol().is_empty()
            && self
                .txn_info
                .actions
                .iter()
                .any(|a| matches!(a, Action::Protocol(_)))
        {
            return Err(CommitConflictError::ProtocolChanged(
                "protocol changed".into(),
            ));
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
        // Skip check, if the operation can be downgraded to snapshot isolation
        if matches!(self.isolation_level, IsolationLevel::SnapshotIsolation) {
            return Ok(());
        }

        // Fail if new files have been added that the txn should have read.
        let added_files_to_check = match self.isolation_level {
            IsolationLevel::WriteSerializable if !self.txn_info.metadata_changed() => {
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
                let added_files_matching_predicates = if let (Some(predicate), false) = (
                    &self.txn_info.read_predicates,
                    self.txn_info.read_whole_table(),
                ) {
                    let arrow_schema = self.txn_info.read_snapshot.arrow_schema().map_err(|err| {
                        CommitConflictError::CorruptedState {
                            source: Box::new(err),
                        }
                    })?;
                    let partition_columns = &self
                        .txn_info
                        .read_snapshot
                        .metadata()
                        .partition_columns;
                    AddContainer::new(&added_files_to_check, partition_columns, arrow_schema)
                        .predicate_matches(predicate.clone())
                        .map_err(|err| CommitConflictError::Predicate {
                            source: Box::new(err),
                        })?
                        .cloned()
                        .collect::<Vec<_>>()
                } else if self.txn_info.read_whole_table() {
                    added_files_to_check
                } else {
                    vec![]
                };
            } else {
                let added_files_matching_predicates = if self.txn_info.read_whole_table()
                {
                    added_files_to_check
                } else {
                    vec![]
                };
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
            .txn_info
            .read_files()?
            .map(|f| f.path.clone())
            .collect();
        let deleted_read_overlap = self
            .winning_commit_summary
            .removed_files()
            .iter()
            .find(|&f| read_file_path.contains(&f.path))
            .cloned();
        if deleted_read_overlap.is_some()
            || (!self.winning_commit_summary.removed_files().is_empty()
                && self.txn_info.read_whole_table())
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
            .txn_info
            .actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Remove(remove) => Some(remove.path),
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
            .intersection(&self.txn_info.read_app_ids)
            .collect();
        if !txn_overlap.is_empty() {
            Err(CommitConflictError::ConcurrentTransaction)
        } else {
            Ok(())
        }
    }
}

// implementation and comments adopted from
// https://github.com/delta-io/delta/blob/1c18c1d972e37d314711b3a485e6fb7c98fce96d/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1268
//
// For no-data-change transactions such as OPTIMIZE/Auto Compaction/ZorderBY, we can
// change the isolation level to SnapshotIsolation. SnapshotIsolation allows reduced conflict
// detection by skipping the
// [ConflictChecker::check_for_added_files_that_should_have_been_read_by_current_txn] check i.e.
// don't worry about concurrent appends.
//
// We can also use SnapshotIsolation for empty transactions. e.g. consider a commit:
// t0 - Initial state of table
// t1 - Q1, Q2 starts
// t2 - Q1 commits
// t3 - Q2 is empty and wants to commit.
// In this scenario, we can always allow Q2 to commit without worrying about new files
// generated by Q1.
//
// The final order which satisfies both Serializability and WriteSerializability is: Q2, Q1
// Note that Metadata only update transactions shouldn't be considered empty. If Q2 above has
// a Metadata update (say schema change/identity column high watermark update), then Q2 can't
// be moved above Q1 in the final SERIALIZABLE order. This is because if Q2 is moved above Q1,
// then Q1 should see the updates from Q2 - which actually didn't happen.
pub(super) fn can_downgrade_to_snapshot_isolation<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
    operation: &DeltaOperation,
    isolation_level: &IsolationLevel,
) -> bool {
    let mut data_changed = false;
    let mut has_non_file_actions = false;
    for action in actions {
        match action {
            Action::Add(act) if act.data_change => data_changed = true,
            Action::Remove(rem) if rem.data_change => data_changed = true,
            _ => has_non_file_actions = true,
        }
    }

    if has_non_file_actions {
        // if Non-file-actions are present (e.g. METADATA etc.), then don't downgrade the isolation level.
        return false;
    }

    match isolation_level {
        IsolationLevel::Serializable => !data_changed,
        IsolationLevel::WriteSerializable => !data_changed && !operation.changes_data(),
        IsolationLevel::SnapshotIsolation => false, // this case should never happen, since spanpshot isolation canot be configured on table
    }
}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::collections::HashMap;

    #[cfg(feature = "datafusion")]
    use datafusion_expr::{col, lit};
    use serde_json::json;

    use super::*;
    use crate::kernel::Action;
    use crate::test_utils::{ActionFactory, TestSchemas};

    fn simple_add(data_change: bool, min: &str, max: &str) -> Add {
        ActionFactory::add(
            TestSchemas::simple(),
            HashMap::from_iter([("value", (min, max))]),
            Default::default(),
            true,
        )
    }

    fn init_table_actions() -> Vec<Action> {
        vec![
            ActionFactory::protocol(None, None, None::<Vec<_>>, None::<Vec<_>>).into(),
            ActionFactory::metadata(TestSchemas::simple(), None::<Vec<&str>>, None).into(),
        ]
    }

    #[test]
    fn test_can_downgrade_to_snapshot_isolation() {
        let isolation = IsolationLevel::WriteSerializable;
        let operation = DeltaOperation::Optimize {
            predicate: None,
            target_size: 0,
        };
        let add =
            ActionFactory::add(TestSchemas::simple(), HashMap::new(), Vec::new(), true).into();
        let res = can_downgrade_to_snapshot_isolation(&[add], &operation, &isolation);
        assert!(!res)
    }

    // Check whether the test transaction conflict with the concurrent writes by executing the
    // given params in the following order:
    // - setup (including setting table isolation level
    // - reads
    // - concurrentWrites
    // - actions
    #[cfg(feature = "datafusion")]
    fn execute_test(
        setup: Option<Vec<Action>>,
        reads: Option<Expr>,
        concurrent: Vec<Action>,
        actions: Vec<Action>,
        read_whole_table: bool,
    ) -> Result<(), CommitConflictError> {
        use crate::table::state::DeltaTableState;

        let setup_actions = setup.unwrap_or_else(init_table_actions);
        let state = DeltaTableState::from_actions(setup_actions).unwrap();
        let snapshot = state.snapshot();
        let transaction_info = TransactionInfo::new(snapshot, reads, &actions, read_whole_table);
        let summary = WinningCommitSummary {
            actions: concurrent,
            commit_info: None,
        };
        let checker = ConflictChecker::new(transaction_info, summary, None);
        checker.check_conflicts()
    }

    #[tokio::test]
    #[cfg(feature = "datafusion")]
    // tests adopted from https://github.com/delta-io/delta/blob/24c025128612a4ae02d0ad958621f928cda9a3ec/core/src/test/scala/org/apache/spark/sql/delta/OptimisticTransactionSuite.scala#L40-L94
    async fn test_allowed_concurrent_actions() {
        // append - append
        // append file to table while a concurrent writer also appends a file
        let file1 = simple_add(true, "1", "10").into();
        let file2 = simple_add(true, "1", "10").into();

        let result = execute_test(None, None, vec![file1], vec![file2], false);
        assert!(result.is_ok());

        // disjoint delete - read
        // the concurrent transaction deletes a file that the current transaction did NOT read
        let file_not_read = simple_add(true, "1", "10");
        let file_read = simple_add(true, "100", "10000").into();
        let mut setup_actions = init_table_actions();
        setup_actions.push(file_not_read.clone().into());
        setup_actions.push(file_read);
        let result = execute_test(
            Some(setup_actions),
            Some(col("value").gt(lit::<i32>(10))),
            vec![ActionFactory::remove(&file_not_read, true).into()],
            vec![],
            false,
        );
        assert!(result.is_ok());

        // disjoint add - read
        // concurrently add file, that the current transaction would not have read
        let file_added = simple_add(true, "1", "10").into();
        let file_read = simple_add(true, "100", "10000").into();
        let mut setup_actions = init_table_actions();
        setup_actions.push(file_read);
        let result = execute_test(
            Some(setup_actions),
            Some(col("value").gt(lit::<i32>(10))),
            vec![file_added],
            vec![],
            false,
        );
        assert!(result.is_ok());

        // TODO enable test once we have isolation level downcast
        // add / read + no write
        // transaction would have read file added by concurrent txn, but does not write data,
        // so no real conflicting change even though data was added
        // let file_added = tu::create_add_action("file_added", true, get_stats(1, 10));
        // let result = execute_test(
        //     None,
        //     Some(col("value").gt(lit::<i32>(5))),
        //     vec![file_added],
        //     vec![],
        //     false,
        // );
        // assert!(result.is_ok());

        // TODO disjoint transactions
    }

    #[tokio::test]
    #[cfg(feature = "datafusion")]
    // tests adopted from https://github.com/delta-io/delta/blob/24c025128612a4ae02d0ad958621f928cda9a3ec/core/src/test/scala/org/apache/spark/sql/delta/OptimisticTransactionSuite.scala#L40-L94
    async fn test_disallowed_concurrent_actions() {
        // delete - delete
        // remove file from table that has previously been removed
        let removed_file = simple_add(true, "1", "10");
        let removed_file: Action = ActionFactory::remove(&removed_file, true).into();
        let result = execute_test(
            None,
            None,
            vec![removed_file.clone()],
            vec![removed_file],
            false,
        );
        assert!(matches!(
            result,
            Err(CommitConflictError::ConcurrentDeleteDelete)
        ));

        // add / read + write
        // a file is concurrently added that should have been read by the current transaction
        let file_added = simple_add(true, "1", "10").into();
        let file_should_have_read = simple_add(true, "1", "10").into();
        let result = execute_test(
            None,
            Some(col("value").lt_eq(lit::<i32>(10))),
            vec![file_should_have_read],
            vec![file_added],
            false,
        );
        assert!(matches!(result, Err(CommitConflictError::ConcurrentAppend)));

        // delete / read
        // transaction reads a file that is removed by concurrent transaction
        let file_read = simple_add(true, "1", "10");
        let mut setup_actions = init_table_actions();
        setup_actions.push(file_read.clone().into());
        let result = execute_test(
            Some(setup_actions),
            Some(col("value").lt_eq(lit::<i32>(10))),
            vec![ActionFactory::remove(&file_read, true).into()],
            vec![],
            false,
        );
        assert!(matches!(
            result,
            Err(CommitConflictError::ConcurrentDeleteRead)
        ));

        // schema change
        // concurrent transactions changes table metadata
        let result = execute_test(
            None,
            None,
            vec![ActionFactory::metadata(TestSchemas::simple(), None::<Vec<&str>>, None).into()],
            vec![],
            false,
        );
        assert!(matches!(result, Err(CommitConflictError::MetadataChanged)));

        // upgrade / upgrade
        // current and concurrent transactions change the protocol version
        let result = execute_test(
            None,
            None,
            vec![ActionFactory::protocol(None, None, None::<Vec<_>>, None::<Vec<_>>).into()],
            vec![ActionFactory::protocol(None, None, None::<Vec<_>>, None::<Vec<_>>).into()],
            false,
        );
        assert!(matches!(
            result,
            Err(CommitConflictError::ProtocolChanged(_))
        ));

        // taint whole table
        // `read_whole_table` should disallow any concurrent change, even if the change
        // is disjoint with the earlier filter
        let file_part1 = simple_add(true, "1", "10").into();
        let file_part2 = simple_add(true, "11", "100").into();
        let file_part3 = simple_add(true, "101", "1000").into();
        let mut setup_actions = init_table_actions();
        setup_actions.push(file_part1);
        let result = execute_test(
            Some(setup_actions),
            // filter matches neither exisiting nor added files
            Some(col("value").lt(lit::<i32>(0))),
            vec![file_part2],
            vec![file_part3],
            true,
        );
        assert!(matches!(result, Err(CommitConflictError::ConcurrentAppend)));

        // taint whole table + concurrent remove
        // `read_whole_table` should disallow any concurrent remove actions
        let file_part1 = simple_add(true, "1", "10");
        let file_part2 = simple_add(true, "11", "100").into();
        let mut setup_actions = init_table_actions();
        setup_actions.push(file_part1.clone().into());
        let result = execute_test(
            Some(setup_actions),
            None,
            vec![ActionFactory::remove(&file_part1, true).into()],
            vec![file_part2],
            true,
        );
        assert!(matches!(
            result,
            Err(CommitConflictError::ConcurrentDeleteRead)
        ));

        // TODO "add in part=2 / read from part=1,2 and write to part=1"

        // TODO conflicting txns
    }
}
