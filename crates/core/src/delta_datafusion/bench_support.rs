#![doc(hidden)]

//! Diagnostic only, unstable bridge for file selection internals.
//! This is not a supported public API.

use datafusion::catalog::Session;
use datafusion::common::{HashSet, Result as DataFusionResult};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::errors::DeltaResult;
use crate::kernel::{Add, EagerSnapshot};
use crate::logstore::LogStoreRef;

use super::DeltaSessionExt as _;

/// Wrapper result for the internal file-selection entrypoint.
pub struct FindFilesResult {
    pub candidates: Vec<Add>,
    pub partition_scan: bool,
}

impl From<super::FindFiles> for FindFilesResult {
    fn from(result: super::FindFiles) -> Self {
        Self {
            candidates: result.candidates,
            partition_scan: result.partition_scan,
        }
    }
}

/// Minimal wrapper around the internal matched-files scan plan.
pub struct MatchedFilesScan(super::MatchedFilesScan);

impl MatchedFilesScan {
    pub fn scan(&self) -> &LogicalPlan {
        self.0.scan()
    }

    pub fn files_set(&self) -> HashSet<String> {
        self.0.files_set()
    }

    pub fn predicate(&self) -> &Expr {
        &self.0.predicate
    }
}

fn prepare_session(session: &dyn Session, log_store: &LogStoreRef) -> DeltaResult<()> {
    super::update_datafusion_session(session, log_store.as_ref(), None)?;
    session.ensure_log_store_registered(log_store.as_ref())?;
    Ok(())
}

pub async fn find_files(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    predicate: Option<Expr>,
) -> DeltaResult<FindFilesResult> {
    prepare_session(session, &log_store)?;
    super::find_files(snapshot, log_store, session, predicate)
        .await
        .map(Into::into)
}

pub async fn find_files_scan(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    predicate: Expr,
) -> DeltaResult<Vec<Add>> {
    prepare_session(session, &log_store)?;
    super::find_files_scan(snapshot, log_store, session, predicate).await
}

pub async fn scan_files_where_matches(
    session: &dyn Session,
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    predicate: Expr,
) -> DataFusionResult<Option<MatchedFilesScan>> {
    prepare_session(session, &log_store)?;
    super::scan_files_where_matches(session, snapshot, predicate)
        .await
        .map(|scan| scan.map(MatchedFilesScan))
}

pub fn add_actions_partition_mem_table(snapshot: &EagerSnapshot) -> DeltaResult<Option<MemTable>> {
    super::add_actions_partition_mem_table(snapshot)
}
