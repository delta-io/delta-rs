//! Branch-scoped commit authority and write-set lifecycle for LakeFS.
//!
//! [`LakeFSLogStore::begin_operation`](crate::logstore::LakeFSLogStore) creates a hidden
//! transaction branch and hands core an [`OperationContext`] built from the types in this module:
//!
//! - [`LakeFSBranchCommitter`] commits one Delta version: it puts `N.json` on the branch, makes a
//!   LakeFS commit of the branch and squash-merges the branch into the source branch. A merge
//!   conflict is reported as [`CommitResponse::Conflict`] so that core's commit loop can retry.
//! - [`LakeFSTransaction`] publishes file-only work (checkpoints, log cleanup, vacuum deletes)
//!   when the scope finishes and deletes the branch when the scope finishes or aborts.
//! - [`LakeFSSourceCommitter`] is the commit authority of the unscoped store: a conditional put of
//!   `N.json` on the source branch followed by a LakeFS commit of the source branch.
//!
//! [`OperationContext`]: deltalake_core::logstore::OperationContext

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use deltalake_core::kernel::Version;
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::logstore::{
    CommitOrBytes, CommitResponse, CommitStrategy, Committer, FileSystemCommitter,
    OperationTransaction, PayloadKind, commit_uri_from_version,
};
use deltalake_core::{DeltaResult, DeltaTableError};
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt as _};
use tracing::{debug, warn};

use crate::client::{LakeFSClient, LakeFSLocation, MergeError};
use crate::errors::LakeFSOperationError;

/// How often a merge into a dirty destination branch is retried before the typed
/// [`LakeFSOperationError::DirtyBranch`] error is returned. Another delta-rs writer that commits
/// through the unscoped path leaves the source branch dirty for a moment, so a short retry is
/// worth it; a branch that stays dirty needs operator attention.
pub const DIRTY_BRANCH_RETRIES: usize = 5;

/// Base delay between two dirty-branch retries. The delay grows linearly with the attempt.
pub const DIRTY_BRANCH_BACKOFF: Duration = Duration::from_millis(200);

/// Delete `path` from `store`, treating a missing object as deleted.
async fn delete_if_present(
    store: &dyn ObjectStore,
    path: &object_store::path::Path,
) -> Result<(), TransactionError> {
    match store.delete(path).await {
        Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
        Err(err) => Err(TransactionError::from(err)),
    }
}

/// Merge `branch` into `location.branch`, retrying while the destination is dirty.
async fn merge_into_source(
    client: &LakeFSClient,
    location: &LakeFSLocation,
    branch: &str,
    message: &str,
    allow_empty: bool,
    backoff: Duration,
) -> Result<(), MergeError> {
    let mut attempt = 0;
    loop {
        match client
            .merge(
                &location.repo,
                &location.branch,
                branch,
                message,
                allow_empty,
            )
            .await
        {
            Err(MergeError::DirtyBranch(reason)) if attempt < DIRTY_BRANCH_RETRIES => {
                attempt += 1;
                warn!(
                    branch = %location.branch,
                    attempt,
                    "LakeFS destination branch has uncommitted changes, retrying merge"
                );
                tokio::time::sleep(backoff * attempt as u32).await;
                let _ = reason;
            }
            result => return result,
        }
    }
}

/// Commit authority for one transaction branch.
pub struct LakeFSBranchCommitter {
    client: LakeFSClient,
    /// Store rooted at the table root on the transaction branch.
    branch_store: Arc<dyn ObjectStore>,
    /// Stages `N.json` on the transaction branch with a create-only put.
    staging: FileSystemCommitter,
    /// The source table location (`lakefs://{repo}/{source branch}/{table}`).
    location: LakeFSLocation,
    /// The transaction branch.
    branch: String,
    dirty_branch_backoff: Duration,
}

impl LakeFSBranchCommitter {
    pub(crate) fn new(
        client: LakeFSClient,
        branch_store: Arc<dyn ObjectStore>,
        location: LakeFSLocation,
        branch: String,
    ) -> Self {
        Self {
            client,
            staging: FileSystemCommitter::new(branch_store.clone(), CommitStrategy::ConditionalPut),
            branch_store,
            location,
            branch,
            dirty_branch_backoff: DIRTY_BRANCH_BACKOFF,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_dirty_branch_backoff(mut self, backoff: Duration) -> Self {
        self.dirty_branch_backoff = backoff;
        self
    }
}

#[async_trait]
impl Committer for LakeFSBranchCommitter {
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<CommitResponse, TransactionError> {
        // 1. Stage the commit file on the transaction branch. The conditional put reports a
        //    version that already exists on the branch before any LakeFS call is made.
        if let conflict @ CommitResponse::Conflict { .. } =
            self.staging.commit(version, payload).await?
        {
            return Ok(conflict);
        }
        let commit_path = commit_uri_from_version(Some(version));

        // 2. Commit the branch, so that the data files and the commit file become one LakeFS commit.
        let table = &self.location.table;
        self.client
            .commit(
                &self.location.repo,
                &self.branch,
                &format!("Delta commit {{ table: {table}, version: {version}}}"),
                false,
            )
            .await
            .map_err(|e| TransactionError::LogStoreError {
                msg: e.to_string(),
                source: Box::new(e),
            })?;

        // 3. Squash-merge the branch into the source branch. LakeFS serialises merges, so exactly
        //    one writer wins a version.
        match merge_into_source(
            &self.client,
            &self.location,
            &self.branch,
            &format!("Finished deltalake transaction {{ table: {table}, version: {version} }}"),
            false,
            self.dirty_branch_backoff,
        )
        .await
        {
            Ok(()) => Ok(CommitResponse::Committed),
            Err(MergeError::Conflict(reason)) => {
                debug!(
                    version,
                    reason, "LakeFS merge conflict, another writer won this version"
                );
                // Remove the losing commit file so that the retry at `version + 1` on this
                // branch does not carry a stale `N.json` into the next merge.
                delete_if_present(self.branch_store.as_ref(), &commit_path).await?;
                Ok(CommitResponse::Conflict { version })
            }
            Err(MergeError::DirtyBranch(reason)) => Err(LakeFSOperationError::DirtyBranch {
                branch: self.location.branch.clone(),
                reason,
            }
            .into()),
            Err(MergeError::Other(err)) => Err(err.into()),
        }
    }

    async fn abort(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        // Only the attempted commit file is removed. The branch itself belongs to the scope and
        // is deleted by `LakeFSTransaction::abort`.
        if let CommitOrBytes::LogBytes(_) = payload {
            delete_if_present(
                self.branch_store.as_ref(),
                &commit_uri_from_version(Some(version)),
            )
            .await?;
        }
        Ok(())
    }

    fn payload_kind(&self) -> PayloadKind {
        self.staging.payload_kind()
    }
}

/// Commit authority of the unscoped LakeFS store.
///
/// Callers that commit without an operation scope (for example a `RecordBatchWriter`) have
/// already staged their files on the source branch. The commit is a conditional put of `N.json`
/// on the source branch followed by a LakeFS commit of the source branch, which commits
/// everything staged on that branch.
pub struct LakeFSSourceCommitter {
    client: LakeFSClient,
    /// Stages `N.json` on the source branch with a create-only put.
    staging: FileSystemCommitter,
    location: LakeFSLocation,
}

impl LakeFSSourceCommitter {
    /// `store` must be rooted at the table root on the source branch.
    pub(crate) fn new(
        client: LakeFSClient,
        store: Arc<dyn ObjectStore>,
        location: LakeFSLocation,
    ) -> Self {
        Self {
            client,
            staging: FileSystemCommitter::new(store, CommitStrategy::ConditionalPut),
            location,
        }
    }
}

#[async_trait]
impl Committer for LakeFSSourceCommitter {
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<CommitResponse, TransactionError> {
        if let conflict @ CommitResponse::Conflict { .. } =
            self.staging.commit(version, payload).await?
        {
            return Ok(conflict);
        }

        let table = &self.location.table;
        self.client
            .commit(
                &self.location.repo,
                &self.location.branch,
                &format!("Delta commit {{ table: {table}, version: {version}}}"),
                false,
            )
            .await
            .map_err(|e| TransactionError::LogStoreError {
                msg: e.to_string(),
                source: Box::new(e),
            })?;
        Ok(CommitResponse::Committed)
    }

    async fn abort(
        &self,
        _version: Version,
        _payload: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        // Nothing to undo: a staged `N.json` on the source branch is either ours and valid, or it
        // belongs to the writer that won the version and must not be touched.
        Ok(())
    }

    fn payload_kind(&self) -> PayloadKind {
        self.staging.payload_kind()
    }
}

/// Lifecycle of one transaction branch.
pub struct LakeFSTransaction {
    client: LakeFSClient,
    location: LakeFSLocation,
    branch: String,
    closed: AtomicBool,
    dirty_branch_backoff: Duration,
}

impl LakeFSTransaction {
    pub(crate) fn new(client: LakeFSClient, location: LakeFSLocation, branch: String) -> Self {
        Self {
            client,
            location,
            branch,
            closed: AtomicBool::new(false),
            dirty_branch_backoff: DIRTY_BRANCH_BACKOFF,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_dirty_branch_backoff(mut self, backoff: Duration) -> Self {
        self.dirty_branch_backoff = backoff;
        self
    }

    /// The transaction branch name.
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Commit and merge file-only work that was written after the last Delta commit.
    async fn publish(&self) -> DeltaResult<()> {
        let table = &self.location.table;
        self.client
            .commit(
                &self.location.repo,
                &self.branch,
                &format!("Delta file operations {{ table: {table}}}"),
                true,
            )
            .await?;

        if !self
            .client
            .has_changes(&self.location.repo, &self.location.branch, &self.branch)
            .await?
        {
            debug!("No changes on the transaction branch, skipping merge");
            return Ok(());
        }

        merge_into_source(
            &self.client,
            &self.location,
            &self.branch,
            &format!("Finished delta file operations {{ table: {table}}}"),
            true,
            self.dirty_branch_backoff,
        )
        .await
        .map_err(|err| match err {
            MergeError::Conflict(reason) => LakeFSOperationError::MergeConflict {
                source_branch: self.branch.clone(),
                target_branch: self.location.branch.clone(),
                reason,
            }
            .into(),
            MergeError::DirtyBranch(reason) => LakeFSOperationError::DirtyBranch {
                branch: self.location.branch.clone(),
                reason,
            }
            .into(),
            MergeError::Other(err) => DeltaTableError::from(err),
        })
    }
}

#[async_trait]
impl OperationTransaction for LakeFSTransaction {
    async fn finish(&self, dirty: bool) -> DeltaResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let published = if dirty { self.publish().await } else { Ok(()) };
        // The branch is deleted whether or not publishing succeeded, so a failed merge never
        // leaves a stale transaction branch behind.
        let deleted = self
            .client
            .delete_branch(&self.location.repo, &self.branch)
            .await;
        self.closed.store(true, Ordering::SeqCst);
        published.and(deleted)
    }

    async fn abort(&self) -> DeltaResult<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.client
            .delete_branch(&self.location.repo, &self.branch)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use deltalake_core::logstore::{
        CommitOrBytes, CommitResponse, Committer, OperationTransaction,
    };
    use mockito::{Matcher, ServerGuard};
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use reqwest::StatusCode;
    use serde_json::json;

    use super::*;
    use crate::client::LakeFSConfig;

    fn location() -> LakeFSLocation {
        LakeFSLocation {
            repo: "repo".into(),
            branch: "main".into(),
            table: "table".into(),
        }
    }

    fn client(server: &ServerGuard) -> LakeFSClient {
        LakeFSClient::with_config(LakeFSConfig::new(
            server.url(),
            "user".into(),
            "pass".into(),
        ))
    }

    fn commit_path(version: Version) -> Path {
        commit_uri_from_version(Some(version))
    }

    fn payload() -> CommitOrBytes {
        CommitOrBytes::LogBytes(Bytes::from_static(b"{}"))
    }

    fn branch_committer(server: &ServerGuard, store: Arc<InMemory>) -> LakeFSBranchCommitter {
        LakeFSBranchCommitter::new(client(server), store, location(), "delta-tx-1".into())
            .with_dirty_branch_backoff(Duration::ZERO)
    }

    #[tokio::test]
    async fn branch_commit_puts_commits_and_merges() {
        let mut server = mockito::Server::new_async().await;
        let commit = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/branches/delta-tx-1/commits",
            )
            .match_body(Matcher::PartialJson(json!({"allow_empty": false})))
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;
        let merge = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/refs/delta-tx-1/merge/main",
            )
            .match_body(Matcher::PartialJson(json!({"squash_merge": true})))
            .with_status(StatusCode::OK.as_u16().into())
            .create_async()
            .await;

        let store = Arc::new(InMemory::new());
        let committer = branch_committer(&server, store.clone());
        assert_eq!(committer.payload_kind(), PayloadKind::Bytes);
        let response = committer.commit(1, payload()).await.unwrap();
        assert_eq!(response, CommitResponse::Committed);
        assert!(store.head(&commit_path(1)).await.is_ok());
        commit.assert_async().await;
        merge.assert_async().await;
    }

    #[tokio::test]
    async fn branch_commit_conflict_removes_commit_file_and_retries_on_same_branch() {
        let mut server = mockito::Server::new_async().await;
        let commits = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/branches/delta-tx-1/commits",
            )
            .with_status(StatusCode::CREATED.as_u16().into())
            .expect(2)
            .create_async()
            .await;
        let losing_merge = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/refs/delta-tx-1/merge/main",
            )
            .match_body(Matcher::Regex("version: 1 ".into()))
            .with_status(StatusCode::CONFLICT.as_u16().into())
            .with_body(r#"{"message":"conflict found"}"#)
            .create_async()
            .await;
        let winning_merge = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/refs/delta-tx-1/merge/main",
            )
            .match_body(Matcher::Regex("version: 2 ".into()))
            .with_status(StatusCode::OK.as_u16().into())
            .create_async()
            .await;

        let store = Arc::new(InMemory::new());
        let committer = branch_committer(&server, store.clone());

        let response = committer.commit(1, payload()).await.unwrap();
        assert_eq!(response, CommitResponse::Conflict { version: 1 });
        assert!(
            store.head(&commit_path(1)).await.is_err(),
            "the losing commit file is removed from the branch"
        );

        let response = committer.commit(2, payload()).await.unwrap();
        assert_eq!(response, CommitResponse::Committed);
        assert!(store.head(&commit_path(2)).await.is_ok());
        commits.assert_async().await;
        losing_merge.assert_async().await;
        winning_merge.assert_async().await;
    }

    #[tokio::test]
    async fn branch_commit_into_dirty_branch_retries_then_fails_typed() {
        let mut server = mockito::Server::new_async().await;
        let _commit = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/branches/delta-tx-1/commits",
            )
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;
        let merge = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/refs/delta-tx-1/merge/main",
            )
            .with_status(StatusCode::BAD_REQUEST.as_u16().into())
            .with_body(r#"{"message":"cannot merge into a dirty branch"}"#)
            .expect(DIRTY_BRANCH_RETRIES + 1)
            .create_async()
            .await;

        let store = Arc::new(InMemory::new());
        let committer = branch_committer(&server, store.clone());
        let err = committer.commit(1, payload()).await.unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("uncommitted changes") && message.contains("`main`"),
            "unexpected error: {message}"
        );
        merge.assert_async().await;
    }

    #[tokio::test]
    async fn branch_commit_refuses_a_temporary_commit_file_before_any_lakefs_call() {
        let mut server = mockito::Server::new_async().await;
        let lakefs = server
            .mock("POST", Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let store = Arc::new(InMemory::new());
        let committer = branch_committer(&server, store.clone());
        let payload = CommitOrBytes::TmpCommit(Path::from("_delta_log/_commit_x.json.tmp"));
        let err = committer.commit(1, payload).await.unwrap_err();
        assert!(err.to_string().contains("payload"), "{err}");
        assert!(store.head(&commit_path(1)).await.is_err());
        lakefs.assert_async().await;
    }

    #[tokio::test]
    async fn branch_abort_removes_only_the_commit_file() {
        let server = mockito::Server::new_async().await;
        let store = Arc::new(InMemory::new());
        store.put(&commit_path(3), "{}".into()).await.unwrap();
        store
            .put(&Path::from("part-1.parquet"), "d".into())
            .await
            .unwrap();

        let committer = branch_committer(&server, store.clone());
        committer.abort(3, payload()).await.unwrap();
        assert!(store.head(&commit_path(3)).await.is_err());
        assert!(store.head(&Path::from("part-1.parquet")).await.is_ok());
        // A second abort of the same attempt is a no-op.
        committer.abort(3, payload()).await.unwrap();
    }

    #[tokio::test]
    async fn source_committer_puts_then_commits_the_source_branch() {
        let mut server = mockito::Server::new_async().await;
        let commit = server
            .mock("POST", "/api/v1/repositories/repo/branches/main/commits")
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;

        let store = Arc::new(InMemory::new());
        let committer = LakeFSSourceCommitter::new(client(&server), store.clone(), location());
        assert_eq!(committer.payload_kind(), PayloadKind::Bytes);
        let response = committer.commit(0, payload()).await.unwrap();
        assert_eq!(response, CommitResponse::Committed);
        assert!(store.head(&commit_path(0)).await.is_ok());
        commit.assert_async().await;

        // The conflict is detected by the conditional put, before any LakeFS call.
        let response = committer.commit(0, payload()).await.unwrap();
        assert_eq!(response, CommitResponse::Conflict { version: 0 });
        committer.abort(0, payload()).await.unwrap();
        assert!(
            store.head(&commit_path(0)).await.is_ok(),
            "abort never touches the source branch"
        );
    }

    fn transaction(server: &ServerGuard) -> LakeFSTransaction {
        LakeFSTransaction::new(client(server), location(), "delta-tx-1".into())
            .with_dirty_branch_backoff(Duration::ZERO)
    }

    fn delete_mock(server: &mut ServerGuard, expect: usize) -> mockito::Mock {
        server
            .mock("DELETE", "/api/v1/repositories/repo/branches/delta-tx-1")
            .with_status(StatusCode::NO_CONTENT.as_u16().into())
            .expect(expect)
            .create()
    }

    #[tokio::test]
    async fn finish_without_writes_only_deletes_the_branch() {
        let mut server = mockito::Server::new_async().await;
        let delete = delete_mock(&mut server, 1);

        let transaction = transaction(&server);
        transaction.finish(false).await.unwrap();
        // Idempotent: neither a second finish nor an abort after finish calls LakeFS again.
        transaction.finish(false).await.unwrap();
        transaction.abort().await.unwrap();
        delete.assert_async().await;
    }

    #[tokio::test]
    async fn finish_with_writes_commits_merges_and_deletes() {
        let mut server = mockito::Server::new_async().await;
        let commit = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/branches/delta-tx-1/commits",
            )
            .match_body(Matcher::PartialJson(json!({"allow_empty": true})))
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;
        let diff = server
            .mock("GET", "/api/v1/repositories/repo/refs/main/diff/delta-tx-1")
            .with_status(StatusCode::OK.as_u16().into())
            .with_body(r#"{"results": [{"path": "table/_delta_log/1.checkpoint.parquet"}]}"#)
            .create_async()
            .await;
        let merge = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/refs/delta-tx-1/merge/main",
            )
            .with_status(StatusCode::OK.as_u16().into())
            .create_async()
            .await;
        let delete = delete_mock(&mut server, 1);

        transaction(&server).finish(true).await.unwrap();
        commit.assert_async().await;
        diff.assert_async().await;
        merge.assert_async().await;
        delete.assert_async().await;
    }

    #[tokio::test]
    async fn finish_with_writes_skips_the_merge_when_the_diff_is_empty() {
        let mut server = mockito::Server::new_async().await;
        let _commit = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/branches/delta-tx-1/commits",
            )
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;
        let _diff = server
            .mock("GET", "/api/v1/repositories/repo/refs/main/diff/delta-tx-1")
            .with_status(StatusCode::OK.as_u16().into())
            .with_body(r#"{"results": []}"#)
            .create_async()
            .await;
        let merge = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/refs/delta-tx-1/merge/main",
            )
            .expect(0)
            .create_async()
            .await;
        let delete = delete_mock(&mut server, 1);

        transaction(&server).finish(true).await.unwrap();
        merge.assert_async().await;
        delete.assert_async().await;
    }

    #[tokio::test]
    async fn failed_finish_still_deletes_the_branch_and_reports_the_error() {
        let mut server = mockito::Server::new_async().await;
        let _commit = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/branches/delta-tx-1/commits",
            )
            .with_status(StatusCode::INTERNAL_SERVER_ERROR.as_u16().into())
            .create_async()
            .await;
        let delete = delete_mock(&mut server, 1);

        let transaction = transaction(&server);
        let err = transaction.finish(true).await.unwrap_err();
        assert!(err.to_string().contains("LakeFS commit failed"), "{err}");
        // The scope aborts after a failed finish; the branch is already gone.
        transaction.abort().await.unwrap();
        delete.assert_async().await;
    }

    #[tokio::test]
    async fn finish_into_dirty_branch_fails_typed_and_deletes_the_branch() {
        let mut server = mockito::Server::new_async().await;
        let _commit = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/branches/delta-tx-1/commits",
            )
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;
        let _diff = server
            .mock("GET", "/api/v1/repositories/repo/refs/main/diff/delta-tx-1")
            .with_status(StatusCode::OK.as_u16().into())
            .with_body(r#"{"results": [{"path": "x"}]}"#)
            .create_async()
            .await;
        let merge = server
            .mock(
                "POST",
                "/api/v1/repositories/repo/refs/delta-tx-1/merge/main",
            )
            .with_status(StatusCode::BAD_REQUEST.as_u16().into())
            .with_body(r#"{"message":"dirty branch"}"#)
            .expect(DIRTY_BRANCH_RETRIES + 1)
            .create_async()
            .await;
        let delete = delete_mock(&mut server, 1);

        let err = transaction(&server).finish(true).await.unwrap_err();
        assert!(err.to_string().contains("uncommitted changes"), "{err}");
        merge.assert_async().await;
        delete.assert_async().await;
    }

    #[tokio::test]
    async fn abort_deletes_the_branch_once() {
        let mut server = mockito::Server::new_async().await;
        let delete = delete_mock(&mut server, 1);

        let transaction = transaction(&server);
        transaction.abort().await.unwrap();
        transaction.abort().await.unwrap();
        transaction.finish(true).await.unwrap();
        delete.assert_async().await;
    }
}
