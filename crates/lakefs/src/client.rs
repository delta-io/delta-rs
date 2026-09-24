//! Slim HTTP client for the LakeFS branch, commit and merge API.

use std::time::Duration;

use deltalake_core::DeltaResult;
use object_store::RetryConfig;
use reqwest::{Client, Response, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::{ExponentialBackoff, ExponentialBackoffTimed};
use reqwest_retry::{RetryTransientMiddleware, Retryable, RetryableStrategy};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;

use crate::errors::LakeFSOperationError;

/// Time allowed to open a connection to LakeFS.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Time allowed for one attempt of a request that is safe to repeat. Commits and merges have no
/// limit: an attempt that is cut off can still complete in LakeFS.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct LakeFSConfig {
    host: String,
    username: String,
    password: String,
    retry: RetryConfig,
}

impl LakeFSConfig {
    pub fn new(host: String, username: String, password: String) -> Self {
        LakeFSConfig {
            host,
            username,
            password,
            retry: RetryConfig::default(),
        }
    }

    /// Retry limits and backoff of the API requests.
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }
}

/// Who writes to the branch of a commit. This decides which failed commits are retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchAccess {
    /// Only the caller, as on a transaction branch. Retried after every transient failure.
    /// "No changes" counts as committed: only an earlier attempt can have taken the staged changes.
    Exclusive,
    /// Other writers too, as on the source branch. Retried only when LakeFS did not process the
    /// request, so a retry cannot commit files that other writers staged.
    Shared,
}

/// Why a merge was rejected.
#[derive(Debug)]
pub enum MergeError {
    /// The destination already contains a conflicting change (HTTP 409).
    Conflict(String),
    /// The destination branch has uncommitted changes (HTTP 400 with a dirty-branch reason).
    DirtyBranch(String),
    /// Any other failure.
    Other(LakeFSOperationError),
}

impl From<LakeFSOperationError> for MergeError {
    fn from(err: LakeFSOperationError) -> Self {
        MergeError::Other(err)
    }
}

/// The three parts of a `lakefs://{repo}/{branch}/{table}` URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeFSLocation {
    pub repo: String,
    pub branch: String,
    pub table: String,
}

impl LakeFSLocation {
    /// Split a `lakefs://{repo}/{branch}/{table}` URL into its parts.
    pub fn parse(url: &str) -> Option<Self> {
        let rest = url.strip_prefix("lakefs://")?;
        let mut parts = rest.split('/');
        let repo = parts.next()?.to_owned();
        let branch = parts.next()?.to_owned();
        let table = parts
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>()
            .join("/");
        if repo.is_empty() || branch.is_empty() {
            return None;
        }
        Some(Self {
            repo,
            branch,
            table,
        })
    }

    /// The URL of the same table on another branch.
    pub fn on_branch(&self, branch: &str) -> String {
        format!("lakefs://{}/{branch}/{}", self.repo, self.table)
    }
}

/// Retries only failures where LakeFS did not process the request: connection errors, 429 and
/// 503. Any other failure can come after LakeFS applied the request.
struct RetryUnprocessed;

impl RetryableStrategy for RetryUnprocessed {
    fn handle(&self, res: &Result<Response, reqwest_middleware::Error>) -> Option<Retryable> {
        match res {
            Ok(response) => matches!(
                response.status(),
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
            )
            .then_some(Retryable::Transient),
            Err(reqwest_middleware::Error::Reqwest(err)) if err.is_connect() => {
                Some(Retryable::Transient)
            }
            Err(_) => Some(Retryable::Fatal),
        }
    }
}

/// Backoff from the object store retry settings.
fn backoff(retry: &RetryConfig) -> ExponentialBackoffTimed {
    let backoff = &retry.backoff;
    // retry-policies panics when min > max and takes a whole-number base.
    ExponentialBackoff::builder()
        .retry_bounds(
            backoff.init_backoff.min(backoff.max_backoff),
            backoff.max_backoff,
        )
        .base(backoff.base.round().max(1.0) as u32)
        .build_with_total_retry_duration_and_max_retries(
            retry.retry_timeout,
            u32::try_from(retry.max_retries).unwrap_or(u32::MAX),
        )
}

/// Slim LakeFS client for lakefs branch operations.
///
/// Requests that are safe to repeat are retried after every transient failure. Merges and commits
/// on a shared branch are retried only when LakeFS did not process them.
#[derive(Debug, Clone)]
pub struct LakeFSClient {
    /// configuration of the lakefs client
    config: LakeFSConfig,
    /// Retries connection errors, timeouts, 408, 429 and 5xx responses.
    retry_transient: ClientWithMiddleware,
    /// Retries with [`RetryUnprocessed`].
    retry_unprocessed: ClientWithMiddleware,
}

impl LakeFSClient {
    pub fn with_config(config: LakeFSConfig) -> Self {
        // Fails only where `Client::new()` panics too.
        let http_client = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .expect("failed to build the LakeFS HTTP client");
        let retry_transient = ClientBuilder::new(http_client.clone())
            .with(RetryTransientMiddleware::new_with_policy(backoff(
                &config.retry,
            )))
            .build();
        let retry_unprocessed = ClientBuilder::new(http_client)
            .with(RetryTransientMiddleware::new_with_policy_and_strategy(
                backoff(&config.retry),
                RetryUnprocessed,
            ))
            .build();
        Self {
            config,
            retry_transient,
            retry_unprocessed,
        }
    }

    /// Create the hidden branch `branch` from `source_branch`. An existing branch counts as
    /// created, because a retry can find the branch of an earlier attempt. Use unique names.
    pub async fn create_branch(
        &self,
        repo: &str,
        source_branch: &str,
        branch: &str,
    ) -> DeltaResult<()> {
        let request_url = format!("{}/api/v1/repositories/{repo}/branches", self.config.host);

        let body = json!({
            "name": branch,
            "source": source_branch,
            "force": false,
            "hidden": true,
        });

        debug!("Creating LakeFS branch `{branch}` from `{source_branch}` in repo `{repo}`");
        let response = self
            .retry_transient
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        match response.status() {
            StatusCode::CREATED | StatusCode::CONFLICT => Ok(()),
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            status_code => {
                let body = response.text().await.unwrap_or_default();
                Err(LakeFSOperationError::CreateBranchFailed(format!(
                    "Unknown error occurred during branch creation. Response code was {status_code}, body: {body}"
                ))
                .into())
            }
        }
    }

    /// Delete `branch`. A branch that no longer exists counts as deleted.
    pub async fn delete_branch(&self, repo: &str, branch: &str) -> DeltaResult<()> {
        let request_url = format!(
            "{}/api/v1/repositories/{repo}/branches/{branch}",
            self.config.host,
        );
        debug!("Deleting LakeFS branch `{branch}` in repo `{repo}`");
        let response = self
            .retry_transient
            .delete(&request_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        match response.status() {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(()),
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            status_code => {
                let body = response.text().await.unwrap_or_default();
                Err(LakeFSOperationError::DeleteBranchFailed(format!(
                    "Unknown error occurred during branch deletion. Response code was {status_code}, body: {body}"
                ))
                .into())
            }
        }
    }

    /// Commit the staging area of `branch`. `access` decides the retries, see [`BranchAccess`].
    pub async fn commit(
        &self,
        repo: &str,
        branch: &str,
        commit_message: &str,
        allow_empty: bool,
        access: BranchAccess,
    ) -> DeltaResult<()> {
        let request_url = format!(
            "{}/api/v1/repositories/{repo}/branches/{branch}/commits",
            self.config.host,
        );

        let body = json!({
            "message": commit_message,
            "allow_empty": allow_empty,
        });

        let http_client = match access {
            BranchAccess::Exclusive => &self.retry_transient,
            BranchAccess::Shared => &self.retry_unprocessed,
        };
        debug!("Committing to LakeFS Branch: '{branch}' in repo: '{repo}'");
        let response = http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        match response.status() {
            StatusCode::NO_CONTENT | StatusCode::CREATED => Ok(()),
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            status_code => {
                let body = response.text().await.unwrap_or_default();
                if access == BranchAccess::Exclusive
                    && status_code == StatusCode::BAD_REQUEST
                    && body.contains("no changes")
                {
                    debug!("Nothing staged on `{branch}`, an earlier attempt committed it");
                    return Ok(());
                }
                Err(LakeFSOperationError::CommitFailed(format!(
                    "Unknown error occurred during branch commit. Response code was {status_code}, body: {body}"
                ))
                .into())
            }
        }
    }

    /// Squash-merge `source_ref` into `target_branch`.
    pub async fn merge(
        &self,
        repo: &str,
        target_branch: &str,
        source_ref: &str,
        commit_message: &str,
        allow_empty: bool,
    ) -> Result<(), MergeError> {
        let request_url = format!(
            "{}/api/v1/repositories/{repo}/refs/{source_ref}/merge/{target_branch}",
            self.config.host,
        );

        let body = json!({
            "message": commit_message,
            "allow_empty": allow_empty,
            "squash_merge": true,
        });

        debug!(
            "Merging LakeFS, source `{source_ref}` into target `{target_branch}` in repo: {repo}"
        );
        let response = self
            .retry_unprocessed
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::CONFLICT => {
                let body = response.text().await.unwrap_or_default();
                Err(MergeError::Conflict(body))
            }
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            StatusCode::BAD_REQUEST => {
                let body = response.text().await.unwrap_or_default();
                if body.to_ascii_lowercase().contains("dirty") {
                    Err(MergeError::DirtyBranch(body))
                } else {
                    Err(LakeFSOperationError::MergeFailed(format!(
                        "Merge was rejected. Response code was 400, body: {body}"
                    ))
                    .into())
                }
            }
            status_code => {
                let body = response.text().await.unwrap_or_default();
                Err(LakeFSOperationError::MergeFailed(format!(
                    "Unknown error occurred during merge. Response code was {status_code}, body: {body}"
                ))
                .into())
            }
        }
    }

    /// `true` when `compare_branch` differs from `base_branch`.
    pub async fn has_changes(
        &self,
        repo: &str,
        base_branch: &str,
        compare_branch: &str,
    ) -> DeltaResult<bool> {
        let request_url = format!(
            "{}/api/v1/repositories/{repo}/refs/{base_branch}/diff/{compare_branch}",
            self.config.host
        );

        debug!("Checking for changes from `{base_branch}` to `{compare_branch}` in repo: {repo}");
        let response = self
            .retry_transient
            .get(&request_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        match response.status() {
            StatusCode::OK => {
                #[derive(Deserialize, Debug)]
                struct DiffResponse {
                    results: Vec<Value>,
                }

                let diff: DiffResponse = response
                    .json()
                    .await
                    .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e.into() })?;

                Ok(!diff.results.is_empty())
            }
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            status_code => {
                let body = response.text().await.unwrap_or_default();
                Err(LakeFSOperationError::DiffFailed(format!(
                    "Unknown error occurred during branch diffing. Response code was {status_code}, body: {body}"
                ))
                .into())
            }
        }
    }
}

#[cfg(test)]
impl LakeFSClient {
    /// Client for a mock server: two retries without backoff.
    pub(crate) fn for_tests(host: String) -> Self {
        let retry = RetryConfig {
            backoff: object_store::BackoffConfig {
                init_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                base: 2.0,
            },
            max_retries: 2,
            retry_timeout: Duration::from_secs(30),
        };
        Self::with_config(LakeFSConfig::new(host, "user".into(), "pass".into()).with_retry(retry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito;
    use reqwest::StatusCode;

    fn client(server: &mockito::ServerGuard) -> LakeFSClient {
        LakeFSClient::for_tests(server.url())
    }

    #[tokio::test]
    async fn test_create_branch() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/repositories/test_repo/branches")
            .match_body(mockito::Matcher::PartialJson(json!({
                "name": "delta-tx-1234",
                "source": "main",
                "hidden": true,
            })))
            .with_status(StatusCode::CREATED.as_u16().into())
            .with_body("")
            .create_async()
            .await;

        client(&server)
            .create_branch("test_repo", "main", "delta-tx-1234")
            .await
            .unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_branch_retry_finds_the_branch_of_an_earlier_attempt() {
        // The first attempt creates the branch but fails; the retry finds the branch.
        let mut server = mockito::Server::new_async().await;
        let failed = server
            .mock("POST", "/api/v1/repositories/test_repo/branches")
            .with_status(StatusCode::INTERNAL_SERVER_ERROR.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        let exists = server
            .mock("POST", "/api/v1/repositories/test_repo/branches")
            .with_status(StatusCode::CONFLICT.as_u16().into())
            .with_body(r#"{"message":"branch already exists: not unique"}"#)
            .expect(1)
            .create_async()
            .await;

        client(&server)
            .create_branch("test_repo", "main", "delta-tx-1234")
            .await
            .unwrap();
        failed.assert_async().await;
        exists.assert_async().await;
    }

    #[tokio::test]
    async fn test_delete_branch_retries_transient_failures() {
        let mut server = mockito::Server::new_async().await;
        let path = "/api/v1/repositories/test_repo/branches/delta-tx-1234";
        let failed = server
            .mock("DELETE", path)
            .with_status(StatusCode::BAD_GATEWAY.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        let deleted = server
            .mock("DELETE", path)
            .with_status(StatusCode::NO_CONTENT.as_u16().into())
            .expect(1)
            .create_async()
            .await;

        client(&server)
            .delete_branch("test_repo", "delta-tx-1234")
            .await
            .unwrap();
        failed.assert_async().await;
        deleted.assert_async().await;
    }

    #[tokio::test]
    async fn test_delete_branch_treats_missing_branch_as_deleted() {
        let mut server = mockito::Server::new_async().await;
        let deleted = server
            .mock(
                "DELETE",
                "/api/v1/repositories/test_repo/branches/delta-tx-1234",
            )
            .with_status(StatusCode::NO_CONTENT.as_u16().into())
            .create_async()
            .await;
        let missing = server
            .mock(
                "DELETE",
                "/api/v1/repositories/test_repo/branches/delta-tx-gone",
            )
            .with_status(StatusCode::NOT_FOUND.as_u16().into())
            .create_async()
            .await;

        let client = client(&server);
        client
            .delete_branch("test_repo", "delta-tx-1234")
            .await
            .unwrap();
        client
            .delete_branch("test_repo", "delta-tx-gone")
            .await
            .unwrap();
        deleted.assert_async().await;
        missing.assert_async().await;
    }

    #[tokio::test]
    async fn test_commit() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/branches/delta-tx-1234/commits",
            )
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;

        client(&server)
            .commit(
                "test_repo",
                "delta-tx-1234",
                "Test commit",
                false,
                BranchAccess::Exclusive,
            )
            .await
            .unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_commit_on_an_exclusive_branch_counts_no_changes_as_committed() {
        // The first attempt commits but fails; the retry finds nothing staged.
        let mut server = mockito::Server::new_async().await;
        let path = "/api/v1/repositories/test_repo/branches/delta-tx-1234/commits";
        let failed = server
            .mock("POST", path)
            .with_status(StatusCode::INTERNAL_SERVER_ERROR.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        let no_changes = server
            .mock("POST", path)
            .with_status(StatusCode::BAD_REQUEST.as_u16().into())
            .with_body(r#"{"message":"commit: no changes"}"#)
            .expect(1)
            .create_async()
            .await;

        client(&server)
            .commit(
                "test_repo",
                "delta-tx-1234",
                "m",
                false,
                BranchAccess::Exclusive,
            )
            .await
            .unwrap();
        failed.assert_async().await;
        no_changes.assert_async().await;
    }

    #[tokio::test]
    async fn test_commit_on_a_shared_branch_retries_only_unprocessed_failures() {
        let mut server = mockito::Server::new_async().await;
        // Not processed by LakeFS: retried.
        let unavailable = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/branches/retried/commits",
            )
            .with_status(StatusCode::SERVICE_UNAVAILABLE.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        let committed = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/branches/retried/commits",
            )
            .with_status(StatusCode::CREATED.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        // Possibly committed: not retried.
        let failed = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/branches/failed/commits",
            )
            .with_status(StatusCode::INTERNAL_SERVER_ERROR.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        // "No changes" is an error on a shared branch.
        let no_changes = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/branches/empty/commits",
            )
            .with_status(StatusCode::BAD_REQUEST.as_u16().into())
            .with_body(r#"{"message":"commit: no changes"}"#)
            .expect(1)
            .create_async()
            .await;

        let client = client(&server);
        client
            .commit("test_repo", "retried", "m", false, BranchAccess::Shared)
            .await
            .unwrap();
        for branch in ["failed", "empty"] {
            let err = client
                .commit("test_repo", branch, "m", false, BranchAccess::Shared)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("LakeFS commit failed"), "{err}");
        }
        unavailable.assert_async().await;
        committed.assert_async().await;
        failed.assert_async().await;
        no_changes.assert_async().await;
    }

    #[tokio::test]
    async fn test_merge_outcomes() {
        let mut server = mockito::Server::new_async().await;
        let ok = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/refs/tx-ok/merge/main",
            )
            .with_status(StatusCode::OK.as_u16().into())
            .create_async()
            .await;
        let conflict = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/refs/tx-conflict/merge/main",
            )
            .with_status(StatusCode::CONFLICT.as_u16().into())
            .with_body(r#"{"message":"conflict found"}"#)
            .create_async()
            .await;
        let dirty = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/refs/tx-dirty/merge/main",
            )
            .with_status(StatusCode::BAD_REQUEST.as_u16().into())
            .with_body(r#"{"message":"cannot merge into a dirty branch"}"#)
            .create_async()
            .await;
        let other = server
            .mock(
                "POST",
                "/api/v1/repositories/test_repo/refs/tx-other/merge/main",
            )
            .with_status(StatusCode::INTERNAL_SERVER_ERROR.as_u16().into())
            .create_async()
            .await;

        let client = client(&server);
        assert!(
            client
                .merge("test_repo", "main", "tx-ok", "m", false)
                .await
                .is_ok()
        );
        assert!(matches!(
            client
                .merge("test_repo", "main", "tx-conflict", "m", false)
                .await,
            Err(MergeError::Conflict(_))
        ));
        assert!(matches!(
            client
                .merge("test_repo", "main", "tx-dirty", "m", false)
                .await,
            Err(MergeError::DirtyBranch(_))
        ));
        assert!(matches!(
            client
                .merge("test_repo", "main", "tx-other", "m", false)
                .await,
            Err(MergeError::Other(LakeFSOperationError::MergeFailed(_)))
        ));
        // No retries: LakeFS can have applied the merge before any of these responses.
        ok.assert_async().await;
        conflict.assert_async().await;
        dirty.assert_async().await;
        other.assert_async().await;
    }

    #[tokio::test]
    async fn test_merge_retries_failures_that_lakefs_did_not_process() {
        let mut server = mockito::Server::new_async().await;
        let path = "/api/v1/repositories/test_repo/refs/tx/merge/main";
        let throttled = server
            .mock("POST", path)
            .with_status(StatusCode::TOO_MANY_REQUESTS.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        let unavailable = server
            .mock("POST", path)
            .with_status(StatusCode::SERVICE_UNAVAILABLE.as_u16().into())
            .expect(1)
            .create_async()
            .await;
        let merged = server
            .mock("POST", path)
            .with_status(StatusCode::OK.as_u16().into())
            .expect(1)
            .create_async()
            .await;

        client(&server)
            .merge("test_repo", "main", "tx", "m", false)
            .await
            .unwrap();
        throttled.assert_async().await;
        unavailable.assert_async().await;
        merged.assert_async().await;
    }

    #[tokio::test]
    async fn test_merge_retries_a_refused_connection_and_reports_the_cause() {
        // Nothing listens on the port.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let err = LakeFSClient::for_tests(host)
            .merge("test_repo", "main", "tx", "m", false)
            .await
            .unwrap_err();
        let MergeError::Other(err) = err else {
            panic!("unexpected merge error: {err:?}");
        };
        let message = err.to_string();
        assert!(
            message.contains("after 2 retries") && message.contains("tcp connect error"),
            "{message}"
        );
    }

    #[test]
    fn test_backoff_accepts_an_initial_backoff_above_the_maximum() {
        let mut retry = RetryConfig::default();
        retry.backoff.init_backoff = Duration::from_secs(60);
        retry.backoff.max_backoff = Duration::from_secs(1);
        retry.max_retries = 3;
        assert_eq!(backoff(&retry).max_retries(), Some(3));
    }

    #[test]
    fn test_parse_location() {
        let location = LakeFSLocation::parse("lakefs://test_repo/test_branch/test_table").unwrap();
        assert_eq!(location.repo, "test_repo");
        assert_eq!(location.branch, "test_branch");
        assert_eq!(location.table, "test_table");

        let location =
            LakeFSLocation::parse("lakefs://test_repo/test_branch/data/test_table/").unwrap();
        assert_eq!(location.table, "data/test_table");
        assert_eq!(
            location.on_branch("delta-tx-1"),
            "lakefs://test_repo/delta-tx-1/data/test_table"
        );

        assert!(LakeFSLocation::parse("s3://bucket/table").is_none());
        assert!(LakeFSLocation::parse("lakefs://repo").is_none());
    }

    #[tokio::test]
    async fn test_has_changes() {
        let test_cases = vec![
            ("with_changes", r#"{"results": [{"some": "change"}]}"#, true),
            ("without_changes", r#"{"results": []}"#, false),
        ];

        for (test_name, response_body, expected_has_changes) in test_cases {
            let mut server = mockito::Server::new_async().await;
            let mock = server
                .mock(
                    "GET",
                    "/api/v1/repositories/test_repo/refs/base_branch/diff/compare_branch",
                )
                .with_status(StatusCode::OK.as_u16().into())
                .with_body(response_body)
                .create_async()
                .await;

            let has_changes = client(&server)
                .has_changes("test_repo", "base_branch", "compare_branch")
                .await
                .unwrap_or_else(|e| panic!("Test case '{test_name}' failed: {e}"));
            assert_eq!(
                has_changes, expected_has_changes,
                "Test case '{test_name}' failed: expected has_changes to be {expected_has_changes}"
            );
            mock.assert_async().await;
        }
    }
}
