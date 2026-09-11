//! Slim HTTP client for the LakeFS branch, commit and merge API.

use deltalake_core::DeltaResult;
use reqwest::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;

use crate::errors::LakeFSOperationError;

#[derive(Debug, Clone)]
pub struct LakeFSConfig {
    host: String,
    username: String,
    password: String,
}

impl LakeFSConfig {
    pub fn new(host: String, username: String, password: String) -> Self {
        LakeFSConfig {
            host,
            username,
            password,
        }
    }
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

/// Slim LakeFS client for lakefs branch operations.
#[derive(Debug, Clone)]
pub struct LakeFSClient {
    /// configuration of the lakefs client
    config: LakeFSConfig,
    http_client: Client,
}

impl LakeFSClient {
    pub fn with_config(config: LakeFSConfig) -> Self {
        let http_client = Client::new();
        Self {
            config,
            http_client,
        }
    }

    /// Create the hidden branch `branch` from `source_branch`.
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
            .http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        match response.status() {
            StatusCode::CREATED => Ok(()),
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
            .http_client
            .delete(&request_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
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

    /// Commit the staging area of `branch`.
    pub async fn commit(
        &self,
        repo: &str,
        branch: &str,
        commit_message: &str,
        allow_empty: bool,
    ) -> DeltaResult<()> {
        let request_url = format!(
            "{}/api/v1/repositories/{repo}/branches/{branch}/commits",
            self.config.host,
        );

        let body = json!({
            "message": commit_message,
            "allow_empty": allow_empty,
        });

        debug!("Committing to LakeFS Branch: '{branch}' in repo: '{repo}'");
        let response = self
            .http_client
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
            .http_client
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
            .http_client
            .get(&request_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
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
                    .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

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
mod tests {
    use super::*;
    use mockito;
    use reqwest::StatusCode;

    fn client(server: &mockito::ServerGuard) -> LakeFSClient {
        LakeFSClient::with_config(LakeFSConfig::new(
            server.url(),
            "test_user".to_string(),
            "test_pass".to_string(),
        ))
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
            .commit("test_repo", "delta-tx-1234", "Test commit", false)
            .await
            .unwrap();
        mock.assert_async().await;
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
        ok.assert_async().await;
        conflict.assert_async().await;
        dirty.assert_async().await;
        other.assert_async().await;
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
