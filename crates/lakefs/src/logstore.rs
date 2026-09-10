//! [`LogStore`] implementation for LakeFS.
//!
//! Every writing operation runs inside an operation scope. [`LakeFSLogStore::begin_operation`]
//! creates a hidden transaction branch `delta-tx-{uuid}` from the source branch and returns the
//! write-side handles for it. Core routes all writes of the operation to that branch and reads
//! the log from the source branch. Each Delta commit is one LakeFS commit of the branch followed
//! by a squash merge into the source branch, see [`crate::transaction`].
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::logstore::*;
use deltalake_core::table::normalize_table_url;
use deltalake_core::{DeltaResult, DeltaTableError, kernel::Version};
use object_store::ObjectStore;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::client::{LakeFSClient, LakeFSConfig, LakeFSLocation};
use crate::errors::LakeFSConfigError;
use crate::transaction::{LakeFSBranchCommitter, LakeFSSourceCommitter, LakeFSTransaction};

/// Return the [LakeFSLogStore] implementation with the provided configuration options
pub fn lakefs_logstore(
    store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    location: &Url,
    options: &StorageConfig,
) -> DeltaResult<Arc<dyn LogStore>> {
    let host = options
        .raw
        .get("aws_endpoint")
        .ok_or(LakeFSConfigError::EndpointMissing)?
        .to_string();
    let username = options
        .raw
        .get("aws_access_key_id")
        .ok_or(LakeFSConfigError::UsernameCredentialMissing)?
        .to_string();
    let password = options
        .raw
        .get("aws_secret_access_key")
        .ok_or(LakeFSConfigError::PasswordCredentialMissing)?
        .to_string();

    let client = LakeFSClient::with_config(LakeFSConfig::new(host, username, password));
    Ok(Arc::new(LakeFSLogStore::new(
        store,
        root_store,
        LogStoreConfig::new(location, options.clone()),
        client,
    )))
}

/// [`LogStore`] for tables on a LakeFS branch.
#[derive(Debug, Clone)]
pub struct LakeFSLogStore {
    /// Store rooted at the table root on the source branch.
    prefixed_store: ObjectStoreRef,
    /// Store rooted at the repository. The branch is the first path segment, so this store
    /// resolves paths on the source branch and on every transaction branch.
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
    client: LakeFSClient,
}

impl LakeFSLogStore {
    /// Create a new instance of [`LakeFSLogStore`]
    ///
    /// # Arguments
    ///
    /// * `prefixed_store` - A shared reference to an [`object_store::ObjectStore`]
    ///   with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `root_store` - A shared reference to an [`object_store::ObjectStore`] with "/"
    ///   pointing at root of the storage system.
    /// * `location` - A url corresponding to the storage location of `storage`.
    pub fn new(
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        config: LogStoreConfig,
        client: LakeFSClient,
    ) -> Self {
        Self {
            prefixed_store,
            root_store,
            config,
            client,
        }
    }

    /// The LakeFS client this store uses.
    pub fn client(&self) -> &LakeFSClient {
        &self.client
    }

    fn location(&self) -> DeltaResult<LakeFSLocation> {
        LakeFSLocation::parse(self.config.location().as_str()).ok_or_else(|| {
            DeltaTableError::InvalidTableLocation(self.config.location().to_string())
        })
    }
}

#[async_trait::async_trait]
impl LogStore for LakeFSLogStore {
    fn name(&self) -> String {
        "LakeFSLogStore".into()
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        read_commit_entry(self.prefixed_store.as_ref(), version).await
    }

    async fn get_latest_version(&self, current_version: Version) -> DeltaResult<Version> {
        get_latest_version(self, current_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    /// The unscoped commit authority: a conditional put on the source branch followed by a
    /// LakeFS commit of the source branch. Operations that run inside a scope get a branch
    /// committer from [`LakeFSLogStore::begin_operation`] instead.
    fn committer(&self) -> Arc<dyn Committer> {
        let location = self
            .location()
            .expect("a LakeFSLogStore is only built for lakefs:// locations");
        Arc::new(LakeFSSourceCommitter::new(
            self.client.clone(),
            self.prefixed_store.clone(),
            location,
        ))
    }

    /// Create a hidden transaction branch and return the write-side handles for it.
    async fn begin_operation(&self) -> DeltaResult<Option<OperationContext>> {
        let location = self.location()?;
        let branch = format!("delta-tx-{}", Uuid::new_v4());
        self.client
            .create_branch(&location.repo, &location.branch, &branch)
            .await?;
        debug!(branch, "created LakeFS transaction branch");

        let write_root = normalize_table_url(
            &Url::parse(&location.on_branch(&branch))
                .map_err(|_| DeltaTableError::InvalidTableLocation(location.on_branch(&branch)))?,
        );
        let branch_store: Arc<dyn ObjectStore> = Arc::new(
            self.config
                .decorate_store(self.root_store.clone(), Some(&write_root))?,
        );

        Ok(Some(OperationContext {
            object_store: branch_store.clone(),
            root_object_store: self.root_store.clone(),
            write_root,
            committer: Arc::new(LakeFSBranchCommitter::new(
                self.client.clone(),
                branch_store,
                location.clone(),
                branch.clone(),
            )),
            transaction: Arc::new(LakeFSTransaction::new(
                self.client.clone(),
                location,
                branch,
            )),
        }))
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use deltalake_core::logstore::{LogStore, PayloadKind, StorageConfig};
    use mockito::Matcher;
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::prefix::PrefixStore;
    use reqwest::StatusCode;
    use serde_json::json;

    use super::*;

    fn store(server: &mockito::ServerGuard, root: Arc<InMemory>) -> LakeFSLogStore {
        let location = Url::parse("lakefs://repo/main/table").unwrap();
        let prefixed = Arc::new(PrefixStore::new(root.clone(), "main/table"));
        LakeFSLogStore::new(
            prefixed,
            root,
            LogStoreConfig::new(&location, StorageConfig::default()),
            LakeFSClient::with_config(LakeFSConfig::new(
                server.url(),
                "user".into(),
                "pass".into(),
            )),
        )
    }

    #[tokio::test]
    async fn begin_operation_creates_a_hidden_branch_and_routes_writes_to_it() {
        let mut server = mockito::Server::new_async().await;
        let create = server
            .mock("POST", "/api/v1/repositories/repo/branches")
            .match_body(Matcher::AllOf(vec![
                Matcher::PartialJson(json!({"source": "main", "hidden": true})),
                Matcher::Regex(r#""name":"delta-tx-"#.into()),
            ]))
            .with_status(StatusCode::CREATED.as_u16().into())
            .create_async()
            .await;

        let root = Arc::new(InMemory::new());
        let log_store = store(&server, root.clone());
        let ctx = log_store.begin_operation().await.unwrap().unwrap();
        create.assert_async().await;

        let write_root = ctx.write_root.as_str().to_string();
        assert!(
            write_root.starts_with("lakefs://repo/delta-tx-") && write_root.ends_with("/table/"),
            "unexpected write root {write_root}"
        );
        let branch = write_root
            .trim_start_matches("lakefs://repo/")
            .trim_end_matches("/table/")
            .to_string();

        ctx.object_store
            .put(&Path::from("part-1.parquet"), "data".into())
            .await
            .unwrap();
        assert!(
            root.head(&Path::from(format!("{branch}/table/part-1.parquet")))
                .await
                .is_ok(),
            "writes through the context land on the transaction branch"
        );
        assert!(
            root.head(&Path::from("main/table/part-1.parquet"))
                .await
                .is_err(),
            "nothing reaches the source branch before the merge"
        );
        assert_eq!(ctx.committer.payload_kind(), PayloadKind::Bytes);
    }

    #[tokio::test]
    async fn unscoped_store_reads_and_commits_on_the_source_branch() {
        let server = mockito::Server::new_async().await;
        let root = Arc::new(InMemory::new());
        let log_store = store(&server, root.clone());
        assert_eq!(log_store.name(), "LakeFSLogStore");
        assert_eq!(log_store.committer().payload_kind(), PayloadKind::Bytes);
        assert_eq!(log_store.write_root_url(), *log_store.root_url());

        log_store
            .object_store()
            .put(&Path::from("x"), "data".into())
            .await
            .unwrap();
        assert!(root.head(&Path::from("main/table/x")).await.is_ok());
    }
}
