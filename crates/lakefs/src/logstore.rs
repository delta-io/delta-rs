//! Default implementation of [`LakeFSLogStore`] for LakeFS

use std::sync::{Arc, OnceLock};

use crate::client::LakeFSConfig;
use crate::errors::LakeFSConfigError;

use super::client::LakeFSClient;
use bytes::Bytes;
use deltalake_core::storage::{
    commit_uri_from_version, DefaultObjectStoreRegistry, ObjectStoreRegistry,
};
use deltalake_core::storage::{url_prefix_handler, DeltaIOStorageBackend, IORuntime};
use deltalake_core::{logstore::*, DeltaTableError, Path};
use deltalake_core::{
    operations::transaction::TransactionError,
    storage::{ObjectStoreRef, StorageOptions},
    DeltaResult,
};
use object_store::{Attributes, Error as ObjectStoreError, ObjectStore, PutOptions, TagSet};
use tracing::debug;
use url::Url;
use uuid::Uuid;

/// Return the [LakeFSLogStore] implementation with the provided configuration options
pub fn lakefs_logstore(
    store: ObjectStoreRef,
    location: &Url,
    options: &StorageOptions,
) -> DeltaResult<Arc<dyn LogStore>> {
    let host = options
        .0
        .get("aws_endpoint")
        .ok_or(LakeFSConfigError::EndpointMissing)?
        .to_string();
    let username = options
        .0
        .get("aws_access_key_id")
        .ok_or(LakeFSConfigError::UsernameCredentialMissing)?
        .to_string();
    let password = options
        .0
        .get("aws_secret_access_key")
        .ok_or(LakeFSConfigError::PasswordCredentialMissing)?
        .to_string();

    let client = LakeFSClient::with_config(LakeFSConfig::new(host, username, password));
    Ok(Arc::new(LakeFSLogStore::new(
        store,
        LogStoreConfig {
            location: location.clone(),
            options: options.clone(),
        },
        client,
    )))
}

/// Default [`LogStore`] implementation
#[derive(Debug, Clone)]
pub(crate) struct LakeFSLogStore {
    pub(crate) storage: DefaultObjectStoreRegistry,
    pub(crate) config: LogStoreConfig,
    pub(crate) client: LakeFSClient,
}

impl LakeFSLogStore {
    /// Create a new instance of [`LakeFSLogStore`]
    ///
    /// # Arguments
    ///
    /// * `storage` - A shared reference to an [`object_store::ObjectStore`] with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `location` - A url corresponding to the storage location of `storage`.
    pub fn new(storage: ObjectStoreRef, config: LogStoreConfig, client: LakeFSClient) -> Self {
        let registry = DefaultObjectStoreRegistry::new();
        registry.register_store(&config.location, storage);
        Self {
            storage: registry,
            config,
            client,
        }
    }

    /// Build a new object store for an URL using the existing storage options. After
    /// branch creation a new object store needs to be created for the branch uri
    fn build_new_store(&self, url: &Url) -> DeltaResult<ObjectStoreRef> {
        // turn location into scheme
        let scheme = Url::parse(&format!("{}://", url.scheme()))
            .map_err(|_| DeltaTableError::InvalidTableLocation(url.clone().into()))?;

        if let Some(entry) = deltalake_core::storage::factories().get(&scheme) {
            debug!("Creating new storage with storage provider for {scheme} ({url})");

            let (store, _prefix) = entry
                .value()
                .parse_url_opts(url, &self.config().options.clone())?;
            return Ok(store);
        }
        Err(DeltaTableError::InvalidTableLocation(url.to_string()))
    }

    fn register_object_store(&self, url: &Url, store: ObjectStoreRef) {
        self.storage.register_store(url, store);
    }

    fn get_transaction_objectstore(
        &self,
        operation_id: Uuid,
    ) -> DeltaResult<(String, ObjectStoreRef)> {
        let (repo, _, table) = self.client.decompose_url(self.config.location.to_string());
        let string_url = format!(
            "lakefs://{repo}/{}/{table}",
            self.client.get_transaction(operation_id)?,
        );
        let transaction_url = Url::parse(&string_url).unwrap();
        Ok((string_url, self.storage.get_store(&transaction_url)?))
    }

    pub async fn pre_execute(&self, operation_id: Uuid) -> DeltaResult<()> {
        // Create LakeFS Branch for transaction
        let (lakefs_url, tnx_branch) = self
            .client
            .create_branch(&self.config.location, operation_id)
            .await?;

        // Build new object store store using the new lakefs url
        let txn_store = url_prefix_handler(
            Arc::new(DeltaIOStorageBackend::new(
                self.build_new_store(&lakefs_url)?,
                IORuntime::default().get_handle(),
            )) as ObjectStoreRef,
            Path::parse(lakefs_url.path())?,
        );

        // Register transaction branch as ObjectStore in log_store storages
        self.register_object_store(&lakefs_url, txn_store);

        // set transaction in client for easy retrieval
        self.client.set_transaction(operation_id, tnx_branch);
        Ok(())
    }

    pub async fn commit_merge(&self, operation_id: Uuid) -> DeltaResult<()> {
        let (transaction_url, _) = self.get_transaction_objectstore(operation_id)?;

        // Do LakeFS Commit
        let (repo, transaction_branch, table) = self.client.decompose_url(transaction_url);
        self.client
            .commit(
                repo,
                transaction_branch,
                format!("Delta file operations {{ table: {table}}}"),
                true, // Needs to be true, it could be a file operation but no logs were deleted.
            )
            .await?;

        // Try LakeFS Branch merge of transaction branch in source branch
        let (repo, target_branch, table) =
            self.client.decompose_url(self.config.location.to_string());
        match self
            .client
            .merge(
                repo,
                target_branch,
                self.client.get_transaction(operation_id)?,
                0,
                format!("Finished delta file operations {{ table: {table}}}"),
                true, // Needs to be true, it could be a file operation but no logs were deleted.
            )
            .await
        {
            Ok(_) => {
                let (repo, _, _) = self.client.decompose_url(self.config.location.to_string());
                self.client
                    .delete_branch(repo, self.client.get_transaction(operation_id)?)
                    .await?;
                Ok(())
            }
            // TODO: propagate better LakeFS errors.
            Err(TransactionError::VersionAlreadyExists(_)) => {
                Err(TransactionError::LogStoreError {
                    msg: "Merge Failed".to_string(),
                    source: Box::new(DeltaTableError::generic("Merge Failed")),
                })
            }
            Err(err) => Err(err),
        }?;

        self.client.clear_transaction(operation_id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl LogStore for LakeFSLogStore {
    fn name(&self) -> String {
        "LakeFSLogStore".into()
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        read_commit_entry(&self.storage.get_store(&self.config.location)?, version).await
    }

    /// Tries to commit a prepared commit file. Returns [`TransactionError`]
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        let (transaction_url, store) =
            self.get_transaction_objectstore(operation_id)
                .map_err(|e| TransactionError::LogStoreError {
                    msg: e.to_string(),
                    source: Box::new(e),
                })?;

        match commit_or_bytes {
            CommitOrBytes::LogBytes(log_bytes) => {
                // Put commit
                store
                    .put_opts(
                        &commit_uri_from_version(version),
                        log_bytes.into(),
                        put_options().clone(),
                    )
                    .await
                    .map_err(|err| -> TransactionError {
                        match err {
                            ObjectStoreError::AlreadyExists { .. } => {
                                TransactionError::VersionAlreadyExists(version)
                            }
                            _ => TransactionError::from(err),
                        }
                    })?;

                // Do LakeFS Commit
                let (repo, transaction_branch, table) = self.client.decompose_url(transaction_url);
                self.client
                    .commit(
                        repo,
                        transaction_branch,
                        format!("Delta commit {{ table: {table}, version: {version}}}"),
                        false,
                    )
                    .await
                    .map_err(|e| TransactionError::LogStoreError {
                        msg: e.to_string(),
                        source: Box::new(e),
                    })?;

                // Try LakeFS Branch merge of transaction branch in source branch
                let (repo, target_branch, table) =
                    self.client.decompose_url(self.config.location.to_string());
                match self
                    .client
                    .merge(
                        repo,
                        target_branch,
                        self.client.get_transaction(operation_id)?,
                        version,
                        format!("Finished deltalake transaction {{ table: {table}, version: {version} }}"),
                        false,
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(TransactionError::VersionAlreadyExists(version)) => {
                        store
                            .delete(&commit_uri_from_version(version))
                            .await
                            .map_err(TransactionError::from)?;
                        return Err(TransactionError::VersionAlreadyExists(version));
                    }
                    Err(err) => Err(err),
                }?;
            }
            _ => unreachable!(), // Default log store should never get a tmp_commit, since this is for conditional put stores
        };
        Ok(())
    }

    async fn abort_commit_entry(
        &self,
        _version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        match &commit_or_bytes {
            CommitOrBytes::LogBytes(_) => {
                let (repo, _, _) = self.client.decompose_url(self.config.location.to_string());
                self.client
                    .delete_branch(repo, self.client.get_transaction(operation_id)?)
                    .await?;
                self.client.clear_transaction(operation_id);
                Ok(())
            }
            _ => unreachable!(), // Default log store should never get a tmp_commit, since this is for conditional put stores
        }
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        get_latest_version(self, current_version).await
    }

    async fn get_earliest_version(&self, current_version: i64) -> DeltaResult<i64> {
        get_earliest_version(self, current_version).await
    }

    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        match operation_id {
            Some(id) => {
                let (_, store) = self.get_transaction_objectstore(id).unwrap_or_else(|_| panic!("The object_store registry inside LakeFSLogstore didn't have a store for operation_id {id} Something went wrong."));
                store
            }
            _ => self.storage.get_store(&self.config.location).unwrap(),
        }
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

fn put_options() -> &'static PutOptions {
    static PUT_OPTS: OnceLock<PutOptions> = OnceLock::new();
    PUT_OPTS.get_or_init(|| PutOptions {
        mode: object_store::PutMode::Create, // Creates if file doesn't exists yet
        tags: TagSet::default(),
        attributes: Attributes::default(),
    })
}
