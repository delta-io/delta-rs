//! Default implementation of [`LakeFSLogStore`] for LakeFS
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use deltalake_core::logstore::{
    commit_uri_from_version, DefaultObjectStoreRegistry, ObjectStoreRegistry,
};
use deltalake_core::{
    kernel::transaction::TransactionError, logstore::ObjectStoreRef, DeltaResult,
};
use deltalake_core::{logstore::*, DeltaTableError};
use object_store::{Error as ObjectStoreError, ObjectStore, PutOptions};
use tracing::debug;
use url::Url;
use uuid::Uuid;

use super::client::LakeFSClient;
use crate::client::LakeFSConfig;
use crate::errors::LakeFSConfigError;

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
    pub(crate) prefixed_registry: DefaultObjectStoreRegistry,
    root_registry: DefaultObjectStoreRegistry,
    config: LogStoreConfig,
    pub(crate) client: LakeFSClient,
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
        let prefixed_registry = DefaultObjectStoreRegistry::new();
        prefixed_registry.register_store(&config.location, prefixed_store);
        let root_registry = DefaultObjectStoreRegistry::new();
        root_registry.register_store(&config.location, root_store);
        Self {
            prefixed_registry,
            root_registry,
            config,
            client,
        }
    }

    /// Build a new object store for an URL using the existing storage options. After
    /// branch creation a new object store needs to be created for the branch uri
    fn build_new_store(
        &self,
        url: &Url,
        io_runtime: Option<IORuntime>,
    ) -> DeltaResult<ObjectStoreRef> {
        // turn location into scheme
        let scheme = Url::parse(&format!("{}://", url.scheme()))
            .map_err(|_| DeltaTableError::InvalidTableLocation(url.clone().into()))?;

        if let Some(entry) = self.config().object_store_factory().get(&scheme) {
            debug!("Creating new storage with storage provider for {scheme} ({url})");
            let (store, _prefix) = entry.value().parse_url_opts(
                url,
                &self.config().options.raw,
                &self.config().options.retry,
                io_runtime.map(|rt| rt.get_handle()),
            )?;
            return Ok(store);
        }
        Err(DeltaTableError::InvalidTableLocation(url.to_string()))
    }

    fn register_object_store(&self, url: &Url, store: ObjectStoreRef) {
        self.prefixed_registry.register_store(url, store);
    }

    fn register_root_object_store(&self, url: &Url, store: ObjectStoreRef) {
        self.root_registry.register_store(url, store);
    }

    fn get_transaction_url(&self, operation_id: Uuid, base: String) -> DeltaResult<Url> {
        let (repo, _, table) = self.client.decompose_url(base);
        let string_url = format!(
            "lakefs://{repo}/{}/{table}",
            self.client.get_transaction(operation_id)?,
        );
        Ok(Url::parse(&string_url).unwrap())
    }

    fn get_transaction_objectstore(
        &self,
        operation_id: Uuid,
    ) -> DeltaResult<(String, ObjectStoreRef, ObjectStoreRef)> {
        let transaction_url =
            self.get_transaction_url(operation_id, self.config.location.to_string())?;
        Ok((
            transaction_url.clone().to_string(),
            self.prefixed_registry.get_store(&transaction_url)?,
            self.root_registry.get_store(&transaction_url)?,
        ))
    }

    pub async fn pre_execute(&self, operation_id: Uuid) -> DeltaResult<()> {
        // Create LakeFS Branch for transaction
        let (lakefs_url, tnx_branch) = self
            .client
            .create_branch(&self.config.location, operation_id)
            .await?;

        // Build new object store store using the new lakefs url
        let txn_root_store =
            self.build_new_store(&lakefs_url, self.config().options.runtime.clone())?;
        let txn_store = Arc::new(
            self.config
                .decorate_store(txn_root_store.clone(), Some(&lakefs_url))?,
        );

        // Register transaction branch as ObjectStore in log_store storages
        self.register_root_object_store(&lakefs_url, txn_root_store);
        self.register_object_store(&lakefs_url, txn_store);

        // set transaction in client for easy retrieval
        self.client.set_transaction(operation_id, tnx_branch);
        Ok(())
    }

    pub async fn commit_merge(&self, operation_id: Uuid) -> DeltaResult<()> {
        let (transaction_url, _, _) = self.get_transaction_objectstore(operation_id)?;

        // Do LakeFS Commit
        let (repo, transaction_branch, table) = self.client.decompose_url(transaction_url);
        self.client
            .commit(
                repo.clone(),
                transaction_branch.clone(),
                format!("Delta file operations {{ table: {table}}}"),
                true, // Needs to be true, it could be a file operation but no logs were deleted.
            )
            .await?;

        // Get target branch information
        let (repo, target_branch, table) =
            self.client.decompose_url(self.config.location.to_string());

        // Check if there are any changes before attempting to merge
        let has_changes = self
            .client
            .has_changes(&repo, &target_branch, &transaction_branch)
            .await
            .map_err(|e| DeltaTableError::generic(format!("Failed to check for changes: {e}")))?;

        // Only perform merge if there are changes
        if has_changes {
            debug!("Changes detected, proceeding with merge");
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
                    // Merge successful
                }
                // TODO: propagate better LakeFS errors.
                Err(TransactionError::VersionAlreadyExists(_)) => {
                    return Err(DeltaTableError::Transaction {
                        source: TransactionError::LogStoreError {
                            msg: "Merge Failed".to_string(),
                            source: Box::new(DeltaTableError::generic("Merge Failed")),
                        },
                    });
                }
                Err(err) => return Err(DeltaTableError::Transaction { source: err }),
            };
        } else {
            debug!("No changes detected, skipping merge");
        }

        // Always delete the transaction branch when done
        let (repo, _, _) = self.client.decompose_url(self.config.location.to_string());
        self.client
            .delete_branch(repo, self.client.get_transaction(operation_id)?)
            .await?;

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
        read_commit_entry(
            &self.prefixed_registry.get_store(&self.config.location)?,
            version,
        )
        .await
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
        let (transaction_url, store, _root_store) = self
            .get_transaction_objectstore(operation_id)
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

    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        match operation_id {
            Some(id) => {
                let (_, store, _) = self.get_transaction_objectstore(id).unwrap_or_else(|_| panic!("The object_store registry inside LakeFSLogstore didn't have a store for operation_id {id} Something went wrong."));
                store
            }
            _ => self
                .prefixed_registry
                .get_store(&self.config.location)
                .unwrap(),
        }
    }

    fn root_object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        match operation_id {
            Some(id) => {
                let (_, _, root_store) = self.get_transaction_objectstore(id).unwrap_or_else(|_| panic!("The object_store registry inside LakeFSLogstore didn't have a store for operation_id {id} Something went wrong."));
                root_store
            }
            _ => self.root_registry.get_store(&self.config.location).unwrap(),
        }
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }

    fn transaction_url(&self, operation_id: Uuid, base: &Url) -> DeltaResult<Url> {
        self.get_transaction_url(operation_id, base.to_string())
    }
}

fn put_options() -> &'static PutOptions {
    static PUT_OPTS: OnceLock<PutOptions> = OnceLock::new();
    PUT_OPTS.get_or_init(|| PutOptions {
        mode: object_store::PutMode::Create, // Creates if file doesn't exists yet
        tags: Default::default(),
        attributes: Default::default(),
        extensions: Default::default(),
    })
}
