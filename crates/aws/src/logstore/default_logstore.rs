//! Default implementation of [`LogStore`] for S3 storage backends

use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::{
    logstore::{
        abort_commit_entry, get_latest_version, read_commit_entry, write_commit_entry,
        CommitOrBytes, LogStore, LogStoreConfig,
    },
    operations::transaction::TransactionError,
    storage::{ObjectStoreRef, StorageOptions},
    DeltaResult,
};
use object_store::{Error as ObjectStoreError, ObjectStore};
use url::Url;

/// Return the [S3LogStore] implementation with the provided configuration options
pub fn default_s3_logstore(
    store: ObjectStoreRef,
    location: &Url,
    options: &StorageOptions,
) -> Arc<dyn LogStore> {
    Arc::new(S3LogStore::new(
        store,
        LogStoreConfig {
            location: location.clone(),
            options: options.clone(),
        },
    ))
}

/// Default [`LogStore`] implementation
#[derive(Debug, Clone)]
pub struct S3LogStore {
    pub(crate) storage: Arc<dyn ObjectStore>,
    config: LogStoreConfig,
}

impl S3LogStore {
    /// Create a new instance of [`S3LogStore`]
    ///
    /// # Arguments
    ///
    /// * `storage` - A shared reference to an [`object_store::ObjectStore`] with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `location` - A url corresponding to the storage location of `storage`.
    pub fn new(storage: ObjectStoreRef, config: LogStoreConfig) -> Self {
        Self { storage, config }
    }
}

#[async_trait::async_trait]
impl LogStore for S3LogStore {
    fn name(&self) -> String {
        "S3LogStore".into()
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        read_commit_entry(self.storage.as_ref(), version).await
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
    ) -> Result<(), TransactionError> {
        match commit_or_bytes {
            CommitOrBytes::TmpCommit(tmp_commit) => {
                Ok(write_commit_entry(&self.object_store(), version, &tmp_commit).await?)
            }
            _ => unreachable!(), // S3 Log Store should never receive bytes
        }
        .map_err(|err| -> TransactionError {
            match err {
                ObjectStoreError::AlreadyExists { .. } => {
                    TransactionError::VersionAlreadyExists(version)
                }
                _ => TransactionError::from(err),
            }
        })?;
        Ok(())
    }

    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        match &commit_or_bytes {
            CommitOrBytes::TmpCommit(tmp_commit) => {
                abort_commit_entry(self.storage.as_ref(), version, tmp_commit).await
            }
            _ => unreachable!(), // S3 Log Store should never receive bytes
        }
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        get_latest_version(self, current_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.storage.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}
