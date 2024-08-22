//! Default implementation of [`LogStore`] for storage backends with atomic put-if-absent operation

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use object_store::{Attributes, Error as ObjectStoreError, ObjectStore, PutOptions, TagSet};

use super::{CommitOrBytes, LogStore, LogStoreConfig};
use crate::{
    operations::transaction::TransactionError,
    storage::{commit_uri_from_version, ObjectStoreRef},
    DeltaResult,
};

fn put_options() -> &'static PutOptions {
    static PUT_OPTS: OnceLock<PutOptions> = OnceLock::new();
    PUT_OPTS.get_or_init(|| PutOptions {
        mode: object_store::PutMode::Create, // Creates if file doesn't exists yet
        tags: TagSet::default(),
        attributes: Attributes::default(),
    })
}

/// Default [`LogStore`] implementation
#[derive(Debug, Clone)]
pub struct DefaultLogStore {
    pub(crate) storage: Arc<dyn ObjectStore>,
    config: LogStoreConfig,
}

impl DefaultLogStore {
    /// Create a new instance of [`DefaultLogStore`]
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
impl LogStore for DefaultLogStore {
    fn name(&self) -> String {
        "DefaultLogStore".into()
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        super::read_commit_entry(self.storage.as_ref(), version).await
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
            CommitOrBytes::LogBytes(log_bytes) => self
                .object_store()
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
                })?,
            _ => unreachable!(), // Default log store should never get a tmp_commit, since this is for conditional put stores
        };
        Ok(())
    }

    async fn abort_commit_entry(
        &self,
        _version: i64,
        commit_or_bytes: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        match &commit_or_bytes {
            CommitOrBytes::LogBytes(_) => Ok(()),
            _ => unreachable!(), // Default log store should never get a tmp_commit, since this is for conditional put stores
        }
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        super::get_latest_version(self, current_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.storage.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}
