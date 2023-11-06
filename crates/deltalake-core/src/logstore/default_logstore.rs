//! Default implementation of [`LogStore`] for storage backends with atomic put-if-absent operation

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use url::Url;

use super::LogStore;
use crate::{
    operations::transaction::TransactionError,
    storage::{config::StorageOptions, DeltaObjectStore, ObjectStoreRef},
    DeltaResult,
};

/// Default [`LogStore`] implementation
#[derive(Debug, Clone)]
pub struct DefaultLogStore {
    pub(crate) storage: ObjectStoreRef,
}

impl DefaultLogStore {
    /// Create log store
    pub fn try_new(location: Url, options: impl Into<StorageOptions> + Clone) -> DeltaResult<Self> {
        let object_store = DeltaObjectStore::try_new(location, options.clone())?;
        Ok(Self {
            storage: Arc::new(object_store),
        })
    }
}

#[async_trait::async_trait]
impl LogStore for DefaultLogStore {
    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Bytes> {
        super::read_commit_entry(self.storage.as_ref(), version).await
    }

    /// Tries to commit a prepared commit file. Returns [DeltaTableError::VersionAlreadyExists]
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    async fn write_commit_entry(
        &self,
        version: i64,
        tmp_commit: &Path,
    ) -> Result<(), TransactionError> {
        super::write_commit_entry(self.storage.as_ref(), version, tmp_commit).await
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        super::get_latest_version(&self.storage, current_version).await
    }

    fn object_store(&self) -> ObjectStoreRef {
        self.storage.clone()
    }
}
