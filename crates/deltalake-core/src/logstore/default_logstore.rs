//! Default implementation of [`LogStore`] for storage backends with atomic put-if-absent operation

use std::sync::Arc;

use bytes::Bytes;
#[cfg(feature = "datafusion")]
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::{path::Path, ObjectStore};
use url::Url;

use super::{LogStore, LogStoreConfig};
use crate::{
    operations::transaction::TransactionError,
    storage::{
        config::{self, StorageOptions},
        ObjectStoreRef,
    },
    DeltaResult,
};

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

    /// Create log store
    pub fn try_new(location: Url, options: impl Into<StorageOptions> + Clone) -> DeltaResult<Self> {
        let mut options = options.into();
        let storage = config::configure_store(&location, &mut options)?;
        Ok(Self {
            storage: Arc::new(storage),
            config: LogStoreConfig { location, options },
        })
    }
}

#[async_trait::async_trait]
impl LogStore for DefaultLogStore {
    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Bytes> {
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
        tmp_commit: &Path,
    ) -> Result<(), TransactionError> {
        super::write_commit_entry(self.storage.as_ref(), version, tmp_commit).await
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        super::get_latest_version(self, current_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.storage.clone()
    }

    fn to_uri(&self, location: &Path) -> String {
        super::to_uri(&self.config.location, location)
    }

    #[cfg(feature = "datafusion")]
    fn object_store_url(&self) -> ObjectStoreUrl {
        super::object_store_url(&self.config.location)
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}
