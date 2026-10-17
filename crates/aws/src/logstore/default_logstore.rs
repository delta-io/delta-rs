//! Default implementation of [`LogStore`] for S3 storage backends

use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::logstore::*;
use deltalake_core::{DeltaResult, kernel::Version, logstore::ObjectStoreRef};
use object_store::ObjectStore;
use url::Url;

/// Return the [S3LogStore] implementation with the provided configuration options
pub fn default_s3_logstore(
    store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    location: &Url,
    options: &StorageConfig,
) -> Arc<dyn LogStore> {
    Arc::new(S3LogStore::new(
        store,
        root_store,
        LogStoreConfig::new(location, options.clone()),
    ))
}

/// Default [`LogStore`] implementation
///
/// Commits are written as a temporary file first and moved into place with a
/// rename-if-not-exists, which the S3 object store provides through its locking client.
#[derive(Debug, Clone)]
pub struct S3LogStore {
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
}

impl S3LogStore {
    /// Create a new instance of [`S3LogStore`]
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
    ) -> Self {
        Self {
            prefixed_store,
            root_store,
            config,
        }
    }
}

#[async_trait::async_trait]
impl LogStore for S3LogStore {
    fn name(&self) -> String {
        "S3LogStore".into()
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

    /// Commits move a temporary commit file into place with rename-if-not-exists.
    fn committer(&self) -> Arc<dyn Committer> {
        Arc::new(FileSystemCommitter::new(
            self.prefixed_store.clone(),
            CommitStrategy::TmpCommit,
        ))
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}
