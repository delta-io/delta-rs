//! Default implementation of [`LogStore`] for storage backends with atomic put-if-absent operation

use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;

use super::storage::ObjectStoreRef;
use super::{CommitStrategy, Committer, FileSystemCommitter, LogStore, LogStoreConfig};
use crate::DeltaResult;
use crate::kernel::Version;

/// Default [`LogStore`] implementation
#[derive(Debug, Clone)]
pub struct DefaultLogStore {
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
}

impl DefaultLogStore {
    /// Create a new instance of [`DefaultLogStore`]
    ///
    /// # Arguments
    ///
    /// * `prefixed_store` - A shared reference to an [`object_store::ObjectStore`] with "/"
    ///   pointing at delta table root (i.e. where `_delta_log` is located).
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
impl LogStore for DefaultLogStore {
    fn name(&self) -> String {
        "DefaultLogStore".into()
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        super::read_commit_entry(self.prefixed_store.as_ref(), version).await
    }

    async fn get_latest_version(&self, current_version: Version) -> DeltaResult<Version> {
        super::get_latest_version(self, current_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    /// Commits are put-if-absent writes of the commit bytes.
    fn committer(&self) -> Arc<dyn Committer> {
        Arc::new(FileSystemCommitter::new(
            self.prefixed_store.clone(),
            CommitStrategy::ConditionalPut,
        ))
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}
