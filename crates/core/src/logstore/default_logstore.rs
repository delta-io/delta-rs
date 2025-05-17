//! Default implementation of [`LogStore`] for storage backends with atomic put-if-absent operation

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::{engine::default::DefaultEngine, Engine, Table};
use object_store::{Attributes, Error as ObjectStoreError, ObjectStore, PutOptions, TagSet};
use tracing::log::*;
use uuid::Uuid;

use super::storage::{utils::commit_uri_from_version, ObjectStoreRef};
use super::{CommitOrBytes, LogStore, LogStoreConfig};
use crate::kernel::transaction::TransactionError;
use crate::DeltaResult;

fn put_options() -> &'static PutOptions {
    static PUT_OPTS: OnceLock<PutOptions> = OnceLock::new();
    PUT_OPTS.get_or_init(|| PutOptions {
        mode: object_store::PutMode::Create, // Creates if file doesn't exists yet
        tags: TagSet::default(),
        attributes: Attributes::default(),
        extensions: Default::default(),
    })
}

/// Default [`LogStore`] implementation
#[derive(Clone)]
pub struct DefaultLogStore {
    pub(crate) storage: ObjectStoreRef,
    config: LogStoreConfig,
    /// Implementation of the [delta_kernel::Engine] which this [LogStore] implementation can defer
    /// to when necessary
    engine: Option<Arc<dyn Engine>>,
    table: Option<Table>,
}

impl std::fmt::Debug for DefaultLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("DefaultLogStore")
            .field("storage", &self.storage)
            .field("config", &self.config)
            .finish()
    }
}

impl DefaultLogStore {
    /// Create a new instance of [`DefaultLogStore`]
    ///
    /// # Arguments
    ///
    /// * `storage` - A shared reference to an [`object_store::ObjectStore`] with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `location` - A url corresponding to the storage location of `storage`.
    pub fn new(storage: ObjectStoreRef, config: LogStoreConfig) -> Self {
        // NOTE: There are a number of "sync" code paths currently in delta-rs which will cause
        // Handle::current() to panic. The synchronous poaths make it impossible to construct
        // an engine at the [DefaultLogStore] creation time, thus the Option<>
        //
        // The codepaths to logstores should likely be all async in the future since they are
        // unusable without an async runtime anyways
        let engine = match tokio::runtime::Handle::try_current() {
            Err(_) => None,
            Ok(handle) => {
                let engine: Arc<dyn Engine> = match handle.runtime_flavor() {
                    tokio::runtime::RuntimeFlavor::MultiThread => Arc::new(DefaultEngine::new(
                        storage.clone(),
                        Arc::new(TokioMultiThreadExecutor::new(handle)),
                    )),
                    tokio::runtime::RuntimeFlavor::CurrentThread => Arc::new(DefaultEngine::new(
                        storage.clone(),
                        Arc::new(TokioBackgroundExecutor::new()),
                    )),
                    err => panic!(
                        "Unsupported runtime flavor from tokio, this is fatal sorry! {err:?}"
                    ),
                };
                Some(engine)
            }
        };

        let table = match engine {
            None => None,
            Some(_) => Some(Table::new(
                url::Url::parse("nonexistent:///").expect("Failed to parse a contrived URL"),
            )),
        };

        Self {
            engine,
            storage,
            config,
            table,
        }
    }
}

#[async_trait::async_trait]
impl LogStore for DefaultLogStore {
    fn name(&self) -> String {
        "DefaultLogStore".into()
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        super::read_commit_entry(self.object_store(None).as_ref(), version).await
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
        _: Uuid,
    ) -> Result<(), TransactionError> {
        match commit_or_bytes {
            CommitOrBytes::LogBytes(log_bytes) => self
                .object_store(None)
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
        _: Uuid,
    ) -> Result<(), TransactionError> {
        match &commit_or_bytes {
            CommitOrBytes::LogBytes(_) => Ok(()),
            _ => unreachable!(), // Default log store should never get a tmp_commit, since this is for conditional put stores
        }
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        if let (Some(engine), Some(table)) = (self.engine.as_ref(), self.table.as_ref()) {
            if let Ok(snapshot) = table.snapshot(engine.as_ref(), None) {
                return Ok(snapshot.version() as i64);
            }
        }
        warn!("The kernel-based retrieval of get_latest_version() was not possible for some reason, falling back");
        super::get_latest_version(self, current_version).await
    }

    async fn get_earliest_version(&self, current_version: i64) -> DeltaResult<i64> {
        super::get_earliest_version(self, current_version).await
    }

    fn object_store(&self, _: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.storage.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use url::Url;

    #[tokio::test]
    async fn test_initialize_default_logstore() {
        let store = Arc::new(InMemory::default());
        let config = LogStoreConfig {
            location: Url::parse("s3://example").expect("Failed to parse a URL"),
            options: crate::logstore::StorageConfig::default(),
        };
        let logstore = DefaultLogStore::new(store, config);
        assert_eq!(logstore.name(), "DefaultLogStore");
    }

    /// This is a silly sanity check to make sure that the latest version can be gotten by the
    /// default log store under normal conditions
    #[tokio::test]
    async fn test_get_latest_version() -> DeltaResult<()> {
        let location = "../test/tests/data/simple_table";
        let store = Arc::new(LocalFileSystem::new_with_prefix(&location)?);
        let config = LogStoreConfig {
            location: Url::from_file_path(std::fs::canonicalize(location).unwrap()).unwrap(),
            options: crate::logstore::StorageConfig::default(),
        };
        let logstore = DefaultLogStore::new(store, config);
        assert_eq!(logstore.get_latest_version(0).await?, 4);
        Ok(())
    }
}
