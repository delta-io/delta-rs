//! LakeFS support for delta-rs
//!
//! [`LakeFSLogStore`](logstore::LakeFSLogStore) runs every writing Delta operation on a hidden
//! LakeFS transaction branch and squash-merges that branch into the source branch once per Delta
//! commit. The branch is created by [`LogStore::begin_operation`] and released by core when the
//! operation finishes or fails, so callers need no extra wiring: a table opened with a
//! `lakefs://repo/branch/table` URL gets this behaviour for every operation.

pub mod client;
pub mod errors;
pub mod logstore;
pub mod storage;
pub mod transaction;
use deltalake_core::DeltaResult;
use deltalake_core::logstore::{LogStore, LogStoreFactory, logstore_factories};
use deltalake_core::logstore::{ObjectStoreRef, StorageConfig, object_store_factories};
use logstore::lakefs_logstore;
use std::sync::Arc;
use storage::LakeFSObjectStoreFactory;
use storage::S3StorageOptionsConversion;
use tracing::debug;
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct LakeFSLogStoreFactory {}

impl S3StorageOptionsConversion for LakeFSLogStoreFactory {}

impl LogStoreFactory for LakeFSLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        let options = StorageConfig::parse_options(self.with_env_s3(&config.raw.clone()))?;
        debug!("LakeFSLogStoreFactory has been asked to create a LogStore");
        lakefs_logstore(prefixed_store, root_store, location, &options)
    }
}

/// Register an [ObjectStoreFactory] for common LakeFS [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let object_stores = Arc::new(LakeFSObjectStoreFactory::default());
    let log_stores = Arc::new(LakeFSLogStoreFactory::default());
    let scheme = "lakefs";
    let url = Url::parse(&format!("{scheme}://")).unwrap();
    object_store_factories().insert(url.clone(), object_stores.clone());
    logstore_factories().insert(url.clone(), log_stores.clone());
}
