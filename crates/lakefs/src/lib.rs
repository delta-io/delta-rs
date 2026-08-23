//! LakeFS and similar tooling for delta-rs
//!
//! This module also contains the [LakeFSLogStore] implementation for delta operations executed in transaction branches
//! where deltalake commits only happen when the branch can be safely merged.

pub mod client;
pub mod errors;
pub mod execute;
pub mod logstore;
pub mod storage;
use deltalake_core::DeltaResult;
use deltalake_core::logstore::{LogStore, LogStoreFactory, logstore_factories};
use deltalake_core::logstore::{ObjectStoreRef, StorageConfig, object_store_factories};
pub use execute::LakeFSCustomExecuteHandler;
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
