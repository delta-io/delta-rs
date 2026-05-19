mod config;
mod storage;

use std::sync::Arc;

use deltalake_core::logstore::{logstore_factories, object_store_factories};
use storage::{HfLogStoreFactory, HfObjectStoreFactory};
use url::Url;

/// Register the HuggingFace Hub storage and log-store factories for the `hf://` scheme.
///
/// Call this once at program start, or rely on automatic registration via the `hf`
/// feature flag in the top-level `deltalake` crate.
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let object_stores = Arc::new(HfObjectStoreFactory);
    let log_stores = Arc::new(HfLogStoreFactory);
    let url = Url::parse("hf://").unwrap();
    object_store_factories().insert(url.clone(), object_stores);
    logstore_factories().insert(url, log_stores);
}
