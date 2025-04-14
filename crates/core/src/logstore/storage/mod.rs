//! Object storage backend abstraction layer for Delta Table transaction logs and data
use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore};
use url::Url;

use super::config;
use crate::{DeltaResult, DeltaTableError};

pub use retry_ext::ObjectStoreRetryExt;
pub use runtime::{DeltaIOStorageBackend, IORuntime};

#[cfg(feature = "delta-cache")]
pub(super) mod cache;
pub(super) mod retry_ext;
pub(super) mod runtime;
pub(super) mod utils;

static DELTA_LOG_PATH: LazyLock<Path> = LazyLock::new(|| Path::from("_delta_log"));

/// Sharable reference to [`ObjectStore`]
pub type ObjectStoreRef = Arc<DynObjectStore>;

pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static + Clone {
    /// If a store with the same key existed before, it is replaced and returned
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>>;

    /// Get a suitable store for the provided URL. For example:
    /// If no [`ObjectStore`] found for the `url`, ad-hoc discovery may be executed depending on
    /// the `url` and [`ObjectStoreRegistry`] implementation. An [`ObjectStore`] may be lazily
    /// created and registered.
    fn get_store(&self, url: &Url) -> DeltaResult<Arc<dyn ObjectStore>>;

    fn all_stores(&self) -> &DashMap<String, Arc<dyn ObjectStore>>;
}

/// The default [`ObjectStoreRegistry`]
#[derive(Clone)]
pub struct DefaultObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    object_stores: DashMap<String, Arc<dyn ObjectStore>>,
}

impl Default for DefaultObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultObjectStoreRegistry {
    pub fn new() -> Self {
        let object_stores: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        Self { object_stores }
    }
}

impl std::fmt::Debug for DefaultObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DefaultObjectStoreRegistry")
            .field(
                "schemes",
                &self
                    .object_stores
                    .iter()
                    .map(|o| o.key().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ObjectStoreRegistry for DefaultObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.object_stores.insert(url.to_string(), store)
    }

    fn get_store(&self, url: &Url) -> DeltaResult<Arc<dyn ObjectStore>> {
        self.object_stores
            .get(&url.to_string())
            .map(|o| Arc::clone(o.value()))
            .ok_or_else(|| {
                DeltaTableError::generic(format!(
                    "No suitable object store found for {url}. See `RuntimeEnv::register_object_store`"
                ))
            })
    }

    fn all_stores(&self) -> &DashMap<String, Arc<dyn ObjectStore>> {
        &self.object_stores
    }
}

#[derive(Debug, Clone, Default)]
pub struct LimitConfig {
    pub max_concurrency: Option<usize>,
}

impl config::TryUpdateKey for LimitConfig {
    fn try_update_key(&mut self, key: &str, v: &str) -> DeltaResult<Option<()>> {
        match key {
            // The number of concurrent connections the underlying object store can create
            // Reference [LimitStore](https://docs.rs/object_store/latest/object_store/limit/struct.LimitStore.html)
            // for more information
            "OBJECT_STORE_CONCURRENCY_LIMIT" | "concurrency_limit" => {
                self.max_concurrency = Some(config::parse_usize(v)?);
            }
            _ => return Ok(None),
        }
        Ok(Some(()))
    }
}
