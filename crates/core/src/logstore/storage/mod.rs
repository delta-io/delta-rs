//! Object storage backend abstraction layer for Delta Table transaction logs and data
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, OnceLock};

use dashmap::DashMap;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore, RetryConfig};
use url::Url;

use super::{config, StorageConfig};
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

/// Factory trait for creating [ObjectStoreRef] instances at runtime
pub trait ObjectStoreFactory: Send + Sync {
    /// Parse URL options and create an object store instance.
    ///
    /// The object store instance returned by this method must point at the root of the storage location.
    /// Root in this case means scheme, authority/host and maybe port.
    /// The path segment is returned as second element of the tuple. It must point at the path
    /// corresponding to the path segment of the URL.
    ///
    /// The store should __NOT__ apply the decorations via the passed `StorageConfig`
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &HashMap<String, String>,
        retry: &RetryConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)>;
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DefaultObjectStoreFactory {}

impl ObjectStoreFactory for DefaultObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &HashMap<String, String>,
        _retry: &RetryConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        match url.scheme() {
            "memory" | "file" => {
                let (store, path) = object_store::parse_url_opts(url, options)?;
                Ok((Arc::new(store), path))
            }
            _ => Err(DeltaTableError::InvalidTableLocation(url.clone().into())),
        }
    }
}

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

/// TODO
pub type FactoryRegistry = Arc<DashMap<Url, Arc<dyn ObjectStoreFactory>>>;

/// TODO
pub fn factories() -> FactoryRegistry {
    static REGISTRY: OnceLock<FactoryRegistry> = OnceLock::new();
    REGISTRY
        .get_or_init(|| {
            let registry = FactoryRegistry::default();
            registry.insert(
                Url::parse("memory://").unwrap(),
                Arc::new(DefaultObjectStoreFactory::default()),
            );
            registry.insert(
                Url::parse("file://").unwrap(),
                Arc::new(DefaultObjectStoreFactory::default()),
            );
            registry
        })
        .clone()
}

/// Simpler access pattern for the [FactoryRegistry] to get a single store
pub fn store_for<K, V, I>(url: &Url, options: I) -> DeltaResult<ObjectStoreRef>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    let scheme = Url::parse(&format!("{}://", url.scheme())).unwrap();
    let storage_config = StorageConfig::parse_options(options)?;
    if let Some(factory) = factories().get(&scheme) {
        let (store, _prefix) =
            factory.parse_url_opts(url, &storage_config.raw, &storage_config.retry)?;
        let store = storage_config.decorate_store(store, url, None)?;
        Ok(Arc::new(store))
    } else {
        Err(DeltaTableError::InvalidTableLocation(url.clone().into()))
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
