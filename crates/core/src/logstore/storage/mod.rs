//! Object storage backend abstraction layer for Delta Table transaction logs and data
use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use deltalake_derive::DeltaConfig;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore};
use url::Url;

use crate::table::normalize_table_url;
use crate::{DeltaResult, DeltaTableError};

pub use retry_ext::ObjectStoreRetryExt;
pub use runtime::{DeltaIOStorageBackend, IORuntime};

pub(super) mod retry_ext;
pub(super) mod runtime;
pub(super) mod utils;

static DELTA_LOG_PATH: LazyLock<Path> = LazyLock::new(|| Path::from("_delta_log"));

/// Sharable reference to [`ObjectStore`]
pub type ObjectStoreRef = Arc<DynObjectStore>;

pub trait ObjectStoreRegistry: Send + Sync + std::fmt::Debug + 'static {
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
}

/// The default [`ObjectStoreRegistry`]
#[derive(Clone)]
pub struct DefaultObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    object_stores: DashMap<Url, Arc<dyn ObjectStore>>,
}

impl Default for DefaultObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultObjectStoreRegistry {
    pub fn new() -> Self {
        let object_stores: DashMap<Url, Arc<dyn ObjectStore>> = DashMap::new();
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
        self.object_stores.insert(normalize_table_url(url), store)
    }

    fn get_store(&self, url: &Url) -> DeltaResult<Arc<dyn ObjectStore>> {
        self.object_stores
            .get(&normalize_table_url(url))
            .map(|o| Arc::clone(o.value()))
            .ok_or_else(|| {
                DeltaTableError::generic(format!("No suitable object store found for '{url}'."))
            })
    }
}

#[derive(Debug, Clone, Default, DeltaConfig)]
pub struct LimitConfig {
    #[delta(alias = "concurrency_limit", env = "OBJECT_STORE_CONCURRENCY_LIMIT")]
    pub max_concurrency: Option<usize>,
}

#[cfg(test)]
#[allow(drop_bounds, unused)]
mod tests {
    use std::collections::HashMap;
    use std::env;

    use rstest::*;

    use super::*;
    use crate::logstore::config::TryUpdateKey;
    use crate::test_utils::with_env;

    #[test]
    fn test_limit_config() {
        let mut config = LimitConfig::default();
        assert!(config.max_concurrency.is_none());

        config.try_update_key("concurrency_limit", "10").unwrap();
        assert_eq!(config.max_concurrency, Some(10));

        config.try_update_key("max_concurrency", "20").unwrap();
        assert_eq!(config.max_concurrency, Some(20));
    }

    #[rstest]
    fn test_limit_config_env() {
        let _env = with_env(vec![("OBJECT_STORE_CONCURRENCY_LIMIT", "100")]);

        let mut config = LimitConfig::default();
        assert!(config.max_concurrency.is_none());

        config.load_from_environment().unwrap();
        assert_eq!(config.max_concurrency, Some(100));
    }

    #[rstest]
    fn test_limit_config_env_error() {
        let registry = DefaultObjectStoreRegistry::default();

        // try get non-existent key
        let url = Url::parse("not-registered://host").unwrap();
        let err = registry.get_store(&url).unwrap_err();
        assert!(
            err.to_string()
                .contains("No suitable object store found for 'not-registered://host'.")
        );
    }
}
