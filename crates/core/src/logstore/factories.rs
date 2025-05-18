use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use dashmap::DashMap;
use object_store::RetryConfig;
use object_store::{path::Path, DynObjectStore};
use tokio::runtime::Handle;
use url::Url;

use super::{default_logstore, DeltaIOStorageBackend, LogStore, ObjectStoreRef, StorageConfig};
use crate::{DeltaResult, DeltaTableError};

/// Factory registry to manage [`ObjectStoreFactory`] instances
pub type ObjectStoreFactoryRegistry = Arc<DashMap<Url, Arc<dyn ObjectStoreFactory>>>;

/// Factory trait for creating [`ObjectStore`](::object_store::ObjectStore) instances at runtime
pub trait ObjectStoreFactory: Send + Sync {
    /// Parse URL options and create an object store instance.
    ///
    /// The object store instance returned by this method must point at the root of the storage location.
    /// Root in this case means scheme, authority/host and maybe port.
    /// The path segment is returned as second element of the tuple. It must point at the path
    /// corresponding to the path segment of the URL.
    ///
    /// The store should __NOT__ apply the decorations via the passed `options`
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &HashMap<String, String>,
        retry: &RetryConfig,
        handle: Option<Handle>,
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
        handle: Option<Handle>,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let (mut store, path) = default_parse_url_opts(url, options)?;

        if let Some(handle) = handle {
            store = Arc::new(DeltaIOStorageBackend::new(store, handle)) as Arc<DynObjectStore>;
        }
        Ok((store, path))
    }
}

fn default_parse_url_opts(
    url: &Url,
    options: &HashMap<String, String>,
) -> DeltaResult<(ObjectStoreRef, Path)> {
    match url.scheme() {
        "memory" | "file" => {
            let (store, path) = object_store::parse_url_opts(url, options)?;
            tracing::debug!("building store with:\n\tParsed URL: {url}\n\tPath in store: {path}");
            Ok((Arc::new(store), path))
        }
        _ => Err(DeltaTableError::InvalidTableLocation(url.clone().into())),
    }
}

/// Access global registry of object store factories
pub fn object_store_factories() -> ObjectStoreFactoryRegistry {
    static REGISTRY: OnceLock<ObjectStoreFactoryRegistry> = OnceLock::new();
    let factory = Arc::new(DefaultObjectStoreFactory::default());
    REGISTRY
        .get_or_init(|| {
            let registry = ObjectStoreFactoryRegistry::default();
            registry.insert(Url::parse("memory://").unwrap(), factory.clone());
            registry.insert(Url::parse("file://").unwrap(), factory);
            registry
        })
        .clone()
}

/// Simpler access pattern for the [ObjectStoreFactoryRegistry] to get a single store
pub fn store_for<K, V, I>(url: &Url, options: I) -> DeltaResult<ObjectStoreRef>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    let scheme = Url::parse(&format!("{}://", url.scheme())).unwrap();
    let storage_config = StorageConfig::parse_options(options)?;
    if let Some(factory) = object_store_factories().get(&scheme) {
        let (store, _prefix) =
            factory.parse_url_opts(url, &storage_config.raw, &storage_config.retry, None)?;
        let store = storage_config.decorate_store(store, url)?;
        Ok(Arc::new(store))
    } else {
        Err(DeltaTableError::InvalidTableLocation(url.clone().into()))
    }
}

/// Registry of [`LogStoreFactory`] instances
pub type LogStoreFactoryRegistry = Arc<DashMap<Url, Arc<dyn LogStoreFactory>>>;

/// Trait for generating [LogStore] implementations
pub trait LogStoreFactory: Send + Sync {
    /// Create a new [`LogStore`] from options.
    ///
    /// This method is responsible for creating a new instance of the [LogStore] implementation.
    ///
    /// ## Parameters
    /// - `prefixed_store`: A reference to the object store.
    /// - `location`: A reference to the URL of the location.
    /// - `options`: A reference to the storage configuration options.
    ///
    /// It returns a [DeltaResult] containing an [Arc] to the newly created [LogStore] implementation.
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>>;
}

#[derive(Clone, Debug, Default)]
struct DefaultLogStoreFactory {}

impl LogStoreFactory for DefaultLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Access global registry of logstore factories.
pub fn logstore_factories() -> LogStoreFactoryRegistry {
    static REGISTRY: OnceLock<LogStoreFactoryRegistry> = OnceLock::new();
    REGISTRY
        .get_or_init(|| {
            let registry = LogStoreFactoryRegistry::default();
            registry.insert(
                Url::parse("memory://").unwrap(),
                Arc::new(DefaultLogStoreFactory::default()),
            );
            registry.insert(
                Url::parse("file://").unwrap(),
                Arc::new(DefaultLogStoreFactory::default()),
            );
            registry
        })
        .clone()
}
