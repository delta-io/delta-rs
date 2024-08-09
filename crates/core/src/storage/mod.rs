//! Object storage backend abstraction layer for Delta Table transaction logs and data

use dashmap::DashMap;
use object_store::limit::LimitStore;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use url::Url;

pub mod file;
pub mod retry_ext;
pub mod utils;

use crate::{DeltaResult, DeltaTableError};

pub use object_store;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
pub use object_store::path::{Path, DELIMITER};
use object_store::prefix::PrefixStore;
pub use object_store::{
    DynObjectStore, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result as ObjectStoreResult,
};
pub use retry_ext::ObjectStoreRetryExt;
pub use utils::*;

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Sharable reference to [`ObjectStore`]
pub type ObjectStoreRef = Arc<DynObjectStore>;

/// Factory trait for creating [ObjectStoreRef] instances at runtime
pub trait ObjectStoreFactory: Send + Sync {
    #[allow(missing_docs)]
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)>;
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DefaultObjectStoreFactory {}

impl ObjectStoreFactory for DefaultObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        match url.scheme() {
            "memory" => {
                let path = Path::from_url_path(url.path())?;
                let inner = Arc::new(InMemory::new()) as ObjectStoreRef;
                let store = limit_store_handler(url_prefix_handler(inner, path.clone()), options);
                Ok((store, path))
            }
            "file" => {
                let inner = Arc::new(LocalFileSystem::new_with_prefix(
                    url.to_file_path().unwrap(),
                )?) as ObjectStoreRef;
                let store = limit_store_handler(inner, options);
                Ok((store, Path::from("/")))
            }
            _ => Err(DeltaTableError::InvalidTableLocation(url.clone().into())),
        }
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
pub fn store_for(url: &Url) -> DeltaResult<ObjectStoreRef> {
    let scheme = Url::parse(&format!("{}://", url.scheme())).unwrap();
    if let Some(factory) = factories().get(&scheme) {
        let (store, _prefix) = factory.parse_url_opts(url, &StorageOptions::default())?;
        Ok(store)
    } else {
        Err(DeltaTableError::InvalidTableLocation(url.clone().into()))
    }
}

/// Options used for configuring backend storage
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct StorageOptions(pub HashMap<String, String>);

impl From<HashMap<String, String>> for StorageOptions {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

/// Return the uri of commit version.
///
/// ```rust
/// # use deltalake_core::storage::*;
/// use object_store::path::Path;
/// let uri = commit_uri_from_version(1);
/// assert_eq!(uri, Path::from("_delta_log/00000000000000000001.json"));
/// ```
pub fn commit_uri_from_version(version: i64) -> Path {
    let version = format!("{version:020}.json");
    DELTA_LOG_PATH.child(version.as_str())
}

/// Return true for all the stringly values typically associated with true
///
/// aka YAML booleans
///
/// ```rust
/// # use deltalake_core::storage::*;
/// for value in ["1", "true", "on", "YES", "Y"] {
///     assert!(str_is_truthy(value));
/// }
/// for value in ["0", "FALSE", "off", "NO", "n", "bork"] {
///     assert!(!str_is_truthy(value));
/// }
/// ```
pub fn str_is_truthy(val: &str) -> bool {
    val.eq_ignore_ascii_case("1")
        | val.eq_ignore_ascii_case("true")
        | val.eq_ignore_ascii_case("on")
        | val.eq_ignore_ascii_case("yes")
        | val.eq_ignore_ascii_case("y")
}

/// Simple function to wrap the given [ObjectStore] in a [PrefixStore] if necessary
///
/// This simplifies the use of the storage since it ensures that list/get/etc operations
/// start from the prefix in the object storage rather than from the root configured URI of the
/// [ObjectStore]
pub fn url_prefix_handler<T: ObjectStore>(store: T, prefix: Path) -> ObjectStoreRef {
    if prefix != Path::from("/") {
        Arc::new(PrefixStore::new(store, prefix))
    } else {
        Arc::new(store)
    }
}

/// Simple function to wrap the given [ObjectStore] in a [LimitStore] if configured
///
/// Limits the number of concurrent connections the underlying object store
/// Reference [LimitStore](https://docs.rs/object_store/latest/object_store/limit/struct.LimitStore.html) for more information
pub fn limit_store_handler<T: ObjectStore>(store: T, options: &StorageOptions) -> ObjectStoreRef {
    let concurrency_limit = options
        .0
        .get(storage_constants::OBJECT_STORE_CONCURRENCY_LIMIT)
        .and_then(|v| v.parse().ok());

    if let Some(limit) = concurrency_limit {
        Arc::new(LimitStore::new(store, limit))
    } else {
        Arc::new(store)
    }
}

/// Storage option keys to use when creating [ObjectStore].
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
/// Must be implemented for a given storage provider
pub mod storage_constants {

    /// The number of concurrent connections the underlying object store can create
    /// Reference [LimitStore](https://docs.rs/object_store/latest/object_store/limit/struct.LimitStore.html) for more information
    pub const OBJECT_STORE_CONCURRENCY_LIMIT: &str = "OBJECT_STORE_CONCURRENCY_LIMIT";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_prefix_handler() {
        let store = InMemory::new();
        let path = Path::parse("/databases/foo/bar").expect("Failed to parse path");

        let prefixed = url_prefix_handler(store, path.clone());

        assert_eq!(
            String::from("PrefixObjectStore(databases/foo/bar)"),
            format!("{prefixed}")
        );
    }

    #[test]
    fn test_limit_store_handler() {
        let store = InMemory::new();

        let options = StorageOptions(HashMap::from_iter(vec![(
            "OBJECT_STORE_CONCURRENCY_LIMIT".into(),
            "500".into(),
        )]));

        let limited = limit_store_handler(store, &options);

        assert_eq!(
            String::from("LimitStore(500, InMemory)"),
            format!("{limited}")
        );
    }
}
