//! Object storage backend abstraction layer for Delta Table transaction logs and data

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use url::Url;

pub mod file;
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
        _options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        match url.scheme() {
            "memory" => {
                let path = Path::from_url_path(url.path())?;
                let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new()) as ObjectStoreRef;
                Ok((url_prefix_handler(store, path.clone())?, path))
            }
            "file" => {
                let store = Arc::new(LocalFileSystem::new_with_prefix(
                    url.to_file_path().unwrap(),
                )?) as ObjectStoreRef;
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
/// This simplifies the use of t he storage since it ensures that list/get/etc operations
/// start from the prefix in the object storage rather than from the root configured URI of the
/// [ObjectStore]
pub fn url_prefix_handler<T: ObjectStore>(store: T, prefix: Path) -> DeltaResult<ObjectStoreRef> {
    if prefix != Path::from("/") {
        Ok(Arc::new(PrefixStore::new(store, prefix)))
    } else {
        Ok(Arc::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_prefix_handler() {
        let store = InMemory::new();
        let path = Path::parse("/databases/foo/bar").expect("Failed to parse path");

        let prefixed = url_prefix_handler(store, path);
        assert!(prefixed.is_ok());
    }
}
