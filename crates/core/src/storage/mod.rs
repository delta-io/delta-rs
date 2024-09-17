//! Object storage backend abstraction layer for Delta Table transaction logs and data
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::{DeltaResult, DeltaTableError};
use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::TryFutureExt;
use lazy_static::lazy_static;
use object_store::limit::LimitStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::prefix::PrefixStore;
use object_store::{GetOptions, PutOptions, PutPayload, PutResult};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder as RuntimeBuilder, Handle, Runtime};
use url::Url;

use bytes::Bytes;
use futures::stream::BoxStream;
pub use object_store;
pub use object_store::path::{Path, DELIMITER};
pub use object_store::{
    DynObjectStore, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result as ObjectStoreResult,
};
use object_store::{MultipartUpload, PutMultipartOpts};
pub use retry_ext::ObjectStoreRetryExt;
use std::ops::Range;
pub use utils::*;

pub mod file;
pub mod retry_ext;
pub mod utils;

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Creates static IO Runtime with optional configuration
fn io_rt(config: Option<&RuntimeConfig>) -> &Runtime {
    static IO_RT: OnceLock<Runtime> = OnceLock::new();
    IO_RT.get_or_init(|| {
        let rt = match config {
            Some(config) => {
                let mut builder = if config.multi_threaded {
                    RuntimeBuilder::new_multi_thread()
                } else {
                    RuntimeBuilder::new_current_thread()
                };
                let mut builder = builder.worker_threads(config.worker_threads);
                let mut builder = if config.enable_io && config.enable_time {
                    builder.enable_all()
                } else if !config.enable_io && config.enable_time {
                    builder.enable_time()
                } else {
                    builder
                };
                #[cfg(unix)]
                {
                    if config.enable_io && !config.enable_time {
                        builder = builder.enable_io();
                    }
                }
                builder
                    .thread_name(
                        config
                            .thread_name
                            .clone()
                            .unwrap_or("IO-runtime".to_string()),
                    )
                    .build()
            }
            _ => Runtime::new(),
        };
        rt.expect("Failed to create a tokio runtime for IO.")
    })
}

/// Configuration for Tokio runtime
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    multi_threaded: bool,
    worker_threads: usize,
    thread_name: Option<String>,
    enable_io: bool,
    enable_time: bool,
}

/// Provide custom Tokio RT or a runtime config
#[derive(Debug, Clone)]
pub enum IORuntime {
    /// Tokio RT handle
    RT(Handle),
    /// Configuration for tokio runtime
    Config(RuntimeConfig),
}

impl Default for IORuntime {
    fn default() -> Self {
        IORuntime::RT(io_rt(None).handle().clone())
    }
}

impl IORuntime {
    /// Retrieves the Tokio runtime for IO bound operations
    pub fn get_handle(&self) -> Handle {
        match self {
            IORuntime::RT(handle) => handle,
            IORuntime::Config(config) => io_rt(Some(config)).handle(),
        }
        .clone()
    }
}

/// Wraps any object store and runs IO in it's own runtime [EXPERIMENTAL]
pub struct DeltaIOStorageBackend {
    inner: ObjectStoreRef,
    rt_handle: Handle,
}

impl DeltaIOStorageBackend {
    /// create wrapped object store which spawns tasks in own runtime
    pub fn new(storage: ObjectStoreRef, rt_handle: Handle) -> Self {
        Self {
            inner: storage,
            rt_handle,
        }
    }

    /// spawn taks on IO runtime
    pub fn spawn_io_rt<F, O>(
        &self,
        f: F,
        store: &Arc<dyn ObjectStore>,
        path: Path,
    ) -> BoxFuture<'_, ObjectStoreResult<O>>
    where
        F: for<'a> FnOnce(
                &'a Arc<dyn ObjectStore>,
                &'a Path,
            ) -> BoxFuture<'a, ObjectStoreResult<O>>
            + Send
            + 'static,
        O: Send + 'static,
    {
        let store = Arc::clone(store);
        let fut = self.rt_handle.spawn(async move { f(&store, &path).await });
        fut.unwrap_or_else(|e| match e.try_into_panic() {
            Ok(p) => std::panic::resume_unwind(p),
            Err(e) => Err(ObjectStoreError::JoinError { source: e }),
        })
        .boxed()
    }

    /// spawn taks on IO runtime
    pub fn spawn_io_rt_from_to<F, O>(
        &self,
        f: F,
        store: &Arc<dyn ObjectStore>,
        from: Path,
        to: Path,
    ) -> BoxFuture<'_, ObjectStoreResult<O>>
    where
        F: for<'a> FnOnce(
                &'a Arc<dyn ObjectStore>,
                &'a Path,
                &'a Path,
            ) -> BoxFuture<'a, ObjectStoreResult<O>>
            + Send
            + 'static,
        O: Send + 'static,
    {
        let store = Arc::clone(store);
        let fut = self
            .rt_handle
            .spawn(async move { f(&store, &from, &to).await });
        fut.unwrap_or_else(|e| match e.try_into_panic() {
            Ok(p) => std::panic::resume_unwind(p),
            Err(e) => Err(ObjectStoreError::JoinError { source: e }),
        })
        .boxed()
    }
}

impl std::fmt::Debug for DeltaIOStorageBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "DeltaIOStorageBackend")
    }
}

impl std::fmt::Display for DeltaIOStorageBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "DeltaIOStorageBackend")
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeltaIOStorageBackend {
    async fn put(&self, location: &Path, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.spawn_io_rt(
            |store, path| store.put(path, bytes),
            &self.inner,
            location.clone(),
        )
        .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.spawn_io_rt(
            |store, path| store.put_opts(path, bytes, options),
            &self.inner,
            location.clone(),
        )
        .await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.spawn_io_rt(|store, path| store.get(path), &self.inner, location.clone())
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.spawn_io_rt(
            |store, path| store.get_opts(path, options),
            &self.inner,
            location.clone(),
        )
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        self.spawn_io_rt(
            |store, path| store.get_range(path, range),
            &self.inner,
            location.clone(),
        )
        .await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.spawn_io_rt(
            |store, path| store.head(path),
            &self.inner,
            location.clone(),
        )
        .await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.spawn_io_rt(
            |store, path| store.delete(path),
            &self.inner,
            location.clone(),
        )
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.spawn_io_rt_from_to(
            |store, from_path, to_path| store.copy(from_path, to_path),
            &self.inner,
            from.clone(),
            to.clone(),
        )
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.spawn_io_rt_from_to(
            |store, from_path, to_path| store.copy_if_not_exists(from_path, to_path),
            &self.inner,
            from.clone(),
            to.clone(),
        )
        .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.spawn_io_rt_from_to(
            |store, from_path, to_path| store.rename_if_not_exists(from_path, to_path),
            &self.inner,
            from.clone(),
            to.clone(),
        )
        .await
    }

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.spawn_io_rt(
            |store, path| store.put_multipart(path),
            &self.inner,
            location.clone(),
        )
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.spawn_io_rt(
            |store, path| store.put_multipart_opts(path, options),
            &self.inner,
            location.clone(),
        )
        .await
    }
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
