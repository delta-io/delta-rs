use std::ops::Range;
use std::sync::OnceLock;

use bytes::Bytes;
use deltalake_derive::DeltaConfig;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::FutureExt;
use futures::TryFutureExt;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore,
    PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use object_store::{MultipartUpload, PutMultipartOpts};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder as RuntimeBuilder, Handle, Runtime};

/// Creates static IO Runtime with optional configuration
fn io_rt(config: Option<&RuntimeConfig>) -> &Runtime {
    static IO_RT: OnceLock<Runtime> = OnceLock::new();
    IO_RT.get_or_init(|| {
        let rt = match config {
            Some(config) => {
                let mut builder = if let Some(true) = config.multi_threaded {
                    RuntimeBuilder::new_multi_thread()
                } else {
                    RuntimeBuilder::new_current_thread()
                };

                if let Some(threads) = config.worker_threads {
                    builder.worker_threads(threads);
                }

                match (config.enable_io, config.enable_time) {
                    (Some(true), Some(true)) => {
                        builder.enable_all();
                    }
                    (Some(false), Some(true)) => {
                        builder.enable_time();
                    }
                    _ => (),
                };

                #[cfg(unix)]
                {
                    if let (Some(true), Some(false)) = (config.enable_io, config.enable_time) {
                        builder.enable_io();
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
#[derive(Debug, Clone, Serialize, Deserialize, Default, DeltaConfig)]
pub struct RuntimeConfig {
    /// Whether to use a multi-threaded runtime
    pub(crate) multi_threaded: Option<bool>,
    /// Number of worker threads to use
    pub(crate) worker_threads: Option<usize>,
    /// Name of the thread
    pub(crate) thread_name: Option<String>,
    /// Whether to enable IO
    pub(crate) enable_io: Option<bool>,
    /// Whether to enable time
    pub(crate) enable_time: Option<bool>,
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
#[derive(Clone)]
pub struct DeltaIOStorageBackend<T: ObjectStore + Clone> {
    pub inner: T,
    pub rt_handle: Handle,
}

impl<T> DeltaIOStorageBackend<T>
where
    T: ObjectStore + Clone,
{
    pub fn new(store: T, handle: Handle) -> Self {
        Self {
            inner: store,
            rt_handle: handle,
        }
    }
}

impl<T: ObjectStore + Clone> DeltaIOStorageBackend<T> {
    /// spawn tasks on IO runtime
    pub fn spawn_io_rt<F, O>(
        &self,
        f: F,
        store: &T,
        path: Path,
    ) -> BoxFuture<'_, ObjectStoreResult<O>>
    where
        F: for<'a> FnOnce(&'a T, &'a Path) -> BoxFuture<'a, ObjectStoreResult<O>> + Send + 'static,
        O: Send + 'static,
    {
        let store = store.clone();
        let fut = self.rt_handle.spawn(async move { f(&store, &path).await });
        fut.unwrap_or_else(|e| match e.try_into_panic() {
            Ok(p) => std::panic::resume_unwind(p),
            Err(e) => Err(ObjectStoreError::JoinError { source: e }),
        })
        .boxed()
    }

    /// spawn tasks on IO runtime
    pub fn spawn_io_rt_from_to<F, O>(
        &self,
        f: F,
        store: &T,
        from: Path,
        to: Path,
    ) -> BoxFuture<'_, ObjectStoreResult<O>>
    where
        F: for<'a> FnOnce(&'a T, &'a Path, &'a Path) -> BoxFuture<'a, ObjectStoreResult<O>>
            + Send
            + 'static,
        O: Send + 'static,
    {
        let store = store.clone();
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

impl<T: ObjectStore + Clone> std::fmt::Debug for DeltaIOStorageBackend<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "DeltaIOStorageBackend({:?})", self.inner)
    }
}

impl<T: ObjectStore + Clone> std::fmt::Display for DeltaIOStorageBackend<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "DeltaIOStorageBackend({})", self.inner)
    }
}

#[async_trait::async_trait]
impl<T: ObjectStore + Clone> ObjectStore for DeltaIOStorageBackend<T> {
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

    async fn get_range(&self, location: &Path, range: Range<u64>) -> ObjectStoreResult<Bytes> {
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

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
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
