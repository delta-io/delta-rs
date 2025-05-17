use std::{fmt::Debug, sync::Arc};

use super::ObjectStoreRef;
use dashmap::DashMap;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, Error as ObjectStoreError, GetOptions, GetResult,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload,
    PutResult, Result as ObjectStoreResult,
};
use tokio::sync::Mutex;

#[async_trait::async_trait]
pub trait FileCachePolicy: Debug + Send + Sync + 'static {
    fn should_use_cache(&self, _location: &Path) -> bool {
        true
    }

    async fn should_update_cache(
        &self,
        cache_meta: &ObjectMeta,
        object_store: ObjectStoreRef,
    ) -> ObjectStoreResult<bool> {
        let meta = object_store.head(&cache_meta.location).await?;
        Ok(cache_meta.last_modified < meta.last_modified)
    }
}

#[derive(Debug)]
pub struct DefaultFileCachePolicy {}

#[async_trait::async_trait]
impl FileCachePolicy for DefaultFileCachePolicy {}

#[derive(Debug)]
pub struct FileCacheStorageBackend {
    inner: ObjectStoreRef,
    file_cache: Arc<LocalFileSystem>,
    cache_policy: Arc<dyn FileCachePolicy>,
    // Threads must hold the lock to download the file to cache to prevent
    // multiple threads from downloading the same file at the same time.
    in_progress_files: Arc<DashMap<Path, Arc<Mutex<()>>>>,
}

impl FileCacheStorageBackend {
    pub fn try_new(
        inner: ObjectStoreRef,
        path: impl AsRef<std::path::Path>,
        cache_policy: Arc<dyn FileCachePolicy>,
    ) -> ObjectStoreResult<Self> {
        Ok(Self {
            inner,
            file_cache: Arc::new(LocalFileSystem::new_with_prefix(path)?),
            cache_policy,
            in_progress_files: Arc::new(DashMap::new()),
        })
    }
}

impl std::fmt::Display for FileCacheStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FileCacheStorageBackend {{ inner: {:?}, file_backend: {:?} }}",
            self.inner, self.file_cache
        )
    }
}

impl FileCacheStorageBackend {
    async fn ensure_cache_populated(
        &self,
        location: &Path,
        options: &GetOptions,
    ) -> ObjectStoreResult<()> {
        // NOTE: LocalFileSystem has different support for various options, e.g. version.
        // I don't think they're used in delta-rs

        let in_progress_file = self
            .in_progress_files
            .entry(location.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = in_progress_file.lock().await;

        match self.file_cache.head(location).await {
            Ok(meta) => {
                if !self
                    .cache_policy
                    .should_update_cache(&meta, self.inner.clone())
                    .await?
                {
                    tracing::debug!("Cache is up to date: {location:?}");

                    self.in_progress_files.remove(location);
                    return Ok(());
                }
            }
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(err) => return Err(err),
        }

        tracing::debug!("Downloading file to cache: {location:?}");

        let options_without_range = GetOptions {
            range: None,
            ..options.clone()
        };

        let bytes = self
            .inner
            .get_opts(location, options_without_range)
            .await?
            .bytes()
            .await?;

        self.file_cache
            .put(location, PutPayload::from_bytes(bytes))
            .await?;

        tracing::debug!("Finished downloading file to cache: {location:?}");

        self.in_progress_files.remove(location);

        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for FileCacheStorageBackend {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        if !self.cache_policy.should_use_cache(location) {
            return self.inner.get_opts(location, options).await;
        }

        self.ensure_cache_populated(location, &options).await?;

        // NOTE: GetResult also contains meta and other attributes which may be different
        // when using a local cache.
        self.file_cache.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
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
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}
