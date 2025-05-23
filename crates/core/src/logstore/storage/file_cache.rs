use deltalake_derive::DeltaConfig;
use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::{table::builder::ensure_table_uri, DeltaResult, DeltaTableError};

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
struct DeltaFileCachePolicy {
    last_checkpoint_valid_duration: Duration,
}

impl DeltaFileCachePolicy {
    pub fn new(last_checkpoint_valid_duration: Duration) -> Self {
        Self {
            last_checkpoint_valid_duration,
        }
    }
}

#[async_trait::async_trait]
impl FileCachePolicy for DeltaFileCachePolicy {
    async fn should_update_cache(
        &self,
        cache_meta: &ObjectMeta,
        object_store: ObjectStoreRef,
    ) -> object_store::Result<bool> {
        let location = &cache_meta.location;
        if location.filename() == Some("_last_checkpoint") {
            let meta = object_store.head(location).await?;
            Ok(cache_meta.last_modified + self.last_checkpoint_valid_duration < meta.last_modified)
        } else {
            // Other delta files are always immutable, so no need to update cache
            Ok(false)
        }
    }
}

#[derive(Debug, Clone, Default, DeltaConfig)]
pub struct FileCacheConfig {
    /// The path to the file cache.
    #[delta(alias = "file_cache_path", env = "FILE_CACHE_PATH")]
    pub path: String,

    /// The duration for which the file cache is valid for _last_checkpoint files.
    /// If not set, it defaults to zero, which means the cache is not used.
    #[delta(
        alias = "file_cache_last_checkpoint_valid_duration",
        env = "FILE_CACHE_LAST_CHECKPOINT_VALID_DURATION"
    )]
    pub last_checkpoint_valid_duration: Option<Duration>,
}

/// Decorates the given object store with a file cache.
pub fn decorate_store(
    store: ObjectStoreRef,
    config: &FileCacheConfig,
) -> DeltaResult<Box<dyn ObjectStore>> {
    let location = &config.path;
    let path = ensure_table_uri(location)?.to_file_path().map_err(|_| {
        DeltaTableError::generic(format!(
            "Expected file_cache_path to be a valid file path: {location}",
        ))
    })?;

    let last_checkpoint_valid_duration = config
        .last_checkpoint_valid_duration
        .unwrap_or(Duration::ZERO);

    Ok(Box::new(FileCacheStorageBackend::try_new(
        store,
        path,
        Arc::new(DeltaFileCachePolicy::new(last_checkpoint_valid_duration)),
    )?))
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logstore::storage::ObjectStoreRef;
    use object_store::memory::InMemory;
    use object_store::{path::Path, ObjectMeta, ObjectStore};
    use std::sync::Arc;
    use tempfile::tempdir;

    // Helper struct to count the number of times get is called for each file
    #[derive(Debug)]
    struct CountingObjectStore {
        inner: ObjectStoreRef,
        get_counts: Arc<DashMap<Path, usize>>,
    }

    impl std::fmt::Display for CountingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "CountingObjectStore {{ get_count: {:?} }}",
                self.get_counts
            )
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            let mut entry = self.get_counts.entry(location.to_owned()).or_insert(0);
            *entry += 1;

            self.inner.get_opts(location, options).await
        }

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

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
            self.inner.copy(from, to).await
        }

        async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
            self.inner.delete(location).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[tokio::test]
    async fn test_file_cached_on_get() {
        let inner_store = Arc::new(InMemory::new());
        let cache_dir = tempdir().unwrap();
        let file_cache_store = Arc::new(
            FileCacheStorageBackend::try_new(
                inner_store.clone(),
                cache_dir.path(),
                Arc::new(DefaultFileCachePolicy {}),
            )
            .unwrap(),
        );

        let file_path = Path::from("test_file.txt");
        let file_content = b"hello world";

        // Put file in inner store
        inner_store
            .put(&file_path, file_content.to_vec().into())
            .await
            .unwrap();

        // Get file, which should cache it
        let result = file_cache_store.get(&file_path).await.unwrap();
        let data = result.bytes().await.unwrap();
        assert_eq!(data, file_content.to_vec());

        // Check if file is in cache, and content is correct
        let cache_file_path = cache_dir.path().join(file_path.as_ref());
        assert!(cache_file_path.exists());

        let cached_data = tokio::fs::read(&cache_file_path).await.unwrap();
        assert_eq!(cached_data, file_content);
    }

    #[tokio::test]
    async fn test_should_only_cache_files_matching_policy() {
        let inner_store = Arc::new(CountingObjectStore {
            inner: Arc::new(InMemory::new()),
            get_counts: Arc::new(DashMap::new()),
        });
        let cache_dir = tempdir().unwrap();

        #[derive(Debug)]
        struct CacheSpecificFilePolicy {}

        #[async_trait::async_trait]
        impl FileCachePolicy for CacheSpecificFilePolicy {
            fn should_use_cache(&self, location: &Path) -> bool {
                location.to_string() == "test_file_cache.txt"
            }
        }

        let file_cache_store = Arc::new(
            FileCacheStorageBackend::try_new(
                inner_store.clone(),
                cache_dir.path(),
                Arc::new(CacheSpecificFilePolicy {}),
            )
            .unwrap(),
        );

        let file_path = Path::from("test_file_no_cache.txt");
        let file_path_cached = Path::from("test_file_cache.txt");
        let file_content = b"should not be cached";
        let file_content_cached = b"should be cached";

        inner_store
            .put(&file_path, file_content.to_vec().into())
            .await
            .unwrap();

        inner_store
            .put(&file_path_cached, file_content_cached.to_vec().into())
            .await
            .unwrap();

        // Check file that should not be cached
        let data = file_cache_store
            .get(&file_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, file_content.to_vec());

        // Verify inner store was accessed once
        assert_eq!(*inner_store.get_counts.get(&file_path).unwrap().value(), 1);

        // Check no file has been saved to cache
        let cache_file_path = cache_dir.path().join(file_path.as_ref());
        assert!(!cache_file_path.exists());

        // Try accessing it again.
        let data = file_cache_store
            .get(&file_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, file_content.to_vec());

        // Verify inner store was accessed again
        assert_eq!(*inner_store.get_counts.get(&file_path).unwrap().value(), 2);

        // Get file that should be cached
        let data = file_cache_store
            .get(&file_path_cached)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, file_content_cached.to_vec());

        // Verify inner store was accessed once
        assert_eq!(
            *inner_store
                .get_counts
                .get(&file_path_cached)
                .unwrap()
                .value(),
            1
        );

        let cache_file_path = cache_dir.path().join(file_path_cached.as_ref());
        assert!(cache_file_path.exists());

        // Try accessing it again.
        let data = file_cache_store
            .get(&file_path_cached)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, file_content_cached.to_vec());

        // Verify inner store was not accessed again
        assert_eq!(
            *inner_store
                .get_counts
                .get(&file_path_cached)
                .unwrap()
                .value(),
            1
        );
    }
}
