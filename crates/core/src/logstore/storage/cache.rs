use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use url::Url;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::config;

pub(super) type DeltaLogCache = Arc<HybridCache<Url, Entry>>;

#[derive(thiserror::Error, Debug)]
enum CachedStoreError {
    #[error("IO error")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("Cache initialization error")]
    CacheInitialization,

    #[error("Failed to fetch cache entry")]
    CacheFetch,

    #[error("Invalid url: {0}")]
    InvalidUrl(String),
}

impl From<CachedStoreError> for DeltaTableError {
    fn from(err: CachedStoreError) -> Self {
        DeltaTableError::Generic(err.to_string())
    }
}

impl From<CachedStoreError> for ObjectStoreError {
    fn from(err: CachedStoreError) -> Self {
        ObjectStoreError::Generic {
            store: "delta-cache",
            source: Box::new(err),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogCacheConfig {
    /// Maximum size of the cache in memory in megabytes.
    ///
    /// Default is 64MB.
    pub max_size_memory_mb: usize,

    /// Maximum size of the cache on disk in megabytes.
    ///
    /// Default is 256MB.
    pub max_size_disk_mb: usize,

    /// Directory where the cache is stored.
    ///
    /// If this is not provided, a temporary directory will be created
    /// and deleted once the cache is dropped. When a valid directory is
    /// provided, the cache will be not be cleaned up and all housekeeping
    /// is the responsibility of the caller.
    pub cache_directory: Option<String>,
}

impl Default for LogCacheConfig {
    fn default() -> Self {
        Self {
            max_size_memory_mb: 64,
            max_size_disk_mb: 256,
            cache_directory: None,
        }
    }
}

impl config::TryUpdateKey for LogCacheConfig {
    fn try_update_key(&mut self, key: &str, v: &str) -> DeltaResult<Option<()>> {
        match key {
            "max_size_memory_mb" => self.max_size_memory_mb = config::parse_usize(v)?,
            "max_size_disk_mb" => self.max_size_disk_mb = config::parse_usize(v)?,
            "cache_directory" => self.cache_directory = Some(v.to_string()),
            _ => return Ok(None),
        }
        Ok(Some(()))
    }
}

async fn get_default_cache(
    config: LogCacheConfig,
) -> DeltaResult<(DeltaLogCache, Option<TempDir>), CachedStoreError> {
    let dir = tempfile::tempdir()?;

    Ok((
        Arc::new(
            HybridCacheBuilder::new()
                .memory(config.max_size_memory_mb * 1024 * 1024)
                .storage(Engine::Large)
                .with_device_options(
                    DirectFsDeviceOptions::new(dir.path())
                        .with_capacity(config.max_size_disk_mb * 1024 * 1024),
                )
                .build()
                .await
                .map_err(|_| CachedStoreError::CacheInitialization)?,
        ),
        Some(dir),
    ))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Entry {
    data: Bytes,
    last_modified: DateTime<Utc>,
    e_tag: Option<String>,
}

impl Entry {
    fn new(data: Bytes, last_modified: DateTime<Utc>, e_tag: Option<String>) -> Self {
        Self {
            data,
            last_modified,
            e_tag,
        }
    }
}

/// An object store implementation that conditionally caches file requests.
///
/// This implementation caches the file requests based on on the evaluation
/// of a condition. The condition is evaluated on the path of the file and
/// can be configured to meet the requirements of the user.
///
/// This is __not__ a general purpose cache and is specifically designed to cache
/// the commit files of a Delta table. E.g. it is assumed that files written to
/// the object store are immutable and no attempt is made to invalidate the cache
/// when files are updated in the remote object store.
pub(super) struct CommitCacheObjectStore {
    /// The wrapped object store.
    inner: Arc<dyn ObjectStore>,

    /// Hybrid cache that stores entries in memory and on disk.
    cache: DeltaLogCache,

    /// Fully qualified URL of the object store.
    ///
    /// This is required to avoid collisions with other cache files
    /// when the cache is used across multiple instances.
    root_url: url::Url,

    /// Directory where the cache files are stored.
    ///
    /// We need to keep a reference to the temporary directory to ensure that it is
    /// not deleted while the cache is still in use - i.e. TempDir will be deleted
    /// when the CommitCacheObjectStore is dropped.
    cache_dir: Option<TempDir>,
}

impl std::fmt::Debug for CommitCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitCacheObjectStore")
            .field("object_store", &self.inner)
            .finish()
    }
}

impl std::fmt::Display for CommitCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitCacheObjectStore({})", self.inner)
    }
}

fn cache_delta(path: &Url) -> bool {
    let Ok(Some(log_path)) = ParsedLogPath::try_from(path.clone()) else {
        return false;
    };
    match log_path.file_type {
        LogPathFileType::Commit | LogPathFileType::CompactedCommit { .. } => true,
        _ => false,
    }
}

impl CommitCacheObjectStore {
    pub(super) fn new(
        inner: Arc<dyn ObjectStore>,
        root_url: url::Url,
        cache: Arc<HybridCache<Url, Entry>>,
        cache_dir: Option<TempDir>,
    ) -> Self {
        Self {
            inner,
            cache,
            root_url,
            cache_dir,
        }
    }

    /// Create a new conditionally cached object store.
    pub(super) async fn try_new(
        inner: Arc<dyn ObjectStore>,
        root_url: url::Url,
        config: LogCacheConfig,
    ) -> DeltaResult<Self> {
        let (cache, cache_dir) = get_default_cache(config).await?;
        Ok(Self::new(inner, root_url, cache, cache_dir))
    }

    fn cache_key(&self, location: &Path) -> ObjectStoreResult<Url> {
        let full_path = self
            .root_url
            .join(location.as_ref())
            .map_err(|_| CachedStoreError::InvalidUrl(location.to_string()))?;
        Ok(full_path)
    }

    async fn get_opts_impl(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let cache_key = self.cache_key(location)?;

        if options.range.is_some() || !cache_delta(&cache_key) || options.head {
            return self.inner.get_opts(location, options).await;
        }

        let store = self.inner.clone();
        let loc = location.clone();

        let entry = self
            .cache
            .fetch(cache_key, || async move {
                let response = store.get_opts(&loc, options).await?;
                let meta = response.meta.clone();
                let data = response.bytes().await?;
                let entry = Entry::new(data, meta.last_modified, meta.e_tag);
                Ok(entry)
            })
            .await
            .map_err(|_| CachedStoreError::CacheFetch)?;
        let entry = entry.value().clone();

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: entry.last_modified,
            size: entry.data.len(),
            e_tag: entry.e_tag,
            version: None,
        };
        let (range, data) = (0..entry.data.len(), entry.data);
        let stream = futures::stream::once(futures::future::ready(Ok(data)));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.boxed()),
            attributes: Default::default(),
            meta,
            range,
        })
    }
}

#[async_trait::async_trait]
impl ObjectStore for CommitCacheObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.get_opts_impl(location, options).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.cache.remove(&self.cache_key(location)?);
        self.inner.delete(location).await
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
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.rename_if_not_exists(from, to).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}
