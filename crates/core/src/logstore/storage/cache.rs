use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use deltalake_derive::DeltaConfig;
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

pub type DeltaLogCache = Arc<HybridCache<Url, Entry>>;
pub type LogCachePolicy = fn(&Url) -> bool;

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

#[derive(Clone, Debug, DeltaConfig)]
pub struct LogCacheConfig {
    /// Whether to enable caching of delta commit files.
    ///
    /// The cache will be enabled if this flag is set to true,
    /// or if any of the configuration keys for the cache have
    /// been explicitly set.
    pub enable_cache: bool,

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
            enable_cache: false,
            max_size_memory_mb: 64,
            max_size_disk_mb: 256,
            cache_directory: None,
        }
    }
}

impl LogCacheConfig {
    pub fn decorate<T: ObjectStore + Clone>(
        &self,
        store: T,
        table_root: url::Url,
        cache: DeltaLogCache,
        policy: Option<LogCachePolicy>,
    ) -> DeltaResult<CommitCacheObjectStore<T>> {
        Ok(CommitCacheObjectStore::new(
            store,
            table_root,
            cache,
            None,
            policy.unwrap_or(should_cache_delta_commit),
        ))
    }

    pub async fn try_decorate<T: ObjectStore + Clone>(
        &self,
        store: T,
        table_root: url::Url,
        policy: Option<LogCachePolicy>,
    ) -> DeltaResult<CommitCacheObjectStore<T>> {
        CommitCacheObjectStore::try_new(store, table_root, self, policy).await
    }
}

/// A cache entry when caching raw object store returns.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Entry {
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
/// This is __NOT__ a general purpose cache and is specifically designed to cache
/// the commit files of a Delta table. E.g. it is assumed that files written to
/// the object store are immutable and no attempt is made to invalidate the cache
/// when files are updated in the remote object store.
pub struct CommitCacheObjectStore<T: ObjectStore + Clone> {
    /// The wrapped object store.
    inner: T,

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
    #[allow(unused)]
    cache_dir: Option<TempDir>,

    policy: LogCachePolicy,
}

impl<T: ObjectStore + Clone> std::fmt::Debug for CommitCacheObjectStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitCacheObjectStore")
            .field("object_store", &self.inner)
            .finish()
    }
}

impl<T: ObjectStore + Clone> std::fmt::Display for CommitCacheObjectStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitCacheObjectStore({})", self.inner)
    }
}

impl<T: ObjectStore + Clone> CommitCacheObjectStore<T> {
    pub(super) fn new(
        inner: T,
        root_url: url::Url,
        cache: Arc<HybridCache<Url, Entry>>,
        cache_dir: Option<TempDir>,
        policy: LogCachePolicy,
    ) -> Self {
        Self {
            inner,
            cache,
            root_url,
            cache_dir,
            policy,
        }
    }

    /// Create a new conditionally cached object store.
    pub(super) async fn try_new(
        inner: T,
        root_url: url::Url,
        config: &LogCacheConfig,
        policy: Option<LogCachePolicy>,
    ) -> DeltaResult<CommitCacheObjectStore<T>> {
        let (path, cache_dir) = if let Some(dir) = &config.cache_directory {
            let path = std::fs::canonicalize(dir)?;
            (path, None)
        } else {
            let tmp_dir = tempfile::tempdir()?;
            (tmp_dir.path().to_path_buf(), Some(tmp_dir))
        };
        let cache = get_default_cache(config, path).await?;
        Ok(Self::new(
            inner,
            root_url,
            cache,
            cache_dir,
            policy.unwrap_or(should_cache_delta_commit),
        ))
    }

    fn cache_key(&self, location: &Path) -> ObjectStoreResult<Url> {
        Ok(self
            .root_url
            .join(location.as_ref())
            .map_err(|_| CachedStoreError::InvalidUrl(location.to_string()))?)
    }

    async fn get_opts_impl(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let cache_key = self.cache_key(location)?;

        if options.range.is_some() || !(self.policy)(&cache_key) || options.head {
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
            size: entry.data.len() as u64,
            e_tag: entry.e_tag,
            version: None,
        };
        let (range, data) = (0..entry.data.len() as u64, entry.data);
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
impl<T: ObjectStore + Clone> ObjectStore for CommitCacheObjectStore<T> {
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

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}

/// Determine if a path is a delta commit and should be cached.
// TODO: implement or upstream logic to identify deletion vectors
//       as they are also a promising caching candidate.
fn should_cache_delta_commit(path: &Url) -> bool {
    if path.scheme() == "file" {
        return false;
    }
    let Ok(Some(log_path)) = ParsedLogPath::try_from(path.clone()) else {
        return false;
    };
    match log_path.file_type {
        LogPathFileType::Commit | LogPathFileType::CompactedCommit { .. } => true,
        _ => false,
    }
}

pub async fn get_default_cache(
    config: &LogCacheConfig,
    cache_dir: std::path::PathBuf,
) -> DeltaResult<DeltaLogCache> {
    Ok(Arc::new(
        HybridCacheBuilder::new()
            .memory(config.max_size_memory_mb * 1024 * 1024)
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(cache_dir.as_path())
                    .with_capacity(config.max_size_disk_mb * 1024 * 1024),
            )
            .build()
            .await
            .map_err(|_| CachedStoreError::CacheInitialization)?,
    ))
}

#[cfg(test)]
mod tests {

    use object_store::local::LocalFileSystem;

    use super::*;

    pub fn path_from_version(version: i64) -> object_store::path::Path {
        let version = format!("_delta_log/{version:020}.json");
        object_store::path::Path::from(version)
    }

    // A policy that also caches local files.
    fn policy_for_tests(path: &Url) -> bool {
        let Ok(Some(log_path)) = ParsedLogPath::try_from(path.clone()) else {
            return false;
        };
        match log_path.file_type {
            LogPathFileType::Commit | LogPathFileType::CompactedCommit { .. } => true,
            _ => false,
        }
    }

    async fn get_stores(
        config: LogCacheConfig,
    ) -> (
        Arc<dyn ObjectStore>,
        CommitCacheObjectStore<Arc<dyn ObjectStore>>,
    ) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let root_url = url::Url::from_directory_path(tmp_dir.path()).unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap())
            as Arc<dyn ObjectStore>;

        let tmp_cache_dir = tempfile::tempdir().unwrap();
        let cache = get_default_cache(&config, tmp_cache_dir.path().into())
            .await
            .unwrap();

        let cc_store = CommitCacheObjectStore::new(
            store.clone(),
            root_url,
            cache.clone(),
            Some(tmp_dir),
            policy_for_tests,
        );
        (store, cc_store)
    }

    #[tokio::test]
    async fn test_cache_hits() {
        let config = LogCacheConfig::default();
        let (store, cc_store) = get_stores(config).await;

        let commit_path = path_from_version(0);
        let data = bytes::Bytes::from("delta");

        // write sone data
        cc_store
            .put(&commit_path, data.clone().into())
            .await
            .unwrap();

        //  and read it again.
        let data_1 = cc_store
            .get(&commit_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // first time reading
        assert_eq!(data_1, data);

        // delete the file on disk to make sure we are reading from the cache.
        store.head(&commit_path).await.unwrap();
        store.delete(&commit_path).await.unwrap();
        assert!(store.head(&commit_path).await.is_err());

        // read the data we've just proven is removed from the backing store.
        let data_2 = cc_store
            .get(&commit_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // finally should see a hit.
        assert_eq!(data_2, data);
    }

    #[tokio::test]
    async fn test_cache_dir() {
        let mut config = LogCacheConfig::default();
        config.max_size_memory_mb = 1;

        let (store, cc_store) = get_stores(config).await;

        let commit_path = path_from_version(0);
        let data = bytes::Bytes::from("delta");

        // write sone data
        cc_store
            .put(&commit_path, data.clone().into())
            .await
            .unwrap();

        //  and read it again.
        let data_1 = cc_store
            .get(&commit_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // first time reading
        assert_eq!(data_1, data);

        // delete the file on disk to make sure we are reading from the cache.
        store.head(&commit_path).await.unwrap();
        store.delete(&commit_path).await.unwrap();
        assert!(store.head(&commit_path).await.is_err());

        // read the data we've just proven is removed from the backing store.
        let data_2 = cc_store
            .get(&commit_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // finally should see a hit.
        assert_eq!(data_2, data);
    }
}
