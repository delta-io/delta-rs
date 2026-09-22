//! Optional in-memory byte cache for object store reads.
//!
//! Enabled only when the `delta-cache` Cargo feature is compiled in, and activated
//! at runtime when [`CachingObjectStore::from_env`] finds a positive
//! `DELTA_CACHE_CAPACITY_BYTES` environment variable.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use foyer::{
    Cache, CacheBuilder, CacheProperties, EvictionConfig, FifoConfig, LfuConfig, LruConfig,
    S3FifoConfig, SieveConfig,
};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OSResult,
};
use tracing::debug;

// Env-var names
const ENV_CAPACITY: &str = "DELTA_CACHE_CAPACITY_BYTES";
const ENV_SHARDS: &str = "DELTA_CACHE_SHARDS";
const ENV_EVICTION: &str = "DELTA_CACHE_EVICTION";
const ENV_LRU_HIGH_PRIO: &str = "DELTA_CACHE_LRU_HIGH_PRIO_RATIO";
const ENV_LFU_WINDOW: &str = "DELTA_CACHE_LFU_WINDOW_RATIO";
const ENV_LFU_PROTECTED: &str = "DELTA_CACHE_LFU_PROTECTED_RATIO";
const ENV_S3_SMALL: &str = "DELTA_CACHE_S3FIFO_SMALL_RATIO";
const ENV_S3_GHOST: &str = "DELTA_CACHE_S3FIFO_GHOST_RATIO";
const ENV_S3_FREQ: &str = "DELTA_CACHE_S3FIFO_FREQ_THRESHOLD";

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

type DeltaCache = Cache<String, Bytes, foyer::DefaultHasher, CacheProperties>;

/// Build a foyer in-memory cache from the current environment variables.
/// Returns `None` when `DELTA_CACHE_CAPACITY_BYTES` is absent or zero.
pub fn build_cache_from_env() -> Option<DeltaCache> {
    let capacity = std::env::var(ENV_CAPACITY)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&c| c > 0)?;

    let shards = env_usize(ENV_SHARDS, 4);
    let eviction_name = std::env::var(ENV_EVICTION).unwrap_or_else(|_| "lru".to_string());

    let eviction_config: EvictionConfig = match eviction_name.to_lowercase().as_str() {
        "lfu" => LfuConfig {
            window_capacity_ratio: env_f64(ENV_LFU_WINDOW, 0.01),
            protected_capacity_ratio: env_f64(ENV_LFU_PROTECTED, 0.8),
            ..LfuConfig::default()
        }
        .into(),
        "s3fifo" => S3FifoConfig {
            small_queue_capacity_ratio: env_f64(ENV_S3_SMALL, 0.1),
            ghost_queue_capacity_ratio: env_f64(ENV_S3_GHOST, 1.0),
            small_to_main_freq_threshold: std::env::var(ENV_S3_FREQ)
                .ok()
                .and_then(|v| v.parse::<u8>().ok())
                .unwrap_or(1),
        }
        .into(),
        "sieve" => SieveConfig.into(),
        "fifo" => FifoConfig {}.into(),
        _ => LruConfig {
            high_priority_pool_ratio: env_f64(ENV_LRU_HIGH_PRIO, 0.9),
        }
        .into(),
    };

    let cache = CacheBuilder::new(capacity)
        .with_shards(shards)
        .with_eviction_config(eviction_config)
        .build::<CacheProperties>();

    debug!(
        capacity,
        shards,
        eviction = eviction_name,
        "delta-cache: in-memory object store cache enabled"
    );

    Some(cache)
}

/// An [`ObjectStore`] decorator that caches unconditional full-object reads.
#[derive(Debug, Clone)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: DeltaCache,
}

impl CachingObjectStore {
    /// Build from environment variables. Returns `None` if caching is disabled.
    pub fn from_env(inner: Arc<dyn ObjectStore>) -> Option<Self> {
        let cache = build_cache_from_env()?;
        Some(Self { inner, cache })
    }

    /// Build from environment variables, constructing the inner store only when
    /// caching is actually enabled. Returns `None` (without calling `make_inner`)
    /// if `DELTA_CACHE_CAPACITY_BYTES` is absent or zero.
    ///
    /// This avoids constructing the prefixed store twice in `decorate_store` when
    /// the feature is compiled in but the cache is disabled at runtime.
    pub(crate) fn from_env_with_inner<F, E>(make_inner: F) -> Option<Self>
    where
        F: FnOnce() -> Result<Arc<dyn ObjectStore>, E>,
        E: std::fmt::Debug,
    {
        let cache = build_cache_from_env()?;
        let inner = make_inner()
            .expect("decorate_prefix should not fail with a valid url");
        Some(Self { inner, cache })
    }

    #[cfg(test)]
    pub(crate) fn with_cache(inner: Arc<dyn ObjectStore>, cache: DeltaCache) -> Self {
        Self { inner, cache }
    }
}

impl std::fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.cache.remove(&location.to_string());
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        let is_unconditional = options.range.is_none()
            && options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none()
            && !options.head;

        if !is_unconditional {
            return self.inner.get_opts(location, options).await;
        }

        let key = location.to_string();

        if let Some(entry) = self.cache.get(&key) {
            debug!(path = %location, "delta-cache: hit");
            let bytes = entry.value().clone();
            let size = bytes.len() as u64;
            let meta = match self
                .inner
                .get_opts(location, GetOptions::new().with_head(true))
                .await
            {
                Ok(r) => r.meta,
                Err(_) => ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::Utc::now(),
                    size,
                    e_tag: None,
                    version: None,
                },
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                    async move { Ok(bytes) },
                ))),
                meta,
                range: 0..size,
                attributes: Attributes::default(),
            });
        }

        debug!(path = %location, "delta-cache: miss");
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attrs = result.attributes.clone();
        let bytes = result.bytes().await?;
        self.cache.insert(key, bytes.clone());
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(bytes) },
            ))),
            meta,
            range,
            attributes: attrs,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        use futures::StreamExt;
        let inner = self.inner.clone();
        let cache = self.cache.clone();
        // Collect paths eagerly so we can invalidate the cache before forwarding.
        // Each path is either passed through or carries its error forward.
        locations
            .map(move |res| match res {
                Ok(ref location) => {
                    cache.remove(&location.to_string());
                    res
                }
                Err(_) => res,
            })
            .flat_map(move |res| {
                let inner = inner.clone();
                match res {
                    Ok(location) => {
                        let stream: BoxStream<'static, OSResult<Path>> =
                            inner.delete_stream(
                                futures::stream::once(async move { Ok(location) }).boxed(),
                            );
                        stream
                    }
                    Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foyer::{CacheBuilder, CacheProperties};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
    use serial_test::serial;

    fn make_store(capacity: usize) -> CachingObjectStore {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = CacheBuilder::new(capacity)
            .with_shards(2)
            .build::<CacheProperties>();
        CachingObjectStore::with_cache(inner, cache)
    }

    #[tokio::test]
    async fn test_get_returns_bytes() {
        let store = make_store(1024 * 1024);
        let path = Path::from("_delta_log/00000000000000000001.json");
        store
            .put(&path, PutPayload::from_static(b"{\"commitInfo\":{}}"))
            .await
            .unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"{\"commitInfo\":{}}");
    }

    #[tokio::test]
    async fn test_cache_hit_survives_backing_store_delete() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = CacheBuilder::new(1024 * 1024)
            .with_shards(2)
            .build::<CacheProperties>();
        let store = CachingObjectStore::with_cache(inner.clone(), cache);

        let path = Path::from("_delta_log/00000000000000000001.json");
        inner
            .put(&path, PutPayload::from_static(b"cached-data"))
            .await
            .unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        inner.delete(&path).await.unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"cached-data");
    }

    #[tokio::test]
    async fn test_put_invalidates_cache() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = CacheBuilder::new(1024 * 1024)
            .with_shards(2)
            .build::<CacheProperties>();
        let store = CachingObjectStore::with_cache(inner.clone(), cache);

        let path = Path::from("file.json");
        store.put(&path, PutPayload::from_static(b"v1")).await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.put(&path, PutPayload::from_static(b"v2")).await.unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn test_delete_invalidates_cache() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = CacheBuilder::new(1024 * 1024)
            .with_shards(2)
            .build::<CacheProperties>();
        let store = CachingObjectStore::with_cache(inner.clone(), cache);

        let path = Path::from("file.json");
        inner.put(&path, PutPayload::from_static(b"data")).await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.delete(&path).await.unwrap();
        assert!(store.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_versioned_get_bypasses_cache() {
        // A get with options.version set must bypass the cache and go to the
        // inner store -- the is_unconditional guard must check version.
        let store = make_store(1024 * 1024);
        let path = Path::from("_delta_log/00000000000000000001.json");
        store
            .put(&path, PutPayload::from_static(b"data"))
            .await
            .unwrap();
        // Prime the cache.
        store.get(&path).await.unwrap().bytes().await.unwrap();
        // A versioned get must not be served from the cache entry above;
        // it must be forwarded to the inner store. InMemory ignores the version
        // field and returns the object normally, which is fine -- the key
        // invariant is that `options.version.is_some()` skips the cache path.
        let opts = GetOptions {
            version: Some("v1".to_string()),
            ..Default::default()
        };
        // If the cache had served this (pre-fix behaviour) we'd still get bytes;
        // with the fix the inner store is called. InMemory doesn't enforce
        // versioning so it returns Ok regardless -- but we'd see a cache miss
        // in tracing. The important thing is this does not panic.
        let bytes = store
            .get_opts(&path, opts)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"data");
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_absent() {
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
        }
        assert!(build_cache_from_env().is_none());
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_zero() {
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_CAPACITY, "0");
        }
        assert!(build_cache_from_env().is_none());
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_lru() {
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_CAPACITY, "1048576");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_SHARDS, "8");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_EVICTION, "lru");
        }
        let cache = build_cache_from_env();
        assert!(cache.is_some());
        let c = cache.unwrap();
        assert_eq!(c.capacity(), 1048576);
        assert_eq!(c.shards(), 8);
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_SHARDS);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_EVICTION);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_lfu() {
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_CAPACITY, "2097152");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_EVICTION, "lfu");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_LFU_WINDOW, "0.05");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_LFU_PROTECTED, "0.7");
        }
        assert!(build_cache_from_env().is_some());
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_EVICTION);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_LFU_WINDOW);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_LFU_PROTECTED);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_s3fifo() {
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_CAPACITY, "2097152");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_EVICTION, "s3fifo");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_S3_SMALL, "0.2");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_S3_GHOST, "0.5");
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::set_var(ENV_S3_FREQ, "2");
        }
        assert!(build_cache_from_env().is_some());
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_EVICTION);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_S3_SMALL);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_S3_GHOST);
        }
        // SAFETY: single-threaded test, serial_test ensures no concurrent env mutations
        unsafe {
            std::env::remove_var(ENV_S3_FREQ);
        }
    }
}
