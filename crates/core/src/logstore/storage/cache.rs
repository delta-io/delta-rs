//! Object-store byte cache for Delta log replay.
//!
//! Enabled only when the `delta-cache` Cargo feature is compiled in, and activated
//! at runtime when [`CachingObjectStore::from_env`] finds a positive
//! `DELTA_CACHE_CAPACITY_BYTES` environment variable.
//!
//! Two tiers are supported:
//!
//! * **Memory-only** (default): a `foyer::Cache` kept entirely in process memory.
//! * **Hybrid** (enabled by `DELTA_CACHE_DIR`): a `foyer::HybridCache` with a
//!   memory tier backed by a disk spill tier. Bytes evicted from memory are written
//!   to the directory named by `DELTA_CACHE_DIR`, which is typically faster than the
//!   remote object store. Requires a **multi-thread** Tokio runtime.
//!
//! # Environment variables
//!
//! | Variable | Default | Description |
//! |---|---|---|
//! | `DELTA_CACHE_CAPACITY_BYTES` | — | Memory tier byte budget **(required to enable)** |
//! | `DELTA_CACHE_SHARDS` | `4` | Memory-tier access shards |
//! | `DELTA_CACHE_EVICTION` | `lru` | Algorithm: `lru`, `lfu`, `s3fifo`, `sieve`, `fifo` |
//! | `DELTA_CACHE_LRU_HIGH_PRIO_RATIO` | `0.9` | LRU high-priority pool ratio [0,1] |
//! | `DELTA_CACHE_LFU_WINDOW_RATIO` | `0.01` | LFU window-queue capacity ratio |
//! | `DELTA_CACHE_LFU_PROTECTED_RATIO` | `0.8` | LFU protected-segment capacity ratio |
//! | `DELTA_CACHE_S3FIFO_SMALL_RATIO` | `0.1` | S3-FIFO small-queue capacity ratio |
//! | `DELTA_CACHE_S3FIFO_GHOST_RATIO` | `1.0` | S3-FIFO ghost-queue capacity ratio |
//! | `DELTA_CACHE_S3FIFO_FREQ_THRESHOLD` | `1` | S3-FIFO small-to-main promotion threshold |
//! | `DELTA_CACHE_DIR` | — | Directory for disk spill tier; enables `HybridCache` |
//! | `DELTA_CACHE_DISK_CAPACITY_BYTES` | 80% free | Disk-tier byte budget (0 = foyer default) |

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, CacheProperties, DeviceBuilder, EvictionConfig,
    FifoConfig, FsDeviceBuilder, HybridCache, HybridCacheBuilder, LfuConfig, LruConfig,
    S3FifoConfig, SieveConfig, StorageKey, StorageValue,
};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OSResult, UploadPart,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

// -- Env-var names ------------------------------------------------------------

const ENV_CAPACITY: &str = "DELTA_CACHE_CAPACITY_BYTES";
const ENV_SHARDS: &str = "DELTA_CACHE_SHARDS";
const ENV_EVICTION: &str = "DELTA_CACHE_EVICTION";
const ENV_LRU_HIGH_PRIO: &str = "DELTA_CACHE_LRU_HIGH_PRIO_RATIO";
const ENV_LFU_WINDOW: &str = "DELTA_CACHE_LFU_WINDOW_RATIO";
const ENV_LFU_PROTECTED: &str = "DELTA_CACHE_LFU_PROTECTED_RATIO";
const ENV_S3_SMALL: &str = "DELTA_CACHE_S3FIFO_SMALL_RATIO";
const ENV_S3_GHOST: &str = "DELTA_CACHE_S3FIFO_GHOST_RATIO";
const ENV_S3_FREQ: &str = "DELTA_CACHE_S3FIFO_FREQ_THRESHOLD";
const ENV_DIR: &str = "DELTA_CACHE_DIR";
const ENV_DISK_CAPACITY: &str = "DELTA_CACHE_DISK_CAPACITY_BYTES";

// -- Helpers ------------------------------------------------------------------

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

// -- CachedBytes newtype ------------------------------------------------------

/// Newtype wrapper for [`bytes::Bytes`] that satisfies foyer's `StorageValue`
/// bound (which requires `serde::Serialize + serde::de::DeserializeOwned`).
/// Serialized as a sequence of bytes (plain `Vec<u8>`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedBytes(Vec<u8>);

impl From<Bytes> for CachedBytes {
    fn from(b: Bytes) -> Self {
        CachedBytes(b.into())
    }
}

impl From<CachedBytes> for Bytes {
    fn from(c: CachedBytes) -> Self {
        c.0.into()
    }
}

// -- Unified cache enum -------------------------------------------------------

type MemCache = Cache<String, Bytes, foyer::DefaultHasher, CacheProperties>;
type HybCache = HybridCache<String, CachedBytes>;

/// Unified cache handle: either an in-process memory cache or a foyer hybrid
/// cache that spills evicted entries to a local disk directory.
#[derive(Debug, Clone)]
enum DeltaCache {
    Memory(MemCache),
    Hybrid(HybCache),
}

impl DeltaCache {
    /// Look up `key`. Returns the cached bytes, or `None` on a miss.
    /// For the hybrid tier, disk I/O may be involved; errors are treated as
    /// misses (the read falls through to the object store).
    async fn lookup(&self, key: &str) -> Option<Bytes> {
        match self {
            DeltaCache::Memory(c) => c.get(key).map(|e| e.value().clone()),
            DeltaCache::Hybrid(c) => c
                .get(key)
                .await
                .ok()
                .flatten()
                .map(|e| bytes::Bytes::from(e.value().clone())),
        }
    }

    /// Insert `bytes` under `key`.
    fn store(&self, key: String, bytes: Bytes) {
        match self {
            DeltaCache::Memory(c) => {
                c.insert(key, bytes);
            }
            DeltaCache::Hybrid(c) => {
                c.insert(key, CachedBytes::from(bytes));
            }
        }
    }

    /// Remove the entry for `key` from both memory and disk tiers.
    fn evict(&self, key: &str) {
        match self {
            DeltaCache::Memory(c) => {
                c.remove(key);
            }
            DeltaCache::Hybrid(c) => {
                c.remove(key);
            }
        }
    }

    /// Memory-tier capacity in bytes (test helper).
    #[cfg(test)]
    fn memory_capacity(&self) -> usize {
        match self {
            DeltaCache::Memory(c) => c.capacity(),
            DeltaCache::Hybrid(_) => panic!("capacity() not exposed for hybrid cache"),
        }
    }

    /// Memory-tier shard count (test helper).
    #[cfg(test)]
    fn memory_shards(&self) -> usize {
        match self {
            DeltaCache::Memory(c) => c.shards(),
            DeltaCache::Hybrid(_) => panic!("shards() not exposed for hybrid cache"),
        }
    }
}

// -- Eviction config builder (shared between memory and hybrid paths) ---------

/// Parse the eviction algorithm env vars and return an [`EvictionConfig`].
/// Returns `None` if a configured ratio violates foyer's constraints (in which
/// case the cache should be disabled rather than panicking).
fn build_eviction_config(eviction_name: &str) -> Option<EvictionConfig> {
    match eviction_name.to_lowercase().as_str() {
        "lfu" => {
            let window = env_f64(ENV_LFU_WINDOW, 0.01);
            let protected = env_f64(ENV_LFU_PROTECTED, 0.8);
            if !(window > 0.0 && window < 1.0) {
                tracing::warn!(
                    window,
                    "DELTA_CACHE_LFU_WINDOW_RATIO must be in (0, 1); disabling delta-cache"
                );
                return None;
            }
            if !(protected > 0.0 && protected < 1.0) {
                tracing::warn!(
                    protected,
                    "DELTA_CACHE_LFU_PROTECTED_RATIO must be in (0, 1); disabling delta-cache"
                );
                return None;
            }
            if window + protected >= 1.0 {
                tracing::warn!(
                    window,
                    protected,
                    "DELTA_CACHE_LFU_WINDOW_RATIO + DELTA_CACHE_LFU_PROTECTED_RATIO must be \
                     < 1.0 (got {}); disabling delta-cache",
                    window + protected
                );
                return None;
            }
            Some(
                LfuConfig {
                    window_capacity_ratio: window,
                    protected_capacity_ratio: protected,
                    ..LfuConfig::default()
                }
                .into(),
            )
        }
        "s3fifo" => {
            let small = env_f64(ENV_S3_SMALL, 0.1);
            if !(small > 0.0 && small < 1.0) {
                tracing::warn!(
                    small,
                    "DELTA_CACHE_S3FIFO_SMALL_RATIO must be in (0, 1); disabling delta-cache"
                );
                return None;
            }
            Some(
                S3FifoConfig {
                    small_queue_capacity_ratio: small,
                    ghost_queue_capacity_ratio: env_f64(ENV_S3_GHOST, 1.0),
                    small_to_main_freq_threshold: std::env::var(ENV_S3_FREQ)
                        .ok()
                        .and_then(|v| v.parse::<u8>().ok())
                        .unwrap_or(1),
                }
                .into(),
            )
        }
        "sieve" => Some(SieveConfig.into()),
        "fifo" => Some(FifoConfig {}.into()),
        _ => Some(
            LruConfig {
                high_priority_pool_ratio: env_f64(ENV_LRU_HIGH_PRIO, 0.9),
            }
            .into(),
        ),
    }
}

// -- Cache construction -------------------------------------------------------

/// Build a memory-only foyer cache.
fn build_memory_cache(capacity: usize, shards: usize, eviction: EvictionConfig) -> DeltaCache {
    DeltaCache::Memory(
        CacheBuilder::new(capacity)
            .with_shards(shards)
            .with_eviction_config(eviction)
            .build::<CacheProperties>(),
    )
}

/// Build a hybrid (memory + disk) foyer cache.
/// Requires a multi-thread Tokio runtime for `block_in_place`.
async fn build_hybrid_cache(
    dir: &str,
    memory_capacity: usize,
    disk_capacity: usize,
    shards: usize,
    eviction: EvictionConfig,
) -> Option<DeltaCache> {
    let mut device_builder = FsDeviceBuilder::new(dir);
    if disk_capacity > 0 {
        device_builder = device_builder.with_capacity(disk_capacity);
    }
    let device = match DeviceBuilder::build(device_builder) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                dir,
                error = %e,
                "delta-cache: failed to build disk device; using memory-only cache"
            );
            return None;
        }
    };

    let cache = HybridCacheBuilder::new()
        .memory(memory_capacity)
        .with_shards(shards)
        .with_eviction_config(eviction)
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .build()
        .await;

    match cache {
        Ok(c) => {
            debug!(
                memory_capacity,
                disk_capacity, dir, "delta-cache: hybrid (memory + disk) cache enabled"
            );
            Some(DeltaCache::Hybrid(c))
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "delta-cache: hybrid cache build failed; using memory-only cache"
            );
            None
        }
    }
}

/// Build a [`DeltaCache`] from the current environment variables.
///
/// Returns `None` when `DELTA_CACHE_CAPACITY_BYTES` is absent or zero.
///
/// When `DELTA_CACHE_DIR` is set:
/// - In a **multi-thread** Tokio runtime, a hybrid cache is built via
///   `block_in_place`.
/// - In a **current-thread** runtime a hybrid cache cannot be built without
///   blocking the thread; the function falls back to a memory-only cache and
///   logs a warning.
pub fn build_cache_from_env() -> Option<DeltaCache> {
    let capacity = std::env::var(ENV_CAPACITY)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&c| c > 0)?;

    let shards = env_usize(ENV_SHARDS, 4);
    let eviction_name = std::env::var(ENV_EVICTION).unwrap_or_else(|_| "lru".to_string());
    let eviction = build_eviction_config(&eviction_name)?;

    let dir = std::env::var(ENV_DIR).ok();

    if let Some(dir) = dir {
        let disk_capacity = env_usize(ENV_DISK_CAPACITY, 0);

        // Hybrid cache build is async. Use block_in_place if we're in a
        // multi-thread runtime; fall back to memory-only otherwise.
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                tracing::warn!(
                    "delta-cache: DELTA_CACHE_DIR set but no Tokio runtime found; \
                     using memory-only cache"
                );
                return Some(build_memory_cache(capacity, shards, eviction));
            }
        };

        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                let hybrid = tokio::task::block_in_place(|| {
                    handle.block_on(build_hybrid_cache(
                        &dir,
                        capacity,
                        disk_capacity,
                        shards,
                        eviction.clone(),
                    ))
                });
                // Fall back to memory-only if the hybrid build fails.
                Some(hybrid.unwrap_or_else(|| build_memory_cache(capacity, shards, eviction)))
            }
            _ => {
                tracing::warn!(
                    "delta-cache: DELTA_CACHE_DIR requires a multi-thread Tokio runtime; \
                     using memory-only cache"
                );
                Some(build_memory_cache(capacity, shards, eviction))
            }
        }
    } else {
        let cache = build_memory_cache(capacity, shards, eviction);
        debug!(
            capacity,
            shards,
            eviction = eviction_name,
            "delta-cache: in-memory object store cache enabled"
        );
        Some(cache)
    }
}

// -- InvalidatingMultipartUpload ----------------------------------------------

/// A [`MultipartUpload`] wrapper that evicts the cache entry for `key` when
/// `complete()` succeeds, keeping the cache consistent after multipart writes.
struct InvalidatingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    cache: DeltaCache,
    key: String,
}

impl std::fmt::Debug for InvalidatingMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvalidatingMultipartUpload")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl MultipartUpload for InvalidatingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> OSResult<PutResult> {
        let result = self.inner.complete().await?;
        // Evict only on success; a failed complete leaves the object unchanged.
        self.cache.evict(&self.key);
        Ok(result)
    }

    async fn abort(&mut self) -> OSResult<()> {
        self.inner.abort().await
    }
}

// -- CachingObjectStore -------------------------------------------------------

/// An [`ObjectStore`] decorator that caches unconditional full-object `get()`
/// results.
///
/// Construct via [`CachingObjectStore::from_env`]. Returns `None` when
/// `DELTA_CACHE_CAPACITY_BYTES` is absent or zero -- no behaviour change for
/// existing users.
///
/// Write operations evict the cached entry for the affected path. Range reads
/// and conditional gets bypass the cache.
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
        let inner = make_inner().expect("decorate_prefix should not fail with a valid url");
        Some(Self { inner, cache })
    }

    /// Build from a pre-constructed cache (for tests).
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
        self.cache.evict(&location.to_string());
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(InvalidatingMultipartUpload {
            inner,
            cache: self.cache.clone(),
            key: location.to_string(),
        }))
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

        if let Some(bytes) = self.cache.lookup(&key).await {
            debug!(path = %location, "delta-cache: hit");
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
                payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
                    Ok(bytes)
                }))),
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
        self.cache.store(key, bytes.clone());
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
        use futures::{StreamExt, TryStreamExt};
        let inner = self.inner.clone();
        let cache = self.cache.clone();
        locations
            .and_then(move |location| {
                let inner = inner.clone();
                let cache = cache.clone();
                async move {
                    let mut stream = inner.delete_stream(
                        futures::stream::once(async move { Ok(location.clone()) }).boxed(),
                    );
                    let deleted =
                        stream
                            .try_next()
                            .await?
                            .ok_or_else(|| object_store::Error::Generic {
                                store: "CachingObjectStore",
                                source: "delete_stream yielded no result".into(),
                            })?;
                    cache.evict(&deleted.to_string());
                    Ok(deleted)
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

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use foyer::{CacheBuilder, CacheProperties};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
    use serial_test::serial;

    fn make_memory_store(capacity: usize) -> CachingObjectStore {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = DeltaCache::Memory(
            CacheBuilder::new(capacity)
                .with_shards(2)
                .build::<CacheProperties>(),
        );
        CachingObjectStore::with_cache(inner, cache)
    }

    #[tokio::test]
    async fn test_get_returns_bytes() {
        let store = make_memory_store(1024 * 1024);
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
        let cache = DeltaCache::Memory(
            CacheBuilder::new(1024 * 1024)
                .with_shards(2)
                .build::<CacheProperties>(),
        );
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
        let cache = DeltaCache::Memory(
            CacheBuilder::new(1024 * 1024)
                .with_shards(2)
                .build::<CacheProperties>(),
        );
        let store = CachingObjectStore::with_cache(inner.clone(), cache);

        let path = Path::from("file.json");
        store
            .put(&path, PutPayload::from_static(b"v1"))
            .await
            .unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store
            .put(&path, PutPayload::from_static(b"v2"))
            .await
            .unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn test_delete_invalidates_cache() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = DeltaCache::Memory(
            CacheBuilder::new(1024 * 1024)
                .with_shards(2)
                .build::<CacheProperties>(),
        );
        let store = CachingObjectStore::with_cache(inner.clone(), cache);

        let path = Path::from("file.json");
        inner
            .put(&path, PutPayload::from_static(b"data"))
            .await
            .unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.delete(&path).await.unwrap();
        assert!(store.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_put_multipart_invalidates_cache() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = DeltaCache::Memory(
            CacheBuilder::new(1024 * 1024)
                .with_shards(2)
                .build::<CacheProperties>(),
        );
        let store = CachingObjectStore::with_cache(inner.clone(), cache);

        let path = Path::from("_delta_log/checkpoint.parquet");
        store
            .put(&path, PutPayload::from_static(b"v1"))
            .await
            .unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();

        let mut upload = store.put_multipart(&path).await.unwrap();
        upload.put_part(b"v2".as_ref().into()).await.unwrap();
        upload.complete().await.unwrap();

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn test_versioned_get_bypasses_cache() {
        let store = make_memory_store(1024 * 1024);
        let path = Path::from("_delta_log/00000000000000000001.json");
        store
            .put(&path, PutPayload::from_static(b"data"))
            .await
            .unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        let opts = GetOptions {
            version: Some("v1".to_string()),
            ..Default::default()
        };
        let bytes = store
            .get_opts(&path, opts)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"data");
    }

    // -- Hybrid cache (disk tier) integration test ----------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_hybrid_cache_read_after_write() {
        let dir = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let hybrid = build_hybrid_cache(
            dir.path().to_str().unwrap(),
            4 * 1024 * 1024,  // 4 MiB memory
            16 * 1024 * 1024, // 16 MiB disk
            2,
            LruConfig::default().into(),
        )
        .await
        .expect("hybrid cache build failed");

        let store = CachingObjectStore::with_cache(inner.clone(), hybrid);

        let path = Path::from("_delta_log/00000000000000000001.json");
        store
            .put(&path, PutPayload::from_static(b"hybrid-data"))
            .await
            .unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hybrid-data");

        // Second read should hit the cache.
        let bytes2 = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes2.as_ref(), b"hybrid-data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_hybrid_cache_put_invalidates() {
        let dir = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let hybrid = build_hybrid_cache(
            dir.path().to_str().unwrap(),
            4 * 1024 * 1024,
            16 * 1024 * 1024,
            2,
            LruConfig::default().into(),
        )
        .await
        .expect("hybrid cache build failed");

        let store = CachingObjectStore::with_cache(inner.clone(), hybrid);
        let path = Path::from("file.json");

        store
            .put(&path, PutPayload::from_static(b"v1"))
            .await
            .unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store
            .put(&path, PutPayload::from_static(b"v2"))
            .await
            .unwrap();

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"v2");
    }

    // -- build_cache_from_env env-var tests ------------------------------------

    #[test]
    #[serial]
    fn test_build_cache_from_env_absent() {
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
        }
        assert!(build_cache_from_env().is_none());
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_zero() {
        unsafe {
            std::env::set_var(ENV_CAPACITY, "0");
        }
        assert!(build_cache_from_env().is_none());
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_lfu_invalid_sum_returns_none() {
        unsafe {
            std::env::set_var(ENV_CAPACITY, "1048576");
            std::env::set_var(ENV_EVICTION, "lfu");
            std::env::set_var(ENV_LFU_WINDOW, "0.5");
            std::env::set_var(ENV_LFU_PROTECTED, "0.5");
        }
        assert!(build_cache_from_env().is_none());
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
            std::env::remove_var(ENV_EVICTION);
            std::env::remove_var(ENV_LFU_WINDOW);
            std::env::remove_var(ENV_LFU_PROTECTED);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_s3fifo_invalid_small_ratio_returns_none() {
        unsafe {
            std::env::set_var(ENV_CAPACITY, "1048576");
            std::env::set_var(ENV_EVICTION, "s3fifo");
            std::env::set_var(ENV_S3_SMALL, "0.0");
        }
        assert!(build_cache_from_env().is_none());
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
            std::env::remove_var(ENV_EVICTION);
            std::env::remove_var(ENV_S3_SMALL);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_lru() {
        unsafe {
            std::env::set_var(ENV_CAPACITY, "1048576");
            std::env::set_var(ENV_SHARDS, "8");
            std::env::set_var(ENV_EVICTION, "lru");
        }
        let cache = build_cache_from_env();
        assert!(cache.is_some());
        let c = cache.unwrap();
        assert_eq!(c.memory_capacity(), 1048576);
        assert_eq!(c.memory_shards(), 8);
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
            std::env::remove_var(ENV_SHARDS);
            std::env::remove_var(ENV_EVICTION);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_lfu() {
        unsafe {
            std::env::set_var(ENV_CAPACITY, "2097152");
            std::env::set_var(ENV_EVICTION, "lfu");
            std::env::set_var(ENV_LFU_WINDOW, "0.05");
            std::env::set_var(ENV_LFU_PROTECTED, "0.7");
        }
        assert!(build_cache_from_env().is_some());
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
            std::env::remove_var(ENV_EVICTION);
            std::env::remove_var(ENV_LFU_WINDOW);
            std::env::remove_var(ENV_LFU_PROTECTED);
        }
    }

    #[test]
    #[serial]
    fn test_build_cache_from_env_s3fifo() {
        unsafe {
            std::env::set_var(ENV_CAPACITY, "2097152");
            std::env::set_var(ENV_EVICTION, "s3fifo");
            std::env::set_var(ENV_S3_SMALL, "0.2");
            std::env::set_var(ENV_S3_GHOST, "0.5");
            std::env::set_var(ENV_S3_FREQ, "2");
        }
        assert!(build_cache_from_env().is_some());
        unsafe {
            std::env::remove_var(ENV_CAPACITY);
            std::env::remove_var(ENV_EVICTION);
            std::env::remove_var(ENV_S3_SMALL);
            std::env::remove_var(ENV_S3_GHOST);
            std::env::remove_var(ENV_S3_FREQ);
        }
    }
}
