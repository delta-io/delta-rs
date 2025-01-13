use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use quick_cache::sync::Cache;

#[derive(Debug, Clone)]
struct Entry {
    data: Bytes,
    last_modified: DateTime<Utc>,
    attributes: Attributes,
    e_tag: Option<String>,
}

impl Entry {
    fn new(
        data: Bytes,
        last_modified: DateTime<Utc>,
        e_tag: Option<String>,
        attributes: Attributes,
    ) -> Self {
        Self {
            data,
            last_modified,
            e_tag,
            attributes,
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
#[derive(Clone)]
pub(crate) struct CommitCacheObjectStore {
    inner: Arc<dyn ObjectStore>,
    check: Arc<dyn Fn(&Path) -> bool + Send + Sync>,
    cache: Arc<Cache<Path, Entry>>,
    has_ordered_listing: bool,
}

impl std::fmt::Debug for CommitCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConditionallyCachedObjectStore")
            .field("object_store", &self.inner)
            .finish()
    }
}

impl std::fmt::Display for CommitCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConditionallyCachedObjectStore({})", self.inner)
    }
}

fn cache_json(path: &Path) -> bool {
    path.extension()
        .map_or(false, |ext| ext.eq_ignore_ascii_case("json"))
}

impl CommitCacheObjectStore {
    /// Create a new conditionally cached object store.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        let store_str = format!("{}", inner);
        let is_local = store_str.starts_with("LocalFileSystem");
        Self {
            inner,
            check: Arc::new(cache_json),
            cache: Arc::new(Cache::new(100)),
            has_ordered_listing: !is_local,
        }
    }

    async fn get_opts_impl(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        if options.range.is_some() || !(self.check)(location) || options.head {
            return self.inner.get_opts(location, options).await;
        }

        let entry = if let Some(entry) = self.cache.get(location) {
            entry
        } else {
            let response = self.inner.get_opts(location, options.clone()).await?;
            let attributes = response.attributes.clone();
            let meta = response.meta.clone();
            let data = response.bytes().await?;
            let entry = Entry::new(data, meta.last_modified, meta.e_tag, attributes);
            self.cache.insert(location.clone(), entry.clone());
            entry
        };

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
            attributes: entry.attributes,
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

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.cache.remove(location);
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

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}
