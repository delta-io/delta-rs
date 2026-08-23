//! A wrapper that restores `object_store`'s lexicographic listing guarantee.
//!
//! `OpendalStore` streams list results in whatever order the underlying OpenDAL
//! service returns them (e.g. filesystem readdir order for `fs`), but the
//! `object_store` contract — and delta-kernel's log replay — require listings
//! sorted by path. This wrapper buffers and sorts each listing.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;

/// Wraps an inner store and sorts every listing by path.
pub struct SortedListStore {
    inner: Arc<dyn ObjectStore>,
}

impl SortedListStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for SortedListStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortedListStore").finish()
    }
}

impl std::fmt::Display for SortedListStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SortedListStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for SortedListStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            let mut metas: Vec<ObjectMeta> = match inner.list(prefix.as_ref()).try_collect().await {
                Ok(metas) => metas,
                Err(e) => return stream::iter(vec![Err(e)]),
            };
            metas.sort_by(|a, b| a.location.cmp(&b.location));
            stream::iter(metas.into_iter().map(Ok).collect::<Vec<_>>())
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        let offset = offset.clone();
        stream::once(async move {
            let collected: ObjectStoreResult<Vec<ObjectMeta>> =
                inner.list(prefix.as_ref()).try_collect().await;
            let mut metas = match collected {
                Ok(metas) => metas,
                Err(e) => return stream::iter(vec![Err(e)]),
            };
            metas.retain(|m| m.location > offset);
            metas.sort_by(|a, b| a.location.cmp(&b.location));
            stream::iter(metas.into_iter().map(Ok).collect::<Vec<_>>())
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects.sort_by(|a, b| a.location.cmp(&b.location));
        result.common_prefixes.sort();
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}
