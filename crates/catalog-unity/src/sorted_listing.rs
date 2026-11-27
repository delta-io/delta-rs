//! Object store that lists its objects in the order of their paths.

use std::ops::Range;

use bytes::Bytes;
use deltalake_core::logstore::ObjectStoreRef;
use deltalake_core::logstore::object_store::path::Path;
use deltalake_core::logstore::object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};

/// Lists the objects of the inner store in the order of their paths.
///
/// Kernel needs the commit files of a log listing in version order. It sorts the listing of a
/// `file://` table itself, but a Unity Catalog table has a `uc://` URL. The local file system
/// lists in no fixed order, so this store sorts the listing of a table that is on it.
#[derive(Debug)]
pub(crate) struct SortedListingStore {
    inner: ObjectStoreRef,
}

impl SortedListingStore {
    pub(crate) fn new(inner: ObjectStoreRef) -> Self {
        Self { inner }
    }
}

impl std::fmt::Display for SortedListingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SortedListingStore({})", self.inner)
    }
}

fn sorted(
    listing: BoxStream<'static, Result<ObjectMeta>>,
) -> BoxStream<'static, Result<ObjectMeta>> {
    stream::once(async move {
        let mut objects: Vec<ObjectMeta> = listing.try_collect().await?;
        objects.sort_unstable_by(|a, b| a.location.cmp(&b.location));
        Ok::<_, Error>(stream::iter(objects.into_iter().map(Ok)))
    })
    .try_flatten()
    .boxed()
}

#[async_trait::async_trait]
impl ObjectStore for SortedListingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        sorted(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        sorted(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use deltalake_core::logstore::object_store::ObjectStoreExt;
    use deltalake_core::logstore::object_store::local::LocalFileSystem;

    use super::*;

    #[tokio::test]
    async fn lists_in_path_order() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let mut expected: Vec<Path> = (0..64)
            .map(|version| Path::from(format!("_delta_log/{version:020}.json")))
            .collect();
        for location in &expected {
            local
                .put(location, PutPayload::from_static(b"{}"))
                .await
                .unwrap();
        }
        expected.sort_unstable();

        let store = SortedListingStore::new(local);
        let locations = |listing: BoxStream<'static, Result<ObjectMeta>>| async move {
            let objects: Vec<ObjectMeta> = listing.try_collect().await.unwrap();
            objects.into_iter().map(|o| o.location).collect::<Vec<_>>()
        };

        let prefix = Path::from("_delta_log");
        assert_eq!(locations(store.list(Some(&prefix))).await, expected);
        assert_eq!(
            locations(store.list_with_offset(Some(&prefix), &expected[9])).await,
            expected[10..]
        );
    }
}
