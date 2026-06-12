//! A reusable wrapper that emulates conditional creates for OpenDAL services
//! that don't declare the `write_with_if_not_exists` capability.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use futures::stream::BoxStream;
use object_store::path::Path;

/// Wraps an inner store and emulates [`PutMode::Create`] with a HEAD-then-PUT.
///
/// OpenDAL backends that don't implement `write_with_if_not_exists` reject
/// `PutMode::Create` with `Unsupported`. This shim approximates the conditional
/// semantics: it is racy across concurrent writers, so it is only appropriate
/// for single-writer stores.
pub struct ConditionalPutShim {
    inner: Arc<dyn ObjectStore>,
}

impl ConditionalPutShim {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for ConditionalPutShim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConditionalPutShim").finish()
    }
}

impl std::fmt::Display for ConditionalPutShim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConditionalPutShim")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ConditionalPutShim {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if matches!(options.mode, PutMode::Create) {
            match self.inner.head(location).await {
                Ok(_) => {
                    return Err(object_store::Error::AlreadyExists {
                        path: location.to_string(),
                        source: "object already exists".into(),
                    });
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e),
            }
            let mut opts = options;
            opts.mode = PutMode::Overwrite;
            return self.inner.put_opts(location, bytes, opts).await;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn create_is_exclusive() {
        let store = ConditionalPutShim::new(Arc::new(InMemory::new()));
        let path = Path::from("a/b.json");

        let create = || PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };

        store
            .put_opts(&path, "first".into(), create())
            .await
            .expect("first create succeeds");

        let err = store
            .put_opts(&path, "second".into(), create())
            .await
            .expect_err("second create must fail");
        assert!(matches!(err, object_store::Error::AlreadyExists { .. }));

        // Overwrite always succeeds and is visible.
        store
            .put_opts(&path, "third".into(), PutOptions::default())
            .await
            .expect("overwrite succeeds");
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"third");
    }
}
