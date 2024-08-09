//! GCP GCS storage backend.

use bytes::Bytes;
use deltalake_core::storage::ObjectStoreRef;
use deltalake_core::Path;
use futures::stream::BoxStream;
use object_store::{MultipartUpload, PutMultipartOpts, PutPayload};
use std::ops::Range;

use deltalake_core::storage::object_store::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, PutOptions, PutResult,
    Result as ObjectStoreResult,
};

pub(crate) struct GcsStorageBackend {
    inner: ObjectStoreRef,
}

impl GcsStorageBackend {
    pub fn try_new(storage: ObjectStoreRef) -> ObjectStoreResult<Self> {
        Ok(Self { inner: storage })
    }
}

impl std::fmt::Debug for GcsStorageBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "GcsStorageBackend")
    }
}

impl std::fmt::Display for GcsStorageBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "GcsStorageBackend")
    }
}

#[async_trait::async_trait]
impl ObjectStore for GcsStorageBackend {
    async fn put(&self, location: &Path, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
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
        let res = self.inner.rename_if_not_exists(from, to).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                match e {
                    object_store::Error::Generic { store, source } => {
                        // If this is a 429 (rate limit) error it means more than 1 mutation operation per second
                        // Was attempted on this same key
                        // That means we're experiencing concurrency conflicts, so return a transaction error
                        // Source would be a reqwest error which we don't have access to so the easiest thing to do is check
                        // for "429" in the error message
                        if format!("{:?}", source).contains("429") {
                            Err(object_store::Error::AlreadyExists {
                                path: to.to_string(),
                                source,
                            })
                        } else {
                            Err(object_store::Error::Generic { store, source })
                        }
                    }
                    _ => Err(e),
                }
            }
        }
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
