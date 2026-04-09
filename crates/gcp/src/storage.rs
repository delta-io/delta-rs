//! GCP GCS storage backend.

use deltalake_core::Path;
use deltalake_core::logstore::ObjectStoreRef;
use futures::stream::BoxStream;
use object_store::{CopyOptions, MultipartUpload, PutMultipartOptions, PutPayload};

use deltalake_core::logstore::object_store::{
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

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
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

    #[test]
    fn test_it() {}
}
