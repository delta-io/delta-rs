//! An object store that can lose the response of a create-only put, for exercising the commit
//! loop when its own commit is reported as a conflict.

use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Wraps [`InMemory`]. Once armed, the next create-only put is written and then reported as
/// `AlreadyExists`, like an S3 client retrying a put whose 5xx response hid a success.
#[derive(Debug, Default)]
pub(crate) struct LostResponseStore {
    inner: InMemory,
    /// When set, the next create-only put lands but reports `AlreadyExists`.
    pub(crate) lose_next_create: AtomicBool,
}

impl Display for LostResponseStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LostResponseStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LostResponseStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let lose_response = matches!(opts.mode, PutMode::Create)
            && self.lose_next_create.swap(false, Ordering::SeqCst);
        let result = self.inner.put_opts(location, payload, opts).await?;
        if lose_response {
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "the retried put found the object".into(),
            });
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
