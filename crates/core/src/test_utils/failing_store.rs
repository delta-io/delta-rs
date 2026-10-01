//! An object store whose multipart uploads fail on demand, for exercising the
//! writers' abort/cleanup paths.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};

/// Wraps [`InMemory`], but every multipart upload it hands out fails `put_part`
/// and `complete`, and records whether `abort` was called on it. Plain `put`s
/// are delegated unchanged, so table setup (log writes, small files) works.
#[derive(Debug, Default)]
pub(crate) struct FailingMultipartStore {
    inner: InMemory,
    /// Set when any handed-out multipart upload is aborted.
    pub(crate) multipart_aborted: Arc<AtomicBool>,
    /// Set when any multipart upload is started.
    pub(crate) multipart_started: Arc<AtomicBool>,
    /// When set, `put_multipart_opts` itself fails, so the failure surfaces in
    /// the very first sink write instead of at part upload / completion time.
    pub(crate) fail_multipart_create: Arc<AtomicBool>,
}

#[derive(Debug)]
struct FailingUpload {
    aborted: Arc<AtomicBool>,
}

fn upload_failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "FailingMultipartStore",
        source: "injected multipart failure".into(),
    }
}

#[async_trait::async_trait]
impl MultipartUpload for FailingUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        Box::pin(std::future::ready(Err(upload_failure())))
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        Err(upload_failure())
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborted.store(true, Ordering::Release);
        Ok(())
    }
}

impl Display for FailingMultipartStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingMultipartStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingMultipartStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if self.fail_multipart_create.load(Ordering::Acquire) {
            return Err(upload_failure());
        }
        self.multipart_started.store(true, Ordering::Release);
        Ok(Box::new(FailingUpload {
            aborted: self.multipart_aborted.clone(),
        }))
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
