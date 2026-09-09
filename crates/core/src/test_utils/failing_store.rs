//! An object store whose multipart uploads fail on demand, for exercising the
//! writers' abort/cleanup paths.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    MultipartUpload, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};

use super::impl_object_store_delegating_reads;

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

impl_object_store_delegating_reads! {
    for FailingMultipartStore {
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
    }
}
