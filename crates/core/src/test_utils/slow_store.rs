//! An object store that delays every upload and records how many were in
//! flight at once, for exercising the writers' upload bounds.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};

/// Wraps [`InMemory`]: every `put` sleeps `delay` before it lands, and every
/// multipart upload sleeps `delay` on `complete`. An upload counts as in flight
/// from the start of `put` (or `put_multipart_opts`) until it lands, completes,
/// aborts, or is dropped.
#[derive(Debug)]
pub(crate) struct SlowCountingStore {
    inner: InMemory,
    delay: Duration,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

impl SlowCountingStore {
    pub(crate) fn new(delay: Duration) -> Self {
        Self {
            inner: InMemory::new(),
            delay,
            in_flight: Arc::default(),
            max_in_flight: Arc::default(),
        }
    }

    /// Highest number of uploads that were in flight at the same time.
    pub(crate) fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::Acquire)
    }

    fn track(&self) -> InFlightGuard {
        InFlightGuard::new(self.in_flight.clone(), self.max_in_flight.clone())
    }
}

/// Counts one in-flight upload for as long as it lives.
#[derive(Debug)]
struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl InFlightGuard {
    fn new(in_flight: Arc<AtomicUsize>, max_in_flight: Arc<AtomicUsize>) -> Self {
        let now = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        max_in_flight.fetch_max(now, Ordering::AcqRel);
        Self { in_flight }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct SlowUpload {
    inner: Box<dyn MultipartUpload>,
    delay: Duration,
    _guard: InFlightGuard,
}

#[async_trait::async_trait]
impl MultipartUpload for SlowUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        tokio::time::sleep(self.delay).await;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

impl Display for SlowCountingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowCountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowCountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let _guard = self.track();
        tokio::time::sleep(self.delay).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let guard = self.track();
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(SlowUpload {
            inner,
            delay: self.delay,
            _guard: guard,
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
