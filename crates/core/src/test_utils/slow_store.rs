//! An object store that delays every upload and records how many ran at once.
//! It exercises the writers' upload bounds.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    MultipartUpload, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};

use super::impl_object_store_delegating_reads;

/// Wraps [`InMemory`]: every `put` sleeps `delay` before it lands, and every
/// multipart upload sleeps `delay` on `complete`. An upload counts as in flight
/// from the start of `put` (or `put_multipart_opts`) until it lands, completes,
/// aborts, or is dropped.
#[derive(Debug)]
pub(crate) struct SlowCountingStore {
    inner: InMemory,
    delay: Duration,
    in_flight: Arc<AtomicUsize>,
    /// Only ever touched through `&self`, so it needs no `Arc`.
    max_in_flight: AtomicUsize,
}

impl SlowCountingStore {
    pub(crate) fn new(delay: Duration) -> Self {
        Self {
            inner: InMemory::new(),
            delay,
            in_flight: Arc::default(),
            max_in_flight: AtomicUsize::default(),
        }
    }

    /// Highest number of uploads that were in flight at the same time.
    pub(crate) fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::Acquire)
    }

    fn track(&self) -> InFlightGuard {
        let now = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight.fetch_max(now, Ordering::AcqRel);
        InFlightGuard {
            in_flight: self.in_flight.clone(),
        }
    }
}

/// Counts one in-flight upload for as long as it lives.
#[derive(Debug)]
struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
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

impl_object_store_delegating_reads! {
    for SlowCountingStore {
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
    }
}
