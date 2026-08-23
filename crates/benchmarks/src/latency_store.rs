//! Object-store wrapper that injects delay on LIST operations.
//!
//! Used by vacuum listing benches to simulate high-latency cloud LIST on a
//! local filesystem table. Only `list` / `list_with_offset` /
//! `list_with_delimiter` are delayed; reads/writes pass through.
//!
//! Flat `list()` streams are delayed **per page** (default 1000 keys) to mimic
//! S3-style pagination. A single delay-at-start would under-penalize flat LIST
//! on local disk (one stream, all keys) versus many parallel prefix LISTs.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OSResult,
};

/// Default keys per simulated LIST page (S3 ListObjects max-keys default).
const DEFAULT_LIST_PAGE_SIZE: usize = 1000;

/// Wraps an [`ObjectStore`] and sleeps on LIST-family APIs.
#[derive(Debug)]
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    list_delay: Duration,
    /// Simulated page size for `list` / `list_with_offset` streams.
    page_size: usize,
}

impl LatencyStore {
    pub fn new(inner: Arc<dyn ObjectStore>, list_delay: Duration) -> Self {
        Self {
            inner,
            list_delay,
            page_size: DEFAULT_LIST_PAGE_SIZE,
        }
    }

    pub fn list_delay(&self) -> Duration {
        self.list_delay
    }

    async fn delay(&self) {
        if !self.list_delay.is_zero() {
            tokio::time::sleep(self.list_delay).await;
        }
    }
}

impl fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LatencyStore(list_delay={:?}, page_size={}, inner={})",
            self.list_delay, self.page_size, self.inner
        )
    }
}

fn delayed_list_stream(
    inner: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    offset: Option<Path>,
    delay: Duration,
    page_size: usize,
) -> BoxStream<'static, OSResult<ObjectMeta>> {
    let page_size = page_size.max(1);
    async_stream_list(inner, prefix, offset, delay, page_size)
}

fn async_stream_list(
    inner: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    offset: Option<Path>,
    delay: Duration,
    page_size: usize,
) -> BoxStream<'static, OSResult<ObjectMeta>> {
    // Use unfold over the inner stream so we can sleep every `page_size` yields
    // without pulling in the async-stream crate.
    let inner_stream = match offset {
        Some(offset) => inner.list_with_offset(prefix.as_ref(), &offset),
        None => inner.list(prefix.as_ref()),
    };

    futures::stream::try_unfold(
        (inner_stream, 0usize, true),
        move |(mut s, mut in_page, first)| async move {
            if first && !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            match s.next().await {
                None => Ok(None),
                Some(Err(e)) => Err(e),
                Some(Ok(item)) => {
                    in_page += 1;
                    // After finishing a page, sleep before the next key (next "RPC").
                    let at_page_boundary = in_page >= page_size;
                    if at_page_boundary {
                        in_page = 0;
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                    }
                    Ok(Some((item, (s, in_page, false))))
                }
            }
        },
    )
    .boxed()
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        delayed_list_stream(
            Arc::clone(&self.inner),
            prefix.cloned(),
            None,
            self.list_delay,
            self.page_size,
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        delayed_list_stream(
            Arc::clone(&self.inner),
            prefix.cloned(),
            Some(offset.clone()),
            self.list_delay,
            self.page_size,
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.delay().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OSResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
