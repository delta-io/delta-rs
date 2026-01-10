use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::TaskContext;
use delta_kernel::{
    DeltaResult, Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
};
use futures::{StreamExt as _, stream::BoxStream};
use tokio::runtime::Handle;
use tracing::Instrument;
use url::Url;

pub(crate) use self::expressions::*;
use self::file_formats::DataFusionFileFormatHandler;
pub use self::storage::{AsObjectStoreUrl, DataFusionStorageHandler};
use crate::kernel::ARROW_HANDLER;

mod expressions;
mod file_formats;
mod storage;

#[derive(Clone, Debug)]
pub struct TracedHandle(Handle);

impl From<Handle> for TracedHandle {
    fn from(handle: Handle) -> Self {
        TracedHandle(handle)
    }
}

impl TracedHandle {
    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        let current_span = tracing::Span::current();
        let task = async move { future.instrument(current_span).await };
        self.0.block_on(task)
    }
}

/// A Datafusion based Kernel Engine
#[derive(Clone, Debug)]
pub struct DataFusionEngine {
    storage: Arc<DataFusionStorageHandler>,
    formats: Arc<DataFusionFileFormatHandler>,
}

impl DataFusionEngine {
    pub fn new_from_session(session: &dyn Session) -> Arc<Self> {
        Self::new(session.task_ctx(), Handle::current()).into()
    }

    pub fn new_from_context(ctx: Arc<TaskContext>) -> Arc<Self> {
        Self::new(ctx, Handle::current()).into()
    }

    pub fn new(ctx: Arc<TaskContext>, handle: impl Into<TracedHandle>) -> Self {
        let handle = handle.into();
        let storage = Arc::new(DataFusionStorageHandler::new(ctx.clone(), handle.clone()));
        let formats = Arc::new(DataFusionFileFormatHandler::new(ctx, handle));
        Self { storage, formats }
    }
}

impl Engine for DataFusionEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        ARROW_HANDLER.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.formats.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.formats.clone()
    }
}

/// Converts a Stream-producing future to a synchronous iterator.
///
/// This method performs the initial blocking call to extract the stream from the future, and each
/// subsequent call to `next` on the iterator translates to a blocking `stream.next()` call, using
/// the provided `task_executor`. Buffered streams allow concurrency in the form of prefetching,
/// because that initial call will attempt to populate the N buffer slots; every call to
/// `stream.next()` leaves an empty slot (out of N buffer slots) that the stream immediately
/// attempts to fill by launching another future that can make progress in the background while we
/// block on and consume each of the N-1 entries that precede it.
///
/// This is an internal utility for bridging object_store's async API to
/// Delta Kernel's synchronous handler traits.
pub(crate) fn stream_future_to_iter<T: Send + 'static>(
    handle: TracedHandle,
    stream_future: impl Future<Output = DeltaResult<BoxStream<'static, T>>> + Send + 'static,
) -> DeltaResult<Box<dyn Iterator<Item = T> + Send>> {
    Ok(Box::new(BlockingStreamIterator {
        stream: Some(handle.block_on(stream_future)?),
        handle,
    }))
}

struct BlockingStreamIterator<T: Send + 'static> {
    stream: Option<BoxStream<'static, T>>,
    handle: TracedHandle,
}

impl<T: Send + 'static> Iterator for BlockingStreamIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Move the stream into the future so we can block on it.
        let mut stream = self.stream.take()?;
        let task = async move { (stream.next().await, stream) };
        let (item, stream) = self.handle.block_on(task);

        // We must not poll an exhausted stream after it returned None.
        if item.is_some() {
            self.stream = Some(stream);
        }

        item
    }
}

trait UrlExt {
    // Check if a given url is a presigned url and can be used
    // to access the object store via simple http requests
    fn is_presigned(&self) -> bool;
}

impl UrlExt for Url {
    fn is_presigned(&self) -> bool {
        matches!(self.scheme(), "http" | "https")
            && (
                // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
                // https://developers.cloudflare.com/r2/api/s3/presigned-urls/
                self
                .query_pairs()
                .any(|(k, _)| k.eq_ignore_ascii_case("X-Amz-Signature")) ||
                // https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#version-2020-12-06-and-later
                // note signed permission (sp) must always be present
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("sp")) ||
                // https://cloud.google.com/storage/docs/authentication/signatures
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("X-Goog-Credential")) ||
                // https://www.alibabacloud.com/help/en/oss/user-guide/upload-files-using-presigned-urls
                self
                .query_pairs().any(|(k, _)| k.eq_ignore_ascii_case("X-OSS-Credential"))
            )
    }
}
