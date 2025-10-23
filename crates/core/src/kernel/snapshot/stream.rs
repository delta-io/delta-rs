//! the code in this file is hoisted from datafusion with only slight modifications
//!
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::stream::BoxStream;
use futures::{Future, Stream, StreamExt};
use std::pin::Pin;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use tracing::dispatcher;
use tracing::Span;

use crate::errors::DeltaResult;
use crate::DeltaTableError;

/// Trait for types that stream [RecordBatch]
///
/// See [`SendableRecordBatchStream`] for more details.
pub trait RecordBatchStream: Stream<Item = DeltaResult<RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a [`Stream`] of [`RecordBatch`]es that can be passed between threads
///
/// This trait is used to retrieve the results of DataFusion execution plan nodes.
///
/// The trait is a specialized Rust Async [`Stream`] that also knows the schema
/// of the data it will return (even if the stream has no data). Every
/// `RecordBatch` returned by the stream should have the same schema as returned
/// by [`schema`](`RecordBatchStream::schema`).
///
/// # See Also
///
/// * [`RecordBatchStreamAdapter`] to convert an existing [`Stream`]
///   to [`SendableRecordBatchStream`]
///
/// [`RecordBatchStreamAdapter`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/stream/struct.RecordBatchStreamAdapter.html
///
/// # Error Handling
///
/// Once a stream returns an error, it should not be polled again (the caller
/// should stop calling `next`) and handle the error.
///
/// However, returning `Ready(None)` (end of stream) is likely the safest
/// behavior after an error. Like [`Stream`]s, `RecordBatchStream`s should not
/// be polled after end of stream or returning an error. However, also like
/// [`Stream`]s there is no mechanism to prevent callers polling  so returning
/// `Ready(None)` is recommended.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub type SendableRBStream = Pin<Box<dyn Stream<Item = DeltaResult<RecordBatch>> + Send>>;

/// Creates a stream from a collection of producing tasks, routing panics to the stream.
///
/// Note that this is similar to  [`ReceiverStream` from tokio-stream], with the differences being:
///
/// 1. Methods to bound and "detach"  tasks (`spawn()` and `spawn_blocking()`).
///
/// 2. Propagates panics, whereas the `tokio` version doesn't propagate panics to the receiver.
///
/// 3. Automatically cancels any outstanding tasks when the receiver stream is dropped.
///
/// [`ReceiverStream` from tokio-stream]: https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html
pub(crate) struct ReceiverStreamBuilder<O> {
    tx: Sender<DeltaResult<O>>,
    rx: Receiver<DeltaResult<O>>,
    join_set: JoinSet<DeltaResult<()>>,
}

impl<O: Send + 'static> ReceiverStreamBuilder<O> {
    /// Create new channels with the specified buffer size
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);

        Self {
            tx,
            rx,
            join_set: JoinSet::new(),
        }
    }

    /// Get a handle for sending data to the output
    pub fn tx(&self) -> Sender<DeltaResult<O>> {
        self.tx.clone()
    }

    /// Spawn task that will be aborted if this builder (or the stream
    /// built from it) are dropped
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = DeltaResult<()>>,
        F: Send + 'static,
    {
        self.join_set.spawn(task);
    }

    /// Spawn a blocking task that will be aborted if this builder (or the stream
    /// built from it) are dropped.
    ///
    /// This is often used to spawn tasks that write to the sender
    /// retrieved from `Self::tx`.
    pub fn spawn_blocking<F>(&mut self, f: F)
    where
        F: FnOnce() -> DeltaResult<()>,
        F: Send + 'static,
    {
        // Capture the current dispatcher and span
        let dispatch = dispatcher::get_default(|d| d.clone());
        let span = Span::current();

        self.join_set.spawn_blocking(move || {
            dispatcher::with_default(&dispatch, || {
                let _enter = span.enter();
                f()
            })
        });
    }

    /// Create a stream of all data written to `tx`
    pub fn build(self) -> BoxStream<'static, DeltaResult<O>> {
        let Self {
            tx,
            rx,
            mut join_set,
        } = self;

        // Doesn't need tx
        drop(tx);

        // future that checks the result of the join set, and propagates panic if seen
        let check = async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(task_result) => {
                        match task_result {
                            // Nothing to report
                            Ok(_) => continue,
                            // This means a blocking task error
                            Err(error) => return Some(Err(error)),
                        }
                    }
                    // This means a tokio task error, likely a panic
                    Err(e) => {
                        if e.is_panic() {
                            // resume on the main thread
                            std::panic::resume_unwind(e.into_panic());
                        } else {
                            // This should only occur if the task is
                            // cancelled, which would only occur if
                            // the JoinSet were aborted, which in turn
                            // would imply that the receiver has been
                            // dropped and this code is not running
                            return Some(Err(DeltaTableError::Generic(format!(
                                "Non Panic Task error: {e}"
                            ))));
                        }
                    }
                }
            }
            None
        };

        let check_stream = futures::stream::once(check)
            // unwrap Option / only return the error
            .filter_map(|item| async move { item });

        // Convert the receiver into a stream
        let rx_stream = futures::stream::unfold(rx, |mut rx| async move {
            let next_item = rx.recv().await;
            next_item.map(|next_item| (next_item, rx))
        });

        // Merge the streams together so whichever is ready first
        // produces the batch
        futures::stream::select(rx_stream, check_stream).boxed()
    }
}

pub(crate) type RecordBatchReceiverStreamBuilder = ReceiverStreamBuilder<RecordBatch>;
