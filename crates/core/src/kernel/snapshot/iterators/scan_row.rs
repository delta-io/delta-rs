use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use futures::Stream;
use pin_project_lite::pin_project;

use crate::kernel::arrow::engine_ext::SnapshotExt;
use crate::DeltaResult;

pin_project! {
    pub(crate) struct ScanRowOutStream<S> {
        snapshot: Arc<KernelSnapshot>,

        #[pin]
        stream: S,
    }
}

impl<S> ScanRowOutStream<S> {
    pub fn new(snapshot: Arc<KernelSnapshot>, stream: S) -> Self {
        Self { snapshot, stream }
    }
}

impl<S> Stream for ScanRowOutStream<S>
where
    S: Stream<Item = DeltaResult<RecordBatch>>,
{
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => match this.snapshot.parse_stats_column(&batch) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => Poll::Ready(Some(Err(err))),
            },
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
