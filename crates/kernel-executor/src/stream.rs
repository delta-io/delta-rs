//! Adapts a DataFusion record batch stream to kernel's `EngineData` iterator.
//!
//! [`PlanResult::Data`] is a synchronous iterator. [`into_engine_data`] spawns one task on the
//! executor's runtime that drains the stream into a bounded channel, and the iterator receives
//! from that channel. The bound gives backpressure: the task waits while the kernel is still
//! consuming earlier batches, and it stops when the iterator is dropped.
//!
//! [`PlanResult::Data`]: delta_kernel::plans::PlanResult::Data

use datafusion::execution::SendableRecordBatchStream;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::{DeltaResultIteratorStatic, EngineData};
use futures::StreamExt;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::runtime::block_in_place;

/// Batches buffered between the stream task and the kernel's iterator.
const BATCH_QUEUE_DEPTH: usize = 2;

/// Turns `stream` into the boxed iterator the kernel consumes, one `ArrowEngineData` per batch.
pub fn into_engine_data(
    handle: &Handle,
    mut stream: SendableRecordBatchStream,
) -> DeltaResultIteratorStatic<Box<dyn EngineData>> {
    let (sender, mut receiver) = mpsc::channel(BATCH_QUEUE_DEPTH);
    handle.spawn(async move {
        while let Some(batch) = stream.next().await {
            // The iterator was dropped: stop pulling from the stream.
            if sender.send(batch).await.is_err() {
                break;
            }
        }
    });
    Box::new(std::iter::from_fn(move || {
        block_in_place(|| receiver.blocking_recv()).map(|batch| {
            batch
                .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
                .map_err(|err| crate::ExecutorError::from(err).into())
        })
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use super::*;

    const BATCHES: usize = 5;

    fn batches() -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let rows = (0..BATCHES as i64).map({
            let schema = schema.clone();
            move |i| {
                Ok(
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![i]))])
                        .unwrap(),
                )
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(rows),
        ))
    }

    fn count(handle: &Handle) -> usize {
        into_engine_data(handle, batches())
            .map(|batch| batch.unwrap().len())
            .sum()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drains_from_a_runtime_worker() {
        assert_eq!(count(&Handle::current()), BATCHES);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drains_from_a_plain_thread() {
        let handle = Handle::current();
        let total = std::thread::spawn(move || count(&handle)).join().unwrap();
        assert_eq!(total, BATCHES);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_the_iterator_stops_the_stream() {
        let mut iter = into_engine_data(&Handle::current(), batches());
        assert!(iter.next().is_some());
        drop(iter);
    }
}
