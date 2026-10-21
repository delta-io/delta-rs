//! Adapts a DataFusion record batch stream to the kernel's `EngineData` iterator.
//!
//! [`PlanResult::Data`] is a synchronous iterator. [`into_engine_data`] pulls each batch from the
//! async stream by blocking on the executor's runtime handle and wraps it in
//! [`ArrowEngineData`].
//!
//! [`PlanResult::Data`]: delta_kernel::plans::PlanResult::Data

use datafusion::execution::SendableRecordBatchStream;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::{DeltaResultIteratorStatic, EngineData};
use futures::StreamExt;
use tokio::runtime::Handle;

/// Turns `stream` into the boxed iterator the kernel consumes, one `ArrowEngineData` per batch.
pub fn into_engine_data(
    handle: Handle,
    mut stream: SendableRecordBatchStream,
) -> DeltaResultIteratorStatic<Box<dyn EngineData>> {
    Box::new(std::iter::from_fn(move || {
        handle.block_on(stream.next()).map(|batch| {
            batch
                .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
                .map_err(|err| crate::Error::from(err).into())
        })
    }))
}
