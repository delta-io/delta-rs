//! This module contains helper functions to create a LazyTableProvider from an ArrowArrayStreamReader

use crate::DeltaResult;
use arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::memory::LazyBatchGenerator;
use delta_datafusion::LazyTableProvider;
use parking_lot::RwLock;
use std::fmt::{self};
use std::sync::{Arc, Mutex};

use crate::delta_datafusion;

/// Convert an [ArrowArrayStreamReader] into a [LazyTableProvider]
pub fn to_lazy_table(source: ArrowArrayStreamReader) -> DeltaResult<Arc<dyn TableProvider>> {
    use arrow::array::RecordBatchReader;
    let schema = source.schema();
    let arrow_stream: Arc<Mutex<ArrowArrayStreamReader>> = Arc::new(Mutex::new(source));
    let arrow_stream_batch_generator: Arc<RwLock<dyn LazyBatchGenerator>> =
        Arc::new(RwLock::new(ArrowStreamBatchGenerator::new(arrow_stream)));

    Ok(Arc::new(LazyTableProvider::try_new(
        schema.clone(),
        vec![arrow_stream_batch_generator],
    )?))
}

#[derive(Debug)]
pub(crate) struct ArrowStreamBatchGenerator {
    pub array_stream: Arc<Mutex<ArrowArrayStreamReader>>,
}

impl fmt::Display for ArrowStreamBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArrowStreamBatchGenerator {{ array_stream: {:?} }}",
            self.array_stream
        )
    }
}

impl ArrowStreamBatchGenerator {
    pub fn new(array_stream: Arc<Mutex<ArrowArrayStreamReader>>) -> Self {
        Self { array_stream }
    }
}

impl LazyBatchGenerator for ArrowStreamBatchGenerator {
    fn generate_next_batch(
        &mut self,
    ) -> datafusion::error::Result<Option<arrow::array::RecordBatch>> {
        let mut stream_reader = self.array_stream.lock().map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "Failed to lock the ArrowArrayStreamReader".to_string(),
            )
        })?;

        match stream_reader.next() {
            Some(Ok(record_batch)) => Ok(Some(record_batch)),
            Some(Err(err)) => Err(datafusion::error::DataFusionError::ArrowError(err, None)),
            None => Ok(None), // End of stream
        }
    }
}
