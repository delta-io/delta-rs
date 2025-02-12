//! The write module contains shared code used for writes by the write_to_deltalake function and
//! the merge cod

use deltalake::arrow::ffi_stream::ArrowArrayStreamReader;
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::physical_plan::memory::LazyBatchGenerator;
use deltalake::delta_datafusion::LazyTableProvider;
use deltalake::DeltaResult;
use parking_lot::RwLock;
use std::fmt::{self};
use std::sync::{Arc, Mutex};

/// Convert an [ArrowArrayStreamReader] into a [LazyTableProvider]
pub(crate) fn to_lazy_table(source: ArrowArrayStreamReader) -> DeltaResult<Arc<dyn TableProvider>> {
    use deltalake::arrow::array::RecordBatchReader;
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
    ) -> deltalake::datafusion::error::Result<Option<deltalake::arrow::array::RecordBatch>> {
        let mut stream_reader = self.array_stream.lock().map_err(|_| {
            deltalake::datafusion::error::DataFusionError::Execution(
                "Failed to lock the ArrowArrayStreamReader".to_string(),
            )
        })?;

        match stream_reader.next() {
            Some(Ok(record_batch)) => Ok(Some(record_batch)),
            Some(Err(err)) => Err(deltalake::datafusion::error::DataFusionError::ArrowError(
                err, None,
            )),
            None => Ok(None), // End of stream
        }
    }
}
