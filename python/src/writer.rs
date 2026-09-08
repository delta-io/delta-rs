//! This module contains helper functions to create a LazyTableProvider from an ArrowArrayStreamReader

use std::any::Any;
use std::fmt::{self};
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::error::Result as ArrowResult;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::physical_plan::memory::LazyBatchGenerator;
use deltalake::kernel::schema::cast_record_batch;
use parking_lot::RwLock;

use crate::DeltaResult;
use crate::datafusion::LazyTableProvider;

/// Convert an [ArrowArrayStreamReader] into a [LazyTableProvider]
pub fn to_lazy_table(
    source: Box<dyn RecordBatchReader + Send + 'static>,
) -> DeltaResult<Arc<dyn TableProvider>> {
    let schema = source.schema();
    let arrow_stream_batch_generator: Arc<RwLock<dyn LazyBatchGenerator>> =
        Arc::new(RwLock::new(ArrowStreamBatchGenerator::new(source)));

    Ok(Arc::new(LazyTableProvider::try_new(
        schema.clone(),
        vec![arrow_stream_batch_generator],
    )?))
}
pub struct ReaderWrapper {
    /// The reader, or `None` after it's been handed off to a fresh generator.
    /// A one-shot stream is consumed once.
    reader: Mutex<Option<Box<dyn RecordBatchReader + Send + 'static>>>,
}

impl fmt::Debug for ReaderWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderWrapper")
            .field("reader", &"<RecordBatchReader>")
            .finish()
    }
}

#[derive(Debug)]
pub struct ArrowStreamBatchGenerator {
    pub array_stream: ReaderWrapper,
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
    pub fn new(array_stream: Box<dyn RecordBatchReader + Send + 'static>) -> Self {
        Self {
            array_stream: ReaderWrapper {
                reader: Mutex::new(Some(array_stream)),
            },
        }
    }
}

impl LazyBatchGenerator for ArrowStreamBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(
        &mut self,
    ) -> deltalake::datafusion::error::Result<Option<deltalake::arrow::array::RecordBatch>> {
        let mut stream_reader = self.array_stream.reader.lock().map_err(|_| {
            deltalake::datafusion::error::DataFusionError::Execution(
                "Failed to lock the ArrowArrayStreamReader".to_string(),
            )
        })?;

        let Some(reader) = stream_reader.as_mut() else {
            return Err(deltalake::datafusion::error::DataFusionError::Execution(
                "Stream-based generator cannot be reset; the original stream has been consumed. \
                 Buffer input data if plan re-execution is required."
                    .to_string(),
            ));
        };

        match reader.next() {
            Some(Ok(record_batch)) => Ok(Some(record_batch)),
            Some(Err(err)) => Err(deltalake::datafusion::error::DataFusionError::ArrowError(
                Box::new(err),
                None,
            )),
            None => Ok(None), // End of stream
        }
    }

    /// Hands the reader to a new generator on the first call, then leaves an
    /// [`ExhaustedStreamGenerator`] behind so later calls error instead of
    /// returning no rows.
    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        match self.array_stream.reader.lock() {
            Ok(mut guard) => match guard.take() {
                Some(reader) => Arc::new(RwLock::new(ArrowStreamBatchGenerator::new(reader))),
                None => Arc::new(RwLock::new(ExhaustedStreamGenerator)),
            },
            Err(_) => Arc::new(RwLock::new(ExhaustedStreamGenerator)),
        }
    }
}

/// Generator for exhausted streams that cannot be reset.
#[derive(Debug)]
struct ExhaustedStreamGenerator;

impl std::fmt::Display for ExhaustedStreamGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExhaustedStreamGenerator")
    }
}

impl LazyBatchGenerator for ExhaustedStreamGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(
        &mut self,
    ) -> deltalake::datafusion::error::Result<Option<deltalake::arrow::array::RecordBatch>> {
        Err(deltalake::datafusion::error::DataFusionError::Execution(
            "Stream-based generator cannot be reset; the original stream has been consumed. \
             Buffer input data if plan re-execution is required."
                .to_string(),
        ))
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(ExhaustedStreamGenerator))
    }
}

/// A lazy casting wrapper around a RecordBatchReader
struct LazyCastReader {
    input: Box<dyn RecordBatchReader + Send + 'static>,
    target_schema: SchemaRef,
}

impl RecordBatchReader for LazyCastReader {
    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }
}

impl Iterator for LazyCastReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.input.next() {
            Some(Ok(batch)) => Some(
                cast_record_batch(&batch, self.target_schema.clone(), false, false)
                    .map_err(|e| ArrowError::CastError(e.to_string())),
            ),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// Returns a boxed reader that lazily casts each batch to the provided schema.
pub fn maybe_lazy_cast_reader(
    input: Box<dyn RecordBatchReader + Send + 'static>,
    target_schema: SchemaRef,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    if !input.schema().eq(&target_schema) {
        Box::new(LazyCastReader {
            input,
            target_schema,
        })
    } else {
        input
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use deltalake::arrow::array::{Int32Array, RecordBatchIterator};

    fn sample_reader() -> (Box<dyn RecordBatchReader + Send + 'static>, RecordBatch) {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema);
        (Box::new(reader), batch)
    }

    /// The generator from `reset_state` must yield the original data, not fail
    /// with "the original stream has been consumed".
    #[test]
    fn test_first_execution_succeeds_after_reset_state() {
        let (reader, expected) = sample_reader();
        let generator = ArrowStreamBatchGenerator::new(reader);

        // First reset hands the reader to a fresh generator, which must
        // produce the batch.
        let fresh = generator.reset_state();
        let batch = fresh
            .write()
            .generate_next_batch()
            .expect("first execution must succeed after reset_state")
            .expect("expected a batch from the fresh generator");
        assert_eq!(batch, expected);

        // Stream is one-shot: the next pull returns end-of-stream.
        assert!(fresh.write().generate_next_batch().unwrap().is_none());
    }

    /// A second `reset_state` can't replay a consumed stream, so it must error
    /// instead of returning no rows.
    #[test]
    fn test_second_reset_state_is_exhausted() {
        let (reader, _) = sample_reader();
        let generator = ArrowStreamBatchGenerator::new(reader);

        let _first = generator.reset_state();
        let second = generator.reset_state();
        let err = second
            .write()
            .generate_next_batch()
            .expect_err("re-executing a consumed stream must error");
        assert!(err.to_string().contains("the original stream has been consumed"));
    }
}
