//! Basic data processing types
use super::DataWriterError;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use futures::stream::{Stream, TryStreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// Definitions for RecordBatchStream are taken from the datafusion crate
// https://github.com/apache/arrow-datafusion/blob/d04790041caecdc53077de37db3e30388f8ff38c/datafusion/src/physical_plan/mod.rs#L46-L87

/// Trait for types that stream [arrow::record_batch::RecordBatch]
pub trait RecordBatchStream: Stream<Item = ArrowResult<RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a stream of record batches.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send + Sync>>;

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Stream of record batches
pub struct SizedRecordBatchStream {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl SizedRecordBatchStream {
    /// Create a new RecordBatchIterator
    pub fn new(schema: SchemaRef, batches: Vec<Arc<RecordBatch>>) -> Self {
        SizedRecordBatchStream {
            schema,
            index: 0,
            batches,
        }
    }
}

impl Stream for SizedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.batches.len() {
            self.index += 1;
            Some(Ok(self.batches[self.index - 1].as_ref().clone()))
        } else {
            None
        })
    }
}

impl RecordBatchStream for SizedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Create a vector of record batches from a stream
pub async fn collect(
    stream: SendableRecordBatchStream,
) -> Result<Vec<RecordBatch>, DataWriterError> {
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(DataWriterError::from)
}
