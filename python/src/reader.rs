use arrow_schema::{ArrowError, SchemaRef};
use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::execution::RecordBatchStream;
use futures::StreamExt;
use std::pin::Pin;

use crate::utils::rt;

/// A lazy adapter to convert an async RecordBatchStream into a sync RecordBatchReader
struct StreamToReaderAdapter {
    schema: SchemaRef,
    stream: Pin<Box<dyn RecordBatchStream + Send>>,
}

impl Iterator for StreamToReaderAdapter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        rt().block_on(self.stream.next())
            .map(|b| b.map_err(|e| ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for StreamToReaderAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Converts a RecordBatchStream into a lazy RecordBatchReader
pub(crate) fn convert_stream_to_reader(
    stream: Pin<Box<dyn RecordBatchStream + Send>>,
) -> Box<dyn RecordBatchReader + Send> {
    Box::new(StreamToReaderAdapter {
        schema: stream.schema(),
        stream,
    })
}
