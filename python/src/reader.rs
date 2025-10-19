use arrow_schema::{ArrowError, SchemaRef};
use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;

use crate::utils::rt;

/// A lazy adapter to convert an async RecordBatchStream into a sync RecordBatchReader
struct StreamToReaderAdapter {
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
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

/// Converts a [`SendableRecordBatchStream`] into a lazy RecordBatchReader
pub(crate) fn convert_stream_to_reader(
    stream: SendableRecordBatchStream,
) -> Box<dyn RecordBatchReader + Send> {
    Box::new(StreamToReaderAdapter {
        schema: stream.schema(),
        stream,
    })
}
