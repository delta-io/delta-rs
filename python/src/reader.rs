use arrow_schema::{ArrowError, SchemaRef};
use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::execution::RecordBatchStream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::mpsc::{sync_channel, Receiver};

use crate::utils::rt;

/// A lazy adapter to convert an async RecordBatchStream into a sync RecordBatchReader
struct StreamToReaderAdapter {
    schema: SchemaRef,
    receiver: Receiver<Result<RecordBatch, ArrowError>>,
}

impl Iterator for StreamToReaderAdapter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl RecordBatchReader for StreamToReaderAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Converts a RecordBatchStream into a lazy RecordBatchReader
pub(crate) fn convert_stream_to_reader(
    mut stream: Pin<Box<dyn RecordBatchStream + Send>>,
) -> Box<dyn RecordBatchReader + Send> {
    let (sender, receiver) = sync_channel(0);
    let schema = stream.schema();

    rt().spawn_blocking(move || {
        rt().block_on(async move {
            while let Some(batch) = stream.next().await {
                if sender
                    .send(batch.map_err(|e| ArrowError::ExternalError(Box::new(e))))
                    .is_err()
                {
                    break;
                }
            }
        });
    });

    Box::new(StreamToReaderAdapter { schema, receiver })
}
