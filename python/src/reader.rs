use std::sync::Arc;

use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;

use crate::utils::rt;

/// A lazy adapter to convert an async RecordBatchStream into a sync RecordBatchReader
struct StreamToReaderAdapter {
    schema: SchemaRef,
    cast_targets: Vec<Option<DataType>>,
    needs_cast: bool,
    stream: SendableRecordBatchStream,
}

fn view_type_contract(schema: &SchemaRef) -> (SchemaRef, Vec<Option<DataType>>, bool) {
    let mut targets = Vec::with_capacity(schema.fields().len());
    let mut needs_cast = false;

    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let dt = match f.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    needs_cast = true;
                    targets.push(Some(DataType::Utf8View));
                    DataType::Utf8View
                }
                DataType::Binary | DataType::LargeBinary => {
                    needs_cast = true;
                    targets.push(Some(DataType::BinaryView));
                    DataType::BinaryView
                }
                _ => {
                    targets.push(None);
                    f.data_type().clone()
                }
            };

            if &dt != f.data_type() {
                Arc::new(f.as_ref().clone().with_data_type(dt))
            } else {
                f.clone()
            }
        })
        .collect::<Vec<_>>();

    let out = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    (out, targets, needs_cast)
}

impl Iterator for StreamToReaderAdapter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = tokio::task::block_in_place(|| {
            rt().block_on(self.stream.next())
                .map(|b| b.map_err(|e| ArrowError::ExternalError(Box::new(e))))
        });

        match next {
            Some(Ok(batch)) if self.needs_cast => Some(self.normalize_batch(batch)),
            other => other,
        }
    }
}

impl StreamToReaderAdapter {
    fn normalize_batch(&self, batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
        let mut cols = Vec::with_capacity(batch.num_columns());

        for i in 0..batch.num_columns() {
            match &self.cast_targets[i] {
                Some(dt) if batch.schema().field(i).data_type() != dt => {
                    cols.push(cast(batch.column(i).as_ref(), dt)?);
                }
                _ => cols.push(batch.column(i).clone()),
            }
        }

        RecordBatch::try_new(self.schema.clone(), cols)
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
    let (schema, cast_targets, needs_cast) = view_type_contract(&stream.schema());
    Box::new(StreamToReaderAdapter {
        schema,
        cast_targets,
        needs_cast,
        stream,
    })
}
