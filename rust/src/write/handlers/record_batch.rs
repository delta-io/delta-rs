//! Handle RecordBatch messages when writing to delta tables
use crate::write::{DataWriterError, MessageHandler};
use arrow::{datatypes::Schema as ArrowSchema, record_batch::*};
use std::collections::HashMap;
use std::sync::Arc;

/// Handle RecordBatch messages when writing to delta tables
pub struct RecordBatchHandler;

impl MessageHandler<RecordBatch> for RecordBatchHandler {
    fn record_batch_from_message(
        &self,
        _arrow_schema: Arc<ArrowSchema>,
        message_buffer: &[RecordBatch],
    ) -> Result<RecordBatch, DataWriterError> {
        Ok(message_buffer[0].clone())
    }

    fn divide_by_partition_values(
        &self,
        _partition_columns: &[String],
        _records: Vec<RecordBatch>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, DataWriterError> {
        todo!()
    }
}
