//! Arrow writers for writing arrow partitions
use super::DeltaWriterError;
use crate::{action::ColumnCountStat, write::stats::apply_null_counts};
use arrow::{datatypes::Schema as ArrowSchema, record_batch::*};
// use log::{info, warn};
use parquet::{
    arrow::ArrowWriter,
    errors::ParquetError,
    file::{properties::WriterProperties, writer::InMemoryWriteableCursor},
};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

type NullCounts = HashMap<String, ColumnCountStat>;

pub(super) struct PartitionWriter {
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    pub(super) cursor: InMemoryWriteableCursor,
    pub(super) arrow_writer: ArrowWriter<InMemoryWriteableCursor>,
    pub(super) partition_values: HashMap<String, Option<String>>,
    pub(super) null_counts: NullCounts,
    pub(super) buffered_record_batch_count: usize,
}

impl PartitionWriter {
    pub fn new(
        arrow_schema: Arc<ArrowSchema>,
        partition_values: HashMap<String, Option<String>>,
        writer_properties: WriterProperties,
    ) -> Result<Self, ParquetError> {
        let cursor = InMemoryWriteableCursor::default();
        let arrow_writer = new_underlying_writer(
            cursor.clone(),
            arrow_schema.clone(),
            writer_properties.clone(),
        )?;

        let null_counts = NullCounts::new();
        let buffered_record_batch_count = 0;

        Ok(Self {
            arrow_schema,
            writer_properties,
            cursor,
            arrow_writer,
            partition_values,
            null_counts,
            buffered_record_batch_count,
        })
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many
    /// record batches and flushed after the appropriate number of bytes has been written.
    pub async fn write(&mut self, record_batch: &RecordBatch) -> Result<(), DeltaWriterError> {
        if record_batch.schema() != self.arrow_schema {
            return Err(DeltaWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: self.arrow_schema.clone(),
            });
        }

        // Copy current cursor bytes so we can recover from failures
        let current_cursor_bytes = self.cursor.data();
        match self.arrow_writer.write(record_batch) {
            Ok(_) => {
                self.buffered_record_batch_count += 1;
                apply_null_counts(&record_batch.clone().into(), &mut self.null_counts, 0);
                Ok(())
            }
            // If a write fails we need to reset the state of the PartitionWriter
            Err(e) => {
                let new_cursor = cursor_from_bytes(current_cursor_bytes.as_slice())?;
                let _ = std::mem::replace(&mut self.cursor, new_cursor.clone());
                let arrow_writer = new_underlying_writer(
                    new_cursor,
                    self.arrow_schema.clone(),
                    self.writer_properties.clone(),
                )?;
                let _ = std::mem::replace(&mut self.arrow_writer, arrow_writer);
                self.partition_values.clear();

                Err(e.into())
            }
        }
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.cursor.len()
    }
}

fn cursor_from_bytes(bytes: &[u8]) -> Result<InMemoryWriteableCursor, std::io::Error> {
    let mut cursor = InMemoryWriteableCursor::default();
    cursor.write_all(bytes)?;
    Ok(cursor)
}

fn new_underlying_writer(
    cursor: InMemoryWriteableCursor,
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
) -> Result<ArrowWriter<InMemoryWriteableCursor>, ParquetError> {
    ArrowWriter::try_new(cursor, arrow_schema, Some(writer_properties))
}
