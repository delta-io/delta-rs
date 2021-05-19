//! The writer module contains the DeltaWriter and DeltaWriterError definitions and provides a
//! higher level API for performing Delta transaction writes to a given Delta Table.
//!
//! Unlike the transaction API on DeltaTable, this higher level writer will also write out the
//! parquet files

use arrow::record_batch::RecordBatch;
use crate::{DeltaTableError, DeltaTransactionError};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::InMemoryWriteableCursor;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::convert::TryFrom;
use std::sync::Arc;

struct ParquetBuffer {
    writer: ArrowWriter<InMemoryWriteableCursor>,
    cursor: InMemoryWriteableCursor,
}

impl ParquetBuffer {
    fn try_new(schema: arrow::datatypes::SchemaRef) -> Result<Self, DeltaTableError> {
        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        let cursor = InMemoryWriteableCursor::default();
        let writer = ArrowWriter::try_new(
            cursor.clone(),
            schema,
            Some(writer_properties),
        )?;

        Ok(Self { writer, cursor })
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        self.writer.write(batch).map_err(|source| DeltaTableError::ParquetError { source })
    }

    fn data(&self) -> Vec<u8> {
        self.cursor.data()
    }

    fn close(&mut self) -> Result<parquet_format::FileMetaData, DeltaTableError> {
        self.writer.close().map_err(|source| DeltaTableError::ParquetError { source })
    }
}

struct BufferedJSONWriter {
    table: crate::DeltaTable,
    buffer: Vec<serde_json::Value>,
    schema: arrow::datatypes::SchemaRef,
}

impl BufferedJSONWriter {
    fn try_new(table: crate::DeltaTable) -> Result<Self, DeltaTableError> {
        let metadata = table.get_metadata()?.clone();
        let schema = metadata.schema;
        let arrow_schema = <arrow::datatypes::Schema as TryFrom<&crate::Schema>>::try_from(&schema).unwrap();
        let schema = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns;

        Ok(Self {
            table,
            schema,
            buffer: vec![],
        })
    }

    /**
     * Return the total Values pending in the buffer
     */
    fn count(&self) -> usize {
        self.buffer.len()
    }

    /**
     * Write a new Value into the buffer
     */
    fn write(&mut self, value: Value) -> std::io::Result<()> {
        self.buffer.push(value);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DeltaTransactionError> {
        use arrow::json::reader::Decoder;

        let mut value_iter = InMemValueIter::from_vec(&self.buffer);
        let decoder = Decoder::new(self.schema.clone(), self.count(), None);
        let record_batch = decoder
            .next_batch(&mut value_iter)
            .map_err(|source| DeltaTableError::ArrowError { source })?;

        if record_batch.is_none() {
            return Ok(());
        }

        let mut pb = ParquetBuffer::try_new(self.schema.clone())?;
        pb.write_batch(&record_batch.unwrap())?;
        let _metadata = pb.close();

        self.table.add_file(&pb.data(), None, None).await?;

        self.buffer.clear();
        Ok(())
    }
}

struct InMemValueIter<'a> {
    buffer: &'a [Value],
    current_index: usize,
}

impl<'a> InMemValueIter<'a> {
    fn from_vec(buffer: &'a [Value]) -> Self {
        Self {
            buffer,
            current_index: 0,
        }
    }
}

impl<'a> Iterator for InMemValueIter<'a> {
    type Item = Result<Value, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.buffer.get(self.current_index);
        self.current_index += 1;
        item.map(|v| Ok(v.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple() {
        let table = crate::open_table("./tests/data/delta-0.8.0").await.unwrap();
        let writer = BufferedJSONWriter::try_new(table).unwrap();
    }
}
