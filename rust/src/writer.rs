//! The writer module contains the DeltaWriter and DeltaWriterError definitions and provides a
//! higher level API for performing Delta transaction writes to a given Delta Table.
//!
//! Unlike the transaction API on DeltaTable, this higher level writer will also write out the
//! parquet files

use crate::action::Txn;
use crate::{DeltaTableError, DeltaTransactionError};
use arrow::record_batch::RecordBatch;
use log::*;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::InMemoryWriteableCursor;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

/// BufferedJsonWriter allows for buffering serde_json::Value rows before flushing to parquet files
/// and a Delta transaction
pub struct BufferedJsonWriter {
    table: crate::DeltaTable,
    buffer: HashMap<WriterPartition, Vec<Value>>,
    schema: arrow::datatypes::SchemaRef,
    partitions: Vec<String>,
    txns: Vec<Txn>,
}

impl BufferedJsonWriter {
    /// Attempt to construct the BufferedJsonWriter, will fail if the table's metadata is not
    /// present
    pub fn try_new(table: crate::DeltaTable) -> Result<Self, DeltaTableError> {
        let metadata = table.get_metadata()?.clone();
        let schema = metadata.schema;
        let arrow_schema =
            <arrow::datatypes::Schema as TryFrom<&crate::Schema>>::try_from(&schema).unwrap();
        let schema = Arc::new(arrow_schema);

        Ok(Self {
            table,
            schema,
            buffer: HashMap::new(),
            partitions: metadata.partition_columns,
            txns: vec![],
        })
    }

    /// Return the total Values pending in the buffer
    pub fn count(&self, partitions: &WriterPartition) -> Option<usize> {
        self.buffer.get(partitions).map(|b| b.len())
    }

    /// Add a txn action to the buffer
    pub fn record_txn(&mut self, txn: Txn) {
        self.txns.push(txn);
    }

    /// Write a new Value into the buffer
    pub fn write(
        &mut self,
        value: Value,
        partitions: WriterPartition,
    ) -> Result<(), DeltaTableError> {
        match partitions {
            WriterPartition::NoPartitions => {
                if !self.partitions.is_empty() {
                    return Err(DeltaTableError::SchemaMismatch {
                        msg: "Table has partitions but noone were supplied on write".to_string(),
                    });
                }
            }
            WriterPartition::KeyValues { .. } => {
                if self.partitions.is_empty() {
                    return Err(DeltaTableError::SchemaMismatch {
                        msg: "Table has no partitions yet they were supplied on write".to_string(),
                    });
                }
            }
        }

        if let Some(buffer) = self.buffer.get_mut(&partitions) {
            buffer.push(value);
        } else {
            self.buffer.insert(partitions, vec![value]);
        }
        Ok(())
    }

    /// Flush the buffer, causing a write of parquet files for each set of partitioned information
    /// as well as any buffered txn actions
    ///
    /// This will create a single transaction in the delta transaction log
    pub async fn flush(&mut self) -> Result<(), DeltaTransactionError> {
        use arrow::json::reader::Decoder;

        let mut parquet_bufs = vec![];

        for (partitions, values) in self.buffer.iter() {
            let count = self.count(partitions).unwrap_or(0);
            let mut value_iter = InMemValueIter::from_vec(values);
            let decoder = Decoder::new(self.schema.clone(), count, None);

            let record_batch = decoder
                .next_batch(&mut value_iter)
                .map_err(|source| DeltaTableError::ArrowError { source })?;

            if record_batch.is_some() {
                let mut pb = ParquetBuffer::try_new(self.schema.clone())?;
                pb.write_batch(&record_batch.unwrap())?;
                let _metadata = pb.close();
                parquet_bufs.push((partitions.clone(), pb.data()));
            } else {
                warn!("Attempted to flush an empty RecordBatch from the BufferedJsonWriter");
            }
        }

        let mut dtx = self.table.create_transaction(None);
        for (partitions, buf) in parquet_bufs {
            match partitions {
                WriterPartition::NoPartitions => {
                    dtx.add_file(&buf, None).await?;
                }
                WriterPartition::KeyValues { partitions } => {
                    dtx.add_file(&buf, Some(partitions)).await?;
                }
            }
        }

        dtx.add_actions(
            self.txns
                .drain(0..)
                .map(crate::action::Action::txn)
                .collect(),
        );

        dtx.commit(None).await?;
        self.buffer.clear();
        Ok(())
    }
}

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
        let writer = ArrowWriter::try_new(cursor.clone(), schema, Some(writer_properties))?;

        Ok(Self { writer, cursor })
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        self.writer
            .write(batch)
            .map_err(|source| DeltaTableError::ParquetError { source })
    }

    fn data(&self) -> Vec<u8> {
        self.cursor.data()
    }

    fn close(&mut self) -> Result<parquet_format::FileMetaData, DeltaTableError> {
        self.writer
            .close()
            .map_err(|source| DeltaTableError::ParquetError { source })
    }
}

pub(crate) struct InMemValueIter<'a> {
    buffer: &'a [Value],
    current_index: usize,
}

impl<'a> InMemValueIter<'a> {
    pub(crate) fn from_vec(buffer: &'a [Value]) -> Self {
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

/// The type of partition for a row being written to a writer
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum WriterPartition {
    /// The row is not partitioned
    NoPartitions,
    /// The row is partitioned with the following sets of key=value partition strings
    KeyValues {
        /// Each entry in the vec should be a key=value pair
        partitions: Vec<(String, String)>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_writer_buffer_nopartition() {
        let table = crate::open_table("./tests/data/delta-0.8.0").await.unwrap();
        let mut writer = BufferedJsonWriter::try_new(table).unwrap();
        assert_eq!(writer.count(&WriterPartition::NoPartitions), None);
        let res = writer.write(json!({"hello":"world"}), WriterPartition::NoPartitions);
        assert!(res.is_ok());
        assert_eq!(writer.count(&WriterPartition::NoPartitions), Some(1));
    }

    #[tokio::test]
    async fn test_writer_write_partition_mismatch() {
        let table = crate::open_table("./tests/data/delta-0.8.0-partitioned")
            .await
            .unwrap();
        let mut writer = BufferedJsonWriter::try_new(table).unwrap();
        let res = writer.write(json!({"hello":"world"}), WriterPartition::NoPartitions);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_writer_write_partitions_to_nopartition() {
        let table = crate::open_table("./tests/data/delta-0.8.0").await.unwrap();
        let mut writer = BufferedJsonWriter::try_new(table).unwrap();
        let partitions = WriterPartition::KeyValues {
            partitions: vec![("year".to_string(), "2021".to_string())],
        };
        let res = writer.write(json!({"hello":"world"}), partitions);
        assert!(res.is_err());
    }
}
