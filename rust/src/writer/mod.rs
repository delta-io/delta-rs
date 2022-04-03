//! Abstractions and implementations for writing data to delta tables
// TODO
// - consider file size when writing parquet files
// - handle writer version
pub mod json;
pub mod record_batch;
mod stats;
#[cfg(test)]
pub mod test_utils;
pub mod utils;

use crate::{
    action::{Action, Add, ColumnCountStat, Stats},
    delta::DeltaTable,
    DeltaDataTypeVersion, DeltaTableError, StorageError, UriError,
};
use arrow::{datatypes::SchemaRef, datatypes::*, error::ArrowError};
use async_trait::async_trait;
pub use json::JsonWriter;
use parquet::{basic::LogicalType, errors::ParquetError};
pub use record_batch::RecordBatchWriter;
use serde_json::Value;
use std::sync::Arc;

/// Enum representing an error when calling [`DeltaWriter`].
#[derive(thiserror::Error, Debug)]
pub enum DeltaWriterError {
    /// Partition column is missing in a record written to delta.
    #[error("Missing partition column: {0}")]
    MissingPartitionColumn(String),

    /// The Arrow RecordBatch schema does not match the expected schema.
    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        /// The record batch schema.
        record_batch_schema: SchemaRef,
        /// The schema of the target delta table.
        expected_schema: Arc<arrow::datatypes::Schema>,
    },

    /// An Arrow RecordBatch could not be created from the JSON buffer.
    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    /// A record was written that was not a JSON object.
    #[error("Record {0} is not a JSON object")]
    InvalidRecord(String),

    /// Indicates that a partial write was performed and error records were discarded.
    #[error("Failed to write some values to parquet. Sample error: {sample_error}.")]
    PartialParquetWrite {
        /// Vec of tuples where the first element of each tuple is the skipped value and the second element is the [`ParquetError`] associated with it.
        skipped_values: Vec<(Value, ParquetError)>,
        /// A sample [`ParquetError`] representing the overall partial write.
        sample_error: ParquetError,
    },

    // TODO: derive Debug for Stats in delta-rs
    /// Serialization of delta log statistics failed.
    #[error("Serialization of delta log statistics failed")]
    StatsSerializationFailed {
        /// The stats object that failed serialization.
        stats: Stats,
    },

    /// Invalid table paths was specified for the delta table.
    #[error("Invalid table path: {}", .source)]
    UriError {
        /// The wrapped [`UriError`].
        #[from]
        source: UriError,
    },

    /// deltalake storage backend returned an error.
    #[error("Storage interaction failed: {source}")]
    Storage {
        /// The wrapped [`StorageError`]
        #[from]
        source: StorageError,
    },

    /// Arrow returned an error.
    #[error("Arrow interaction failed: {source}")]
    Arrow {
        /// The wrapped [`ArrowError`]
        #[from]
        source: ArrowError,
    },

    /// Parquet write failed.
    #[error("Parquet write failed: {source}")]
    Parquet {
        /// The wrapped [`ParquetError`]
        #[from]
        source: ParquetError,
    },

    /// Error returned from std::io
    #[error("std::io::Error: {source}")]
    Io {
        /// The wrapped [`std::io::Error`]
        #[from]
        source: std::io::Error,
    },

    /// Error returned
    #[error(transparent)]
    DeltaTable(#[from] DeltaTableError),
}

#[async_trait]
trait DeltaWriter<T> {
    /// write a chunk of values into the internal write buffers.
    async fn write(&mut self, values: T) -> Result<(), DeltaWriterError>;

    /// Flush the internal write buffers to files in the delta table folder structure.
    /// The corresponding delta [`Add`] actions are returned and should be committed via a transaction.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaWriterError>;

    /// Flush the internal write buffers to files in the delta table folder structure.
    /// and commit the changes to the Delta log, creating a new table version.
    async fn flush_and_commit(
        &mut self,
        table: &mut DeltaTable,
    ) -> Result<DeltaDataTypeVersion, DeltaWriterError> {
        let mut adds = self.flush().await?;
        let mut tx = table.create_transaction(None);
        tx.add_actions(adds.drain(..).map(Action::add).collect());
        let version = tx.commit(None).await?;
        Ok(version)
    }
}
