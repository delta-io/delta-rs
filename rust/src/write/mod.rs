//! Abstractions and implementations for writing data to delta tables
// TODO
// - consider file size when writing parquet files
// - handle writer version

use crate::{
    action::{ColumnCountStat, ColumnValueStat, Stats},
    schema, DeltaTableError, Schema, StorageError, UriError,
};
use arrow::{
    datatypes::*,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef},
    error::ArrowError,
    record_batch::*,
};
use parquet::{basic::LogicalType, errors::ParquetError};
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
pub use writer::*;

mod arrow_buffer;
pub mod json;
mod stats;
pub mod streams;
pub mod writer;

const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

type NullCounts = HashMap<String, ColumnCountStat>;
type MinAndMaxValues = (
    HashMap<String, ColumnValueStat>,
    HashMap<String, ColumnValueStat>,
);

impl TryFrom<Arc<ArrowSchema>> for Schema {
    type Error = DeltaTableError;

    fn try_from(s: Arc<ArrowSchema>) -> Result<Self, DeltaTableError> {
        let fields = s
            .fields()
            .iter()
            .map(<schema::SchemaField as TryFrom<&ArrowField>>::try_from)
            .collect::<Result<Vec<schema::SchemaField>, DeltaTableError>>()?;

        Ok(Schema::new(fields))
    }
}

impl TryFrom<&ArrowField> for schema::SchemaField {
    type Error = DeltaTableError;

    fn try_from(f: &ArrowField) -> Result<Self, DeltaTableError> {
        let field = schema::SchemaField::new(
            f.name().to_string(),
            schema::SchemaDataType::try_from(f.data_type())?,
            f.is_nullable(),
            HashMap::new(),
        );
        Ok(field)
    }
}

impl TryFrom<&ArrowDataType> for schema::SchemaDataType {
    type Error = DeltaTableError;

    fn try_from(t: &ArrowDataType) -> Result<Self, DeltaTableError> {
        match t {
            ArrowDataType::Utf8 => Ok(schema::SchemaDataType::primitive("string".to_string())),
            ArrowDataType::Int64 => Ok(schema::SchemaDataType::primitive("long".to_string())),
            ArrowDataType::Int32 => Ok(schema::SchemaDataType::primitive("integer".to_string())),
            ArrowDataType::Int16 => Ok(schema::SchemaDataType::primitive("short".to_string())),
            ArrowDataType::Int8 => Ok(schema::SchemaDataType::primitive("byte".to_string())),
            ArrowDataType::Float32 => Ok(schema::SchemaDataType::primitive("float".to_string())),
            ArrowDataType::Float64 => Ok(schema::SchemaDataType::primitive("double".to_string())),
            ArrowDataType::Boolean => Ok(schema::SchemaDataType::primitive("boolean".to_string())),
            ArrowDataType::Binary => Ok(schema::SchemaDataType::primitive("binary".to_string())),
            ArrowDataType::Date32 => Ok(schema::SchemaDataType::primitive("date".to_string())),
            // TODO handle missing datatypes, especially struct, array, map
            _ => Err(DeltaTableError::Generic(
                "Error converting Arrow datatype.".to_string(),
            )),
        }
    }
}

/// Enum representing an error when calling [`DataWriter`].
#[derive(thiserror::Error, Debug)]
pub enum DataWriterError {
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

    /// DeltaTable returned an error.
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// The wrapped [`DeltaTableError`]
        #[from]
        source: DeltaTableError,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SchemaDataType, SchemaField};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn convert_schema_arrow_to_delta() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
        ]);
        let ref_schema = get_delta_schema();
        let schema = Schema::try_from(Arc::new(arrow_schema)).unwrap();
        assert_eq!(schema, ref_schema);
    }

    fn get_delta_schema() -> Schema {
        Schema::new(vec![
            SchemaField::new(
                "id".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "value".to_string(),
                SchemaDataType::primitive("integer".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "modified".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
        ])
    }
}
