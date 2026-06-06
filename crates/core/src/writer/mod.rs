//! Abstractions and implementations for writing data to delta tables

use arrow::{datatypes::FieldRef, datatypes::SchemaRef, error::ArrowError};
use async_trait::async_trait;
use object_store::Error as ObjectStoreError;
use parquet::errors::ParquetError;
use serde_json::Value;

use crate::DeltaTable;
use crate::errors::{ColumnMappingOperation, DeltaTableError};
use crate::kernel::schema::symmetric_differences;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{Action, Add, Version};
use crate::protocol::{ColumnCountStat, DeltaOperation, SaveMode};

pub use json::JsonWriter;
pub use record_batch::RecordBatchWriter;

pub mod json;
pub mod record_batch;
pub(crate) mod stats;
pub mod utils;

#[cfg(test)]
pub mod test_utils;

pub(crate) fn ensure_legacy_writer_supports_table(
    table: &DeltaTable,
    operation: &str,
) -> Result<(), DeltaTableError> {
    if table
        .snapshot()?
        .table_config()
        .column_mapping_mode
        .is_some_and(|mode| mode != delta_kernel::table_features::ColumnMappingMode::None)
    {
        return Err(DeltaTableError::unsupported_column_mapping(
            ColumnMappingOperation::Write,
            operation,
        ));
    }

    Ok(())
}

/// Enum representing an error when calling [`DeltaWriter`].
#[derive(thiserror::Error, Debug)]
pub(crate) enum DeltaWriterError {
    /// Partition column is missing in a record written to delta.
    #[error("Missing partition column: {0}")]
    MissingPartitionColumn(String),

    /// The Arrow RecordBatch schema does not match the expected schema.
    #[error("{}", format_schema_mismatch(record_batch_schema, expected_schema))]
    SchemaMismatch {
        /// The record batch schema.
        record_batch_schema: SchemaRef,
        /// The schema of the target delta table.
        expected_schema: SchemaRef,
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

    /// Serialization of delta log statistics failed.
    #[error("Failed to write statistics value {debug_value} with logical type {logical_type:?}")]
    StatsParsingFailed {
        debug_value: String,
        logical_type: Option<parquet::basic::LogicalType>,
    },

    /// JSON serialization failed
    #[error("Failed to serialize data to JSON: {source}")]
    JSONSerializationFailed {
        #[from]
        source: serde_json::Error,
    },

    /// underlying object store returned an error.
    #[error("ObjectStore interaction failed: {source}")]
    ObjectStore {
        /// The wrapped [`ObjectStoreError`]
        #[from]
        source: ObjectStoreError,
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

impl From<DeltaWriterError> for DeltaTableError {
    fn from(err: DeltaWriterError) -> Self {
        match err {
            DeltaWriterError::Arrow { source } => DeltaTableError::Arrow { source },
            DeltaWriterError::Io { source } => DeltaTableError::Io { source },
            DeltaWriterError::ObjectStore { source } => DeltaTableError::ObjectStore { source },
            DeltaWriterError::Parquet { source } => DeltaTableError::Parquet { source },
            DeltaWriterError::DeltaTable(e) => e,
            DeltaWriterError::SchemaMismatch { .. } => DeltaTableError::SchemaMismatch {
                msg: err.to_string(),
            },
            _ => DeltaTableError::Generic(err.to_string()),
        }
    }
}

fn format_schema_mismatch(record_batch_schema: &SchemaRef, expected_schema: &SchemaRef) -> String {
    // We can safely assume this will succeed since we already know the schemas are different
    let differences = symmetric_differences(record_batch_schema, expected_schema).unwrap();

    let (expected_fields, different_fields): (Vec<&FieldRef>, Vec<&FieldRef>) =
        differences.iter().partition(|f| {
            expected_schema
                .field_with_name(f.name())
                .is_ok_and(|e| e == f.as_ref()) // It possible the types are mismatch given the names matches
        });

    match (differences.is_empty(), expected_fields.is_empty()) {
        (true, _) => "Arrow RecordBatch schema is in the wrong order".to_string(),
        (false, false) => format!(
            "Arrow RecordBatch schema does not match: Missing fields: {expected_fields:?} and Found fields {different_fields:?}"
        ),
        (false, true) => {
            format!("Arrow RecordBatch schema does not match: Found extra fields {differences:?}")
        }
    }
}

/// Write mode for the [DeltaWriter]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum WriteMode {
    /// Default write mode which will return an error if schemas do not match correctly
    Default,
    /// Merge the schema of the table with the newly written data
    ///
    /// [Read more here](https://delta.io/blog/2023-02-08-delta-lake-schema-evolution/)
    MergeSchema,
}

#[async_trait]
/// Trait for writing data to Delta tables
pub trait DeltaWriter<T> {
    /// Write a chunk of values into the internal write buffers with the default write mode
    async fn write(&mut self, values: T) -> Result<(), DeltaTableError>;

    /// Wreite a chunk of values into the internal write buffers with the specified [WriteMode]
    async fn write_with_mode(&mut self, values: T, mode: WriteMode) -> Result<(), DeltaTableError>;

    /// Flush the internal write buffers to files in the delta table folder structure.
    /// The corresponding delta [`Add`] actions are returned and should be committed via a transaction.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError>;

    /// Flush the internal write buffers to files in the delta table folder structure.
    /// and commit the changes to the Delta log, creating a new table version.
    async fn flush_and_commit(
        &mut self,
        table: &mut DeltaTable,
    ) -> Result<Version, DeltaTableError> {
        let adds: Vec<_> = self.flush().await?.drain(..).map(Action::Add).collect();
        flush_and_commit(adds, table, None).await
    }
}

/// Method for flushing to be used by writers
pub(crate) async fn flush_and_commit(
    adds: Vec<Action>,
    table: &mut DeltaTable,
    commit_properties: Option<CommitProperties>,
) -> Result<Version, DeltaTableError> {
    let snapshot = table.snapshot()?;
    let partition_cols: Vec<String> = snapshot.metadata().partition_columns().into();
    let partition_by = if !partition_cols.is_empty() {
        Some(partition_cols)
    } else {
        None
    };
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by,
        predicate: None,
    };

    let finalized = CommitBuilder::from(commit_properties.unwrap_or_default())
        .with_actions(adds)
        .build(Some(snapshot), table.log_store.clone(), operation)
        .await?;
    table.state = Some(finalized.snapshot());
    Ok(finalized.version())
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::DataType;

    use super::*;
    use crate::DeltaResult;
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use pretty_assertions::assert_ne;
    use std::sync::Arc;

    /// This test doesn't have a great way to _validate_ that logs are not cleaned up as part of
    /// the second commit.
    ///
    /// Instead I just added some prints in the [PostCommit] logic to validate that the property
    /// was getting pulled through correctly in the non-default case.
    ///
    /// The _ideal_ testing scenario would probably be to propagate metrics out of
    /// [flush_and_commit] but that's an API change we isn't desirable at the moment
    #[tokio::test]
    async fn test_flush_and_commit() -> DeltaResult<()> {
        let mut table = DeltaTable::new_in_memory()
            .create()
            .with_table_name("my_table")
            .with_column(
                "id",
                DataType::Primitive(delta_kernel::schema::PrimitiveType::Long),
                true,
                None,
            )
            .with_configuration_property(
                crate::TableProperty::LogRetentionDuration,
                Some("interval 0 days"),
            )
            .await?;

        let add = Add::default();
        let actions = vec![Action::Add(add)];
        let first_version = flush_and_commit(actions, &mut table, None).await?;

        let add = Add::default();
        let actions = vec![Action::Add(add)];

        let properties = CommitProperties::default().with_cleanup_expired_logs(Some(false));
        let second_version = flush_and_commit(actions, &mut table, Some(properties)).await?;
        assert_ne!(
            second_version, first_version,
            "flush_and_commit did not create a version apparently?"
        );
        Ok(())
    }

    #[test]
    fn test_schema_mismatch_message_different_order() {
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field2", ArrowDataType::Utf8, true),
            Field::new("field1", ArrowDataType::Int32, false),
        ]));

        let existing_schema = Arc::new(Schema::new(vec![
            Field::new("field1", ArrowDataType::Int32, false),
            Field::new("field2", ArrowDataType::Utf8, true),
        ]));

        let format_message = format_schema_mismatch(&new_schema, &existing_schema);
        assert_eq!(
            format_message,
            "Arrow RecordBatch schema is in the wrong order".to_string()
        );
    }

    #[test]
    fn test_schema_mismatch_message_completely_different() {
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field3", ArrowDataType::Utf8, true),
            Field::new("field4", ArrowDataType::Int32, false),
        ]));

        let existing_schema = Arc::new(Schema::new(vec![
            Field::new("field1", ArrowDataType::Int32, false),
            Field::new("field2", ArrowDataType::Utf8, true),
        ]));

        let format_message = format_schema_mismatch(&new_schema, &existing_schema);

        let missing_field = vec![
            Field::new("field1", ArrowDataType::Int32, false),
            Field::new("field2", ArrowDataType::Utf8, true),
        ];

        let found_field = vec![
            Field::new("field3", ArrowDataType::Utf8, true),
            Field::new("field4", ArrowDataType::Int32, false),
        ];
        let expected = format!(
            "Arrow RecordBatch schema does not match: Missing fields: {missing_field:?} and Found fields {found_field:?}"
        );
        assert_eq!(format_message, expected);
    }

    #[test]
    fn test_schema_mismatch_message_extra_field() {
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", ArrowDataType::Int32, false),
            Field::new("field2", ArrowDataType::Utf8, true),
            Field::new("field3", ArrowDataType::Utf8, true),
        ]));

        let existing_schema = Arc::new(Schema::new(vec![
            Field::new("field1", ArrowDataType::Int32, false),
            Field::new("field2", ArrowDataType::Utf8, true),
        ]));

        let format_message = format_schema_mismatch(&new_schema, &existing_schema);

        let extra_field = vec![Field::new("field3", ArrowDataType::Utf8, true)];

        let expected =
            format!("Arrow RecordBatch schema does not match: Found extra fields {extra_field:?}");
        assert_eq!(format_message, expected);
    }
}
