//! Abstractions and implementations for writing data to delta tables

use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};

use crate::errors::DeltaTableError;

mod dataset;
mod file;
mod partition;
mod upload_budget;

#[cfg(feature = "datafusion")]
pub(crate) use dataset::write_batches_timed;
pub use dataset::{DeltaWriter, WriterConfig};
pub use partition::{PartitionWriter, PartitionWriterConfig};
pub(crate) use upload_budget::UploadBudget;

#[derive(thiserror::Error, Debug)]
enum WriteError {
    #[error("Unexpected Arrow schema: got: {schema}, expected: {expected_schema}")]
    SchemaMismatch {
        schema: ArrowSchemaRef,
        expected_schema: ArrowSchemaRef,
    },

    #[error("Error creating add action: {source}")]
    CreateAdd {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Error handling Arrow data: {source}")]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error("Error partitioning record batch: {0}")]
    Partitioning(String),
}

impl From<WriteError> for DeltaTableError {
    fn from(err: WriteError) -> Self {
        match err {
            WriteError::SchemaMismatch { .. } => DeltaTableError::SchemaMismatch {
                msg: err.to_string(),
            },
            WriteError::Arrow { source } => DeltaTableError::Arrow { source },
            _ => DeltaTableError::GenericError {
                source: Box::new(err),
            },
        }
    }
}

#[cfg(test)]
mod test_utils {
    use parquet::file::properties::WriterProperties;

    use crate::crate_version;

    pub(super) fn assert_default_created_by(writer_properties: &WriterProperties) {
        assert_eq!(
            writer_properties.created_by(),
            format!("delta-rs version {}", crate_version())
        );
    }
}
