//! Abstractions and implementations for writing data to delta tables
//!
//! A write fans out three times: over partitions, over the files of each partition, and over
//! the parts of each file's upload.
//!
//! ```text
//!                                  RecordBatch from any source
//!                                               │
//!                                               ▼
//!                                ┌─────────────────────────────┐
//!                                │         DeltaWriter         │
//!                                │    splits each batch by     │
//!                                │   partition value, drops    │
//!                                │    the partition columns    │
//!                                └──────────────┬──────────────┘
//!                ┌──────────────────────────────┼──────────────────────────────┐
//!                │ a=1/                         │ a=2/                         │ a=3/
//!                ▼                              ▼                              ▼
//!      ┌───────────────────┐          ┌───────────────────┐          ┌───────────────────┐
//!      │  PartitionWriter  │          │  PartitionWriter  │          │  PartitionWriter  │
//!      └─────────┬─────┬───┘          └───────────────────┘          └───────────────────┘
//!                │     │ file reaches target_file_size: reserve its bytes in the
//!                │     │ UploadBudget, open the next file, spawn LazyArrowWriter::finish
//!                │     │                                 ┌───────────────────────────────┐
//!                │     └────────────────────────────────►│ finish tasks                  │
//!                │ slices of write_batch_size rows       │ part-00000  part-00001        │
//!                ▼                                       │ each: last row group, footer, │
//!      ┌───────────────────┐                             │ last part, complete upload,   │
//!      │  LazyArrowWriter  │                             │ then release its bytes        │
//!      │ open: part-00002  │                             └───────────────────────────────┘
//!      │ Initialized until │
//!      │ the first batch,  │
//!      │ then Writing      │
//!      └─────────┬─────────┘
//!                │ Writing holds
//!                ▼
//!      ┌───────────────────┐
//!      │ AsyncArrowWriter  │  encodes rows into the open row group, by encoding columns serially
//!      └─────────┬─────────┘
//!                │ each complete row group, through ParquetObjectWriter
//!                ▼
//!      ┌───────────────────┐
//!      │     BufWriter     │  a file under upload_part_size is one PUT at finish; a larger
//!      └─────────┬─────────┘  one starts a multipart upload once its row groups reach that size
//!    ┌───────────┼───────────┐
//!    ▼           ▼           ▼
//! part 1      part 2 ...  part n    upload concurrently; the next row group waits
//!                                   while max_concurrency_tasks parts are in flight
//! ```
//!
//! An unpartitioned table has a single [`PartitionWriter`]. A `LazyArrowWriter` creates its
//! writers on the first batch, so a file without rows is never written. Every file, open or
//! finishing, has its own chain from `AsyncArrowWriter` to the parts. Bytes reach the
//! `BufWriter` only as complete row groups, by default every 1,048,576 rows. A file with several
//! row groups uploads the earlier ones while it is written. A file with one row group is sent
//! entirely by its finish task. All partition writers of one write reserve bytes in the same
//! `UploadBudget`, so a slow store makes them wait instead of holding more files in memory.
//!
//! [`DeltaWriter`] lives in `dataset.rs`, [`PartitionWriter`] in `partition.rs`, the
//! `LazyArrowWriter` and its upload in `file.rs`, and the `UploadBudget` in `upload_budget.rs`.

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
