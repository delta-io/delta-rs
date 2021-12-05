//! High level delta commands that can be executed against a delta table
use crate::{storage::StorageError, DeltaTable, DeltaTableError};
use async_trait::async_trait;

use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use futures::stream::Stream;
use futures::TryStreamExt;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub mod create;
// pub mod delete;
// pub mod merge;
// pub mod update;
pub mod write;

/// Enum representing an error when calling [`DeltaCommandExec`].
#[derive(thiserror::Error, Debug)]
pub enum DeltaCommandError {
    /// Error returned when the table to be created already exists
    #[error("Table: '{0}' already exists")]
    TableAlreadyExists(String),
    /// Error returned when errors occur in underlying delta table instance
    #[error("Error in underlying DeltaTable")]
    DeltaTableError {
        /// Raw internal DeltaTableError
        #[from]
        source: DeltaTableError,
    },
    /// Error returned when errors occur in underlying storage instance
    #[error("Error in underlying storage backend")]
    StorageError {
        /// Raw internal StorageError
        #[from]
        source: StorageError,
    },
    /// Error returned when errors occur in underlying storage instance
    #[error("Error handling arrow data")]
    ArrowError {
        /// Raw internal StorageError
        #[from]
        source: ArrowError,
    },
}

/// The save mode when writing data.
pub enum SaveMode {
    /// append data to existing table
    Append,
    /// overwrite table with new data
    Overwrite,
    /// TODO
    Ignore,
    /// Raise an error if data exists
    ErrorIfExists,
}

/// Shared API between delta commands
#[async_trait]
pub trait DeltaCommandExec {
    /// Execute the delta command
    async fn execute(&self, table: &mut DeltaTable) -> Result<(), DeltaCommandError>;
}

async fn check_table_exists(table: &mut DeltaTable) -> Result<bool, DeltaCommandError> {
    let uri = table.commit_uri_from_version(table.version);
    match table.storage.head_obj(&uri).await {
        Ok(_) => Ok(true),
        Err(StorageError::NotFound) => Ok(false),
        Err(source) => Err(DeltaCommandError::StorageError { source }),
    }
}

// Definitions for RecordBatchStream are taken from the datafusion crate
// https://github.com/apache/arrow-datafusion/blob/d04790041caecdc53077de37db3e30388f8ff38c/datafusion/src/physical_plan/mod.rs#L46-L87

/// Trait for types that stream [arrow::record_batch::RecordBatch]
pub trait RecordBatchStream: Stream<Item = ArrowResult<RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a stream of record batches.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send + Sync>>;

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Stream of record batches
pub struct SizedRecordBatchStream {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl SizedRecordBatchStream {
    /// Create a new RecordBatchIterator
    pub fn new(schema: SchemaRef, batches: Vec<Arc<RecordBatch>>) -> Self {
        SizedRecordBatchStream {
            schema,
            index: 0,
            batches,
        }
    }
}

impl Stream for SizedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.batches.len() {
            self.index += 1;
            Some(Ok(self.batches[self.index - 1].as_ref().clone()))
        } else {
            None
        })
    }
}

impl RecordBatchStream for SizedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Create a vector of record batches from a stream
pub async fn collect(
    stream: SendableRecordBatchStream,
) -> Result<Vec<RecordBatch>, DeltaCommandError> {
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(DeltaCommandError::from)
}
