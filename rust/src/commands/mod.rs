//! High level delta commands that can be executed against a delta table
use crate::{storage::StorageError, write::DataWriterError, DeltaTable, DeltaTableError};
use async_trait::async_trait;

use arrow::error::ArrowError;
use std::fmt::Debug;

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

    /// Errors occurring inside the DeltaWriter modules
    #[error("Error in underlying storage backend")]
    DeltaWriterError {
        /// Raw internal StorageError
        #[from]
        source: DataWriterError,
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
