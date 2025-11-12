//! Error types for logstore operations

use std::sync::Arc;

use object_store::Error as ObjectStoreError;
use thiserror::Error;

/// Error raised during logstore operations
#[derive(Error, Debug)]
pub enum LogStoreError {
    /// Version already exists
    #[error("Tried committing existing table version: {0}")]
    VersionAlreadyExists(i64),

    /// Error returned when reading the delta log object failed.
    #[error("Log storage error: {}", .source)]
    ObjectStore {
        /// Storage error details when reading the delta log object failed.
        #[from]
        source: ObjectStoreError,
    },

    /// The transaction failed to commit due to an error in an implementation-specific layer.
    #[error("Transaction failed: {msg}, error: {source}")]
    TransactionError {
        /// Detailed message for the commit failure.
        msg: String,
        /// underlying error in the log store transactional layer.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Invalid table location
    #[error("Invalid table location: {0}")]
    InvalidTableLocation(String),

    /// Error returned when the log record has an invalid JSON.
    #[error("Invalid JSON in log record, version={}, line=`{}`, err=`{}`", .version, .line, .json_err)]
    InvalidJsonLog {
        /// JSON error details returned when parsing the record JSON.
        json_err: serde_json::error::Error,
        /// invalid log entry content.
        line: String,
        /// corresponding table version for the log file.
        version: i64,
    },

    /// Error returned when parsing a path failed.
    #[error("Failed to parse path: {path} - {source}")]
    InvalidPath {
        /// The path that failed to parse.
        path: String,
        /// Source error details.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error returned when the DeltaTable has an invalid version.
    #[error("Invalid table version: {0}")]
    InvalidVersion(i64),

    /// Error returned when parsing a configuration value failed.
    #[error("Failed to parse \"{value}\" as {type_name}: {source}")]
    ParseError {
        /// The value that failed to parse.
        value: String,
        /// The type name that was expected.
        type_name: String,
        /// Source error details.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error returned when multiple errors occurred during parsing.
    #[error("Failed to parse configuration: {errors:?}")]
    ParseErrors {
        /// The errors that occurred during parsing.
        errors: Vec<(String, Arc<LogStoreError>)>,
    },

    /// Error returned when an object store is not found for a URL.
    #[error("No suitable object store found for '{url}'")]
    ObjectStoreNotFound {
        /// The URL for which no object store was found.
        url: String,
    },

    /// Generic error
    #[error("Logstore error: {source}")]
    Generic {
        /// Source error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

/// Result type for logstore operations
pub type LogStoreResult<T> = Result<T, LogStoreError>;
