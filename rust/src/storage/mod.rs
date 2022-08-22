//! Object storage backend abstraction layer for Delta Table transaction logs and data

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
use hyper::http::uri::InvalidUri;
use object_store::Error as ObjectStoreError;
use std::fmt::Debug;
use walkdir::Error as WalkDirError;

pub mod file;
#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub mod s3;

/// Error enum returned when storage backend interaction fails.
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    /// The requested object does not exist.
    #[error("Object not found")]
    NotFound,
    /// The object written to the storage backend already exists.
    /// This error is expected in some cases.
    /// For example, optimistic concurrency writes between multiple processes expect to compete
    /// for the same URI when writing to _delta_log.
    #[error("Object exists already at path: {0}")]
    AlreadyExists(String),
    /// An IO error occurred while reading from the local file system.
    #[error("Failed to read local object content: {source}")]
    Io {
        /// The raw error returned when trying to read the local file.
        source: std::io::Error,
    },

    #[error("Failed to walk directory: {source}")]
    /// Error raised when failing to traverse a directory
    WalkDir {
        /// The raw error returned when trying to read the local file.
        #[from]
        source: WalkDirError,
    },
    /// The file system represented by the scheme is not known.
    #[error("File system not supported")]
    FileSystemNotSupported,
    /// Wraps a generic storage backend error. The wrapped string contains the details.
    #[error("Generic error: {0}")]
    Generic(String),

    /// Error returned when S3 object get response contains empty body
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("S3 Object missing body content: {0}")]
    S3MissingObjectBody(String),
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    /// Represents a generic S3 error. The wrapped error string describes the details.
    #[error("S3 error: {0}")]
    S3Generic(String),
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    /// Wraps the DynamoDB error
    #[error("DynamoDB error: {source}")]
    DynamoDb {
        /// Wrapped DynamoDB error
        #[from]
        source: dynamodb_lock::DynamoError,
    },
    /// Error representing a failure to retrieve AWS credentials.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to retrieve AWS credentials: {source}")]
    AWSCredentials {
        /// The underlying Rusoto CredentialsError
        #[from]
        source: rusoto_credential::CredentialsError,
    },
    /// Error caused by the http request dispatcher not being able to be created.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to create request dispatcher: {source}")]
    AWSHttpClient {
        /// The underlying Rusoto TlsError
        #[from]
        source: rusoto_core::request::TlsError,
    },

    /// Error returned when the URI is invalid.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Invalid URI parsing")]
    ParsingUri {
        #[from]
        /// Uri error details when the URI parsing is invalid.
        source: InvalidUri,
    },

    /// underlying object store returned an error.
    #[error("ObjectStore interaction failed: {source}")]
    ObjectStore {
        /// The wrapped [`ObjectStoreError`]
        #[from]
        source: ObjectStoreError,
    },
}

impl StorageError {
    /// Creates a StorageError::Io error wrapping the provided error string.
    pub fn other_std_io_err(desc: String) -> Self {
        Self::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, desc),
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => StorageError::NotFound,
            _ => StorageError::Io { source: error },
        }
    }
}

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub(crate) fn str_option(map: &HashMap<String, String>, key: &str) -> Option<String> {
    map.get(key)
        .map_or_else(|| std::env::var(key).ok(), |v| Some(v.to_owned()))
}
