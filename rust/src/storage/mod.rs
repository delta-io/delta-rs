//! Object storage backend abstraction layer for Delta Table transaction logs and data

pub use delta::DeltaObjectStore;
use object_store::Error as ObjectStoreError;
use std::fmt::Debug;

pub mod delta;
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

    /// Wraps a generic storage backend error. The wrapped string contains the details.
    #[error("Generic error: {0}")]
    Generic(String),

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

    /// underlying object store returned an error.
    #[error("ObjectStore interaction failed: {source}")]
    ObjectStore {
        /// The wrapped [`ObjectStoreError`]
        #[from]
        source: ObjectStoreError,
    },
}

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub(crate) fn str_option(
    map: &std::collections::HashMap<String, String>,
    key: &str,
) -> Option<String> {
    map.get(key)
        .map_or_else(|| std::env::var(key).ok(), |v| Some(v.to_owned()))
}
