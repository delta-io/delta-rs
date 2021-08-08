//! Object storage backend abstraction layer for Delta Table transaction logs and data

use std::fmt::Debug;
use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::Stream;

#[cfg(feature = "azure")]
use azure_core::errors::AzureError;
#[cfg(feature = "azure")]
use std::error::Error;

#[cfg(feature = "azure")]
pub mod azure;
pub mod file;
#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub mod s3;

/// Error enum that represents an invalid URI.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum UriError {
    /// Error returned when the URI contains a scheme that is not handled.
    #[error("Invalid URI scheme: {0}")]
    InvalidScheme(String),
    /// Error returned when a local file system path is expected, but the URI is not a local file system path.
    #[error("Expected local path URI, found: {0}")]
    ExpectedSLocalPathUri(String),

    /// Error returned when the URI is expected to be an S3 path, but does not include a bucket part.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Object URI missing bucket")]
    MissingObjectBucket,
    /// Error returned when the URI is expected to be an S3 path, but does not include a key part.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Object URI missing key")]
    MissingObjectKey,
    /// Error returned when an S3 path is expected, but the URI is not an S3 URI.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Expected S3 URI, found: {0}")]
    ExpectedS3Uri(String),

    /// Error returned when an Azure URI is expected, but the URI is not an Azure file system
    /// (abfs\[s\]) URI.
    #[cfg(feature = "azure")]
    #[error("Expected Azure URI, found: {0}")]
    ExpectedAzureUri(String),
    /// Error returned when an Azure URI is expected, but the URI is missing the scheme.
    #[cfg(feature = "azure")]
    #[error("Object URI missing filesystem")]
    MissingObjectFileSystem,
    /// Error returned when an Azure URI is expected, but the URI is missing the account name and
    /// path.
    #[cfg(feature = "azure")]
    #[error("Object URI missing account name and path")]
    MissingObjectAccountAndPath,
    /// Error returned when an Azure URI is expected, but the URI is missing the account name.
    #[cfg(feature = "azure")]
    #[error("Object URI missing account name")]
    MissingObjectAccountName,
    /// Error returned when an Azure URI is expected, but the URI is missing the path.
    #[cfg(feature = "azure")]
    #[error("Object URI missing path")]
    MissingObjectPath,
    /// Error returned when container in an Azure URI doesn't match the expected value
    #[cfg(feature = "azure")]
    #[error("Container mismatch, expected: {expected}, got: {got}")]
    ContainerMismatch {
        /// Expected container value
        expected: String,
        /// Actual container value
        got: String,
    },
}

/// Enum with variants representing each supported storage backend.
#[derive(Debug)]
pub enum Uri<'a> {
    /// URI for local file system backend.
    LocalPath(&'a str),
    /// URI for S3 backend.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    S3Object(s3::S3Object<'a>),
    /// URI for Azure backend.
    #[cfg(feature = "azure")]
    AdlsGen2Object(azure::AdlsGen2Object<'a>),
}

impl<'a> Uri<'a> {
    /// Converts the URI to an S3Object. Returns UriError if the URI is not valid for the S3
    /// backend.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    pub fn into_s3object(self) -> Result<s3::S3Object<'a>, UriError> {
        match self {
            Uri::S3Object(x) => Ok(x),
            #[cfg(feature = "azure")]
            Uri::AdlsGen2Object(x) => Err(UriError::ExpectedS3Uri(x.to_string())),
            Uri::LocalPath(x) => Err(UriError::ExpectedS3Uri(x.to_string())),
        }
    }

    /// Converts the URI to an AdlsGen2Object. Returns UriError if the URI is not valid for the
    /// Azure backend.
    #[cfg(feature = "azure")]
    pub fn into_adlsgen2_object(self) -> Result<azure::AdlsGen2Object<'a>, UriError> {
        match self {
            Uri::AdlsGen2Object(x) => Ok(x),
            #[cfg(any(feature = "s3", feature = "s3-rustls"))]
            Uri::S3Object(x) => Err(UriError::ExpectedAzureUri(x.to_string())),
            Uri::LocalPath(x) => Err(UriError::ExpectedAzureUri(x.to_string())),
        }
    }

    /// Converts the URI to an str representing a local file system path. Returns UriError if the
    /// URI is not valid for the file storage backend.
    pub fn into_localpath(self) -> Result<&'a str, UriError> {
        match self {
            Uri::LocalPath(x) => Ok(x),
            #[cfg(any(feature = "s3", feature = "s3-rustls"))]
            Uri::S3Object(x) => Err(UriError::ExpectedSLocalPathUri(format!("{}", x))),
            #[cfg(feature = "azure")]
            Uri::AdlsGen2Object(x) => Err(UriError::ExpectedSLocalPathUri(format!("{}", x))),
        }
    }

    /// Return URI path component as String
    #[inline]
    pub fn path(&self) -> String {
        match self {
            Uri::LocalPath(x) => x.to_string(),
            #[cfg(any(feature = "s3", feature = "s3-rustls"))]
            Uri::S3Object(x) => x.key.to_string(),
            #[cfg(feature = "azure")]
            Uri::AdlsGen2Object(x) => x.path.to_string(),
        }
    }
}

/// Parses the URI and returns a variant of the Uri enum for the appropriate storage backend based
/// on scheme.
pub fn parse_uri<'a>(path: &'a str) -> Result<Uri<'a>, UriError> {
    let parts: Vec<&'a str> = path.split("://").collect();

    if parts.len() == 1 {
        return Ok(Uri::LocalPath(parts[0]));
    }

    match parts[0] {
        "s3" => {
            cfg_if::cfg_if! {
                if #[cfg(any(feature = "s3", feature = "s3-rustls"))] {
                    let mut path_parts = parts[1].splitn(2, '/');
                    let bucket = match path_parts.next() {
                        Some(x) => x,
                        None => {
                            return Err(UriError::MissingObjectBucket);
                        }
                    };
                    let key = match path_parts.next() {
                        Some(x) => x,
                        None => {
                            return Err(UriError::MissingObjectKey);
                        }
                    };

                    Ok(Uri::S3Object(s3::S3Object { bucket, key }))
                } else {
                    Err(UriError::InvalidScheme(String::from(parts[0])))
                }
            }
        }
        "file" => Ok(Uri::LocalPath(parts[1])),
        "abfss" => {
            cfg_if::cfg_if! {
                if #[cfg(feature = "azure")] {
                    // URI scheme: abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>/<file_name>
                    let mut parts = parts[1].splitn(2, '@');
                    let file_system = parts.next().ok_or(UriError::MissingObjectFileSystem)?;
                    let mut parts = parts.next().map(|x| x.splitn(2, '.')).ok_or(UriError::MissingObjectAccountAndPath)?;
                    let account_name = parts.next().ok_or(UriError::MissingObjectAccountName)?;
                    let mut paths = parts.next().map(|x| x.splitn(2, '/')).ok_or(UriError::MissingObjectPath)?;
                    // assume root when uri ends without `/`
                    let path = paths.nth(1).unwrap_or("");
                    Ok(Uri::AdlsGen2Object(azure::AdlsGen2Object { account_name, file_system, path }))
                } else {
                    Err(UriError::InvalidScheme(String::from(parts[0])))
                }
            }
        }
        _ => Err(UriError::InvalidScheme(String::from(parts[0]))),
    }
}

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
    /// The file system represented by the scheme is not known.
    #[error("File system not supported")]
    FileSystemNotSupported,
    /// Wraps a generic storage backend error. The wrapped string contains the details.
    #[error("Generic error: {0}")]
    Generic(String),

    /// Error representing an S3 GET failure.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to read S3 object content: {source}")]
    S3Get {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::GetObjectError>,
    },
    /// Error representing a failure when executing an S3 HEAD request.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to read S3 object metadata: {source}")]
    S3Head {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::HeadObjectError>,
    },
    /// Error representing a failure when executing an S3 list operation.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to list S3 objects: {source}")]
    S3List {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::ListObjectsV2Error>,
    },
    /// Error representing a failure when executing an S3 PUT request.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to put S3 object: {source}")]
    S3Put {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::PutObjectError>,
    },
    /// Error returned when an S3 response for a requested URI does not include body bytes.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to delete S3 object: {source}")]
    S3Delete {
        /// The underlying Rusoto S3 error.
        #[from]
        source: rusoto_core::RusotoError<rusoto_s3::DeleteObjectError>,
    },
    /// Error representing a failure when copying a S3 object
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Failed to copy S3 object: {source}")]
    S3Copy {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::CopyObjectError>,
    },
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
        source: s3::dynamodb_lock::DynamoError,
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

    /// Azure error
    #[cfg(feature = "azure")]
    #[error("Error interacting with Azure: {source}")]
    Azure {
        /// Azure error reason
        source: AzureError,
    },
    /// Generic Azure error
    #[cfg(feature = "azure")]
    #[error("Generic error: {source}")]
    AzureGeneric {
        /// Generic Azure error reason
        source: Box<dyn Error + Sync + std::marker::Send>,
    },
    /// Azure config error
    #[cfg(feature = "azure")]
    #[error("Azure config error: {0}")]
    AzureConfig(String),

    /// Error returned when the URI is invalid.
    /// The wrapped UriError contains additional details.
    #[error("Invalid object URI")]
    Uri {
        #[from]
        /// Uri error details when the URI is invalid.
        source: UriError,
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

#[cfg(feature = "azure")]
impl From<AzureError> for StorageError {
    fn from(error: AzureError) -> Self {
        match error {
            AzureError::UnexpectedHTTPResult(e) if e.status_code().as_u16() == 404 => {
                StorageError::NotFound
            }
            _ => StorageError::Azure { source: error },
        }
    }
}

/// Describes metadata of a storage object.
pub struct ObjectMeta {
    /// The path where the object is stored. This is the path component of the object URI.
    ///
    /// For example:
    ///   * path for `s3://bucket/foo/bar` should be `foo/bar`.
    ///   * path for `dir/foo/bar` should be `dir/foo/bar`.
    ///
    /// Given a table URI, object URI can be constructed by joining table URI with object path.
    pub path: String,
    /// The last time the object was modified in the storage backend.
    // The timestamp of a commit comes from the remote storage `lastModifiedTime`, and can be
    // adjusted for clock skew.
    pub modified: DateTime<Utc>,
}

/// Abstractions for underlying blob storages hosting the Delta table. To add support for new cloud
/// or local storage systems, simply implement this trait.
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync + Debug {
    /// Create a new path by appending `path_to_join` as a new component to `path`.
    #[inline]
    fn join_path(&self, path: &str, path_to_join: &str) -> String {
        let normalized_path = path.trim_end_matches('/');
        format!("{}/{}", normalized_path, path_to_join)
    }

    /// More efficient path join for multiple path components. Use this method if you need to
    /// combine more than two path components.
    #[inline]
    fn join_paths(&self, paths: &[&str]) -> String {
        paths
            .iter()
            .map(|s| s.trim_end_matches('/'))
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Returns trimed path with trailing path separator removed.
    #[inline]
    fn trim_path(&self, path: &str) -> String {
        path.trim_end_matches('/').to_string()
    }

    /// Fetch object metadata without reading the actual content
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError>;

    /// Fetch object content
    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError>;

    /// Return a list of objects by `path` prefix in an async stream.
    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    >;

    /// Create new object with `obj_bytes` as content.
    ///
    /// Implementation note:
    ///
    /// To support safe concurrent read, if `path` already exists, `put_obj` needs to update object
    /// content in backing store atomically, i.e. reader of the object should never read a partial
    /// write.
    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError>;

    /// Moves object from `src` to `dst`.
    ///
    /// Implementation note:
    ///
    /// For a multi-writer safe backend, `rename_obj` needs to implement `atomic rename` semantic.
    /// In other words, if the destination path already exists, rename should return a
    /// [StorageError::AlreadyExists] error.
    async fn rename_obj(&self, src: &str, dst: &str) -> Result<(), StorageError>;

    /// Deletes object by `path`.
    async fn delete_obj(&self, path: &str) -> Result<(), StorageError>;
}

/// Dynamically construct a Storage backend trait object based on scheme for provided URI
pub fn get_backend_for_uri(uri: &str) -> Result<Box<dyn StorageBackend>, StorageError> {
    match parse_uri(uri)? {
        Uri::LocalPath(root) => Ok(Box::new(file::FileStorageBackend::new(root))),
        #[cfg(any(feature = "s3", feature = "s3-rustls"))]
        Uri::S3Object(_) => Ok(Box::new(s3::S3StorageBackend::new()?)),
        #[cfg(feature = "azure")]
        Uri::AdlsGen2Object(obj) => Ok(Box::new(azure::AdlsGen2Backend::new(obj.file_system)?)),
    }
}
