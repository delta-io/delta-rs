//! Object storage backend abstraction layer for Delta Table transaction logs and data

use std::fmt::{Debug, Display};
use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::Stream;

#[cfg(feature = "azure")]
use azure_core::errors::AzureError;
#[cfg(feature = "azure")]
use std::error::Error;

#[cfg(feature = "azure")]
pub mod azure;
#[cfg(feature = "delta-sharing")]
pub mod deltashare;
pub mod file;
#[cfg(feature = "s3")]
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
    #[cfg(feature = "s3")]
    #[error("Object URI missing bucket")]
    MissingObjectBucket,
    /// Error returned when the URI is expected to be an S3 path, but does not include a key part.
    #[cfg(feature = "s3")]
    #[error("Object URI missing key")]
    MissingObjectKey,
    /// Error returned when an S3 path is expected, but the URI is not an S3 URI.
    #[cfg(feature = "s3")]
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
    /// Error returned when the URI does not appear to be a valid Delta Sharing URI
    #[cfg(feature = "delta-sharing")]
    #[error("Expected a Delta Sharing URI, found: {0}")]
    ExpectedDeltaSharingUri(String),
}

/// Enum with variants representing each supported storage backend.
#[derive(Debug)]
pub enum Uri<'a> {
    /// URI for local file system backend.
    LocalPath(&'a str),
    /// URI for S3 backend.
    #[cfg(feature = "s3")]
    S3Object(s3::S3Object<'a>),
    /// URI for Azure backend.
    #[cfg(feature = "azure")]
    AdlsGen2Object(azure::AdlsGen2Object<'a>),
    /// URI for the Delta Share backend
    #[cfg(feature = "delta-sharing")]
    DeltaShareObject(deltashare::DeltaShareObject<'a>),
}

impl<'a> Display for Uri<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Uri::LocalPath(p) => p.to_string(),
                #[cfg(feature = "s3")]
                Uri::S3Object(s) => s.to_string(),
                #[cfg(feature = "azure")]
                Uri::AdlsGen2Object(a) => a.to_string(),
                #[cfg(feature = "delta-sharing")]
                Uri::DeltaShareObject(d) => d.to_string(),
            }
        )
    }
}

impl<'a> Uri<'a> {
    /// Converts the URI to an S3Object. Returns UriError if the URI is not valid for the S3
    /// backend.
    #[cfg(feature = "s3")]
    pub fn into_s3object(self) -> Result<s3::S3Object<'a>, UriError> {
        match self {
            Uri::S3Object(x) => Ok(x),
            x => Err(UriError::ExpectedS3Uri(x.to_string())),
        }
    }

    /// Converts the URI to an AdlsGen2Object. Returns UriError if the URI is not valid for the
    /// Azure backend.
    #[cfg(feature = "azure")]
    pub fn into_adlsgen2_object(self) -> Result<azure::AdlsGen2Object<'a>, UriError> {
        match self {
            Uri::AdlsGen2Object(x) => Ok(x),
            x => Err(UriError::ExpectedAzureUri(x.to_string())),
        }
    }

    /// Converts the URI into a DeltaShareObject. Returns UriError if the URLis not a valid
    /// Delta Sharing backend
    #[cfg(feature = "delta-sharing")]
    pub fn into_deltashare_object(self) -> Result<deltashare::DeltaShareObject<'a>, UriError> {
        match self {
            Uri::DeltaShareObject(x) => Ok(x),
            x => Err(UriError::ExpectedDeltaSharingUri(x.to_string())),
        }
    }

    /// Converts the URI to an str representing a local file system path. Returns UriError if the
    /// URI is not valid for the file storage backend.
    pub fn into_localpath(self) -> Result<&'a str, UriError> {
        match self {
            Uri::LocalPath(x) => Ok(x),
            #[cfg(any(feature = "delta-sharing", feature = "s3", feature = "azure"))]
            x => Err(UriError::ExpectedSLocalPathUri(x.to_string())),
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
                if #[cfg(feature = "s3")] {
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
        "http" => {
            cfg_if::cfg_if! {
                if #[cfg(feature = "delta-sharing")] {
                    Ok(Uri::DeltaShareObject(deltashare::DeltaShareObject { url: parts[1] }))
                } else {
                    Err(UriError::InvalidScheme(String::from(parts[0])))
                }
            }
        }
        "https" => {
            cfg_if::cfg_if! {
                if #[cfg(feature = "delta-sharing")] {
                    Ok(Uri::DeltaShareObject(deltashare::DeltaShareObject { url: parts[1] }))
                } else {
                    Err(UriError::InvalidScheme(String::from(parts[0])))
                }
            }
        }
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
    #[cfg(feature = "s3")]
    #[error("Failed to read S3 object content: {source}")]
    S3Get {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::GetObjectError>,
    },
    /// Error representing a failure when executing an S3 HEAD request.
    #[cfg(feature = "s3")]
    #[error("Failed to read S3 object metadata: {source}")]
    S3Head {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::HeadObjectError>,
    },
    /// Error representing a failure when executing an S3 list operation.
    #[cfg(feature = "s3")]
    #[error("Failed to list S3 objects: {source}")]
    S3List {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::ListObjectsV2Error>,
    },
    /// Error representing a failure when executing an S3 PUT request.
    #[cfg(feature = "s3")]
    #[error("Failed to put S3 object: {source}")]
    S3Put {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::PutObjectError>,
    },
    /// Error returned when an S3 response for a requested URI does not include body bytes.
    #[cfg(feature = "s3")]
    #[error("Failed to delete S3 object: {source}")]
    S3Delete {
        /// The underlying Rusoto S3 error.
        #[from]
        source: rusoto_core::RusotoError<rusoto_s3::DeleteObjectError>,
    },
    /// Error representing a failure when copying a S3 object
    #[cfg(feature = "s3")]
    #[error("Failed to copy S3 object: {source}")]
    S3Copy {
        /// The underlying Rusoto S3 error.
        source: rusoto_core::RusotoError<rusoto_s3::CopyObjectError>,
    },
    /// Error returned when S3 object get response contains empty body
    #[cfg(feature = "s3")]
    #[error("S3 Object missing body content: {0}")]
    S3MissingObjectBody(String),
    #[cfg(feature = "s3")]
    /// Represents a generic S3 error. The wrapped error string describes the details.
    #[error("S3 error: {0}")]
    S3Generic(String),
    #[cfg(feature = "s3")]
    /// Wraps the DynamoDB error
    #[error("DynamoDB error: {source}")]
    DynamoDb {
        /// Wrapped DynamoDB error
        #[from]
        source: s3::dynamodb_lock::DynamoError,
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

    ///! Error when the storage backend cannot support the requested API
    #[error("The storage backend does not support the requested operation: {0}")]
    UnsupportedOperation(String),

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
    /// The path where the object is stored.
    pub path: String,
    /// The last time the object was modified in the storage backend.
    // The timestamp of a commit comes from the remote storage `lastModifiedTime`, and can be
    // adjusted for clock skew.
    pub modified: DateTime<Utc>,
}

/// Enumeration of the supported storage backends
#[derive(Debug, PartialEq)]
pub enum StorageBackendType {
    /// File system backend
    FileSystem,
    /// S3 backend
    #[cfg(feature = "s3")]
    S3,
    /// Azure backend
    #[cfg(feature = "azure")]
    Azure,
    /// Delta Sharing backend
    #[cfg(feature = "delta-sharing")]
    DeltaSharing,
}

/// Abstractions for underlying blob storages hosting the Delta table. To add support for new cloud
/// or local storage systems, simply implement this trait.
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync + Debug {
    /// Return the type of the storage backend to hint callers
    fn backend_type(&self) -> StorageBackendType;

    /// Create a new path by appending `path_to_join` as a new component to `path`.
    fn join_path(&self, path: &str, path_to_join: &str) -> String {
        let normalized_path = path.trim_end_matches('/');
        format!("{}/{}", normalized_path, path_to_join)
    }

    /// More efficient path join for multiple path components. Use this method if you need to
    /// combine more than two path components.
    fn join_paths(&self, paths: &[&str]) -> String {
        paths
            .iter()
            .map(|s| s.trim_end_matches('/'))
            .collect::<Vec<_>>()
            .join("/")
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
        #[cfg(feature = "s3")]
        Uri::S3Object(_) => Ok(Box::new(s3::S3StorageBackend::new()?)),
        #[cfg(feature = "azure")]
        Uri::AdlsGen2Object(obj) => Ok(Box::new(azure::AdlsGen2Backend::new(obj.file_system)?)),
        #[cfg(feature = "delta-sharing")]
        Uri::DeltaShareObject(_) => Ok(Box::new(deltashare::DeltaShareBackend::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_uri_local_file() {
        let uri = parse_uri("foo/bar").unwrap();
        assert_eq!(uri.into_localpath().unwrap(), "foo/bar");

        let uri2 = parse_uri("file:///foo/bar").unwrap();
        assert_eq!(uri2.into_localpath().unwrap(), "/foo/bar");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn test_parse_s3_object_uri() {
        let uri = parse_uri("s3://foo/bar").unwrap();
        assert_eq!(
            uri.into_s3object().unwrap(),
            s3::S3Object {
                bucket: "foo",
                key: "bar",
            }
        );
    }

    #[cfg(feature = "azure")]
    #[test]
    fn test_parse_azure_object_uri() {
        let uri = parse_uri("abfss://fs@sa.dfs.core.windows.net/foo").unwrap();
        assert_eq!(
            uri.into_adlsgen2_object().unwrap(),
            azure::AdlsGen2Object {
                account_name: "sa",
                file_system: "fs",
                path: "foo",
            }
        );
    }
}
