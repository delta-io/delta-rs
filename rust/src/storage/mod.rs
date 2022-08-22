//! Object storage backend abstraction layer for Delta Table transaction logs and data

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
use hyper::http::uri::InvalidUri;
use object_store::Error as ObjectStoreError;
use std::fmt::Debug;
use walkdir::Error as WalkDirError;

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

    /// Error returned when the URI is expected to be an object storage path, but does not include a bucket part.
    #[cfg(any(feature = "gcs", feature = "s3", feature = "s3-rustls"))]
    #[error("Object URI missing bucket")]
    MissingObjectBucket,
    /// Error returned when the URI is expected to be an object storage path, but does not include a key part.
    #[cfg(any(feature = "gcs", feature = "s3", feature = "s3-rustls"))]
    #[error("Object URI missing key")]
    MissingObjectKey,
    /// Error returned when an S3 path is expected, but the URI is not an S3 URI.
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    #[error("Expected S3 URI, found: {0}")]
    ExpectedS3Uri(String),

    /// Error returned when an GCS path is expected, but the URI is not an GCS URI.
    #[cfg(any(feature = "gcs"))]
    #[error("Expected GCS URI, found: {0}")]
    ExpectedGCSUri(String),

    /// Error returned when an Azure URI is expected, but the URI is not an Azure URI.
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
    MissingObjectAccount,
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
    /// URI for GCS backend
    #[cfg(feature = "gcs")]
    GCSObject(gcs::GCSObject<'a>),
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
            #[cfg(feature = "gcs")]
            Uri::GCSObject(x) => Err(UriError::ExpectedS3Uri(x.to_string())),
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
            #[cfg(feature = "gcs")]
            Uri::GCSObject(x) => Err(UriError::ExpectedAzureUri(x.to_string())),
            Uri::LocalPath(x) => Err(UriError::ExpectedAzureUri(x.to_string())),
        }
    }

    /// Converts the URI to an GCSObject. Returns UriError if the URI is not valid for the
    /// Google Cloud Storage backend.
    #[cfg(feature = "gcs")]
    pub fn into_gcs_object(self) -> Result<gcs::GCSObject<'a>, UriError> {
        match self {
            Uri::GCSObject(x) => Ok(x),
            #[cfg(any(feature = "s3", feature = "s3-rustls"))]
            Uri::S3Object(x) => Err(UriError::ExpectedGCSUri(x.to_string())),
            #[cfg(feature = "azure")]
            Uri::AdlsGen2Object(x) => Err(UriError::ExpectedGCSUri(x.to_string())),
            Uri::LocalPath(x) => Err(UriError::ExpectedGCSUri(x.to_string())),
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
            #[cfg(feature = "gcs")]
            Uri::GCSObject(x) => Err(UriError::ExpectedSLocalPathUri(format!("{}", x))),
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
            #[cfg(feature = "gcs")]
            Uri::GCSObject(x) => x.path.to_string(),
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

        // This can probably be refactored into the above match arm
        "gs" => {
            cfg_if::cfg_if! {
                if #[cfg(any(feature = "gcs"))] {
                    let mut path_parts = parts[1].splitn(2, '/');
                    let bucket = match path_parts.next() {
                        Some(x) => x,
                        None => {
                            return Err(UriError::MissingObjectBucket);
                        }
                    };
                    let path = match path_parts.next() {
                        Some(x) => x,
                        None => {
                            return Err(UriError::MissingObjectKey);
                        }
                    };

                    Ok(Uri::GCSObject(gcs::GCSObject::new(bucket, path)))
                } else {
                    Err(UriError::InvalidScheme(String::from(parts[0])))
                }
            }
        }

        "file" => Ok(Uri::LocalPath(parts[1])),

        // Azure Data Lake Storage Gen2
        // This URI syntax is an invention of delta-rs.
        // ABFS URIs should not be used since delta-rs doesn't use the Hadoop ABFS driver.
        "adls2" => {
            cfg_if::cfg_if! {
                if #[cfg(feature = "azure")] {
                    let mut path_parts = parts[1].splitn(3, '/');
                    let account_name = match path_parts.next() {
                        Some(x) => x,
                        None => {
                            return Err(UriError::MissingObjectAccount);
                        }
                    };
                    let file_system = match path_parts.next() {
                        Some(x) => x,
                        None => {
                            return Err(UriError::MissingObjectFileSystem);
                        }
                    };
                    let path = path_parts.next().unwrap_or("/");

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
