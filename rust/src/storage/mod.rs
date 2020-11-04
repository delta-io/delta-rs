extern crate rusoto_core;
extern crate rusoto_s3;

use self::rusoto_core::RusotoError;
use chrono::{DateTime, Utc};

pub mod file;
pub mod s3;

#[derive(thiserror::Error, Debug)]
pub enum UriError {
    #[error("Invalid URI scheme: {0}")]
    InvalidScheme(String),
    #[error("Object URI missing bucket")]
    MissingObjectBucket,
    #[error("Object URI missing key")]
    MissingObjectKey,
    #[error("Expected S3 URI, found: {0}")]
    ExpectedS3Uri(String),
    #[error("Expected local path URI, found: {0}")]
    ExpectedSLocalPathUri(String),
}

#[derive(Debug)]
pub enum Uri<'a> {
    LocalPath(&'a str),
    S3Object(s3::S3Object<'a>),
}

impl<'a> Uri<'a> {
    pub fn into_s3object(self) -> Result<s3::S3Object<'a>, UriError> {
        match self {
            Uri::S3Object(x) => Ok(x),
            Uri::LocalPath(x) => Err(UriError::ExpectedS3Uri(x.to_string())),
        }
    }

    pub fn into_localpath(self) -> Result<&'a str, UriError> {
        match self {
            Uri::LocalPath(x) => Ok(x),
            Uri::S3Object(x) => Err(UriError::ExpectedSLocalPathUri(format!("{}", x))),
        }
    }
}

pub fn parse_uri<'a>(path: &'a str) -> Result<Uri<'a>, UriError> {
    let parts: Vec<&'a str> = path.split("://").collect();

    if parts.len() == 1 {
        return Ok(Uri::LocalPath(parts[0]));
    }

    match parts[0] {
        "s3" => {
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
        }
        "file" => Ok(Uri::LocalPath(parts[1])),
        _ => Err(UriError::InvalidScheme(String::from(parts[0]))),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Object not found")]
    NotFound,
    #[error("Failed to read local object content")]
    IO { source: std::io::Error },
    #[error("Failed to read S3 object content")]
    S3Get {
        source: RusotoError<rusoto_s3::GetObjectError>,
    },
    #[error("Failed to read S3 object metadata")]
    S3Head {
        source: RusotoError<rusoto_s3::HeadObjectError>,
    },
    #[error("S3 Object missing body content: {0}")]
    S3MissingObjectBody(String),
    #[error("Invalid object URI")]
    Uri {
        #[from]
        source: UriError,
    },
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => StorageError::NotFound,
            _ => StorageError::IO { source: error },
        }
    }
}

impl From<RusotoError<rusoto_s3::GetObjectError>> for StorageError {
    fn from(error: RusotoError<rusoto_s3::GetObjectError>) -> Self {
        match error {
            RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(_)) => StorageError::NotFound,
            _ => StorageError::S3Get { source: error },
        }
    }
}

impl From<RusotoError<rusoto_s3::HeadObjectError>> for StorageError {
    fn from(error: RusotoError<rusoto_s3::HeadObjectError>) -> Self {
        match error {
            RusotoError::Service(rusoto_s3::HeadObjectError::NoSuchKey(_)) => {
                StorageError::NotFound
            }
            _ => StorageError::S3Head { source: error },
        }
    }
}

pub struct ObjectMeta {
    pub path: String,
    // The timestamp of a commit comes from the remote storage `lastModifiedTime`, and can be
    // adjusted for clock skew.
    pub modified: DateTime<Utc>,
}

pub trait StorageBackend: Send + Sync {
    fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError>;
    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError>;
    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = ObjectMeta>>, StorageError>;
}

pub fn get_backend_for_uri(uri: &str) -> Result<Box<dyn StorageBackend>, UriError> {
    match parse_uri(uri)? {
        Uri::LocalPath(_) => Ok(Box::new(file::FileStorageBackend::new())),
        Uri::S3Object(_) => Ok(Box::new(s3::S3StorageBackend::new())),
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

    #[test]
    fn test_parse_object_uri() {
        let uri = parse_uri("s3://foo/bar").unwrap();
        assert_eq!(
            uri.into_s3object().unwrap(),
            s3::S3Object {
                bucket: "foo",
                key: "bar",
            }
        );
    }
}
