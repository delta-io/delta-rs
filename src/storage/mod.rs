use rusoto_core::RusotoError;
use rusoto_s3;
use thiserror;

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
}

#[derive(Debug)]
pub enum Uri<'a> {
    LocalPath(&'a str),
    S3Object(s3::S3Object<'a>),
}

impl<'a> Uri<'a> {
    pub fn as_s3object(self) -> s3::S3Object<'a> {
        match self {
            Uri::S3Object(x) => x,
            _ => panic!("Not a S3 Object"),
        }
    }

    pub fn as_localpath(self) -> &'a str {
        match self {
            Uri::LocalPath(x) => x,
            _ => panic!("Not a S3 Object"),
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
            let mut path_parts = parts[1].splitn(2, "/");
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
            return Ok(Uri::S3Object(s3::S3Object { bucket, key }));
        }
        "file" => {
            return Ok(Uri::LocalPath(parts[1]));
        }
        _ => {
            return Err(UriError::InvalidScheme(String::from(parts[0])));
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Object not found")]
    NotFound,
    #[error("Failed to read local object content")]
    IO { source: std::io::Error },
    #[error("Failed to read S3 object content")]
    S3 {
        source: RusotoError<rusoto_s3::GetObjectError>,
    },
    #[error("Invalid object URI")]
    Uri {
        #[from]
        source: UriError,
    },
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => {
                return StorageError::NotFound;
            }
            _ => {
                return StorageError::IO { source: error };
            }
        }
    }
}

impl From<RusotoError<rusoto_s3::GetObjectError>> for StorageError {
    fn from(error: RusotoError<rusoto_s3::GetObjectError>) -> Self {
        match error {
            RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(_)) => StorageError::NotFound,
            _ => StorageError::S3 { source: error },
        }
    }
}

pub trait StorageBackend {
    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError>;
    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = String>>, StorageError>;
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
        assert_eq!(uri.as_localpath(), "foo/bar");

        let uri2 = parse_uri("file:///foo/bar").unwrap();
        assert_eq!(uri2.as_localpath(), "/foo/bar");
    }

    #[test]
    fn test_parse_object_uri() {
        let uri = parse_uri("s3://foo/bar").unwrap();
        assert_eq!(
            uri.as_s3object(),
            s3::S3Object {
                bucket: "foo",
                key: "bar",
            }
        );
    }
}
