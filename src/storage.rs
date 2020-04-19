use std::{fmt, fs};
use std::error::Error;

use rusoto_core::{Region, RusotoError};
use rusoto_s3::{GetObjectRequest, S3Client, S3};

use tokio::io::AsyncReadExt;
use tokio::runtime;

#[derive(Debug, PartialEq)]
pub struct S3Object<'a> {
    bucket: &'a str,
    key: &'a str,
}

#[derive(Debug)]
pub enum Uri<'a> {
    LocalPath(&'a str),
    S3Object(S3Object<'a>),
}

impl<'a> Uri<'a> {
    pub fn as_s3object(self) -> S3Object<'a> {
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

pub fn parse_uri<'a>(path: &'a str) -> Result<Uri<'a>, &'static str> {
    let parts: Vec<&'a str> = path.split("://").collect();

    if parts.len() == 1 {
        return Ok(Uri::LocalPath(parts[0]));
    }

    match parts[0] {
        "s3" => {
            let mut path_parts = parts[1].splitn(2, "/");
            let bucket = path_parts.next().unwrap();
            let key = path_parts.next().unwrap();

            return Ok(Uri::S3Object(S3Object { bucket, key }));
        }
        "file" => {
            return Ok(Uri::LocalPath(parts[1]));
        }
        _ => {
            panic!("invalid uri scheme: {}", parts[0]);
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    NotFound,
    Unknown(String),
}

impl Error for StorageError {}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::NotFound => write!(f, "Object not found"),
            StorageError::Unknown(s) => write!(f, "Unkown error: {}", s),
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => {
                return StorageError::NotFound;
            }
            _ => {
                return StorageError::Unknown(format!("{:#?}", error));
            }
        }
    }
}

impl From<RusotoError<rusoto_s3::GetObjectError>> for StorageError {
    fn from(error: RusotoError<rusoto_s3::GetObjectError>) -> Self {
        match error {
            RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(_)) => StorageError::NotFound,
            _ => StorageError::Unknown(format!("{:#?}", error)),
        }
    }
}

pub trait StorageBackend {
    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError>;
}

pub struct FileStorageBackend {}

impl FileStorageBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl StorageBackend for FileStorageBackend {
    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        fs::read(path).map_err(|e| StorageError::from(e))
    }
}

pub struct S3StorageBackend {
    client: rusoto_s3::S3Client,
}

impl S3StorageBackend {
    pub fn new() -> Self {
        let client = S3Client::new(Region::UsEast2);
        Self { client }
    }
}

impl StorageBackend for S3StorageBackend {
    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("fetching s3 object: {}...", path);

        let uri = parse_uri(path).unwrap().as_s3object();
        let get_req = GetObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            ..Default::default()
        };

        let mut rt = runtime::Builder::new()
            .enable_time()
            .enable_io()
            .basic_scheduler()
            .build()
            .unwrap();

        let result = rt.block_on(self.client.get_object(get_req))?;

        debug!("streaming data from {}...", path);
        let mut buf = Vec::new();
        let stream = result.body.unwrap();
        rt.block_on(stream.into_async_read().read_to_end(&mut buf))
            .unwrap();

        debug!("s3 object fetched: {}", path);
        Ok(buf)
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
            S3Object {
                bucket: "foo",
                key: "bar",
            }
        );
    }
}
