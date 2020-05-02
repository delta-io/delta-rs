use std::fs;

use rusoto_core::{Region, RusotoError};
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};

use thiserror;

use tokio::io::AsyncReadExt;
use tokio::runtime;

#[derive(Debug, PartialEq)]
pub struct S3Object<'a> {
    bucket: &'a str,
    key: &'a str,
}

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
            return Ok(Uri::S3Object(S3Object { bucket, key }));
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

    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = String>>, StorageError> {
        let readdir = fs::read_dir(path)?;
        Ok(Box::new(readdir.into_iter().map(|entry| {
            String::from(entry.unwrap().path().to_str().unwrap())
        })))
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

    fn gen_tokio_rt() -> runtime::Runtime {
        runtime::Builder::new()
            .enable_time()
            .enable_io()
            .basic_scheduler()
            .build()
            .unwrap()
    }
}

impl StorageBackend for S3StorageBackend {
    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("fetching s3 object: {}...", path);

        let uri = parse_uri(path)?.as_s3object();
        let get_req = GetObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            ..Default::default()
        };

        let mut rt = Self::gen_tokio_rt();
        let result = rt.block_on(self.client.get_object(get_req))?;

        debug!("streaming data from {}...", path);
        let mut buf = Vec::new();
        let stream = result.body.unwrap();
        rt.block_on(stream.into_async_read().read_to_end(&mut buf))
            .unwrap();

        debug!("s3 object fetched: {}", path);
        Ok(buf)
    }

    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = String>>, StorageError> {
        let uri = parse_uri(path)?.as_s3object();

        struct ListContext {
            client: rusoto_s3::S3Client,
            obj_iter: std::vec::IntoIter<rusoto_s3::Object>,
            continuation_token: Option<String>,
            rt: runtime::Runtime,
            bucket: String,
            key: String,
        }
        let mut ctx = ListContext {
            obj_iter: Vec::new().into_iter(),
            continuation_token: Some(String::from("initial_run")),
            rt: Self::gen_tokio_rt(),
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            client: self.client.clone(),
        };

        fn next_key(ctx: &mut ListContext) -> Option<String> {
            return match ctx.obj_iter.next() {
                Some(obj) => {
                    return Some(obj.key.unwrap());
                }
                None => match &ctx.continuation_token {
                    Some(token) => {
                        let tk_opt = if token != "initial_run" {
                            Some(token.clone())
                        } else {
                            None
                        };
                        let list_req = ListObjectsV2Request {
                            bucket: ctx.bucket.clone(),
                            prefix: Some(ctx.key.clone()),
                            continuation_token: tk_opt,
                            ..Default::default()
                        };
                        // TODO: log list objects error
                        let result = ctx.rt.block_on(ctx.client.list_objects_v2(list_req)).ok()?;
                        ctx.continuation_token = result.next_continuation_token;
                        ctx.obj_iter = match result.contents {
                            Some(objs) => objs.into_iter(),
                            None => Vec::new().into_iter(),
                        };

                        return next_key(ctx);
                    }
                    None => None,
                },
            };
        }

        Ok(Box::new(std::iter::from_fn(move || next_key(&mut ctx))))
    }
}

pub fn get_backend_for_uri(uri: &str) -> Result<Box<dyn StorageBackend>, UriError> {
    match parse_uri(uri)? {
        Uri::LocalPath(_) => Ok(Box::new(FileStorageBackend::new())),
        Uri::S3Object(_) => Ok(Box::new(S3StorageBackend::new())),
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
