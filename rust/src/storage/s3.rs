extern crate tokio;

use std::fmt;

use chrono::{DateTime, FixedOffset, Utc};
use log::debug;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, ListObjectsV2Request, S3Client, S3};
use tokio::{io::AsyncReadExt, runtime};

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};

#[derive(Debug, PartialEq)]
pub struct S3Object<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
}

impl<'a> fmt::Display for S3Object<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
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

impl Default for S3StorageBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for S3StorageBackend {
    fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let uri = parse_uri(path)?.into_s3object()?;

        let mut rt = Self::gen_tokio_rt();
        let result = rt.block_on(self.client.head_object(HeadObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            ..Default::default()
        }))?;

        Ok(ObjectMeta {
            path: path.to_string(),
            modified: DateTime::<Utc>::from(
                DateTime::<FixedOffset>::parse_from_rfc2822(&result.last_modified.unwrap())
                    .unwrap(),
            ),
        })
    }

    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("fetching s3 object: {}...", path);

        let uri = parse_uri(path)?.into_s3object()?;
        let get_req = GetObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            ..Default::default()
        };

        let mut rt = Self::gen_tokio_rt();
        let result = rt.block_on(self.client.get_object(get_req))?;

        debug!("streaming data from {}...", path);
        let mut buf = Vec::new();
        let stream = result
            .body
            .ok_or_else(|| StorageError::S3MissingObjectBody(path.to_string()))?;
        rt.block_on(stream.into_async_read().read_to_end(&mut buf))
            .unwrap();

        debug!("s3 object fetched: {}", path);
        Ok(buf)
    }

    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = ObjectMeta>>, StorageError> {
        let uri = parse_uri(path)?.into_s3object()?;

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

        fn next_meta(ctx: &mut ListContext) -> Option<ObjectMeta> {
            match ctx.obj_iter.next() {
                Some(obj) => Some(ObjectMeta {
                    path: obj.key.unwrap(),
                    modified: DateTime::<Utc>::from(
                        DateTime::<FixedOffset>::parse_from_rfc2822(&obj.last_modified.unwrap())
                            .unwrap(),
                    ),
                }),
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

                        next_meta(ctx)
                    }
                    None => None,
                },
            }
        }

        Ok(Box::new(std::iter::from_fn(move || next_meta(&mut ctx))))
    }
}
