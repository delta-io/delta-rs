use std::convert::TryFrom;
use std::{fmt, pin::Pin};

use chrono::{DateTime, FixedOffset, Utc};
use futures::Stream;
use log::debug;
use rusoto_core::Region;
use rusoto_s3::{
    GetObjectRequest, HeadObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3,
};
use tokio::io::AsyncReadExt;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};

enum ContinuationToken {
    Value(Option<String>),
    End,
}

fn parse_obj_last_modified_time(
    last_modified: &Option<String>,
) -> Result<DateTime<Utc>, StorageError> {
    Ok(DateTime::<Utc>::from(
        DateTime::<FixedOffset>::parse_from_rfc2822(last_modified.as_ref().ok_or_else(|| {
            StorageError::S3Generic("S3 Object missing last modified attribute".to_string())
        })?)
        .map_err(|e| StorageError::S3Generic(format!("Failed to parse S3 modified time: {}", e)))?,
    ))
}

impl TryFrom<rusoto_s3::Object> for ObjectMeta {
    type Error = StorageError;

    fn try_from(obj: rusoto_s3::Object) -> Result<Self, Self::Error> {
        Ok(ObjectMeta {
            path: obj.key.ok_or_else(|| {
                StorageError::S3Generic("S3 Object missing key attribute".to_string())
            })?,
            modified: parse_obj_last_modified_time(&obj.last_modified)?,
        })
    }
}

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
        let client = S3Client::new(Region::default());
        Self { client }
    }
}

impl Default for S3StorageBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for S3StorageBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "S3StorageBackend")
    }
}

#[async_trait::async_trait]
impl StorageBackend for S3StorageBackend {
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let uri = parse_uri(path)?.into_s3object()?;

        let result = self
            .client
            .head_object(HeadObjectRequest {
                bucket: uri.bucket.to_string(),
                key: uri.key.to_string(),
                ..Default::default()
            })
            .await?;

        Ok(ObjectMeta {
            path: path.to_string(),
            modified: parse_obj_last_modified_time(&result.last_modified)?,
        })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("fetching s3 object: {}...", path);

        let uri = parse_uri(path)?.into_s3object()?;
        let get_req = GetObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            ..Default::default()
        };

        let result = self.client.get_object(get_req).await?;

        debug!("streaming data from {}...", path);
        let mut buf = Vec::new();
        let stream = result
            .body
            .ok_or_else(|| StorageError::S3MissingObjectBody(path.to_string()))?;
        stream
            .into_async_read()
            .read_to_end(&mut buf)
            .await
            .map_err(|e| {
                StorageError::S3Generic(format!("Failed to read object content: {}", e))
            })?;

        debug!("s3 object fetched: {}", path);
        Ok(buf)
    }

    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + 'a>>, StorageError>
    {
        let uri = parse_uri(path)?.into_s3object()?;

        struct ListContext {
            client: rusoto_s3::S3Client,
            obj_iter: std::vec::IntoIter<rusoto_s3::Object>,
            continuation_token: ContinuationToken,
            bucket: String,
            key: String,
        }
        let ctx = ListContext {
            obj_iter: Vec::new().into_iter(),
            continuation_token: ContinuationToken::Value(None),
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            client: self.client.clone(),
        };

        async fn next_meta(
            mut ctx: ListContext,
        ) -> Option<(Result<ObjectMeta, StorageError>, ListContext)> {
            match ctx.obj_iter.next() {
                Some(obj) => Some((ObjectMeta::try_from(obj), ctx)),
                None => match &ctx.continuation_token {
                    ContinuationToken::End => None,
                    ContinuationToken::Value(v) => {
                        let list_req = ListObjectsV2Request {
                            bucket: ctx.bucket.clone(),
                            prefix: Some(ctx.key.clone()),
                            continuation_token: v.clone(),
                            ..Default::default()
                        };
                        let result = match ctx.client.list_objects_v2(list_req).await {
                            Ok(res) => res,
                            Err(e) => {
                                return Some((Err(e.into()), ctx));
                            }
                        };
                        ctx.continuation_token = result
                            .next_continuation_token
                            .map(|t| ContinuationToken::Value(Some(t)))
                            .unwrap_or(ContinuationToken::End);
                        ctx.obj_iter = result.contents.unwrap_or_else(Vec::new).into_iter();
                        ctx.obj_iter
                            .next()
                            .map(|obj| (ObjectMeta::try_from(obj), ctx))
                    }
                },
            }
        }

        Ok(Box::pin(futures::stream::unfold(ctx, next_meta)))
    }

    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        debug!("put s3 object: {}...", path);

        match self.head_obj(path).await {
            Ok(_) => return Err(StorageError::AlreadyExists(path.to_string())),
            Err(StorageError::NotFound) => (),
            Err(e) => return Err(e),
        }

        let uri = parse_uri(path)?.into_s3object()?;
        let put_req = PutObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            body: Some(obj_bytes.to_vec().into()),
            ..Default::default()
        };

        self.client.put_object(put_req).await?;

        Ok(())
    }
}
