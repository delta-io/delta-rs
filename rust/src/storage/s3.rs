use std::convert::TryFrom;
use std::{fmt, pin::Pin};

use chrono::{DateTime, FixedOffset, Utc};
use futures::Stream;
use log::debug;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectRequest, HeadObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3,
};
use tokio::io::AsyncReadExt;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};

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
            // rusoto tries to parse response body which is missing in HEAD request
            // see https://github.com/rusoto/rusoto/issues/716
            RusotoError::Unknown(r) if r.status == 404 => StorageError::NotFound,
            _ => StorageError::S3Head { source: error },
        }
    }
}

impl From<RusotoError<rusoto_s3::PutObjectError>> for StorageError {
    fn from(error: RusotoError<rusoto_s3::PutObjectError>) -> Self {
        StorageError::S3Put { source: error }
    }
}

impl From<RusotoError<rusoto_s3::ListObjectsV2Error>> for StorageError {
    fn from(error: RusotoError<rusoto_s3::ListObjectsV2Error>) -> Self {
        match error {
            RusotoError::Service(rusoto_s3::ListObjectsV2Error::NoSuchBucket(_)) => {
                StorageError::NotFound
            }
            _ => StorageError::S3List { source: error },
        }
    }
}

fn create_s3_client(region: Region) -> Result<S3Client, StorageError> {
    let client = match std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE") {
        Ok(_) => {
            let provider = rusoto_sts::WebIdentityProvider::from_k8s_env();
            let provider =
                rusoto_credential::AutoRefreshingProvider::new(provider).map_err(|e| {
                    StorageError::S3Generic(format!(
                        "Failed to retrieve S3 credentials with message: {}",
                        e.message
                    ))
                })?;

            let dispatcher = rusoto_core::HttpClient::new().ok_or_else(|| {
                StorageError::S3Generic("Failed to create request dispatcher".to_string())
            })?;
            S3Client::new_with(dispatcher, provider, region)
        }
        Err(_) => S3Client::new(region),
    };
    Ok(client)
}

fn parse_obj_last_modified_time(
    last_modified: &Option<String>,
) -> Result<DateTime<Utc>, StorageError> {
    let dt_str = last_modified.as_ref().ok_or_else(|| {
        StorageError::S3Generic("S3 Object missing last modified attribute".to_string())
    })?;
    // last modified time in object is returned in rfc3339 format
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
    let dt = DateTime::<FixedOffset>::parse_from_rfc3339(dt_str).map_err(|e| {
        StorageError::S3Generic(format!(
            "Failed to parse S3 modified time as rfc3339: {}, got: {:?}",
            e, last_modified,
        ))
    })?;

    Ok(DateTime::<Utc>::from(dt))
}

fn parse_head_obj_last_modified_time(
    last_modified: &Option<String>,
) -> Result<DateTime<Utc>, StorageError> {
    let dt_str = last_modified.as_ref().ok_or_else(|| {
        StorageError::S3Generic("S3 Object missing last modified attribute".to_string())
    })?;
    // head object response sets last-modified time in rfc2822 format:
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
    let dt = DateTime::<FixedOffset>::parse_from_rfc2822(dt_str).map_err(|e| {
        StorageError::S3Generic(format!(
            "Failed to parse S3 modified time as rfc2822: {}, got: {:?}",
            e, last_modified,
        ))
    })?;

    Ok(DateTime::<Utc>::from(dt))
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
            modified: parse_head_obj_last_modified_time(&result.last_modified)?,
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

        /// This enum is used to represent 3 states in our object metadata streaming logic:
        /// * Value(None): the initial state, prior to performing any s3 list call.
        /// * Value(Some(String)): s3 list call returned us a continuation token to be used in
        /// subsequent list call after we got through the current page.
        /// * End: previous s3 list call reached end of page, we should not perform more s3 list
        /// call going forward.
        enum ContinuationToken {
            Value(Option<String>),
            End,
        }

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
