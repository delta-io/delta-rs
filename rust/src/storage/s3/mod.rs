//! AWS S3 storage backend. It only supports a single writer and is not multi-writer safe.

use std::convert::TryFrom;
use std::fmt::Debug;
use std::{fmt, pin::Pin};

use chrono::{DateTime, FixedOffset, Utc};
use futures::Stream;
use log::debug;
use rusoto_core::credential::ChainProvider;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::AutoRefreshingProvider;
use rusoto_s3::{
    CopyObjectRequest, DeleteObjectRequest, GetObjectRequest, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, S3,
};
use rusoto_sts::WebIdentityProvider;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};

pub mod dynamodb_lock;

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

impl From<RusotoError<rusoto_s3::CopyObjectError>> for StorageError {
    fn from(error: RusotoError<rusoto_s3::CopyObjectError>) -> Self {
        match error {
            RusotoError::Unknown(response) if response.status == 404 => StorageError::NotFound,
            _ => StorageError::S3Copy { source: error },
        }
    }
}

fn get_web_identity_provider() -> Result<AutoRefreshingProvider<WebIdentityProvider>, StorageError>
{
    let provider = WebIdentityProvider::from_k8s_env();
    Ok(AutoRefreshingProvider::new(provider)?)
}

fn create_s3_client(region: Region) -> Result<S3Client, StorageError> {
    let dispatcher = HttpClient::new()?;

    let client = match std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE") {
        Ok(_) => S3Client::new_with(dispatcher, get_web_identity_provider()?, region),
        Err(_) => S3Client::new_with(dispatcher, ChainProvider::new(), region),
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

/// Struct describing an object stored in S3.
#[derive(Debug, PartialEq)]
pub struct S3Object<'a> {
    /// The bucket where the object is stored.
    pub bucket: &'a str,
    /// The key of the object within the bucket.
    pub key: &'a str,
}

impl<'a> fmt::Display for S3Object<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

/// An S3 implementation of the `StorageBackend` trait
pub struct S3StorageBackend {
    client: rusoto_s3::S3Client,
    lock_client: Option<Box<dyn LockClient>>,
}

impl S3StorageBackend {
    /// Creates a new S3StorageBackend.
    pub fn new() -> Result<Self, StorageError> {
        let region = if let Ok(url) = std::env::var("AWS_ENDPOINT_URL") {
            Region::Custom {
                name: std::env::var("AWS_REGION").unwrap_or_else(|_| "custom".to_string()),
                endpoint: url,
            }
        } else {
            Region::default()
        };

        let client = create_s3_client(region.clone())?;
        let lock_client = try_create_lock_client(region)?;

        Ok(Self {
            client,
            lock_client,
        })
    }

    /// Creates a new S3StorageBackend with given s3 and lock clients.
    pub fn new_with(client: rusoto_s3::S3Client, lock_client: Option<Box<dyn LockClient>>) -> Self {
        Self {
            client,
            lock_client,
        }
    }

    async fn unsafe_rename_obj(
        self: &S3StorageBackend,
        src: &str,
        dst: &str,
    ) -> Result<(), StorageError> {
        match self.head_obj(dst).await {
            Ok(_) => return Err(StorageError::AlreadyExists(dst.to_string())),
            Err(StorageError::NotFound) => (),
            Err(e) => return Err(e),
        }

        let src = parse_uri(src)?.into_s3object()?;
        let dst = parse_uri(dst)?.into_s3object()?;

        self.client
            .copy_object(CopyObjectRequest {
                bucket: dst.bucket.to_string(),
                key: dst.key.to_string(),
                copy_source: format!("{}/{}", src.bucket, src.key),
                ..Default::default()
            })
            .await?;

        self.client
            .delete_object(DeleteObjectRequest {
                bucket: src.bucket.to_string(),
                key: src.key.to_string(),
                ..Default::default()
            })
            .await?;

        Ok(())
    }
}

impl Default for S3StorageBackend {
    fn default() -> Self {
        Self::new().unwrap()
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
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    > {
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

    async fn rename_obj(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        debug!("rename s3 object: {} -> {}...", src, dst);

        let lock_client = match self.lock_client {
            Some(ref lock_client) => lock_client,
            None => {
                return Err(StorageError::S3Generic(
                    "dynamodb locking is not enabled".to_string(),
                ))
            }
        };

        lock_client.rename_with_lock(self, src, dst).await?;

        Ok(())
    }

    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        debug!("delete s3 object: {}...", path);

        let uri = parse_uri(path)?.into_s3object()?;
        let put_req = DeleteObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            ..Default::default()
        };

        self.client.delete_object(put_req).await?;

        Ok(())
    }
}

/// A lock that has been successfully acquired
#[derive(Clone, Debug)]
pub struct LockItem {
    /// The name of the owner that owns this lock.
    pub owner_name: String,
    /// Current version number of the lock in DynamoDB. This is what tells the lock client
    /// when the lock is stale.
    pub record_version_number: String,
    /// The amount of time (in seconds) that the owner has this lock for.
    /// If lease_duration is None then the lock is non-expirable.
    pub lease_duration: Option<u64>,
    /// Tells whether or not the lock was marked as released when loaded from DynamoDB.
    pub is_released: bool,
    /// Optional data associated with this lock.
    pub data: Option<String>,
    /// The last time this lock was updated or retrieved.
    pub lookup_time: u128,
    /// Tells whether this lock was acquired by expiring existing one
    pub acquired_expired_lock: bool,
}

/// Lock data which stores an attempt to rename `source` into `destination`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LockData {
    /// Source object key
    pub source: String,
    /// Destination object ket
    pub destination: String,
}

impl LockData {
    /// Builds new `LockData` instance and then creates json string from it.
    pub fn json(src: &str, dst: &str) -> Result<String, StorageError> {
        let data = LockData {
            source: src.to_string(),
            destination: dst.to_string(),
        };
        let json = serde_json::to_string(&data)
            .map_err(|_| StorageError::S3Generic("Lock data serialize error".to_string()))?;

        Ok(json)
    }
}

fn try_create_lock_client(region: Region) -> Result<Option<Box<dyn LockClient>>, StorageError> {
    let dispatcher = HttpClient::new()?;

    match std::env::var("AWS_S3_LOCKING_PROVIDER") {
        Ok(p) if p.to_lowercase() == "dynamodb" => {
            let client = match std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE") {
                Ok(_) => rusoto_dynamodb::DynamoDbClient::new_with(
                    dispatcher,
                    get_web_identity_provider()?,
                    region,
                ),
                Err(_) => rusoto_dynamodb::DynamoDbClient::new(region),
            };
            let client =
                dynamodb_lock::DynamoDbLockClient::new(client, dynamodb_lock::Options::default());
            Ok(Some(Box::new(client)))
        }
        _ => Ok(None),
    }
}

/// Abstraction over a distributive lock provider
#[async_trait::async_trait]
pub trait LockClient: Send + Sync + Debug {
    /// Attempts to acquire lock. If successful, returns the lock.
    /// Otherwise returns [`Option::None`] which is retryable action.
    /// Visit implementation docs for more details.
    async fn try_acquire_lock(&self, data: &str) -> Result<Option<LockItem>, StorageError>;

    /// Returns current lock from DynamoDB (if any).
    async fn get_lock(&self) -> Result<Option<LockItem>, StorageError>;

    /// Update data in the upstream lock of the current user still has it.
    /// The returned lock will have a new `rvn` so it'll increase the lease duration
    /// as this method is usually called when the work with a lock is extended.
    async fn update_data(&self, lock: &LockItem) -> Result<LockItem, StorageError>;

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    async fn release_lock(&self, lock: &LockItem) -> Result<bool, StorageError>;
}

const DEFAULT_MAX_RETRY_ACQUIRE_LOCK_ATTEMPTS: u32 = 10_000;

impl dyn LockClient {
    async fn rename_with_lock(
        &self,
        s3: &S3StorageBackend,
        src: &str,
        dst: &str,
    ) -> Result<(), StorageError> {
        let mut lock = self.acquire_lock_loop(src, dst).await?;

        if let Some(ref data) = lock.data {
            let data: LockData = serde_json::from_str(data)
                .map_err(|_| StorageError::S3Generic("Lock data deserialize error".to_string()))?;

            if lock.acquired_expired_lock {
                log::info!(
                    "Acquired expired lock. Repairing the rename of {} to {}.",
                    &data.source,
                    &data.destination
                );
            }

            let mut rename_result = s3.unsafe_rename_obj(&data.source, &data.destination).await;

            if lock.acquired_expired_lock {
                match rename_result {
                    // AlreadyExists when the stale rename is done, but the lock not released
                    // NotFound when the source file of rename is missing
                    Err(StorageError::AlreadyExists(_)) | Err(StorageError::NotFound) => (),
                    _ => rename_result?,
                }

                // If we acquired expired lock then the rename done above is
                // a repair of expired one. So on this time we try the intended rename.
                lock.data = Some(LockData::json(src, dst)?);
                lock = self.update_data(&lock).await?;
                rename_result = s3.unsafe_rename_obj(src, dst).await;
            }

            let release_result = self.release_lock(&lock).await;

            // before unwrapping `rename_result` the `release_result` is called to ensure that we
            // no longer hold the lock
            rename_result?;

            if !release_result? {
                log::error!("Could not release lock {:?}", &lock);
                return Err(StorageError::S3Generic("Lock is not released".to_string()));
            }

            Ok(())
        } else {
            Err(StorageError::S3Generic(
                "Acquired lock with no lock data".to_string(),
            ))
        }
    }

    async fn acquire_lock_loop(&self, src: &str, dst: &str) -> Result<LockItem, StorageError> {
        let data = LockData::json(src, dst)?;

        let lock;
        let mut retries = 0;

        loop {
            match self.try_acquire_lock(&data).await? {
                Some(l) => {
                    lock = l;
                    break;
                }
                None => {
                    retries += 1;
                    if retries > DEFAULT_MAX_RETRY_ACQUIRE_LOCK_ATTEMPTS {
                        return Err(StorageError::S3Generic("Cannot acquire lock".to_string()));
                    }
                }
            }
        }

        Ok(lock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_multiple_paths() {
        let backend = S3StorageBackend::new().unwrap();
        assert_eq!(&backend.join_paths(&["abc", "efg/", "123"]), "abc/efg/123",);
        assert_eq!(&backend.join_paths(&["abc", "efg/"]), "abc/efg",);
        assert_eq!(&backend.join_paths(&["foo"]), "foo",);
        assert_eq!(&backend.join_paths(&[]), "",);
    }

    #[test]
    fn trim_path() {
        let be = S3StorageBackend::new().unwrap();
        assert_eq!(be.trim_path("s3://foo/bar"), "s3://foo/bar");
        assert_eq!(be.trim_path("s3://foo/bar/"), "s3://foo/bar");
        assert_eq!(be.trim_path("/foo/bar//"), "/foo/bar");
    }

    #[test]
    fn parse_s3_object_uri() {
        let uri = parse_uri("s3://foo/bar/baz").unwrap();
        assert_eq!(uri.path(), "bar/baz");
        assert_eq!(
            uri.into_s3object().unwrap(),
            S3Object {
                bucket: "foo",
                key: "bar/baz",
            }
        );
    }
}
