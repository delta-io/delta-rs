//! AWS S3 storage backend. It only supports a single writer and is not multi-writer safe.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::{fmt, pin::Pin};

use chrono::{DateTime, FixedOffset, Utc};
use futures::Stream;
use log::debug;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::AutoRefreshingProvider;
use rusoto_s3::{
    CopyObjectRequest, Delete, DeleteObjectRequest, DeleteObjectsRequest, GetObjectRequest,
    HeadObjectRequest, ListObjectsV2Request, ObjectIdentifier, PutObjectRequest, S3Client, S3,
};
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient, WebIdentityProvider};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};
use uuid::Uuid;

pub mod dynamodb_lock;

/// Storage option keys to use when creating [crate::storage::s3::S3StorageOptions].
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
/// Provided keys may include configuration for the S3 backend and also the optional DynamoDb lock used for atomic rename.
pub mod s3_storage_options {
    /// Custom S3 endpoint.
    pub const AWS_ENDPOINT_URL: &str = "AWS_ENDPOINT_URL";
    /// The AWS region.
    pub const AWS_REGION: &str = "AWS_REGION";
    /// Locking provider to use for safe atomic rename.
    /// `dynamodb` is currently the only supported locking provider.
    /// If not set, safe atomic rename is not available.
    pub const AWS_S3_LOCKING_PROVIDER: &str = "AWS_S3_LOCKING_PROVIDER";
    /// The role to assume for S3 writes.
    pub const AWS_S3_ASSUME_ROLE_ARN: &str = "AWS_S3_ASSUME_ROLE_ARN";
    /// The role session name to use when a role is assumed. If not provided a random session name is generated.
    pub const AWS_S3_ROLE_SESSION_NAME: &str = "AWS_S3_ROLE_SESSION_NAME";

    /// The web identity token file to use when using a web identity provider.
    /// NOTE: web identity related options are set in the environment when creating an instance of [crate::storage::s3::S3StorageOptions].
    /// See also https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env.
    pub const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
    /// The role name to use for web identity.
    /// NOTE: web identity related options are set in the environment when creating an instance of [crate::storage::s3::S3StorageOptions].
    /// See also https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env.
    pub const AWS_ROLE_ARN: &str = "AWS_ROLE_ARN";
    /// The role session name to use for web identity.
    /// NOTE: web identity related options are set in the environment when creating an instance of [crate::storage::s3::S3StorageOptions].
    /// See also https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env.
    pub const AWS_ROLE_SESSION_NAME: &str = "AWS_ROLE_SESSION_NAME";

    /// The list of option keys owned by the S3 module.
    /// Option keys not contained in this list will be added to the `extra_opts` field of [crate::storage::s3::S3StorageOptions].
    /// `extra_opts` are passed to [crate::storage::s3::dynamodb_lock::DynamoDbOptions] to configure the lock client.
    pub const S3_OPTS: &[&str] = &[
        AWS_ENDPOINT_URL,
        AWS_REGION,
        AWS_S3_LOCKING_PROVIDER,
        AWS_S3_ASSUME_ROLE_ARN,
        AWS_S3_ROLE_SESSION_NAME,
        AWS_WEB_IDENTITY_TOKEN_FILE,
        AWS_ROLE_ARN,
        AWS_ROLE_SESSION_NAME,
    ];
}

/// Options used to configure the S3StorageBackend.
///
/// Available options are described in [s3_storage_options].
#[derive(Clone, Debug, PartialEq)]
pub struct S3StorageOptions {
    _endpoint_url: Option<String>,
    region: Region,
    locking_provider: Option<String>,
    assume_role_arn: Option<String>,
    assume_role_session_name: Option<String>,
    use_web_identity: bool,
    extra_opts: HashMap<String, String>,
}

impl S3StorageOptions {
    /// Creates an instance of S3StorageOptions from the given HashMap.
    pub fn from_map(options: HashMap<String, String>) -> S3StorageOptions {
        let extra_opts = options
            .iter()
            .filter(|(k, _)| !s3_storage_options::S3_OPTS.contains(&k.as_str()))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

        let endpoint_url = Self::str_option(&options, s3_storage_options::AWS_ENDPOINT_URL);
        let region = if let Some(endpoint_url) = endpoint_url.as_ref() {
            Region::Custom {
                name: Self::str_or_default(
                    &options,
                    s3_storage_options::AWS_REGION,
                    "custom".to_string(),
                ),
                endpoint: endpoint_url.to_owned(),
            }
        } else {
            Region::default()
        };

        // Copy web identity values provided in options but not the environment into the environment
        // to get picked up by the `from_k8s_env` call in `get_web_identity_provider`.
        Self::ensure_env_var(&options, s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE);
        Self::ensure_env_var(&options, s3_storage_options::AWS_ROLE_ARN);
        Self::ensure_env_var(&options, s3_storage_options::AWS_ROLE_SESSION_NAME);

        Self {
            _endpoint_url: endpoint_url,
            region,
            locking_provider: Self::str_option(
                &options,
                s3_storage_options::AWS_S3_LOCKING_PROVIDER,
            ),
            assume_role_arn: Self::str_option(&options, s3_storage_options::AWS_S3_ASSUME_ROLE_ARN),
            assume_role_session_name: Self::str_option(
                &options,
                s3_storage_options::AWS_S3_ROLE_SESSION_NAME,
            ),
            use_web_identity: std::env::var(s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE)
                .is_ok(),
            extra_opts,
        }
    }

    fn str_or_default(map: &HashMap<String, String>, key: &str, default: String) -> String {
        map.get(key)
            .map(|v| v.to_owned())
            .unwrap_or_else(|| std::env::var(key).unwrap_or(default))
    }

    fn str_option(map: &HashMap<String, String>, key: &str) -> Option<String> {
        map.get(key)
            .map_or_else(|| std::env::var(key).ok(), |v| Some(v.to_owned()))
    }

    fn ensure_env_var(map: &HashMap<String, String>, key: &str) {
        if let Some(val) = Self::str_option(map, key) {
            std::env::set_var(key, val);
        }
    }
}

impl Default for S3StorageOptions {
    /// Creates an instance of S3StorageOptions from environment variables.
    fn default() -> S3StorageOptions {
        Self::from_map(HashMap::new())
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

fn get_sts_assume_role_provider(
    assume_role_arn: String,
    options: &S3StorageOptions,
) -> Result<AutoRefreshingProvider<StsAssumeRoleSessionCredentialsProvider>, StorageError> {
    let sts_client = StsClient::new(options.region.clone());
    let session_name = options
        .assume_role_session_name
        .as_ref()
        .map(|v| v.to_owned())
        .unwrap_or_else(|| format!("delta-rs-{}", Uuid::new_v4()));
    let provider = StsAssumeRoleSessionCredentialsProvider::new(
        sts_client,
        assume_role_arn,
        session_name,
        None,
        None,
        None,
        None,
    );
    Ok(AutoRefreshingProvider::new(provider)?)
}

fn create_s3_client(options: &S3StorageOptions) -> Result<S3Client, StorageError> {
    if options.use_web_identity {
        let provider = get_web_identity_provider()?;
        Ok(S3Client::new_with(
            HttpClient::new()?,
            provider,
            options.region.clone(),
        ))
    } else if let Some(assume_role_arn) = &options.assume_role_arn {
        let provider = get_sts_assume_role_provider(assume_role_arn.to_owned(), options)?;
        Ok(S3Client::new_with(
            HttpClient::new()?,
            provider,
            options.region.clone(),
        ))
    } else {
        Ok(S3Client::new(options.region.clone()))
    }
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
        let options = S3StorageOptions::default();
        let client = create_s3_client(&options)?;
        let lock_client = try_create_lock_client(&options)?;

        Ok(Self {
            client,
            lock_client,
        })
    }

    /// Creates a new S3StorageBackend from the provided options.
    ///
    /// Options are described in
    pub fn new_from_options(options: S3StorageOptions) -> Result<Self, StorageError> {
        let client = create_s3_client(&options)?;
        let lock_client = try_create_lock_client(&options)?;

        Ok(Self {
            client,
            lock_client,
        })
    }

    /// Creates a new S3StorageBackend with given options, s3 client and lock client.
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

    async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError> {
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
        let delete_req = DeleteObjectRequest {
            bucket: uri.bucket.to_string(),
            key: uri.key.to_string(),
            ..Default::default()
        };

        self.client.delete_object(delete_req).await?;

        Ok(())
    }

    async fn delete_objs(&self, paths: &[String]) -> Result<(), StorageError> {
        debug!("delete s3 objects: {:?}...", paths);
        if paths.is_empty() {
            return Ok(());
        }

        let s3_objects = paths
            .iter()
            .map(|path| Ok(parse_uri(path)?.into_s3object()?))
            .collect::<Result<Vec<_>, StorageError>>()?;

        // Check whether all buckets are equal
        let bucket = s3_objects[0].bucket;
        s3_objects.iter().skip(1).try_for_each(|object| {
            let other_bucket = object.bucket;
            if other_bucket != bucket {
                Err(StorageError::S3Generic(
                    format!("All buckets of the paths in `S3StorageBackend::delete_objs` should be the same. Expected '{}', got '{}'", bucket, other_bucket)
                ))
            } else {
                Ok(())
            }
        })?;

        // S3 has a maximum of 1000 files to delete
        let chunks = s3_objects.chunks(1000);
        for chunk in chunks {
            let delete = Delete {
                objects: chunk
                    .iter()
                    .map(|obj| ObjectIdentifier {
                        key: obj.key.to_string(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            let delete_req = DeleteObjectsRequest {
                bucket: bucket.to_string(),
                delete,
                ..Default::default()
            };
            self.client.delete_objects(delete_req).await?;
        }

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
    /// Tells whether this lock was acquired by expiring existing one.
    pub acquired_expired_lock: bool,
    /// If true then this lock could not be acquired.
    pub is_non_acquirable: bool,
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

fn try_create_lock_client(
    options: &S3StorageOptions,
) -> Result<Option<Box<dyn LockClient>>, StorageError> {
    let dispatcher = HttpClient::new()?;

    match &options.locking_provider {
        Some(p) if p.to_lowercase() == "dynamodb" => {
            let dynamodb_client = match options.use_web_identity {
                true => rusoto_dynamodb::DynamoDbClient::new_with(
                    dispatcher,
                    get_web_identity_provider()?,
                    options.region.clone(),
                ),
                false => rusoto_dynamodb::DynamoDbClient::new(options.region.clone()),
            };
            let lock_client = dynamodb_lock::DynamoDbLockClient::new(
                dynamodb_client,
                dynamodb_lock::DynamoDbOptions::from_map(options.extra_opts.clone()),
            );
            Ok(Some(Box::new(lock_client)))
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

    use maplit::hashmap;

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

    #[test]
    fn storage_options_default_test() {
        std::env::set_var(s3_storage_options::AWS_ENDPOINT_URL, "http://localhost");
        std::env::set_var(s3_storage_options::AWS_REGION, "us-west-1");
        std::env::set_var(s3_storage_options::AWS_S3_LOCKING_PROVIDER, "dynamodb");
        std::env::set_var(
            s3_storage_options::AWS_S3_ASSUME_ROLE_ARN,
            "arn:aws:iam::123456789012:role/some_role",
        );
        std::env::set_var(s3_storage_options::AWS_S3_ROLE_SESSION_NAME, "session_name");
        std::env::set_var(
            s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE,
            "token_file",
        );

        let options = S3StorageOptions::default();

        assert_eq!(
            S3StorageOptions {
                _endpoint_url: Some("http://localhost".to_string()),
                region: Region::Custom {
                    name: "us-west-1".to_string(),
                    endpoint: "http://localhost".to_string()
                },
                assume_role_arn: Some("arn:aws:iam::123456789012:role/some_role".to_string()),
                assume_role_session_name: Some("session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("dynamodb".to_string()),
                extra_opts: HashMap::new(),
            },
            options
        );
    }

    #[test]
    fn storage_options_from_map_test() {
        let options = S3StorageOptions::from_map(hashmap! {
            s3_storage_options::AWS_ENDPOINT_URL.to_string() => "http://localhost:1234".to_string(),
            s3_storage_options::AWS_REGION.to_string() => "us-west-2".to_string(),
            s3_storage_options::AWS_S3_LOCKING_PROVIDER.to_string() => "another_locking_provider".to_string(),
            s3_storage_options::AWS_S3_ASSUME_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/another_role".to_string(),
            s3_storage_options::AWS_S3_ROLE_SESSION_NAME.to_string() => "another_session_name".to_string(),
            s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "another_token_file".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                _endpoint_url: Some("http://localhost:1234".to_string()),
                region: Region::Custom {
                    name: "us-west-2".to_string(),
                    endpoint: "http://localhost:1234".to_string()
                },
                assume_role_arn: Some("arn:aws:iam::123456789012:role/another_role".to_string()),
                assume_role_session_name: Some("another_session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("another_locking_provider".to_string()),
                extra_opts: HashMap::new(),
            },
            options
        );
    }

    #[test]
    fn storage_options_mixed_test() {
        std::env::set_var(s3_storage_options::AWS_ENDPOINT_URL, "http://localhost");
        std::env::set_var(s3_storage_options::AWS_REGION, "us-west-1");
        std::env::set_var(s3_storage_options::AWS_S3_LOCKING_PROVIDER, "dynamodb");
        std::env::set_var(
            s3_storage_options::AWS_S3_ASSUME_ROLE_ARN,
            "arn:aws:iam::123456789012:role/some_role",
        );
        std::env::set_var(s3_storage_options::AWS_S3_ROLE_SESSION_NAME, "session_name");
        std::env::set_var(
            s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE,
            "token_file",
        );

        let options = S3StorageOptions::from_map(hashmap! {
            s3_storage_options::AWS_REGION.to_string() => "us-west-2".to_string(),
            "DYNAMO_LOCK_PARTITION_KEY_VALUE".to_string() => "my_lock".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                _endpoint_url: Some("http://localhost".to_string()),
                region: Region::Custom {
                    name: "us-west-2".to_string(),
                    endpoint: "http://localhost".to_string()
                },
                assume_role_arn: Some("arn:aws:iam::123456789012:role/some_role".to_string()),
                assume_role_session_name: Some("session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("dynamodb".to_string()),
                extra_opts: hashmap! {
                    "DYNAMO_LOCK_PARTITION_KEY_VALUE".to_string() => "my_lock".to_string(),
                },
            },
            options
        );
    }
    #[test]
    fn storage_options_web_identity_test() {
        let _options = S3StorageOptions::from_map(hashmap! {
            s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "web_identity_token_file".to_string(),
            s3_storage_options::AWS_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/web_identity_role".to_string(),
            s3_storage_options::AWS_ROLE_SESSION_NAME.to_string() => "web_identity_session_name".to_string(),
        });

        assert_eq!(
            "web_identity_token_file",
            std::env::var(s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE).unwrap()
        );

        assert_eq!(
            "arn:aws:iam::123456789012:role/web_identity_role",
            std::env::var(s3_storage_options::AWS_ROLE_ARN).unwrap()
        );

        assert_eq!(
            "web_identity_session_name",
            std::env::var(s3_storage_options::AWS_ROLE_SESSION_NAME).unwrap()
        );
    }
}
