//! AWS S3 storage backend. It only supports a single writer and is not multi-writer safe.

use super::{str_option, StorageError};
use bytes::Bytes;
use dynamodb_lock::{LockClient, LockItem, DEFAULT_MAX_RETRY_ACQUIRE_LOCK_ATTEMPTS};
use futures::stream::BoxStream;
use object_store::aws::AmazonS3;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::AutoRefreshingProvider;
use rusoto_sts::WebIdentityProvider;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;

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

/// Uses a `LockClient` to support additional features required by S3 Storage.
pub struct S3LockClient {
    lock_client: Box<dyn LockClient>,
}

impl S3LockClient {
    async fn rename_with_lock(
        &self,
        s3: &S3StorageBackend,
        src: &Path,
        dst: &Path,
    ) -> Result<(), ObjectStoreError> {
        let mut lock = self.acquire_lock_loop(src.as_ref(), dst.as_ref()).await?;

        if let Some(ref data) = lock.data {
            let data: LockData =
                serde_json::from_str(data).map_err(|err| ObjectStoreError::Generic {
                    store: "DeltaS3Store",
                    source: Box::new(err),
                })?;

            if lock.acquired_expired_lock {
                log::info!(
                    "Acquired expired lock. Repairing the rename of {} to {}.",
                    &data.source,
                    &data.destination
                );
            }

            let mut rename_result = s3
                .rename(&Path::from(data.source), &Path::from(data.destination))
                .await;

            if lock.acquired_expired_lock {
                match rename_result {
                    // AlreadyExists when the stale rename is done, but the lock not released
                    // NotFound when the source file of rename is missing
                    Err(ObjectStoreError::AlreadyExists { .. })
                    | Err(ObjectStoreError::NotFound { .. }) => (),
                    _ => rename_result?,
                }

                // If we acquired expired lock then the rename done above is
                // a repair of expired one. So on this time we try the intended rename.
                lock.data = Some(LockData::json(src.as_ref(), dst.as_ref())?);
                lock = self
                    .lock_client
                    .update_data(&lock)
                    .await
                    .map_err(|_| ObjectStoreError::NotImplemented)?;
                rename_result = s3.rename(src, dst).await;
            }

            let release_result = self.lock_client.release_lock(&lock).await;

            // before unwrapping `rename_result` the `release_result` is called to ensure that we
            // no longer hold the lock
            rename_result?;

            // TODO implement form DynamoErr
            if !release_result.map_err(|_| ObjectStoreError::NotImplemented)? {
                log::error!("Could not release lock {:?}", &lock);
                return Err(ObjectStoreError::NotImplemented);
            }

            Ok(())
        } else {
            Err(ObjectStoreError::NotImplemented)
        }
    }

    async fn acquire_lock_loop(&self, src: &str, dst: &str) -> Result<LockItem, StorageError> {
        let data = LockData::json(src, dst)?;

        let lock;
        let mut retries = 0;

        loop {
            match self.lock_client.try_acquire_lock(data.as_str()).await? {
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

/// Storage option keys to use when creating [crate::storage::s3::S3StorageOptions].
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
/// Provided keys may include configuration for the S3 backend and also the optional DynamoDb lock used for atomic rename.
pub mod s3_storage_options {
    /// Custom S3 endpoint.
    pub const AWS_ENDPOINT_URL: &str = "AWS_ENDPOINT_URL";
    /// The AWS region.
    pub const AWS_REGION: &str = "AWS_REGION";
    /// The AWS_ACCESS_KEY_ID to use for S3.
    pub const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
    /// The AWS_SECRET_ACCESS_ID to use for S3.
    pub const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
    /// The AWS_SESSION_TOKEN to use for S3.
    pub const AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
    /// Locking provider to use for safe atomic rename.
    /// `dynamodb` is currently the only supported locking provider.
    /// If not set, safe atomic rename is not available.
    pub const AWS_S3_LOCKING_PROVIDER: &str = "AWS_S3_LOCKING_PROVIDER";
    /// The role to assume for S3 writes.
    pub const AWS_S3_ASSUME_ROLE_ARN: &str = "AWS_S3_ASSUME_ROLE_ARN";
    /// The role session name to use when a role is assumed. If not provided a random session name is generated.
    pub const AWS_S3_ROLE_SESSION_NAME: &str = "AWS_S3_ROLE_SESSION_NAME";
    /// The `pool_idle_timeout` option of aws http client. Has to be lower than 20 seconds, which is
    /// default S3 server timeout <https://aws.amazon.com/premiumsupport/knowledge-center/s3-socket-connection-timeout-error/>.
    /// However, since rusoto uses hyper as a client, its default timeout is 90 seconds
    /// <https://docs.rs/hyper/0.13.2/hyper/client/struct.Builder.html#method.keep_alive_timeout>.
    /// Hence, the `connection closed before message completed` could occur.
    /// To avoid that, the default value of this setting is 15 seconds if it's not set otherwise.
    pub const AWS_S3_POOL_IDLE_TIMEOUT_SECONDS: &str = "AWS_S3_POOL_IDLE_TIMEOUT_SECONDS";
    /// The `pool_idle_timeout` for the as3_storage_optionsws sts client. See
    /// the reasoning in `AWS_S3_POOL_IDLE_TIMEOUT_SECONDS`.
    pub const AWS_STS_POOL_IDLE_TIMEOUT_SECONDS: &str = "AWS_STS_POOL_IDLE_TIMEOUT_SECONDS";
    /// The number of retries for S3 GET requests failed with 500 Internal Server Error.
    pub const AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES: &str =
        "AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES";
    /// The web identity token file to use when using a web identity provider.
    /// NOTE: web identity related options are set in the environment when
    /// creating an instance of [crate::storage::s3::S3StorageOptions].
    /// See also <https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env>.
    pub const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
    /// The role name to use for web identity.
    /// NOTE: web identity related options are set in the environment when
    /// creating an instance of [crate::storage::s3::S3StorageOptions].
    /// See also <https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env>.
    pub const AWS_ROLE_ARN: &str = "AWS_ROLE_ARN";
    /// The role session name to use for web identity.
    /// NOTE: web identity related options are set in the environment when
    /// creating an instance of [crate::storage::s3::S3StorageOptions].
    /// See also <https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env>.
    pub const AWS_ROLE_SESSION_NAME: &str = "AWS_ROLE_SESSION_NAME";

    /// The list of option keys owned by the S3 module.
    /// Option keys not contained in this list will be added to the `extra_opts`
    /// field of [crate::storage::s3::S3StorageOptions].
    /// `extra_opts` are passed to [dynamodb_lock::DynamoDbOptions] to configure the lock client.
    pub const S3_OPTS: &[&str] = &[
        AWS_ENDPOINT_URL,
        AWS_REGION,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_SESSION_TOKEN,
        AWS_S3_LOCKING_PROVIDER,
        AWS_S3_ASSUME_ROLE_ARN,
        AWS_S3_ROLE_SESSION_NAME,
        AWS_WEB_IDENTITY_TOKEN_FILE,
        AWS_ROLE_ARN,
        AWS_ROLE_SESSION_NAME,
        AWS_S3_POOL_IDLE_TIMEOUT_SECONDS,
        AWS_STS_POOL_IDLE_TIMEOUT_SECONDS,
        AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
    ];
}

/// Options used to configure the S3StorageBackend.
///
/// Available options are described in [s3_storage_options].
#[derive(Clone, Debug, PartialEq)]
pub struct S3StorageOptions {
    _endpoint_url: Option<String>,
    region: Region,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_session_token: Option<String>,
    locking_provider: Option<String>,
    assume_role_arn: Option<String>,
    assume_role_session_name: Option<String>,
    use_web_identity: bool,
    s3_pool_idle_timeout: Duration,
    sts_pool_idle_timeout: Duration,
    s3_get_internal_server_error_retries: usize,
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

        // Copy web identity values provided in options but not the environment into the environment
        // to get picked up by the `from_k8s_env` call in `get_web_identity_provider`.
        Self::ensure_env_var(&options, s3_storage_options::AWS_REGION);
        Self::ensure_env_var(&options, s3_storage_options::AWS_ACCESS_KEY_ID);
        Self::ensure_env_var(&options, s3_storage_options::AWS_SECRET_ACCESS_KEY);
        Self::ensure_env_var(&options, s3_storage_options::AWS_SESSION_TOKEN);
        Self::ensure_env_var(&options, s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE);
        Self::ensure_env_var(&options, s3_storage_options::AWS_ROLE_ARN);
        Self::ensure_env_var(&options, s3_storage_options::AWS_ROLE_SESSION_NAME);

        let endpoint_url = str_option(&options, s3_storage_options::AWS_ENDPOINT_URL);
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

        let s3_pool_idle_timeout = Self::u64_or_default(
            &options,
            s3_storage_options::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS,
            15,
        );
        let sts_pool_idle_timeout = Self::u64_or_default(
            &options,
            s3_storage_options::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS,
            10,
        );

        let s3_get_internal_server_error_retries = Self::u64_or_default(
            &options,
            s3_storage_options::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
            10,
        ) as usize;

        Self {
            _endpoint_url: endpoint_url,
            region,
            aws_access_key_id: str_option(&options, s3_storage_options::AWS_ACCESS_KEY_ID),
            aws_secret_access_key: str_option(&options, s3_storage_options::AWS_SECRET_ACCESS_KEY),
            aws_session_token: str_option(&options, s3_storage_options::AWS_SESSION_TOKEN),
            locking_provider: str_option(&options, s3_storage_options::AWS_S3_LOCKING_PROVIDER),
            assume_role_arn: str_option(&options, s3_storage_options::AWS_S3_ASSUME_ROLE_ARN),
            assume_role_session_name: str_option(
                &options,
                s3_storage_options::AWS_S3_ROLE_SESSION_NAME,
            ),
            use_web_identity: std::env::var(s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE)
                .is_ok(),
            s3_pool_idle_timeout: Duration::from_secs(s3_pool_idle_timeout),
            sts_pool_idle_timeout: Duration::from_secs(sts_pool_idle_timeout),
            s3_get_internal_server_error_retries,
            extra_opts,
        }
    }

    fn str_or_default(map: &HashMap<String, String>, key: &str, default: String) -> String {
        map.get(key)
            .map(|v| v.to_owned())
            .unwrap_or_else(|| std::env::var(key).unwrap_or(default))
    }

    fn u64_or_default(map: &HashMap<String, String>, key: &str, default: u64) -> u64 {
        str_option(map, key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn ensure_env_var(map: &HashMap<String, String>, key: &str) {
        if let Some(val) = str_option(map, key) {
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

fn get_web_identity_provider() -> Result<AutoRefreshingProvider<WebIdentityProvider>, StorageError>
{
    let provider = WebIdentityProvider::from_k8s_env();
    Ok(AutoRefreshingProvider::new(provider)?)
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

/// An S3 implementation of the [ObjectStore] trait
///
/// The backend can optionally use [dynamodb_lock] to better support concurrent
/// writers. To do so, either pass in a [dynamodb_lock::LockClient] to [S3StorageBackend::new_with]
/// or configure the locking client within [S3StorageOptions]. For example:
///
/// ```rust
/// use std::collections::HashMap;
/// use deltalake::storage::s3::{S3StorageOptions, S3StorageBackend, s3_storage_options};
///
/// let options = S3StorageOptions::from_map([
///     (s3_storage_options::AWS_S3_LOCKING_PROVIDER, "dynamodb"),
///     // Options passed down to dynamodb_lock::DynamoDbOptions (default values shown below)
///     ("table_name", "delta_rs_lock_table"),
///     ("partition_key_value", "delta-rs"),
///     ("owner_name", "<some UUID>"), // Should be unique across writers
///     ("lease_duration", "20"), // seconds
///     ("refresh_period", "1000"), // milliseconds
///     ("additional_time_to_wait_for_lock", "1000"), // milliseconds
/// ].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect());
/// let backend = S3StorageBackend::new_from_options(options);
/// ```
pub struct S3StorageBackend {
    inner: Arc<AmazonS3>,
    s3_lock_client: Option<S3LockClient>,
}

impl std::fmt::Display for S3StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3StorageBackend")
    }
}

impl S3StorageBackend {
    /// Creates a new S3StorageBackend.
    pub fn new() -> Result<Self, StorageError> {
        let options = S3StorageOptions::default();
        let _s3_lock_client = try_create_lock_client(&options)?;

        todo!()
    }

    /// Creates a new S3StorageBackend from the provided options.
    ///
    /// Options are described in [s3_storage_options].
    pub fn new_from_options(
        storage: Arc<AmazonS3>,
        options: S3StorageOptions,
    ) -> Result<Self, StorageError> {
        let s3_lock_client = try_create_lock_client(&options)?;

        Ok(Self {
            inner: storage,
            s3_lock_client,
        })
    }

    /// Creates a new S3StorageBackend with given options, s3 client and lock client.
    pub fn new_with(
        storage: Arc<AmazonS3>,
        lock_client: Option<Box<dyn LockClient>>,
        _options: S3StorageOptions,
    ) -> Self {
        let s3_lock_client = lock_client.map(|lc| S3LockClient { lock_client: lc });
        Self {
            inner: storage,
            s3_lock_client,
        }
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
impl ObjectStore for S3StorageBackend {
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        self.inner.put(location, bytes).await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let lock_client = match self.s3_lock_client {
            Some(ref lock_client) => lock_client,
            None => return Err(ObjectStoreError::NotImplemented),
        };

        lock_client.rename_with_lock(self, from, to).await?;

        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }
}

fn try_create_lock_client(
    options: &S3StorageOptions,
) -> Result<Option<S3LockClient>, StorageError> {
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
            Ok(Some(S3LockClient {
                lock_client: Box::new(lock_client),
            }))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use maplit::hashmap;
    use serial_test::serial;

    #[test]
    #[serial]
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
        std::env::remove_var(s3_storage_options::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS);
        std::env::remove_var(s3_storage_options::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS);
        std::env::remove_var(s3_storage_options::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES);

        let options = S3StorageOptions::default();

        assert_eq!(
            S3StorageOptions {
                _endpoint_url: Some("http://localhost".to_string()),
                region: Region::Custom {
                    name: "us-west-1".to_string(),
                    endpoint: "http://localhost".to_string()
                },
                aws_access_key_id: Some("test".to_string()),
                aws_secret_access_key: Some("test".to_string()),
                aws_session_token: None,
                assume_role_arn: Some("arn:aws:iam::123456789012:role/some_role".to_string()),
                assume_role_session_name: Some("session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("dynamodb".to_string()),
                s3_pool_idle_timeout: Duration::from_secs(15),
                sts_pool_idle_timeout: Duration::from_secs(10),
                s3_get_internal_server_error_retries: 10,
                extra_opts: HashMap::new(),
            },
            options
        );
    }

    #[test]
    #[serial]
    fn storage_options_with_only_region_and_credentials() {
        std::env::remove_var(s3_storage_options::AWS_ENDPOINT_URL);
        let options = S3StorageOptions::from_map(hashmap! {
            s3_storage_options::AWS_REGION.to_string() => "eu-west-1".to_string(),
            s3_storage_options::AWS_ACCESS_KEY_ID.to_string() => "test".to_string(),
            s3_storage_options::AWS_SECRET_ACCESS_KEY.to_string() => "test".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                _endpoint_url: None,
                region: Region::default(),
                aws_access_key_id: Some("test".to_string()),
                aws_secret_access_key: Some("test".to_string()),
                ..Default::default()
            },
            options
        );
    }

    #[test]
    #[serial]
    fn storage_options_from_map_test() {
        let options = S3StorageOptions::from_map(hashmap! {
            s3_storage_options::AWS_ENDPOINT_URL.to_string() => "http://localhost:1234".to_string(),
            s3_storage_options::AWS_REGION.to_string() => "us-west-2".to_string(),
            s3_storage_options::AWS_S3_LOCKING_PROVIDER.to_string() => "another_locking_provider".to_string(),
            s3_storage_options::AWS_S3_ASSUME_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/another_role".to_string(),
            s3_storage_options::AWS_S3_ROLE_SESSION_NAME.to_string() => "another_session_name".to_string(),
            s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "another_token_file".to_string(),
            s3_storage_options::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "1".to_string(),
            s3_storage_options::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "2".to_string(),
            s3_storage_options::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES.to_string() => "3".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                _endpoint_url: Some("http://localhost:1234".to_string()),
                region: Region::Custom {
                    name: "us-west-2".to_string(),
                    endpoint: "http://localhost:1234".to_string()
                },
                aws_access_key_id: Some("test".to_string()),
                aws_secret_access_key: Some("test".to_string()),
                aws_session_token: None,
                assume_role_arn: Some("arn:aws:iam::123456789012:role/another_role".to_string()),
                assume_role_session_name: Some("another_session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("another_locking_provider".to_string()),
                s3_pool_idle_timeout: Duration::from_secs(1),
                sts_pool_idle_timeout: Duration::from_secs(2),
                s3_get_internal_server_error_retries: 3,
                extra_opts: HashMap::new(),
            },
            options
        );
    }

    #[test]
    #[serial]
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

        std::env::set_var(s3_storage_options::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS, "1");
        std::env::set_var(s3_storage_options::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS, "2");
        std::env::set_var(
            s3_storage_options::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
            "3",
        );
        let options = S3StorageOptions::from_map(hashmap! {
            s3_storage_options::AWS_REGION.to_string() => "us-west-2".to_string(),
            "DYNAMO_LOCK_PARTITION_KEY_VALUE".to_string() => "my_lock".to_string(),
            "AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES".to_string() => "3".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                _endpoint_url: Some("http://localhost".to_string()),
                region: Region::Custom {
                    name: "us-west-2".to_string(),
                    endpoint: "http://localhost".to_string()
                },
                aws_access_key_id: Some("test".to_string()),
                aws_secret_access_key: Some("test".to_string()),
                aws_session_token: None,
                assume_role_arn: Some("arn:aws:iam::123456789012:role/some_role".to_string()),
                assume_role_session_name: Some("session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("dynamodb".to_string()),
                s3_pool_idle_timeout: Duration::from_secs(1),
                sts_pool_idle_timeout: Duration::from_secs(2),
                s3_get_internal_server_error_retries: 3,
                extra_opts: hashmap! {
                    "DYNAMO_LOCK_PARTITION_KEY_VALUE".to_string() => "my_lock".to_string(),
                },
            },
            options
        );
    }
    #[test]
    #[serial]
    fn storage_options_web_identity_test() {
        let _options = S3StorageOptions::from_map(hashmap! {
            s3_storage_options::AWS_REGION.to_string() => "eu-west-1".to_string(),
            s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "web_identity_token_file".to_string(),
            s3_storage_options::AWS_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/web_identity_role".to_string(),
            s3_storage_options::AWS_ROLE_SESSION_NAME.to_string() => "web_identity_session_name".to_string(),
        });

        assert_eq!(
            "eu-west-1",
            std::env::var(s3_storage_options::AWS_REGION).unwrap()
        );

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
