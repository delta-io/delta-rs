//! AWS S3 storage backend.
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aws_config::{Region, SdkConfig};
use bytes::Bytes;
use deltalake_core::logstore::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use deltalake_core::logstore::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreScheme,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use deltalake_core::logstore::{
    config::str_is_truthy, ObjectStoreFactory, ObjectStoreRef, StorageConfig,
};
use deltalake_core::{DeltaResult, DeltaTableError, ObjectStoreError, Path};
use futures::stream::BoxStream;
use futures::Future;
use object_store::aws::AmazonS3;
use object_store::client::SpawnedReqwestConnector;
use tracing::log::*;
use typed_builder::TypedBuilder;
use url::Url;

use crate::constants::{
    self, DEFAULT_S3_GET_INTERNAL_SERVER_ERROR_RETRIES, DEFAULT_S3_POOL_IDLE_TIMEOUT_SECONDS,
    DEFAULT_STS_POOL_IDLE_TIMEOUT_SECONDS,
};
use crate::credentials::AWSForObjectStore;
use crate::errors::DynamoDbConfigError;

const STORE_NAME: &str = "DeltaS3ObjectStore";

#[derive(Clone, Default, Debug)]
pub struct S3ObjectStoreFactory {}

impl S3StorageOptionsConversion for S3ObjectStoreFactory {}

impl ObjectStoreFactory for S3ObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let options = self.with_env_s3(&config.raw);

        // All S3-likes should start their builder the same way
        let mut builder = AmazonS3Builder::new()
            .with_url(url.to_string())
            .with_retry(config.retry.clone());

        if let Some(runtime) = &config.runtime {
            builder =
                builder.with_http_connector(SpawnedReqwestConnector::new(runtime.get_handle()));
        }

        for (key, value) in options.iter() {
            if let Ok(key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                builder = builder.with_config(key, value.clone());
            }
        }

        let s3_options = S3StorageOptions::from_map(&options)?;
        if let Some(ref sdk_config) = s3_options.sdk_config {
            builder =
                builder.with_credentials(Arc::new(AWSForObjectStore::new(sdk_config.clone())));
        }

        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        let store = aws_storage_handler(builder.build()?, &s3_options)?;
        debug!("Initialized the object store: {store:?}");

        Ok((store, prefix))
    }
}

fn aws_storage_handler(
    store: AmazonS3,
    s3_options: &S3StorageOptions,
) -> DeltaResult<ObjectStoreRef> {
    // Nearly all S3 Object stores support conditional put, so we change the default to always returning an S3 Object store
    // unless explicitly passing a locking provider key or allow_unsafe_rename. Then we will pass it to the old S3StorageBackend.
    if s3_options.locking_provider.as_deref() == Some("dynamodb") || s3_options.allow_unsafe_rename
    {
        let store = S3StorageBackend::try_new(
            Arc::new(store),
            Some("dynamodb") == s3_options.locking_provider.as_deref()
                || s3_options.allow_unsafe_rename,
        )?;
        Ok(Arc::new(store))
    } else {
        Ok(Arc::new(store))
    }
}

// Determine whether this crate is being configured for use with native AWS S3 or an S3-alike
//
// This function will return true in the default case since it's most likely that the absence of
// options will mean default/S3 configuration
fn is_aws(options: &HashMap<String, String>) -> bool {
    // Checks storage option first then env var for existence of aws force credential load
    // .from_s3_env never inserts these into the options because they are delta-rs specific
    if str_option(options, constants::AWS_FORCE_CREDENTIAL_LOAD).is_some() {
        return true;
    }

    // Checks storage option first then env var for existence of locking provider
    // .from_s3_env never inserts these into the options because they are delta-rs specific
    if str_option(options, constants::AWS_S3_LOCKING_PROVIDER).is_some() {
        return true;
    }

    // Options at this stage should only contain 'aws_endpoint' in lowercase
    // due to with_env_s3
    !(options.contains_key("aws_endpoint") || options.contains_key(constants::AWS_ENDPOINT_URL))
}

/// Options used to configure the [S3StorageBackend].
///
/// Available options are described in [constants].
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct S3StorageOptions {
    /// Whether to use virtual hosted-style requests
    #[builder(default = false)]
    pub virtual_hosted_style_request: bool,
    /// Locking provider to use (e.g., "dynamodb")
    #[builder(default, setter(strip_option, into))]
    pub locking_provider: Option<String>,
    /// Override endpoint for DynamoDB
    #[builder(default, setter(strip_option, into))]
    pub dynamodb_endpoint: Option<String>,
    /// Override region for DynamoDB
    #[builder(default, setter(strip_option, into))]
    pub dynamodb_region: Option<String>,
    /// Override access key ID for DynamoDB
    #[builder(default, setter(strip_option, into))]
    pub dynamodb_access_key_id: Option<String>,
    /// Override secret access key for DynamoDB
    #[builder(default, setter(strip_option, into))]
    pub dynamodb_secret_access_key: Option<String>,
    /// Override session token for DynamoDB
    #[builder(default, setter(strip_option, into))]
    pub dynamodb_session_token: Option<String>,
    /// Idle timeout for S3 connection pool
    #[builder(default = Duration::from_secs(DEFAULT_S3_POOL_IDLE_TIMEOUT_SECONDS))]
    pub s3_pool_idle_timeout: Duration,
    /// Idle timeout for STS connection pool
    #[builder(default = Duration::from_secs(DEFAULT_STS_POOL_IDLE_TIMEOUT_SECONDS))]
    pub sts_pool_idle_timeout: Duration,
    /// Number of retries for S3 internal server errors
    #[builder(default = DEFAULT_S3_GET_INTERNAL_SERVER_ERROR_RETRIES)]
    pub s3_get_internal_server_error_retries: usize,
    /// Allow unsafe rename operations
    #[builder(default = false)]
    pub allow_unsafe_rename: bool,
    /// Extra storage options not handled by other fields
    #[builder(default)]
    pub extra_opts: HashMap<String, String>,
    /// AWS SDK configuration
    #[builder(default, setter(strip_option))]
    pub sdk_config: Option<SdkConfig>,
}

impl Eq for S3StorageOptions {}
impl PartialEq for S3StorageOptions {
    fn eq(&self, other: &Self) -> bool {
        self.virtual_hosted_style_request == other.virtual_hosted_style_request
            && self.locking_provider == other.locking_provider
            && self.dynamodb_endpoint == other.dynamodb_endpoint
            && self.dynamodb_region == other.dynamodb_region
            && self.dynamodb_access_key_id == other.dynamodb_access_key_id
            && self.dynamodb_secret_access_key == other.dynamodb_secret_access_key
            && self.dynamodb_session_token == other.dynamodb_session_token
            && self.s3_pool_idle_timeout == other.s3_pool_idle_timeout
            && self.sts_pool_idle_timeout == other.sts_pool_idle_timeout
            && self.s3_get_internal_server_error_retries
                == other.s3_get_internal_server_error_retries
            && self.allow_unsafe_rename == other.allow_unsafe_rename
            && self.extra_opts == other.extra_opts
    }
}

impl S3StorageOptions {
    /// Creates an instance of [`S3StorageOptions`] from the given HashMap.
    pub fn from_map(options: &HashMap<String, String>) -> DeltaResult<S3StorageOptions> {
        let extra_opts: HashMap<String, String> = options
            .iter()
            .filter(|(k, _)| !constants::S3_OPTS.contains(&k.as_str()))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        // Copy web identity values provided in options but not the environment into the environment
        // to get picked up by the `from_k8s_env` call in `get_web_identity_provider`.
        Self::ensure_env_var(options, constants::AWS_REGION);
        Self::ensure_env_var(options, constants::AWS_PROFILE);
        Self::ensure_env_var(options, constants::AWS_ACCESS_KEY_ID);
        Self::ensure_env_var(options, constants::AWS_SECRET_ACCESS_KEY);
        Self::ensure_env_var(options, constants::AWS_SESSION_TOKEN);
        Self::ensure_env_var(options, constants::AWS_WEB_IDENTITY_TOKEN_FILE);
        Self::ensure_env_var(options, constants::AWS_ROLE_ARN);
        Self::ensure_env_var(options, constants::AWS_ROLE_SESSION_NAME);
        let s3_pool_idle_timeout = Self::u64_or_default(
            options,
            constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS,
            DEFAULT_S3_POOL_IDLE_TIMEOUT_SECONDS,
        );
        let sts_pool_idle_timeout = Self::u64_or_default(
            options,
            constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS,
            DEFAULT_STS_POOL_IDLE_TIMEOUT_SECONDS,
        );

        let s3_get_internal_server_error_retries = Self::u64_or_default(
            options,
            constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
            DEFAULT_S3_GET_INTERNAL_SERVER_ERROR_RETRIES as u64,
        ) as usize;

        let virtual_hosted_style_request: bool =
            str_option(options, constants::AWS_S3_ADDRESSING_STYLE)
                .map(|addressing_style| addressing_style == "virtual")
                .unwrap_or(false);

        let allow_unsafe_rename = str_option(options, constants::AWS_S3_ALLOW_UNSAFE_RENAME)
            .map(|val| str_is_truthy(&val))
            .unwrap_or(false);

        let sdk_config = match is_aws(options) {
            false => None,
            true => {
                debug!("Detected AWS S3 Storage options, resolving AWS credentials");
                Some(execute_sdk_future(
                    crate::credentials::resolve_credentials(options),
                )??)
            }
        };

        Ok(Self {
            virtual_hosted_style_request,
            locking_provider: str_option(options, constants::AWS_S3_LOCKING_PROVIDER),
            dynamodb_endpoint: str_option(options, constants::AWS_ENDPOINT_URL_DYNAMODB),
            dynamodb_region: str_option(options, constants::AWS_REGION_DYNAMODB),
            dynamodb_access_key_id: str_option(options, constants::AWS_ACCESS_KEY_ID_DYNAMODB),
            dynamodb_secret_access_key: str_option(
                options,
                constants::AWS_SECRET_ACCESS_KEY_DYNAMODB,
            ),
            dynamodb_session_token: str_option(options, constants::AWS_SESSION_TOKEN_DYNAMODB),
            s3_pool_idle_timeout: Duration::from_secs(s3_pool_idle_timeout),
            sts_pool_idle_timeout: Duration::from_secs(sts_pool_idle_timeout),
            s3_get_internal_server_error_retries,
            allow_unsafe_rename,
            extra_opts,
            sdk_config,
        })
    }

    /// Return the configured endpoint URL for S3 operations
    pub fn endpoint_url(&self) -> Option<&str> {
        self.sdk_config.as_ref().and_then(|v| v.endpoint_url())
    }

    /// Return the configured region used for S3 operations
    pub fn region(&self) -> Option<&Region> {
        self.sdk_config.as_ref().and_then(|v| v.region())
    }

    fn u64_or_default(map: &HashMap<String, String>, key: &str, default: u64) -> u64 {
        str_option(map, key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn ensure_env_var(map: &HashMap<String, String>, key: &str) {
        if let Some(val) = str_option(map, key) {
            unsafe {
                std::env::set_var(key, val);
            }
        }
    }

    pub fn try_default() -> DeltaResult<Self> {
        Self::from_map(&HashMap::new())
    }
}

fn execute_sdk_future<F, T>(future: F) -> DeltaResult<T>
where
    T: Send,
    F: Future<Output = T> + Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                Ok(tokio::task::block_in_place(move || handle.block_on(future)))
            }
            _ => {
                let mut cfg: Option<T> = None;
                std::thread::scope(|scope| {
                    scope.spawn(|| {
                        cfg = Some(handle.block_on(future));
                    });
                });
                cfg.ok_or(DeltaTableError::ObjectStore {
                    source: ObjectStoreError::Generic {
                        store: STORE_NAME,
                        source: Box::new(DynamoDbConfigError::InitializationError),
                    },
                })
            }
        },
        Err(_) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a tokio runtime is required by the AWS sdk");
            Ok(runtime.block_on(future))
        }
    }
}

/// An S3 implementation of the [ObjectStore] trait
pub struct S3StorageBackend {
    inner: ObjectStoreRef,
    /// Whether allowed to performance rename_if_not_exist as rename
    allow_unsafe_rename: bool,
}

impl std::fmt::Display for S3StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "S3StorageBackend {{ allow_unsafe_rename: {}, inner: {} }}",
            self.allow_unsafe_rename, self.inner
        )
    }
}

impl S3StorageBackend {
    /// Creates a new S3StorageBackend.
    ///
    /// Options are described in [constants].
    pub fn try_new(storage: ObjectStoreRef, allow_unsafe_rename: bool) -> ObjectStoreResult<Self> {
        Ok(Self {
            inner: storage,
            allow_unsafe_rename,
        })
    }
}

impl Debug for S3StorageBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            fmt,
            "S3StorageBackend {{ allow_unsafe_rename: {}, inner: {:?} }}",
            self.allow_unsafe_rename, self.inner
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3StorageBackend {
    async fn put(&self, location: &Path, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
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
        if self.allow_unsafe_rename {
            self.inner.rename(from, to).await
        } else {
            Err(ObjectStoreError::Generic {
                store: STORE_NAME,
                source: Box::new(crate::errors::LockClientError::LockClientRequired),
            })
        }
    }
}

/// Storage option keys to use when creating [`S3StorageOptions`].
///
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
/// Provided keys may include configuration for the S3 backend and also the optional DynamoDb lock used for atomic rename.
#[deprecated(
    since = "0.20.0",
    note = "s3_constants has moved up to deltalake_aws::constants::*"
)]
pub mod s3_constants {
    pub use crate::constants::*;
}

pub(crate) fn str_option(map: &HashMap<String, String>, key: &str) -> Option<String> {
    if let Some(s) = map.get(key) {
        return Some(s.to_owned());
    }

    if let Some(s) = map.get(&key.to_ascii_lowercase()) {
        return Some(s.to_owned());
    }

    std::env::var(key).ok()
}

pub(crate) trait S3StorageOptionsConversion {
    fn with_env_s3(&self, options: &HashMap<String, String>) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = options
            .clone()
            .into_iter()
            .map(|(k, v)| {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()) {
                    (config_key.as_ref().to_string(), v)
                } else {
                    (k, v)
                }
            })
            .collect();

        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                    options
                        .entry(config_key.as_ref().to_string())
                        .or_insert(value.to_string());
                }
            }
        }

        // All S3-like Object Stores use conditional put, object-store crate however still requires you to explicitly
        // set this behaviour. We will however assume, when a locking provider/copy-if-not-exists keys are not provided
        // that PutIfAbsent is supported.
        // With conditional put in S3-like API we can use the deltalake default logstore which use PutIfAbsent
        if !options.keys().any(|key| {
            let key = key.to_ascii_lowercase();
            [
                AmazonS3ConfigKey::ConditionalPut.as_ref(),
                "conditional_put",
            ]
            .contains(&key.as_str())
        }) {
            options.insert("conditional_put".into(), "etag".into());
        }
        options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::constants;
    use serial_test::serial;

    struct ScopedEnv {
        vars: HashMap<std::ffi::OsString, std::ffi::OsString>,
    }

    impl ScopedEnv {
        pub fn new() -> Self {
            let vars = std::env::vars_os().collect();
            Self { vars }
        }

        pub fn run<T>(mut f: impl FnMut() -> T) -> T {
            let _env_scope = Self::new();
            f()
        }
    }

    impl Drop for ScopedEnv {
        fn drop(&mut self) {
            let to_remove: Vec<_> = std::env::vars_os()
                .map(|kv| kv.0)
                .filter(|k| !self.vars.contains_key(k))
                .collect();
            for k in to_remove {
                unsafe {
                    std::env::remove_var(k);
                }
            }
            for (key, value) in self.vars.drain() {
                unsafe {
                    std::env::set_var(key, value);
                }
            }
        }
    }

    fn clear_env_of_aws_keys() {
        let keys_to_clear = std::env::vars().filter_map(|(k, _v)| {
            if AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()).is_ok() {
                Some(k)
            } else {
                None
            }
        });

        for k in keys_to_clear {
            unsafe {
                std::env::remove_var(k);
            }
        }
    }

    #[test]
    #[serial]
    fn storage_options_default_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();

            std::env::set_var(constants::AWS_ENDPOINT_URL, "http://localhost");
            std::env::set_var(constants::AWS_REGION, "us-west-1");
            std::env::set_var(constants::AWS_PROFILE, "default");
            std::env::set_var(constants::AWS_ACCESS_KEY_ID, "default_key_id");
            std::env::set_var(constants::AWS_SECRET_ACCESS_KEY, "default_secret_key");
            std::env::set_var(constants::AWS_S3_LOCKING_PROVIDER, "dynamodb");
            std::env::set_var(
                constants::AWS_IAM_ROLE_ARN,
                "arn:aws:iam::123456789012:role/some_role",
            );
            std::env::set_var(constants::AWS_IAM_ROLE_SESSION_NAME, "session_name");
            std::env::set_var(
                #[allow(deprecated)]
                constants::AWS_S3_ASSUME_ROLE_ARN,
                "arn:aws:iam::123456789012:role/some_role",
            );
            std::env::set_var(
                #[allow(deprecated)]
                constants::AWS_S3_ROLE_SESSION_NAME,
                "session_name",
            );
            std::env::set_var(constants::AWS_WEB_IDENTITY_TOKEN_FILE, "token_file");

            let options = S3StorageOptions::try_default().unwrap();
            assert_eq!(
                S3StorageOptions::builder()
                    .sdk_config(
                        SdkConfig::builder()
                            .endpoint_url("http://localhost".to_string())
                            .region(Region::from_static("us-west-1"))
                            .build()
                    )
                    .locking_provider("dynamodb")
                    .build(),
                options
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_with_only_region_and_credentials() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            std::env::remove_var(constants::AWS_ENDPOINT_URL);

            let options = S3StorageOptions::from_map(&HashMap::from([
                (constants::AWS_REGION.to_string(), "eu-west-1".to_string()),
                (constants::AWS_ACCESS_KEY_ID.to_string(), "test".to_string()),
                (
                    constants::AWS_SECRET_ACCESS_KEY.to_string(),
                    "test_secret".to_string(),
                ),
            ]))
            .unwrap();

            let mut expected = S3StorageOptions::try_default().unwrap();
            expected.sdk_config = Some(
                SdkConfig::builder()
                    .region(Region::from_static("eu-west-1"))
                    .build(),
            );
            assert_eq!(expected, options);
        });
    }

    #[test]
    #[serial]
    fn storage_options_from_map_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let options = S3StorageOptions::from_map(&HashMap::from([
                (
                    constants::AWS_ENDPOINT_URL.to_string(),
                    "http://localhost:1234".to_string(),
                ),
                (constants::AWS_REGION.to_string(), "us-west-2".to_string()),
                (constants::AWS_PROFILE.to_string(), "default".to_string()),
                (
                    constants::AWS_S3_ADDRESSING_STYLE.to_string(),
                    "virtual".to_string(),
                ),
                (
                    constants::AWS_S3_LOCKING_PROVIDER.to_string(),
                    "another_locking_provider".to_string(),
                ),
                (
                    constants::AWS_IAM_ROLE_ARN.to_string(),
                    "arn:aws:iam::123456789012:role/another_role".to_string(),
                ),
                (
                    constants::AWS_IAM_ROLE_SESSION_NAME.to_string(),
                    "another_session_name".to_string(),
                ),
                (
                    constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string(),
                    "another_token_file".to_string(),
                ),
                (
                    constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string(),
                    "1".to_string(),
                ),
                (
                    constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS.to_string(),
                    "2".to_string(),
                ),
                (
                    constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES.to_string(),
                    "3".to_string(),
                ),
                (
                    constants::AWS_ACCESS_KEY_ID.to_string(),
                    "test_id".to_string(),
                ),
                (
                    constants::AWS_SECRET_ACCESS_KEY.to_string(),
                    "test_secret".to_string(),
                ),
            ]))
            .unwrap();

            assert_eq!(
                Some("another_locking_provider"),
                options.locking_provider.as_deref()
            );
            assert_eq!(Duration::from_secs(1), options.s3_pool_idle_timeout);
            assert_eq!(Duration::from_secs(2), options.sts_pool_idle_timeout);
            assert_eq!(3, options.s3_get_internal_server_error_retries);
            assert!(options.virtual_hosted_style_request);
            assert!(!options.allow_unsafe_rename);
            assert_eq!(
                HashMap::from([(
                    constants::AWS_S3_ADDRESSING_STYLE.to_string(),
                    "virtual".to_string()
                ),]),
                options.extra_opts
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_from_map_with_dynamodb_endpoint_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let options = S3StorageOptions::from_map(&HashMap::from([
                (
                    constants::AWS_ENDPOINT_URL.to_string(),
                    "http://localhost:1234".to_string(),
                ),
                (
                    constants::AWS_ENDPOINT_URL_DYNAMODB.to_string(),
                    "http://localhost:2345".to_string(),
                ),
                (constants::AWS_REGION.to_string(), "us-west-2".to_string()),
                (constants::AWS_PROFILE.to_string(), "default".to_string()),
                (
                    constants::AWS_S3_ADDRESSING_STYLE.to_string(),
                    "virtual".to_string(),
                ),
                (
                    constants::AWS_S3_LOCKING_PROVIDER.to_string(),
                    "another_locking_provider".to_string(),
                ),
                (
                    constants::AWS_IAM_ROLE_ARN.to_string(),
                    "arn:aws:iam::123456789012:role/another_role".to_string(),
                ),
                (
                    constants::AWS_IAM_ROLE_SESSION_NAME.to_string(),
                    "another_session_name".to_string(),
                ),
                (
                    constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string(),
                    "another_token_file".to_string(),
                ),
                (
                    constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string(),
                    "1".to_string(),
                ),
                (
                    constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS.to_string(),
                    "2".to_string(),
                ),
                (
                    constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES.to_string(),
                    "3".to_string(),
                ),
                (
                    constants::AWS_ACCESS_KEY_ID.to_string(),
                    "test_id".to_string(),
                ),
                (
                    constants::AWS_SECRET_ACCESS_KEY.to_string(),
                    "test_secret".to_string(),
                ),
            ]))
            .unwrap();

            assert_eq!(
                Some("http://localhost:2345"),
                options.dynamodb_endpoint.as_deref()
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_mixed_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            std::env::set_var(constants::AWS_ENDPOINT_URL, "http://localhost");
            std::env::set_var(
                constants::AWS_ENDPOINT_URL_DYNAMODB,
                "http://localhost:dynamodb",
            );
            std::env::set_var(constants::AWS_REGION, "us-west-1");
            std::env::set_var(constants::AWS_PROFILE, "default");
            std::env::set_var(constants::AWS_ACCESS_KEY_ID, "wrong_key_id");
            std::env::set_var(constants::AWS_SECRET_ACCESS_KEY, "wrong_secret_key");
            std::env::set_var(constants::AWS_S3_LOCKING_PROVIDER, "dynamodb");
            std::env::set_var(
                constants::AWS_IAM_ROLE_ARN,
                "arn:aws:iam::123456789012:role/some_role",
            );
            std::env::set_var(constants::AWS_IAM_ROLE_SESSION_NAME, "session_name");
            std::env::set_var(constants::AWS_WEB_IDENTITY_TOKEN_FILE, "token_file");

            std::env::set_var(constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS, "1");
            std::env::set_var(constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS, "2");
            std::env::set_var(constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES, "3");
            let options = S3StorageOptions::from_map(&HashMap::from([
                (
                    constants::AWS_ACCESS_KEY_ID.to_string(),
                    "test_id_mixed".to_string(),
                ),
                (
                    constants::AWS_SECRET_ACCESS_KEY.to_string(),
                    "test_secret_mixed".to_string(),
                ),
                (constants::AWS_REGION.to_string(), "us-west-2".to_string()),
                (
                    "AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES".to_string(),
                    "3".to_string(),
                ),
            ]))
            .unwrap();

            assert_eq!(
                S3StorageOptions::builder()
                    .sdk_config(
                        SdkConfig::builder()
                            .endpoint_url("http://localhost".to_string())
                            .region(Region::from_static("us-west-2"))
                            .build()
                    )
                    .locking_provider("dynamodb")
                    .dynamodb_endpoint("http://localhost:dynamodb")
                    .s3_pool_idle_timeout(Duration::from_secs(1))
                    .sts_pool_idle_timeout(Duration::from_secs(2))
                    .s3_get_internal_server_error_retries(3)
                    .build(),
                options
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_web_identity_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let _options = S3StorageOptions::from_map(&HashMap::from([
                (constants::AWS_REGION.to_string(), "eu-west-1".to_string()),
                (
                    constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string(),
                    "web_identity_token_file".to_string(),
                ),
                (
                    constants::AWS_ROLE_ARN.to_string(),
                    "arn:aws:iam::123456789012:role/web_identity_role".to_string(),
                ),
                (
                    constants::AWS_ROLE_SESSION_NAME.to_string(),
                    "web_identity_session_name".to_string(),
                ),
            ]))
            .unwrap();

            assert_eq!("eu-west-1", std::env::var(constants::AWS_REGION).unwrap());

            assert_eq!(
                "web_identity_token_file",
                std::env::var(constants::AWS_WEB_IDENTITY_TOKEN_FILE).unwrap()
            );

            assert_eq!(
                "arn:aws:iam::123456789012:role/web_identity_role",
                std::env::var(constants::AWS_ROLE_ARN).unwrap()
            );

            assert_eq!(
                "web_identity_session_name",
                std::env::var(constants::AWS_ROLE_SESSION_NAME).unwrap()
            );
        });
    }

    #[test]
    #[serial]
    fn when_merging_with_env_unsupplied_options_are_added() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let raw_options = HashMap::new();
            std::env::set_var(constants::AWS_ACCESS_KEY_ID, "env_key");
            std::env::set_var(constants::AWS_ENDPOINT_URL, "env_key");
            std::env::set_var(constants::AWS_SECRET_ACCESS_KEY, "env_key");
            std::env::set_var(constants::AWS_REGION, "env_key");
            let combined_options = S3ObjectStoreFactory {}.with_env_s3(&raw_options);

            // Four and then the conditional_put built-in
            assert_eq!(combined_options.len(), 5);

            for (key, v) in combined_options {
                if key != "conditional_put" {
                    assert_eq!(v, "env_key");
                }
            }
        });
    }

    #[tokio::test]
    #[serial]
    async fn when_merging_with_env_supplied_options_take_precedence() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let raw_options = HashMap::from([
                ("AWS_ACCESS_KEY_ID".to_string(), "options_key".to_string()),
                ("AWS_ENDPOINT_URL".to_string(), "options_key".to_string()),
                (
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    "options_key".to_string(),
                ),
                ("AWS_REGION".to_string(), "options_key".to_string()),
            ]);
            std::env::set_var("aws_access_key_id", "env_key");
            std::env::set_var("aws_endpoint", "env_key");
            std::env::set_var("aws_secret_access_key", "env_key");
            std::env::set_var("aws_region", "env_key");

            let combined_options = S3ObjectStoreFactory {}.with_env_s3(&raw_options);

            for (key, v) in combined_options {
                if key != "conditional_put" {
                    assert_eq!(v, "options_key");
                }
            }
        });
    }

    #[test]
    #[serial]
    fn test_is_aws() {
        clear_env_of_aws_keys();
        let options = HashMap::default();
        assert!(is_aws(&options));

        let minio: HashMap<String, String> = HashMap::from([(
            constants::AWS_ENDPOINT_URL.to_string(),
            "http://minio:8080".to_string(),
        )]);
        assert!(!is_aws(&minio));

        let minio: HashMap<String, String> =
            HashMap::from([("aws_endpoint".to_string(), "http://minio:8080".to_string())]);
        assert!(!is_aws(&minio));

        let localstack: HashMap<String, String> = HashMap::from([
            (
                constants::AWS_FORCE_CREDENTIAL_LOAD.to_string(),
                "true".to_string(),
            ),
            ("aws_endpoint".to_string(), "http://minio:8080".to_string()),
        ]);
        assert!(is_aws(&localstack));
    }
}
