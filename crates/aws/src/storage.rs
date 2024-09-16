//! AWS S3 storage backend.

use aws_config::{Region, SdkConfig};
use bytes::Bytes;
use deltalake_core::storage::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use deltalake_core::storage::object_store::{
    parse_url_opts, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreScheme, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use deltalake_core::storage::{
    limit_store_handler, str_is_truthy, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, DeltaTableError, ObjectStoreError, Path};
use futures::stream::BoxStream;
use futures::Future;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::*;
use url::Url;

use crate::constants;
use crate::errors::DynamoDbConfigError;
#[cfg(feature = "native-tls")]
use crate::native;

const STORE_NAME: &str = "DeltaS3ObjectStore";

#[derive(Clone, Default, Debug)]
pub struct S3ObjectStoreFactory {}

impl S3ObjectStoreFactory {
    fn with_env_s3(&self, options: &StorageOptions) -> StorageOptions {
        let mut options = StorageOptions(
            options
                .0
                .clone()
                .into_iter()
                .map(|(k, v)| {
                    if let Ok(config_key) = AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()) {
                        (config_key.as_ref().to_string(), v)
                    } else {
                        (k, v)
                    }
                })
                .collect(),
        );

        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                    if !options.0.contains_key(config_key.as_ref()) {
                        options
                            .0
                            .insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
            }
        }
        options
    }
}

impl ObjectStoreFactory for S3ObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        storage_options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let options = self.with_env_s3(storage_options);
        let sdk_config = execute_sdk_future(crate::credentials::resolve_credentials(
            storage_options.clone(),
        ))??;

        let os_credentials = Arc::new(crate::credentials::AWSForObjectStore::new(sdk_config));

        let mut builder = AmazonS3Builder::new().with_url(url.to_string());

        for (key, value) in options.0.iter() {
            if let Ok(key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                builder = builder.with_config(key, value.clone());
            }
        }

        let (_scheme, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;
        let inner = builder.with_credentials(os_credentials).build()?;

        let store = aws_storage_handler(limit_store_handler(inner, &options), &options)?;
        debug!("Initialized the object store: {store:?}");

        Ok((store, prefix))
    }
}

fn aws_storage_handler(
    store: ObjectStoreRef,
    options: &StorageOptions,
) -> DeltaResult<ObjectStoreRef> {
    // If the copy-if-not-exists env var is set or ConditionalPut is set, we don't need to instantiate a locking client or check for allow-unsafe-rename.
    if options
        .0
        .contains_key(AmazonS3ConfigKey::CopyIfNotExists.as_ref())
        || options
            .0
            .contains_key(AmazonS3ConfigKey::ConditionalPut.as_ref())
    {
        Ok(store)
    } else {
        let s3_options = S3StorageOptions::from_map(&options.0)?;

        let store = S3StorageBackend::try_new(
            store,
            Some("dynamodb") == s3_options.locking_provider.as_deref()
                || s3_options.allow_unsafe_rename,
        )?;
        Ok(Arc::new(store))
    }
}

/// Options used to configure the [S3StorageBackend].
///
/// Available options are described in [s3_constants].
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct S3StorageOptions {
    pub virtual_hosted_style_request: bool,
    pub locking_provider: Option<String>,
    pub dynamodb_endpoint: Option<String>,
    pub s3_pool_idle_timeout: Duration,
    pub sts_pool_idle_timeout: Duration,
    pub s3_get_internal_server_error_retries: usize,
    pub allow_unsafe_rename: bool,
    pub extra_opts: HashMap<String, String>,
    pub sdk_config: SdkConfig,
}

impl Eq for S3StorageOptions {}
impl PartialEq for S3StorageOptions {
    fn eq(&self, other: &Self) -> bool {
        self.virtual_hosted_style_request == other.virtual_hosted_style_request
            && self.locking_provider == other.locking_provider
            && self.dynamodb_endpoint == other.dynamodb_endpoint
            && self.s3_pool_idle_timeout == other.s3_pool_idle_timeout
            && self.sts_pool_idle_timeout == other.sts_pool_idle_timeout
            && self.s3_get_internal_server_error_retries
                == other.s3_get_internal_server_error_retries
            && self.allow_unsafe_rename == other.allow_unsafe_rename
            && self.extra_opts == other.extra_opts
            && self.sdk_config.endpoint_url() == other.sdk_config.endpoint_url()
            && self.sdk_config.region() == other.sdk_config.region()
    }
}

impl S3StorageOptions {
    /// Creates an instance of S3StorageOptions from the given HashMap.
    pub fn from_map(options: &HashMap<String, String>) -> DeltaResult<S3StorageOptions> {
        let extra_opts: HashMap<String, String> = options
            .iter()
            .filter(|(k, _)| !s3_constants::S3_OPTS.contains(&k.as_str()))
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
        let s3_pool_idle_timeout =
            Self::u64_or_default(options, constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS, 15);
        let sts_pool_idle_timeout =
            Self::u64_or_default(options, constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS, 10);

        let s3_get_internal_server_error_retries = Self::u64_or_default(
            options,
            constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
            10,
        ) as usize;

        let virtual_hosted_style_request: bool =
            str_option(options, s3_constants::AWS_S3_ADDRESSING_STYLE)
                .map(|addressing_style| addressing_style == "virtual")
                .unwrap_or(false);

        let allow_unsafe_rename = str_option(options, constants::AWS_S3_ALLOW_UNSAFE_RENAME)
            .map(|val| str_is_truthy(&val))
            .unwrap_or(false);

        let storage_options = StorageOptions(options.clone());
        let sdk_config =
            execute_sdk_future(crate::credentials::resolve_credentials(storage_options))??;

        Ok(Self {
            virtual_hosted_style_request,
            locking_provider: str_option(options, constants::AWS_S3_LOCKING_PROVIDER),
            dynamodb_endpoint: str_option(options, constants::AWS_ENDPOINT_URL_DYNAMODB),
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
        self.sdk_config.endpoint_url()
    }

    /// Return the configured region used for S3 operations
    pub fn region(&self) -> Option<&Region> {
        self.sdk_config.region()
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
    /// Options are described in [s3_constants].
    pub fn try_new(storage: ObjectStoreRef, allow_unsafe_rename: bool) -> ObjectStoreResult<Self> {
        Ok(Self {
            inner: storage,
            allow_unsafe_rename,
        })
    }
}

impl std::fmt::Debug for S3StorageBackend {
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

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
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

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
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

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}

/// Storage option keys to use when creating [crate::storage::s3::S3StorageOptions].
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::constants;
    use maplit::hashmap;
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

        pub async fn run_async<F>(future: F) -> F::Output
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static,
        {
            let _env_scope = Self::new();
            future.await
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
                constants::AWS_S3_ASSUME_ROLE_ARN,
                "arn:aws:iam::123456789012:role/some_role",
            );
            std::env::set_var(constants::AWS_S3_ROLE_SESSION_NAME, "session_name");
            std::env::set_var(constants::AWS_WEB_IDENTITY_TOKEN_FILE, "token_file");

            let options = S3StorageOptions::try_default().unwrap();
            assert_eq!(
                S3StorageOptions {
                    sdk_config: SdkConfig::builder()
                        .endpoint_url("http://localhost".to_string())
                        .region(Region::from_static("us-west-1"))
                        .build(),
                    virtual_hosted_style_request: false,
                    locking_provider: Some("dynamodb".to_string()),
                    dynamodb_endpoint: None,
                    s3_pool_idle_timeout: Duration::from_secs(15),
                    sts_pool_idle_timeout: Duration::from_secs(10),
                    s3_get_internal_server_error_retries: 10,
                    extra_opts: HashMap::new(),
                    allow_unsafe_rename: false,
                },
                options
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_with_only_region_and_credentials() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            std::env::remove_var(s3_constants::AWS_ENDPOINT_URL);
            let options = S3StorageOptions::from_map(&hashmap! {
                s3_constants::AWS_REGION.to_string() => "eu-west-1".to_string(),
                s3_constants::AWS_ACCESS_KEY_ID.to_string() => "test".to_string(),
                s3_constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
            })
            .unwrap();

            let mut expected = S3StorageOptions::try_default().unwrap();
            expected.sdk_config = SdkConfig::builder()
                .region(Region::from_static("eu-west-1"))
                .build();
            assert_eq!(expected, options);
        });
    }

    #[test]
    #[serial]
    fn storage_options_from_map_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let options = S3StorageOptions::from_map(&hashmap! {
                constants::AWS_ENDPOINT_URL.to_string() => "http://localhost:1234".to_string(),
                constants::AWS_REGION.to_string() => "us-west-2".to_string(),
                constants::AWS_PROFILE.to_string() => "default".to_string(),
                constants::AWS_S3_ADDRESSING_STYLE.to_string() => "virtual".to_string(),
                constants::AWS_S3_LOCKING_PROVIDER.to_string() => "another_locking_provider".to_string(),
                constants::AWS_S3_ASSUME_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/another_role".to_string(),
                constants::AWS_S3_ROLE_SESSION_NAME.to_string() => "another_session_name".to_string(),
                constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "another_token_file".to_string(),
                constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "1".to_string(),
                constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "2".to_string(),
                constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES.to_string() => "3".to_string(),
                constants::AWS_ACCESS_KEY_ID.to_string() => "test_id".to_string(),
                constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
            }).unwrap();

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
                hashmap! {
                    constants::AWS_S3_ADDRESSING_STYLE.to_string() => "virtual".to_string()
                },
                options.extra_opts
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_from_map_with_dynamodb_endpoint_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let options = S3StorageOptions::from_map(&hashmap! {
                constants::AWS_ENDPOINT_URL.to_string() => "http://localhost:1234".to_string(),
                constants::AWS_ENDPOINT_URL_DYNAMODB.to_string() => "http://localhost:2345".to_string(),
                constants::AWS_REGION.to_string() => "us-west-2".to_string(),
                constants::AWS_PROFILE.to_string() => "default".to_string(),
                constants::AWS_S3_ADDRESSING_STYLE.to_string() => "virtual".to_string(),
                constants::AWS_S3_LOCKING_PROVIDER.to_string() => "another_locking_provider".to_string(),
                constants::AWS_S3_ASSUME_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/another_role".to_string(),
                constants::AWS_S3_ROLE_SESSION_NAME.to_string() => "another_session_name".to_string(),
                constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "another_token_file".to_string(),
                constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "1".to_string(),
                constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "2".to_string(),
                constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES.to_string() => "3".to_string(),
                constants::AWS_ACCESS_KEY_ID.to_string() => "test_id".to_string(),
                constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
            }).unwrap();

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
                constants::AWS_S3_ASSUME_ROLE_ARN,
                "arn:aws:iam::123456789012:role/some_role",
            );
            std::env::set_var(constants::AWS_S3_ROLE_SESSION_NAME, "session_name");
            std::env::set_var(constants::AWS_WEB_IDENTITY_TOKEN_FILE, "token_file");

            std::env::set_var(constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS, "1");
            std::env::set_var(constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS, "2");
            std::env::set_var(constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES, "3");
            let options = S3StorageOptions::from_map(&hashmap! {
                constants::AWS_ACCESS_KEY_ID.to_string() => "test_id_mixed".to_string(),
                constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret_mixed".to_string(),
                constants::AWS_REGION.to_string() => "us-west-2".to_string(),
                "AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES".to_string() => "3".to_string(),
            })
            .unwrap();

            assert_eq!(
                S3StorageOptions {
                    sdk_config: SdkConfig::builder()
                        .endpoint_url("http://localhost".to_string())
                        .region(Region::from_static("us-west-2"))
                        .build(),
                    virtual_hosted_style_request: false,
                    locking_provider: Some("dynamodb".to_string()),
                    dynamodb_endpoint: Some("http://localhost:dynamodb".to_string()),
                    s3_pool_idle_timeout: Duration::from_secs(1),
                    sts_pool_idle_timeout: Duration::from_secs(2),
                    s3_get_internal_server_error_retries: 3,
                    extra_opts: hashmap! {},
                    allow_unsafe_rename: false,
                },
                options
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_web_identity_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let _options = S3StorageOptions::from_map(&hashmap! {
                constants::AWS_REGION.to_string() => "eu-west-1".to_string(),
                constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "web_identity_token_file".to_string(),
                constants::AWS_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/web_identity_role".to_string(),
                constants::AWS_ROLE_SESSION_NAME.to_string() => "web_identity_session_name".to_string(),
            }).unwrap();

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
            let raw_options = hashmap! {};

            std::env::set_var(constants::AWS_ACCESS_KEY_ID, "env_key");
            std::env::set_var(constants::AWS_ENDPOINT_URL, "env_key");
            std::env::set_var(constants::AWS_SECRET_ACCESS_KEY, "env_key");
            std::env::set_var(constants::AWS_REGION, "env_key");

            let combined_options =
                S3ObjectStoreFactory {}.with_env_s3(&StorageOptions(raw_options));

            assert_eq!(combined_options.0.len(), 4);

            for v in combined_options.0.values() {
                assert_eq!(v, "env_key");
            }
        });
    }

    #[tokio::test]
    #[serial]
    async fn when_merging_with_env_supplied_options_take_precedence() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let raw_options = hashmap! {
                "AWS_ACCESS_KEY_ID".to_string() => "options_key".to_string(),
                "AWS_ENDPOINT_URL".to_string() => "options_key".to_string(),
                "AWS_SECRET_ACCESS_KEY".to_string() => "options_key".to_string(),
                "AWS_REGION".to_string() => "options_key".to_string()
            };

            std::env::set_var("aws_access_key_id", "env_key");
            std::env::set_var("aws_endpoint", "env_key");
            std::env::set_var("aws_secret_access_key", "env_key");
            std::env::set_var("aws_region", "env_key");

            let combined_options =
                S3ObjectStoreFactory {}.with_env_s3(&StorageOptions(raw_options));

            for v in combined_options.0.values() {
                assert_eq!(v, "options_key");
            }
        });
    }
}
