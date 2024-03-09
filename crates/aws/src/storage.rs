//! AWS S3 storage backend.

use aws_config::{Region, SdkConfig};
use bytes::Bytes;
use deltalake_core::storage::object_store::{
    aws::AmazonS3ConfigKey, parse_url_opts, GetOptions, GetResult, ListResult, MultipartId,
    ObjectMeta, ObjectStore, PutOptions, PutResult, Result as ObjectStoreResult,
};
use deltalake_core::storage::{str_is_truthy, ObjectStoreFactory, ObjectStoreRef, StorageOptions};
use deltalake_core::{DeltaResult, ObjectStoreError, Path};
use futures::stream::BoxStream;
use futures::Future;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;
use url::Url;

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
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let options = self.with_env_s3(options);
        let (store, prefix) = parse_url_opts(
            url,
            options.0.iter().filter_map(|(key, value)| {
                let s3_key = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((s3_key, value.clone()))
            }),
        )?;

        if options
            .0
            .contains_key(AmazonS3ConfigKey::CopyIfNotExists.as_ref())
        {
            // If the copy-if-not-exists env var is set, we don't need to instantiate a locking client or check for allow-unsafe-rename.
            return Ok((Arc::from(store), prefix));
        }

        let options = S3StorageOptions::from_map(&options.0)?;

        let store = S3StorageBackend::try_new(
            store.into(),
            Some("dynamodb") == options.locking_provider.as_deref() || options.allow_unsafe_rename,
        )?;

        Ok((Arc::new(store), prefix))
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
        let extra_opts = options
            .iter()
            .filter(|(k, _)| !s3_constants::S3_OPTS.contains(&k.as_str()))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

        // Copy web identity values provided in options but not the environment into the environment
        // to get picked up by the `from_k8s_env` call in `get_web_identity_provider`.
        Self::ensure_env_var(options, s3_constants::AWS_REGION);
        Self::ensure_env_var(options, s3_constants::AWS_PROFILE);
        Self::ensure_env_var(options, s3_constants::AWS_ACCESS_KEY_ID);
        Self::ensure_env_var(options, s3_constants::AWS_SECRET_ACCESS_KEY);
        Self::ensure_env_var(options, s3_constants::AWS_SESSION_TOKEN);
        Self::ensure_env_var(options, s3_constants::AWS_WEB_IDENTITY_TOKEN_FILE);
        Self::ensure_env_var(options, s3_constants::AWS_ROLE_ARN);
        Self::ensure_env_var(options, s3_constants::AWS_ROLE_SESSION_NAME);
        let s3_pool_idle_timeout =
            Self::u64_or_default(options, s3_constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS, 15);
        let sts_pool_idle_timeout =
            Self::u64_or_default(options, s3_constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS, 10);

        let s3_get_internal_server_error_retries = Self::u64_or_default(
            options,
            s3_constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
            10,
        ) as usize;

        let virtual_hosted_style_request: bool =
            str_option(options, s3_constants::AWS_S3_ADDRESSING_STYLE)
                .map(|addressing_style| addressing_style == "virtual")
                .unwrap_or(false);

        let allow_unsafe_rename = str_option(options, s3_constants::AWS_S3_ALLOW_UNSAFE_RENAME)
            .map(|val| str_is_truthy(&val))
            .unwrap_or(false);

        #[cfg(feature = "native-tls")]
        let sdk_config = execute_sdk_future(
            aws_config::ConfigLoader::default()
                .http_client(native::use_native_tls_client(
                    str_option(options, s3_constants::AWS_ALLOW_HTTP)
                        .map(|val| str_is_truthy(&val))
                        .unwrap_or(false),
                ))
                .load(),
        )?;
        #[cfg(feature = "rustls")]
        let sdk_config = execute_sdk_future(aws_config::load_from_env())?;

        let sdk_config =
            if let Some(endpoint_url) = str_option(options, s3_constants::AWS_ENDPOINT_URL) {
                sdk_config.to_builder().endpoint_url(endpoint_url).build()
            } else {
                sdk_config
            };
        Ok(Self {
            virtual_hosted_style_request,
            locking_provider: str_option(options, s3_constants::AWS_S3_LOCKING_PROVIDER),
            s3_pool_idle_timeout: Duration::from_secs(s3_pool_idle_timeout),
            sts_pool_idle_timeout: Duration::from_secs(sts_pool_idle_timeout),
            s3_get_internal_server_error_retries,
            allow_unsafe_rename,
            extra_opts,
            sdk_config,
        })
    }

    pub fn endpoint_url(&self) -> Option<&str> {
        self.sdk_config.endpoint_url()
    }

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
            std::env::set_var(key, val);
        }
    }

    pub fn try_default() -> DeltaResult<Self> {
        Self::from_map(&HashMap::new())
    }
}

fn execute_sdk_future<F: Future<Output = SdkConfig> + Send + 'static>(
    future: F,
) -> DeltaResult<SdkConfig> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                Ok(tokio::task::block_in_place(move || handle.block_on(future)))
            }
            _ => {
                let mut cfg: Option<SdkConfig> = None;
                std::thread::scope(|scope| {
                    scope.spawn(|| {
                        cfg = Some(handle.block_on(future));
                    });
                });
                cfg.ok_or(deltalake_core::DeltaTableError::ObjectStore {
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
        write!(f, "S3StorageBackend")
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
        write!(fmt, "S3StorageBackend")
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3StorageBackend {
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: Bytes,
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

/// Storage option keys to use when creating [crate::storage::s3::S3StorageOptions].
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
/// Provided keys may include configuration for the S3 backend and also the optional DynamoDb lock used for atomic rename.
pub mod s3_constants {
    /// Custom S3 endpoint.
    pub const AWS_ENDPOINT_URL: &str = "AWS_ENDPOINT_URL";
    /// The AWS region.
    pub const AWS_REGION: &str = "AWS_REGION";
    /// The AWS profile.
    pub const AWS_PROFILE: &str = "AWS_PROFILE";
    /// The AWS_ACCESS_KEY_ID to use for S3.
    pub const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
    /// The AWS_SECRET_ACCESS_KEY to use for S3.
    pub const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
    /// The AWS_SESSION_TOKEN to use for S3.
    pub const AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
    /// Uses either "path" (the default) or "virtual", which turns on
    /// [virtual host addressing](http://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html).
    pub const AWS_S3_ADDRESSING_STYLE: &str = "AWS_S3_ADDRESSING_STYLE";
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
    /// The `pool_idle_timeout` for the as3_constants sts client. See
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
    /// Allow http connections - mainly useful for integration tests
    pub const AWS_ALLOW_HTTP: &str = "AWS_ALLOW_HTTP";

    /// If set to "true", allows creating commits without concurrent writer protection.
    /// Only safe if there is one writer to a given table.
    pub const AWS_S3_ALLOW_UNSAFE_RENAME: &str = "AWS_S3_ALLOW_UNSAFE_RENAME";

    /// The list of option keys owned by the S3 module.
    /// Option keys not contained in this list will be added to the `extra_opts`
    /// field of [crate::storage::s3::S3StorageOptions].
    pub const S3_OPTS: &[&str] = &[
        AWS_ENDPOINT_URL,
        AWS_REGION,
        AWS_PROFILE,
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

pub(crate) fn str_option(map: &HashMap<String, String>, key: &str) -> Option<String> {
    map.get(key)
        .map_or_else(|| std::env::var(key).ok(), |v| Some(v.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    impl Drop for ScopedEnv {
        fn drop(&mut self) {
            let to_remove: Vec<_> = std::env::vars_os()
                .map(|kv| kv.0)
                .filter(|k| !self.vars.contains_key(k))
                .collect();
            for k in to_remove {
                std::env::remove_var(k);
            }
            for (key, value) in self.vars.drain() {
                std::env::set_var(key, value);
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
            std::env::remove_var(k);
        }
    }

    #[test]
    #[serial]
    fn storage_options_default_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();

            std::env::set_var(s3_constants::AWS_ENDPOINT_URL, "http://localhost");
            std::env::set_var(s3_constants::AWS_REGION, "us-west-1");
            std::env::set_var(s3_constants::AWS_PROFILE, "default");
            std::env::set_var(s3_constants::AWS_ACCESS_KEY_ID, "default_key_id");
            std::env::set_var(s3_constants::AWS_SECRET_ACCESS_KEY, "default_secret_key");
            std::env::set_var(s3_constants::AWS_S3_LOCKING_PROVIDER, "dynamodb");
            std::env::set_var(
                s3_constants::AWS_S3_ASSUME_ROLE_ARN,
                "arn:aws:iam::123456789012:role/some_role",
            );
            std::env::set_var(s3_constants::AWS_S3_ROLE_SESSION_NAME, "session_name");
            std::env::set_var(s3_constants::AWS_WEB_IDENTITY_TOKEN_FILE, "token_file");

            let options = S3StorageOptions::try_default().unwrap();
            assert_eq!(
                S3StorageOptions {
                    sdk_config: SdkConfig::builder()
                        .endpoint_url("http://localhost".to_string())
                        .region(Region::from_static("us-west-1"))
                        .build(),
                    virtual_hosted_style_request: false,
                    locking_provider: Some("dynamodb".to_string()),
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
                s3_constants::AWS_ENDPOINT_URL.to_string() => "http://localhost:1234".to_string(),
                s3_constants::AWS_REGION.to_string() => "us-west-2".to_string(),
                s3_constants::AWS_PROFILE.to_string() => "default".to_string(),
                s3_constants::AWS_S3_ADDRESSING_STYLE.to_string() => "virtual".to_string(),
                s3_constants::AWS_S3_LOCKING_PROVIDER.to_string() => "another_locking_provider".to_string(),
                s3_constants::AWS_S3_ASSUME_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/another_role".to_string(),
                s3_constants::AWS_S3_ROLE_SESSION_NAME.to_string() => "another_session_name".to_string(),
                s3_constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "another_token_file".to_string(),
                s3_constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "1".to_string(),
                s3_constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "2".to_string(),
                s3_constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES.to_string() => "3".to_string(),
                s3_constants::AWS_ACCESS_KEY_ID.to_string() => "test_id".to_string(),
                s3_constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
            }).unwrap();

            assert_eq!(
                S3StorageOptions {
                    sdk_config: SdkConfig::builder()
                        .endpoint_url("http://localhost:1234".to_string())
                        .region(Region::from_static("us-west-2"))
                        .build(),
                    virtual_hosted_style_request: true,
                    locking_provider: Some("another_locking_provider".to_string()),
                    s3_pool_idle_timeout: Duration::from_secs(1),
                    sts_pool_idle_timeout: Duration::from_secs(2),
                    s3_get_internal_server_error_retries: 3,
                    extra_opts: hashmap! {
                        s3_constants::AWS_S3_ADDRESSING_STYLE.to_string() => "virtual".to_string()
                    },
                    allow_unsafe_rename: false,
                },
                options
            );
        });
    }

    #[test]
    #[serial]
    fn storage_options_mixed_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            std::env::set_var(s3_constants::AWS_ENDPOINT_URL, "http://localhost");
            std::env::set_var(s3_constants::AWS_REGION, "us-west-1");
            std::env::set_var(s3_constants::AWS_PROFILE, "default");
            std::env::set_var(s3_constants::AWS_ACCESS_KEY_ID, "wrong_key_id");
            std::env::set_var(s3_constants::AWS_SECRET_ACCESS_KEY, "wrong_secret_key");
            std::env::set_var(s3_constants::AWS_S3_LOCKING_PROVIDER, "dynamodb");
            std::env::set_var(
                s3_constants::AWS_S3_ASSUME_ROLE_ARN,
                "arn:aws:iam::123456789012:role/some_role",
            );
            std::env::set_var(s3_constants::AWS_S3_ROLE_SESSION_NAME, "session_name");
            std::env::set_var(s3_constants::AWS_WEB_IDENTITY_TOKEN_FILE, "token_file");

            std::env::set_var(s3_constants::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS, "1");
            std::env::set_var(s3_constants::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS, "2");
            std::env::set_var(s3_constants::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES, "3");
            let options = S3StorageOptions::from_map(&hashmap! {
                s3_constants::AWS_ACCESS_KEY_ID.to_string() => "test_id_mixed".to_string(),
                s3_constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret_mixed".to_string(),
                s3_constants::AWS_REGION.to_string() => "us-west-2".to_string(),
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
                s3_constants::AWS_REGION.to_string() => "eu-west-1".to_string(),
                s3_constants::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "web_identity_token_file".to_string(),
                s3_constants::AWS_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/web_identity_role".to_string(),
                s3_constants::AWS_ROLE_SESSION_NAME.to_string() => "web_identity_session_name".to_string(),
            }).unwrap();

            assert_eq!(
                "eu-west-1",
                std::env::var(s3_constants::AWS_REGION).unwrap()
            );

            assert_eq!(
                "web_identity_token_file",
                std::env::var(s3_constants::AWS_WEB_IDENTITY_TOKEN_FILE).unwrap()
            );

            assert_eq!(
                "arn:aws:iam::123456789012:role/web_identity_role",
                std::env::var(s3_constants::AWS_ROLE_ARN).unwrap()
            );

            assert_eq!(
                "web_identity_session_name",
                std::env::var(s3_constants::AWS_ROLE_SESSION_NAME).unwrap()
            );
        });
    }

    #[test]
    #[serial]
    fn when_merging_with_env_unsupplied_options_are_added() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let raw_options = hashmap! {};

            std::env::set_var(s3_constants::AWS_ACCESS_KEY_ID, "env_key");
            std::env::set_var(s3_constants::AWS_ENDPOINT_URL, "env_key");
            std::env::set_var(s3_constants::AWS_SECRET_ACCESS_KEY, "env_key");
            std::env::set_var(s3_constants::AWS_REGION, "env_key");

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
