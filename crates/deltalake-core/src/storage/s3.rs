//! AWS S3 storage backend.

use super::utils::str_is_truthy;
use crate::table::builder::{s3_storage_options, str_option};
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{path::Path, Error as ObjectStoreError};
use object_store::{
    DynObjectStore, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};
use rusoto_core::Region;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;

const STORE_NAME: &str = "DeltaS3ObjectStore";

/// Options used to configure the S3StorageBackend.
///
/// Available options are described in [s3_storage_options].
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct S3StorageOptions {
    pub endpoint_url: Option<String>,
    pub region: Region,
    pub profile: Option<String>,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub virtual_hosted_style_request: bool,
    pub locking_provider: Option<String>,
    pub assume_role_arn: Option<String>,
    pub assume_role_session_name: Option<String>,
    pub use_web_identity: bool,
    pub s3_pool_idle_timeout: Duration,
    pub sts_pool_idle_timeout: Duration,
    pub s3_get_internal_server_error_retries: usize,
    pub allow_unsafe_rename: bool,
    pub extra_opts: HashMap<String, String>,
}

impl S3StorageOptions {
    /// Creates an instance of S3StorageOptions from the given HashMap.
    pub fn from_map(options: &HashMap<String, String>) -> S3StorageOptions {
        let extra_opts = options
            .iter()
            .filter(|(k, _)| !s3_storage_options::S3_OPTS.contains(&k.as_str()))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

        // Copy web identity values provided in options but not the environment into the environment
        // to get picked up by the `from_k8s_env` call in `get_web_identity_provider`.
        Self::ensure_env_var(options, s3_storage_options::AWS_REGION);
        Self::ensure_env_var(options, s3_storage_options::AWS_PROFILE);
        Self::ensure_env_var(options, s3_storage_options::AWS_ACCESS_KEY_ID);
        Self::ensure_env_var(options, s3_storage_options::AWS_SECRET_ACCESS_KEY);
        Self::ensure_env_var(options, s3_storage_options::AWS_SESSION_TOKEN);
        Self::ensure_env_var(options, s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE);
        Self::ensure_env_var(options, s3_storage_options::AWS_ROLE_ARN);
        Self::ensure_env_var(options, s3_storage_options::AWS_ROLE_SESSION_NAME);

        let endpoint_url = str_option(options, s3_storage_options::AWS_ENDPOINT_URL);
        let region = if let Some(endpoint_url) = endpoint_url.as_ref() {
            Region::Custom {
                name: Self::str_or_default(
                    options,
                    s3_storage_options::AWS_REGION,
                    "custom".to_string(),
                ),
                endpoint: endpoint_url.to_owned(),
            }
        } else {
            Region::default()
        };
        let profile = str_option(options, s3_storage_options::AWS_PROFILE);

        let s3_pool_idle_timeout = Self::u64_or_default(
            options,
            s3_storage_options::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS,
            15,
        );
        let sts_pool_idle_timeout = Self::u64_or_default(
            options,
            s3_storage_options::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS,
            10,
        );

        let s3_get_internal_server_error_retries = Self::u64_or_default(
            options,
            s3_storage_options::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
            10,
        ) as usize;

        let virtual_hosted_style_request: bool =
            str_option(options, s3_storage_options::AWS_S3_ADDRESSING_STYLE)
                .map(|addressing_style| addressing_style == "virtual")
                .unwrap_or(false);

        let allow_unsafe_rename =
            str_option(options, s3_storage_options::AWS_S3_ALLOW_UNSAFE_RENAME)
                .map(|val| str_is_truthy(&val))
                .unwrap_or(false);

        Self {
            endpoint_url,
            region,
            profile,
            aws_access_key_id: str_option(options, s3_storage_options::AWS_ACCESS_KEY_ID),
            aws_secret_access_key: str_option(options, s3_storage_options::AWS_SECRET_ACCESS_KEY),
            aws_session_token: str_option(options, s3_storage_options::AWS_SESSION_TOKEN),
            virtual_hosted_style_request,
            locking_provider: str_option(options, s3_storage_options::AWS_S3_LOCKING_PROVIDER),
            assume_role_arn: str_option(options, s3_storage_options::AWS_S3_ASSUME_ROLE_ARN),
            assume_role_session_name: str_option(
                options,
                s3_storage_options::AWS_S3_ROLE_SESSION_NAME,
            ),
            use_web_identity: std::env::var(s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE)
                .is_ok(),
            s3_pool_idle_timeout: Duration::from_secs(s3_pool_idle_timeout),
            sts_pool_idle_timeout: Duration::from_secs(sts_pool_idle_timeout),
            s3_get_internal_server_error_retries,
            allow_unsafe_rename,
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
        Self::from_map(&HashMap::new())
    }
}

/// An S3 implementation of the [ObjectStore] trait
pub struct S3StorageBackend {
    inner: Arc<DynObjectStore>,
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
    /// Options are described in [s3_storage_options].
    ///
    /// ```rust
    /// use object_store::aws::AmazonS3Builder;
    /// use deltalake_core::storage::s3::{S3StorageBackend, S3StorageOptions};
    /// use std::sync::Arc;
    ///
    /// let inner = AmazonS3Builder::new()
    ///     .with_region("us-east-1")
    ///     .with_bucket_name("my-bucket")
    ///     .with_access_key_id("<access-key-id>")
    ///     .with_secret_access_key("<secret-access-key>")
    ///     .build()
    ///     .unwrap();
    /// let allow_unsafe_rename = true;
    /// let store = S3StorageBackend::try_new(Arc::new(inner), allow_unsafe_rename).unwrap();
    /// ```
    pub fn try_new(
        storage: Arc<DynObjectStore>,
        allow_unsafe_rename: bool,
    ) -> ObjectStoreResult<Self> {
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
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        self.inner.put(location, bytes).await
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

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.inner.list_with_offset(prefix, offset).await
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
                source: Box::new(deltalake_aws::errors::LockClientError::LockClientRequired),
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
        std::env::set_var(s3_storage_options::AWS_PROFILE, "default");
        std::env::set_var(s3_storage_options::AWS_ACCESS_KEY_ID, "default_key_id");
        std::env::set_var(
            s3_storage_options::AWS_SECRET_ACCESS_KEY,
            "default_secret_key",
        );
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
                endpoint_url: Some("http://localhost".to_string()),
                region: Region::Custom {
                    name: "us-west-1".to_string(),
                    endpoint: "http://localhost".to_string()
                },
                profile: Some("default".to_string()),
                aws_access_key_id: Some("default_key_id".to_string()),
                aws_secret_access_key: Some("default_secret_key".to_string()),
                aws_session_token: None,
                virtual_hosted_style_request: false,
                assume_role_arn: Some("arn:aws:iam::123456789012:role/some_role".to_string()),
                assume_role_session_name: Some("session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("dynamodb".to_string()),
                s3_pool_idle_timeout: Duration::from_secs(15),
                sts_pool_idle_timeout: Duration::from_secs(10),
                s3_get_internal_server_error_retries: 10,
                extra_opts: HashMap::new(),
                allow_unsafe_rename: false,
            },
            options
        );
    }

    #[test]
    #[serial]
    fn storage_options_with_only_region_and_credentials() {
        std::env::remove_var(s3_storage_options::AWS_ENDPOINT_URL);
        let options = S3StorageOptions::from_map(&hashmap! {
            s3_storage_options::AWS_REGION.to_string() => "eu-west-1".to_string(),
            s3_storage_options::AWS_ACCESS_KEY_ID.to_string() => "test".to_string(),
            s3_storage_options::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                endpoint_url: None,
                region: Region::default(),
                aws_access_key_id: Some("test".to_string()),
                aws_secret_access_key: Some("test_secret".to_string()),
                ..Default::default()
            },
            options
        );
    }

    #[test]
    #[serial]
    fn storage_options_from_map_test() {
        let options = S3StorageOptions::from_map(&hashmap! {
            s3_storage_options::AWS_ENDPOINT_URL.to_string() => "http://localhost:1234".to_string(),
            s3_storage_options::AWS_REGION.to_string() => "us-west-2".to_string(),
            s3_storage_options::AWS_PROFILE.to_string() => "default".to_string(),
            s3_storage_options::AWS_S3_ADDRESSING_STYLE.to_string() => "virtual".to_string(),
            s3_storage_options::AWS_S3_LOCKING_PROVIDER.to_string() => "another_locking_provider".to_string(),
            s3_storage_options::AWS_S3_ASSUME_ROLE_ARN.to_string() => "arn:aws:iam::123456789012:role/another_role".to_string(),
            s3_storage_options::AWS_S3_ROLE_SESSION_NAME.to_string() => "another_session_name".to_string(),
            s3_storage_options::AWS_WEB_IDENTITY_TOKEN_FILE.to_string() => "another_token_file".to_string(),
            s3_storage_options::AWS_S3_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "1".to_string(),
            s3_storage_options::AWS_STS_POOL_IDLE_TIMEOUT_SECONDS.to_string() => "2".to_string(),
            s3_storage_options::AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES.to_string() => "3".to_string(),
            s3_storage_options::AWS_ACCESS_KEY_ID.to_string() => "test_id".to_string(),
            s3_storage_options::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                endpoint_url: Some("http://localhost:1234".to_string()),
                region: Region::Custom {
                    name: "us-west-2".to_string(),
                    endpoint: "http://localhost:1234".to_string()
                },
                profile: Some("default".to_string()),
                aws_access_key_id: Some("test_id".to_string()),
                aws_secret_access_key: Some("test_secret".to_string()),
                aws_session_token: None,
                virtual_hosted_style_request: true,
                assume_role_arn: Some("arn:aws:iam::123456789012:role/another_role".to_string()),
                assume_role_session_name: Some("another_session_name".to_string()),
                use_web_identity: true,
                locking_provider: Some("another_locking_provider".to_string()),
                s3_pool_idle_timeout: Duration::from_secs(1),
                sts_pool_idle_timeout: Duration::from_secs(2),
                s3_get_internal_server_error_retries: 3,
                extra_opts: hashmap! {
                    s3_storage_options::AWS_S3_ADDRESSING_STYLE.to_string() => "virtual".to_string()
                },
                allow_unsafe_rename: false,
            },
            options
        );
    }

    #[test]
    #[serial]
    fn storage_options_mixed_test() {
        std::env::set_var(s3_storage_options::AWS_ENDPOINT_URL, "http://localhost");
        std::env::set_var(s3_storage_options::AWS_REGION, "us-west-1");
        std::env::set_var(s3_storage_options::AWS_PROFILE, "default");
        std::env::set_var(s3_storage_options::AWS_ACCESS_KEY_ID, "wrong_key_id");
        std::env::set_var(
            s3_storage_options::AWS_SECRET_ACCESS_KEY,
            "wrong_secret_key",
        );
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
        let options = S3StorageOptions::from_map(&hashmap! {
            s3_storage_options::AWS_ACCESS_KEY_ID.to_string() => "test_id_mixed".to_string(),
            s3_storage_options::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret_mixed".to_string(),
            s3_storage_options::AWS_REGION.to_string() => "us-west-2".to_string(),
            "DYNAMO_LOCK_PARTITION_KEY_VALUE".to_string() => "my_lock".to_string(),
            "AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES".to_string() => "3".to_string(),
        });

        assert_eq!(
            S3StorageOptions {
                endpoint_url: Some("http://localhost".to_string()),
                region: Region::Custom {
                    name: "us-west-2".to_string(),
                    endpoint: "http://localhost".to_string()
                },
                profile: Some("default".to_string()),
                aws_access_key_id: Some("test_id_mixed".to_string()),
                aws_secret_access_key: Some("test_secret_mixed".to_string()),
                aws_session_token: None,
                virtual_hosted_style_request: false,
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
                allow_unsafe_rename: false,
            },
            options
        );
    }
    #[test]
    #[serial]
    fn storage_options_web_identity_test() {
        let _options = S3StorageOptions::from_map(&hashmap! {
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
