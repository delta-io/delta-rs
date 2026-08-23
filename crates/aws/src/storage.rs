//! AWS S3 storage backend.
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::logstore::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use deltalake_core::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreScheme, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult, path::Path,
};
use deltalake_core::logstore::{
    ObjectStoreFactory, ObjectStoreRef, StorageConfig, client_options_from_certificate,
    config::str_is_truthy,
};
use deltalake_core::{DeltaResult, DeltaTableError};
use futures::Future;
use futures::stream::BoxStream;
use object_store::aws::AmazonS3;
use object_store::client::SpawnedReqwestConnector;
use tracing::log::*;
use typed_builder::TypedBuilder;
use url::Url;

use crate::constants;
use crate::credentials::AWSForObjectStore;

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

        if let Some(ref cert_config) = config.certificate
            && let Some(ref path) = cert_config.certificate_path
        {
            builder = builder.with_client_options(client_options_from_certificate(path)?);
        }

        for (key, value) in options.iter() {
            if let Ok(key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                builder = builder.with_config(key, value.clone());
            }
        }

        let s3_options = S3StorageOptions::from_map(&options)?;
        if is_aws(&options) {
            debug!("Detected AWS S3 Storage options, resolving AWS credentials");

            let sdk_config =
                execute_sdk_future(crate::credentials::resolve_credentials(&options))??;

            builder = builder.with_credentials(Arc::new(AWSForObjectStore::new(sdk_config)));
        };

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
#[derive(Clone, Debug, TypedBuilder, PartialEq)]
#[builder(doc)]
pub struct S3StorageOptions {
    /// Locking provider to use (e.g., "dynamodb")
    #[builder(default, setter(strip_option, into))]
    pub locking_provider: Option<String>,
    /// Allow unsafe rename operations
    #[builder(default = false)]
    pub allow_unsafe_rename: bool,
}

impl S3StorageOptions {
    /// Creates an instance of [`S3StorageOptions`] from the given HashMap.
    pub fn from_map(options: &HashMap<String, String>) -> DeltaResult<S3StorageOptions> {
        Ok(Self {
            locking_provider: str_option(options, constants::AWS_S3_LOCKING_PROVIDER),
            allow_unsafe_rename: str_option(options, constants::AWS_S3_ALLOW_UNSAFE_RENAME)
                .map(|val| str_is_truthy(&val))
                .unwrap_or(false),
        })
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
                cfg.ok_or(DeltaTableError::generic(
                    "Failed to run some aws-sdk configuration",
                ))
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
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
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

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
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
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str())
                && let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
            {
                options
                    .entry(config_key.as_ref().to_string())
                    .or_insert(value.to_string());
            }
        }

        // With object_store 0.13.0 conditional put is supported almost everywhere. The
        // copy_if_not_exists behavior needs to be explicitly specifedj for AWS  S3 however.
        //
        // Users of other stores should define their copy_if_not_exists configuration as needed
        if !options.keys().any(|key| {
            let key = key.to_ascii_lowercase();
            [
                AmazonS3ConfigKey::CopyIfNotExists.as_ref(),
                "copy_if_not_exists",
            ]
            .contains(&key.as_str())
        }) {
            options.insert("copy_if_not_exists".into(), "multipart".into());
        }
        options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::constants;
    use deltalake_core::ObjectStoreError;
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;
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

            unsafe {
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
            }

            let options = S3StorageOptions::try_default().unwrap();
            assert_eq!(
                S3StorageOptions::builder()
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
            unsafe {
                std::env::remove_var(constants::AWS_ENDPOINT_URL);
            }

            let options = S3StorageOptions::from_map(&HashMap::from([
                (constants::AWS_REGION.to_string(), "eu-west-1".to_string()),
                (constants::AWS_ACCESS_KEY_ID.to_string(), "test".to_string()),
                (
                    constants::AWS_SECRET_ACCESS_KEY.to_string(),
                    "test_secret".to_string(),
                ),
            ]))
            .unwrap();

            let expected = S3StorageOptions::try_default().unwrap();
            assert_eq!(expected, options);
        });
    }

    #[test]
    #[serial]
    fn storage_options_from_map_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let options = S3StorageOptions::from_map(&HashMap::from([(
                constants::AWS_S3_LOCKING_PROVIDER.to_string(),
                "another_locking_provider".to_string(),
            )]))
            .unwrap();

            assert_eq!(
                Some("another_locking_provider"),
                options.locking_provider.as_deref()
            );
            assert!(!options.allow_unsafe_rename);
        });
    }

    #[test]
    #[serial]
    fn storage_options_mixed_test() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            unsafe {
                std::env::set_var(constants::AWS_S3_LOCKING_PROVIDER, "dynamodb");
            }
            let options = S3StorageOptions::from_map(&HashMap::from([(
                constants::AWS_S3_ALLOW_UNSAFE_RENAME.to_string(),
                "false".to_string(),
            )]))
            .unwrap();

            assert_eq!(
                S3StorageOptions::builder()
                    .locking_provider("dynamodb")
                    .allow_unsafe_rename(false)
                    .build(),
                options
            );
        });
    }

    #[tokio::test]
    async fn unsafe_rename_create_mode_does_not_overwrite_existing_destination() {
        let backend = S3StorageBackend::try_new(Arc::new(InMemory::new()), true).unwrap();
        let src = Path::from("src");
        let dst = Path::from("dst");

        backend
            .put(&src, Bytes::from_static(b"src").into())
            .await
            .unwrap();
        backend
            .put(&dst, Bytes::from_static(b"dst").into())
            .await
            .unwrap();

        let err = backend.rename_if_not_exists(&src, &dst).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::AlreadyExists { .. }));

        let dst_bytes = backend.get(&dst).await.unwrap().bytes().await.unwrap();
        assert_eq!(dst_bytes.as_ref(), b"dst");

        let src_bytes = backend.get(&src).await.unwrap().bytes().await.unwrap();
        assert_eq!(src_bytes.as_ref(), b"src");
    }

    #[test]
    #[serial]
    fn when_merging_with_env_unsupplied_options_are_added() {
        ScopedEnv::run(|| {
            clear_env_of_aws_keys();
            let raw_options = HashMap::new();
            unsafe {
                std::env::set_var(constants::AWS_ACCESS_KEY_ID, "env_key");
                std::env::set_var(constants::AWS_ENDPOINT_URL, "env_key");
                std::env::set_var(constants::AWS_SECRET_ACCESS_KEY, "env_key");
                std::env::set_var(constants::AWS_REGION, "env_key");
            }
            let combined_options = S3ObjectStoreFactory {}.with_env_s3(&raw_options);

            // Four and then the conditional_put built-in
            assert_eq!(combined_options.len(), 5);

            for (key, v) in combined_options {
                if key != "copy_if_not_exists" {
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
            unsafe {
                std::env::set_var("aws_access_key_id", "env_key");
                std::env::set_var("aws_endpoint", "env_key");
                std::env::set_var("aws_secret_access_key", "env_key");
                std::env::set_var("aws_region", "env_key");
            }

            let combined_options = S3ObjectStoreFactory {}.with_env_s3(&raw_options);

            for (key, v) in combined_options {
                if key != "copy_if_not_exists" {
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
