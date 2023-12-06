//! Create or load DeltaTables

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, Utc};
use object_store::DynObjectStore;
use serde::{Deserialize, Serialize};
use url::Url;

use super::DeltaTable;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::LogStoreRef;
use crate::storage::config::{self, StorageOptions};

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
enum BuilderError {
    #[error("Store {backend} requires host in storage url, got: {url}")]
    MissingHost { backend: String, url: String },
    #[error("Missing configuration {0}")]
    Required(String),
    #[error("Failed to find valid credential.")]
    MissingCredential,
    #[error("Failed to decode SAS key: {0}\nSAS keys must be percent-encoded. They come encoded in the Azure portal and Azure Storage Explorer.")]
    Decode(String),
    #[error("Delta-rs must be build with feature '{feature}' to support url: {url}.")]
    MissingFeature { feature: &'static str, url: String },
    #[error("Failed to parse table uri")]
    TableUri(#[from] url::ParseError),
}

impl From<BuilderError> for DeltaTableError {
    fn from(err: BuilderError) -> Self {
        DeltaTableError::Generic(err.to_string())
    }
}

/// possible version specifications for loading a delta table
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DeltaVersion {
    /// load the newest version
    #[default]
    Newest,
    /// specify the version to load
    Version(i64),
    /// specify the timestamp in UTC
    Timestamp(DateTime<Utc>),
}

/// Configuration options for delta table
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableConfig {
    /// Indicates whether our use case requires tracking tombstones.
    /// This defaults to `true`
    ///
    /// Read-only applications never require tombstones. Tombstones
    /// are only required when writing checkpoints, so even many writers
    /// may want to skip them.
    pub require_tombstones: bool,

    /// Indicates whether DeltaTable should track files.
    /// This defaults to `true`
    ///
    /// Some append-only applications might have no need of tracking any files.
    /// Hence, DeltaTable will be loaded with significant memory reduction.
    pub require_files: bool,
    /// Controls how many files to buffer from the commit log when updating the table.
    /// This defaults to 4 * number of cpus
    ///
    /// Setting a value greater than 1 results in concurrent calls to the storage api.
    /// This can decrease latency if there are many files in the log since the
    /// last checkpoint, but will also increase memory usage. Possible rate limits of the storage backend should
    /// also be considered for optimal performance.
    pub log_buffer_size: usize,
}

impl Default for DeltaTableConfig {
    fn default() -> Self {
        Self {
            require_tombstones: true,
            require_files: true,
            log_buffer_size: num_cpus::get() * 4,
        }
    }
}

/// Load-time delta table configuration options
#[derive(Debug)]
pub struct DeltaTableLoadOptions {
    /// table root uri
    pub table_uri: String,
    /// backend to access storage system
    pub storage_backend: Option<(Arc<DynObjectStore>, Url)>,
    /// specify the version we are going to load: a time stamp, a version, or just the newest
    /// available version
    pub version: DeltaVersion,
    /// Indicates whether our use case requires tracking tombstones.
    /// This defaults to `true`
    ///
    /// Read-only applications never require tombstones. Tombstones
    /// are only required when writing checkpoints, so even many writers
    /// may want to skip them.
    pub require_tombstones: bool,
    /// Indicates whether DeltaTable should track files.
    /// This defaults to `true`
    ///
    /// Some append-only applications might have no need of tracking any files.
    /// Hence, DeltaTable will be loaded with significant memory reduction.
    pub require_files: bool,
    /// Controls how many files to buffer from the commit log when updating the table.
    /// This defaults to 4 * number of cpus
    ///
    /// Setting a value greater than 1 results in concurrent calls to the storage api.
    /// This can be helpful to decrease latency if there are many files in the log since the
    /// last checkpoint, but will also increase memory usage. Possible rate limits of the storage backend should
    /// also be considered for optimal performance.
    pub log_buffer_size: usize,
}

impl DeltaTableLoadOptions {
    /// create default table load options for a table uri
    pub fn new(table_uri: impl Into<String>) -> Self {
        Self {
            table_uri: table_uri.into(),
            storage_backend: None,
            require_tombstones: true,
            require_files: true,
            log_buffer_size: num_cpus::get() * 4,
            version: DeltaVersion::default(),
        }
    }
}

enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// builder for configuring a delta table load.
#[derive(Debug)]
pub struct DeltaTableBuilder {
    options: DeltaTableLoadOptions,
    storage_options: Option<HashMap<String, String>>,
    #[allow(unused_variables)]
    allow_http: Option<bool>,
}

impl DeltaTableBuilder {
    /// Creates `DeltaTableBuilder` from table uri
    pub fn from_uri(table_uri: impl AsRef<str>) -> Self {
        Self {
            options: DeltaTableLoadOptions::new(table_uri.as_ref()),
            storage_options: None,
            allow_http: None,
        }
    }

    /// Creates `DeltaTableBuilder` from verified table uri.
    /// Will fail fast if specified `table_uri` is a local path but doesn't exist.
    pub fn from_valid_uri(table_uri: impl AsRef<str>) -> DeltaResult<Self> {
        let table_uri = table_uri.as_ref();

        if let UriType::LocalPath(path) = resolve_uri_type(table_uri)? {
            if !path.exists() {
                let msg = format!(
                    "Local path \"{}\" does not exist or you don't have access!",
                    table_uri
                );
                return Err(DeltaTableError::InvalidTableLocation(msg));
            }
        }

        Ok(DeltaTableBuilder::from_uri(table_uri))
    }
    /// Sets `require_tombstones=false` to the builder
    pub fn without_tombstones(mut self) -> Self {
        self.options.require_tombstones = false;
        self
    }

    /// Sets `require_files=false` to the builder
    pub fn without_files(mut self) -> Self {
        self.options.require_files = false;
        self
    }

    /// Sets `version` to the builder
    pub fn with_version(mut self, version: i64) -> Self {
        self.options.version = DeltaVersion::Version(version);
        self
    }

    /// Sets `log_buffer_size` to the builder
    pub fn with_log_buffer_size(mut self, log_buffer_size: usize) -> DeltaResult<Self> {
        if log_buffer_size == 0 {
            return Err(DeltaTableError::Generic(String::from(
                "Log buffer size should be positive",
            )));
        }
        self.options.log_buffer_size = log_buffer_size;
        Ok(self)
    }

    /// specify the timestamp given as ISO-8601/RFC-3339 timestamp
    pub fn with_datestring(self, date_string: impl AsRef<str>) -> DeltaResult<Self> {
        let datetime = DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(
            date_string.as_ref(),
        )?);
        Ok(self.with_timestamp(datetime))
    }

    /// specify a timestamp
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.options.version = DeltaVersion::Timestamp(timestamp);
        self
    }

    /// Set the storage backend.
    ///
    /// If a backend is not provided then it is derived from `table_uri`.
    ///
    /// # Arguments
    ///
    /// * `storage` - A shared reference to an [`ObjectStore`](object_store::ObjectStore) with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `location` - A url corresponding to the storagle location of `storage`.
    pub fn with_storage_backend(mut self, storage: Arc<DynObjectStore>, location: Url) -> Self {
        self.options.storage_backend = Some((storage, location));
        self
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables. See documentation of
    /// underlying object store implementation for details.
    ///
    /// - [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants)
    /// - [S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants)
    /// - [Google options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants)
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    /// Allows unsecure connections via http.
    ///
    /// This setting is most useful for testing / development when connecting to emulated services.
    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = Some(allow_http);
        self
    }

    /// Storage options for configuring backend object store
    pub fn storage_options(&self) -> StorageOptions {
        let mut storage_options = self.storage_options.clone().unwrap_or_default();
        if let Some(allow) = self.allow_http {
            storage_options.insert(
                "allow_http".into(),
                if allow { "true" } else { "false" }.into(),
            );
        };
        storage_options.into()
    }

    /// Build a delta storage backend for the given config
    pub fn build_storage(self) -> DeltaResult<LogStoreRef> {
        config::configure_log_store(
            &self.options.table_uri,
            self.storage_options(),
            self.options.storage_backend,
        )
    }

    /// Build the [`DeltaTable`] from specified options.
    ///
    /// This will not load the log, i.e. the table is not initialized. To get an initialized
    /// table use the `load` function
    pub fn build(self) -> DeltaResult<DeltaTable> {
        let config = DeltaTableConfig {
            require_tombstones: self.options.require_tombstones,
            require_files: self.options.require_files,
            log_buffer_size: self.options.log_buffer_size,
        };
        Ok(DeltaTable::new(self.build_storage()?, config))
    }

    /// Build the [`DeltaTable`] and load its state
    pub async fn load(self) -> DeltaResult<DeltaTable> {
        let version = self.options.version.clone();
        let mut table = self.build()?;
        match version {
            DeltaVersion::Newest => table.load().await?,
            DeltaVersion::Version(v) => table.load_version(v).await?,
            DeltaVersion::Timestamp(ts) => table.load_with_datetime(ts).await?,
        }
        Ok(table)
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
    /// The `pool_idle_timeout` for the as3_storage_options sts client. See
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

#[allow(dead_code)]
pub(crate) fn str_option(map: &HashMap<String, String>, key: &str) -> Option<String> {
    map.get(key)
        .map_or_else(|| std::env::var(key).ok(), |v| Some(v.to_owned()))
}

lazy_static::lazy_static! {
    static ref KNOWN_SCHEMES: Vec<&'static str> =
        Vec::from([
            "file", "memory", "az", "abfs", "abfss", "azure", "wasb", "wasbs", "adl", "s3", "s3a",
            "gs", "hdfs", "https", "http",
        ]);
}

/// Utility function to figure out whether string representation of the path
/// is either local path or some kind or URL.
///
/// Will return an error if the path is not valid.
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();

    if let Ok(url) = Url::parse(table_uri) {
        if url.scheme() == "file" {
            Ok(UriType::LocalPath(url.to_file_path().map_err(|err| {
                let msg = format!("Invalid table location: {}\nError: {:?}", table_uri, err);
                DeltaTableError::InvalidTableLocation(msg)
            })?))
        // NOTE this check is required to support absolute windows paths which may properly parse as url
        } else if KNOWN_SCHEMES.contains(&url.scheme()) {
            Ok(UriType::Url(url))
        } else {
            Ok(UriType::LocalPath(PathBuf::from(table_uri)))
        }
    } else {
        Ok(UriType::LocalPath(PathBuf::from(table_uri)))
    }
}

/// Attempt to create a Url from given table location.
///
/// The location could be:
///  * A valid URL, which will be parsed and returned
///  * A path to a directory, which will be created and then converted to a URL.
///
/// If it is a local path, it will be created if it doesn't exist.
///
/// Extra slashes will be removed from the end path as well.
///
/// Will return an error if the location is not valid. For example,
pub fn ensure_table_uri(table_uri: impl AsRef<str>) -> DeltaResult<Url> {
    let table_uri = table_uri.as_ref();

    let uri_type: UriType = resolve_uri_type(table_uri)?;

    // If it is a local path, we need to create it if it does not exist.
    let mut url = match uri_type {
        UriType::LocalPath(path) => {
            if !path.exists() {
                std::fs::create_dir_all(&path).map_err(|err| {
                    let msg = format!(
                        "Could not create local directory: {}\nError: {:?}",
                        table_uri, err
                    );
                    DeltaTableError::InvalidTableLocation(msg)
                })?;
            }
            let path = std::fs::canonicalize(path).map_err(|err| {
                let msg = format!("Invalid table location: {}\nError: {:?}", table_uri, err);
                DeltaTableError::InvalidTableLocation(msg)
            })?;
            Url::from_directory_path(path).map_err(|_| {
                let msg = format!(
                    "Could not construct a URL from canonicalized path: {}.\n\
                    Something must be very wrong with the table path.",
                    table_uri
                );
                DeltaTableError::InvalidTableLocation(msg)
            })?
        }
        UriType::Url(url) => url,
    };

    let trimmed_path = url.path().trim_end_matches('/').to_owned();
    url.set_path(&trimmed_path);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use object_store::path::Path;

    #[test]
    fn test_ensure_table_uri() {
        // parse an existing relative directory
        let uri = ensure_table_uri(".");
        assert!(uri.is_ok());
        let _uri = ensure_table_uri("./nonexistent");
        assert!(uri.is_ok());
        let uri = ensure_table_uri("s3://container/path");
        assert!(uri.is_ok());

        // These cases should all roundtrip to themselves
        cfg_if::cfg_if! {
            if #[cfg(windows)] {
                let roundtrip_cases = &[
                    "s3://tests/data/delta-0.8.0",
                    "memory://",
                    "s3://bucket/my%20table", // Doesn't double-encode
                ];
            } else {
                let roundtrip_cases = &[
                    "s3://tests/data/delta-0.8.0",
                    "memory://",
                    "file:///",
                    "s3://bucket/my%20table", // Doesn't double-encode
                ];
            }
        }

        for case in roundtrip_cases {
            let uri = ensure_table_uri(case).unwrap();
            assert_eq!(case, &uri.as_str());
        }

        // Other cases
        let map_cases = &[
            // extra slashes are removed
            (
                "s3://tests/data/delta-0.8.0//",
                "s3://tests/data/delta-0.8.0",
            ),
            ("s3://bucket/my table", "s3://bucket/my%20table"),
        ];

        for (case, expected) in map_cases {
            let uri = ensure_table_uri(case).unwrap();
            assert_eq!(expected, &uri.as_str());
        }
    }

    #[test]
    #[cfg(windows)]
    fn test_windows_uri() {
        let map_cases = &[
            // extra slashes are removed
            ("c:/", "file:///C:"),
        ];

        for (case, expected) in map_cases {
            let uri = ensure_table_uri(case).unwrap();
            assert_eq!(expected, &uri.as_str());
        }
    }

    #[test]
    fn test_ensure_table_uri_path() {
        let tmp_dir = tempdir::TempDir::new("test").unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();
        let paths = &[
            tmp_path.join("data/delta-0.8.0"),
            tmp_path.join("space in path"),
            tmp_path.join("special&chars/你好/😊"),
        ];

        for path in paths {
            let expected = Url::from_directory_path(path).unwrap();
            let uri = ensure_table_uri(path.as_os_str().to_str().unwrap()).unwrap();
            assert_eq!(expected.as_str().trim_end_matches('/'), uri.as_str());
            assert!(path.exists());
        }

        // Creates non-existent relative directories
        let relative_path = std::path::Path::new("_tmp/test %3F");
        assert!(!relative_path.exists());
        ensure_table_uri(relative_path.as_os_str().to_str().unwrap()).unwrap();
        assert!(relative_path.exists());
        std::fs::remove_dir_all(relative_path).unwrap();
    }

    #[test]
    fn test_ensure_table_uri_url() {
        // Urls should round trips as-is
        let expected = Url::parse("s3://tests/data/delta-0.8.0").unwrap();
        let url = ensure_table_uri(&expected).unwrap();
        assert_eq!(expected, url);

        let tmp_dir = tempdir::TempDir::new("test").unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();
        let path = tmp_path.join("data/delta-0.8.0");
        let expected = Url::from_directory_path(path).unwrap();
        let url = ensure_table_uri(&expected).unwrap();
        assert_eq!(expected.as_str().trim_end_matches('/'), url.as_str());
    }

    #[tokio::test]
    async fn read_delta_table_ignoring_tombstones() {
        let table = DeltaTableBuilder::from_uri("./tests/data/delta-0.8.0")
            .without_tombstones()
            .load()
            .await
            .unwrap();
        assert!(
            table.get_state().all_tombstones().is_empty(),
            "loading without tombstones should skip tombstones"
        );

        assert_eq!(
            table.get_files_iter().collect_vec(),
            vec![
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
                Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet")
            ]
        );
    }

    #[tokio::test]
    async fn read_delta_table_ignoring_files() {
        let table = DeltaTableBuilder::from_uri("./tests/data/delta-0.8.0")
            .without_files()
            .load()
            .await
            .unwrap();

        assert_eq!(table.get_files_iter().count(), 0, "files should be empty");
        assert!(
            table.get_tombstones().next().is_none(),
            "tombstones should be empty"
        );
    }

    #[tokio::test]
    async fn read_delta_table_with_ignoring_files_on_apply_log() {
        let mut table = DeltaTableBuilder::from_uri("./tests/data/delta-0.8.0")
            .with_version(0)
            .without_files()
            .load()
            .await
            .unwrap();

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_iter().count(), 0, "files should be empty");
        assert!(
            table.get_tombstones().next().is_none(),
            "tombstones should be empty"
        );

        table.update().await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_iter().count(), 0, "files should be empty");
        assert!(
            table.get_tombstones().next().is_none(),
            "tombstones should be empty"
        );
    }
}
