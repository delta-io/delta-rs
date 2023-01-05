//! Create or load DeltaTables

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::delta::{DeltaResult, DeltaTable, DeltaTableError};
use crate::schema::DeltaDataTypeVersion;
use crate::storage::file::FileStorageBackend;
use crate::storage::{DeltaObjectStore, ObjectStoreRef};

use chrono::{DateTime, FixedOffset, Utc};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{DynObjectStore, Error as ObjectStoreError, Result as ObjectStoreResult};
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
use crate::storage::s3::{S3StorageBackend, S3StorageOptions};
#[cfg(any(feature = "s3", feature = "s3-rustls"))]
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
#[cfg(feature = "azure")]
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
#[cfg(feature = "gcs")]
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
#[cfg(any(
    feature = "s3",
    feature = "s3-rustls",
    feature = "azure",
    feature = "gcs"
))]
use std::str::FromStr;

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
}

impl From<BuilderError> for DeltaTableError {
    fn from(err: BuilderError) -> Self {
        DeltaTableError::Generic(err.to_string())
    }
}

/// possible version specifications for loading a delta table
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaVersion {
    /// load the newest version
    Newest,
    /// specify the version to load
    Version(DeltaDataTypeVersion),
    /// specify the timestamp in UTC
    Timestamp(DateTime<Utc>),
}

impl Default for DeltaVersion {
    fn default() -> Self {
        DeltaVersion::Newest
    }
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
}

impl Default for DeltaTableConfig {
    fn default() -> Self {
        Self {
            require_tombstones: true,
            require_files: true,
        }
    }
}

/// Load-time delta table configuration options
#[derive(Debug)]
pub struct DeltaTableLoadOptions {
    /// table root uri
    pub table_uri: String,
    /// backend to access storage system
    pub storage_backend: Option<(Arc<DynObjectStore>, Path)>,
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
}

impl DeltaTableLoadOptions {
    /// create default table load options for a table uri
    pub fn new(table_uri: impl Into<String>) -> Self {
        Self {
            table_uri: table_uri.into(),
            storage_backend: None,
            require_tombstones: true,
            require_files: true,
            version: DeltaVersion::default(),
        }
    }
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
    pub fn with_version(mut self, version: DeltaDataTypeVersion) -> Self {
        self.options.version = DeltaVersion::Version(version);
        self
    }

    /// specify the timestamp given as ISO-8601/RFC-3339 timestamp
    pub fn with_datestring(self, date_string: impl AsRef<str>) -> Result<Self, DeltaTableError> {
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
    /// `table_root` denotes the [object_store::path::Path] within the store to the root of the delta.
    /// This is required since we cannot infer the relative location of the table from the `table_uri`
    /// For non-standard object store implementations.
    ///
    /// If a backend is not provided then it is derived from `table_uri`.
    pub fn with_storage_backend(mut self, storage: Arc<DynObjectStore>, table_root: &Path) -> Self {
        self.options.storage_backend = Some((storage, table_root.clone()));
        self
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables. See documentation of
    /// underlying object store implementation for details.
    /// TODO add links once 0.5.3 is published.
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

    /// Build a delta storage backend for the given config
    pub fn build_storage(self) -> Result<ObjectStoreRef, DeltaTableError> {
        let (storage, storage_url) = match self.options.storage_backend {
            // Some(storage) => storage,
            None => get_storage_backend(
                &self.options.table_uri,
                self.storage_options,
                self.allow_http,
            )?,
            _ => todo!(),
        };
        let object_store = Arc::new(DeltaObjectStore::new(storage_url, storage));
        Ok(object_store)
    }

    /// Build the [`DeltaTable`] from specified options.
    ///
    /// This will not load the log, i.e. the table is not initialized. To get an initialized
    /// table use the `load` function
    pub fn build(self) -> Result<DeltaTable, DeltaTableError> {
        let (storage, storage_url) = match self.options.storage_backend {
            Some((store, path)) => {
                let mut uri = self.options.table_uri + path.as_ref();
                if !uri.contains(':') {
                    uri = format!("file://{}", uri);
                }
                let url = Url::parse(uri.as_str())
                    .map_err(|_| DeltaTableError::Generic(format!("Can't parse uri: {}", uri)))?;
                let url = StorageUrl::new(url);
                (store, url)
            }
            None => get_storage_backend(
                &self.options.table_uri,
                self.storage_options,
                self.allow_http,
            )?,
        };
        let config = DeltaTableConfig {
            require_tombstones: self.options.require_tombstones,
            require_files: self.options.require_files,
        };
        let object_store = Arc::new(DeltaObjectStore::new(storage_url, storage));
        Ok(DeltaTable::new(object_store, config))
    }

    /// Build the [`DeltaTable`] and load its state
    pub async fn load(self) -> Result<DeltaTable, DeltaTableError> {
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

/// Well known storage services
pub enum StorageService {
    /// Local filesystem storage
    Local,
    /// S3 compliant service
    S3,
    /// Azure blob service
    Azure,
    /// Google cloud storage
    GCS,
    /// In-memory table
    Memory,
    /// Unrecognized service
    Unknown,
}

/// A parsed URL identifying a storage location
/// for more information on the supported expressions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUrl {
    /// A URL that identifies a file or directory to list files from
    pub(crate) url: Url,
    /// The path prefix
    pub(crate) prefix: Path,
}

impl Serialize for StorageUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(self.url.as_str())?;
        seq.serialize_element(&self.prefix.to_string())?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for StorageUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StorageUrlVisitor {}
        impl<'de> Visitor<'de> for StorageUrlVisitor {
            type Value = StorageUrl;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct StorageUrl")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<StorageUrl, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let url = seq
                    .next_element()?
                    .ok_or_else(|| V::Error::invalid_length(0, &self))?;
                let prefix: &str = seq
                    .next_element()?
                    .ok_or_else(|| V::Error::invalid_length(1, &self))?;
                let url = Url::parse(url).map_err(|_| V::Error::missing_field("url"))?;
                let prefix = Path::parse(prefix).map_err(|_| V::Error::missing_field("prefix"))?;
                let url = StorageUrl { url, prefix };
                Ok(url)
            }
        }
        deserializer.deserialize_seq(StorageUrlVisitor {})
    }
}

impl StorageUrl {
    /// Parse a provided string as a `StorageUrl`
    ///
    /// # Paths without a Scheme
    ///
    /// If no scheme is provided, or the string is an absolute filesystem path
    /// as determined [`std::path::Path::is_absolute`], the string will be
    /// interpreted as a path on the local filesystem using the operating
    /// system's standard path delimiter, i.e. `\` on Windows, `/` on Unix.
    ///
    /// Otherwise, the path will be resolved to an absolute path, returning
    /// an error if it does not exist, and converted to a [file URI]
    ///
    /// If you wish to specify a path that does not exist on the local
    /// machine you must provide it as a fully-qualified [file URI]
    /// e.g. `file:///myfile.txt`
    ///
    /// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
    ///
    /// # Well-known formats
    ///
    /// The lists below enumerates some well known uris, that are understood by the
    /// parse function. We parse uris to refer to a specific storage location, which
    /// is accessed using the internal storage backends.
    ///
    /// ## Azure
    ///
    /// URIs according to <https://github.com/fsspec/adlfs#filesystem-interface-to-azure-datalake-gen1-and-gen2-storage>:
    ///
    ///   * az://<container>/<path>
    ///   * adl://<container>/<path>
    ///   * abfs(s)://<container>/<path>
    ///
    /// URIs according to <https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri>:
    ///   
    ///   * abfs(s)://<file_system>@<account_name>.dfs.core.windows.net/<path>
    ///
    /// and a custom one
    ///
    ///   * azure://<container>/<path>
    ///
    /// ## S3
    ///   * s3://<bucket>/<path>
    ///   * s3a://<bucket>/<path>
    ///
    /// ## GCS
    ///   * gs://<bucket>/<path>
    pub fn parse(s: impl AsRef<str>) -> ObjectStoreResult<Self> {
        let s = s.as_ref();

        // This is necessary to handle the case of a path starting with a drive letter
        if std::path::Path::new(s).is_absolute() {
            return Self::parse_path(s);
        }

        match Url::parse(s) {
            Ok(url) => Ok(Self::new(url)),
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(ObjectStoreError::Generic {
                store: "DeltaObjectStore",
                source: Box::new(e),
            }),
        }
    }

    /// Creates a new [`StorageUrl`] interpreting `s` as a filesystem path
    fn parse_path(s: &str) -> ObjectStoreResult<Self> {
        let path =
            std::path::Path::new(s)
                .canonicalize()
                .map_err(|e| ObjectStoreError::Generic {
                    store: "DeltaObjectStore",
                    source: Box::new(e),
                })?;
        let url = match path.is_file() {
            true => Url::from_file_path(path).unwrap(),
            false => Url::from_directory_path(path).unwrap(),
        };

        Ok(Self::new(url))
    }

    /// Creates a new [`StorageUrl`] from a url
    fn new(url: Url) -> Self {
        let prefix = Path::parse(url.path()).expect("should be URL safe");
        Self { url, prefix }
    }

    /// Returns the URL scheme
    pub(crate) fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Returns this [`StorageUrl`] as a string
    pub(crate) fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl AsRef<str> for StorageUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl std::fmt::Display for StorageUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

enum ObjectStoreKind {
    Local,
    InMemory,
    S3,
    Google,
    Azure,
}

impl ObjectStoreKind {
    pub fn parse_url(url: &Url) -> ObjectStoreResult<Self> {
        match url.scheme() {
            "file" => Ok(ObjectStoreKind::Local),
            "memory" => Ok(ObjectStoreKind::InMemory),
            "az" | "abfs" | "abfss" | "azure" | "wasb" | "adl" => Ok(ObjectStoreKind::Azure),
            "s3" | "s3a" => Ok(ObjectStoreKind::S3),
            "gs" => Ok(ObjectStoreKind::Google),
            "https" => {
                let host = url.host_str().unwrap_or_default();
                if host.contains("amazonaws.com") {
                    Ok(ObjectStoreKind::S3)
                } else if host.contains("dfs.core.windows.net")
                    || host.contains("blob.core.windows.net")
                {
                    Ok(ObjectStoreKind::Azure)
                } else {
                    Err(ObjectStoreError::NotImplemented)
                }
            }
            _ => Err(ObjectStoreError::NotImplemented),
        }
    }
}

/// Create a new storage backend used in Delta table
pub(crate) fn get_storage_backend(
    table_uri: impl AsRef<str>,
    // annotation needed for some feature builds
    #[allow(unused_variables)] options: Option<HashMap<String, String>>,
    #[allow(unused_variables)] allow_http: Option<bool>,
) -> DeltaResult<(Arc<DynObjectStore>, StorageUrl)> {
    let storage_url = StorageUrl::parse(table_uri)?;
    let mut options = options.unwrap_or_default();
    if let Some(allow) = allow_http {
        options.insert(
            "allow_http".into(),
            if allow { "true" } else { "false" }.into(),
        );
    }

    match ObjectStoreKind::parse_url(&storage_url.url)? {
        ObjectStoreKind::Local => Ok((Arc::new(FileStorageBackend::new()), storage_url)),
        ObjectStoreKind::InMemory => Ok((Arc::new(InMemory::new()), storage_url)),
        #[cfg(any(feature = "s3", feature = "s3-rustls"))]
        ObjectStoreKind::S3 => {
            let mut s3_options = options
                .clone()
                .into_iter()
                .filter_map(|(key, value)| {
                    AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                    Some((key.to_ascii_lowercase(), value))
                })
                .collect::<HashMap<_, _>>();
            // TODO add custom options currently used in delta-rs
            let store = AmazonS3Builder::new()
                .with_url(storage_url.as_ref())
                .try_with_options(&s3_options)?
                .build()
                .or_else(|_| {
                    AmazonS3Builder::from_env()
                        .with_url(storage_url.as_ref())
                        .try_with_options(&s3_options)?
                        .build()
                })?;
            Ok((
                Arc::new(S3StorageBackend::try_new(
                    Arc::new(store),
                    S3StorageOptions::from_map(options),
                )?),
                storage_url,
            ))
        }
        #[cfg(feature = "azure")]
        ObjectStoreKind::Azure => {
            let azure_options = options
                .into_iter()
                .filter_map(|(key, value)| {
                    AzureConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                    Some((key.to_ascii_lowercase(), value))
                })
                .collect::<HashMap<_, _>>();
            let store = MicrosoftAzureBuilder::new()
                .with_url(storage_url.as_ref())
                .try_with_options(&azure_options)?
                .build()
                .or_else(|_| {
                    MicrosoftAzureBuilder::from_env()
                        .with_url(storage_url.as_ref())
                        .try_with_options(&azure_options)?
                        .build()
                })?;
            Ok((Arc::new(store), storage_url))
        }
        #[cfg(feature = "gcs")]
        ObjectStoreKind::GCS => {
            let google_options = options
                .into_iter()
                .filter_map(|(key, value)| {
                    GoogleConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                    Some((key.to_ascii_lowercase(), value))
                })
                .collect::<HashMap<_, _>>();
            let store = GoogleCloudStorageBuilder::new()
                .with_url(storage_url.as_ref())
                .try_with_options(&google_options)?
                .build()
                .or_else(|_| {
                    GoogleCloudStorageBuilder::from_env()
                        .with_url(storage_url.as_ref())
                        .try_with_options(&google_options)?
                        .build()
                })?;
            Ok((Arc::new(store), storage_url))
        }
        _ => todo!(),
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
    /// Allow http connections - mainly useful for integration tests
    pub const AWS_STORAGE_ALLOW_HTTP: &str = "AWS_STORAGE_ALLOW_HTTP";

    /// If set to "true", allows creating commits without concurrent writer protection.
    /// Only safe if there is one writer to a given table.
    pub const AWS_S3_ALLOW_UNSAFE_RENAME: &str = "AWS_S3_ALLOW_UNSAFE_RENAME";

    /// The list of option keys owned by the S3 module.
    /// Option keys not contained in this list will be added to the `extra_opts`
    /// field of [crate::storage::s3::S3StorageOptions].
    /// `extra_opts` are passed to [dynamodb_lock::DynamoDbOptions] to configure the lock client.
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
