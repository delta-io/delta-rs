//! Create or load DeltaTables

use crate::delta::{DeltaTable, DeltaTableError};
use crate::schema::DeltaDataTypeVersion;
use crate::storage::file::FileStorageBackend;
use crate::storage::DeltaObjectStore;
use chrono::{DateTime, FixedOffset, Utc};
#[cfg(any(feature = "s3", feature = "s3-rustls"))]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "azure")]
use object_store::azure::MicrosoftAzureBuilder;
#[cfg(feature = "gcs")]
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{DynObjectStore, Error as ObjectStoreError, Result as ObjectStoreResult};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

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
#[derive(Debug)]
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
        DeltaTableBuilder {
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
    pub fn with_datestring(self, date_string: &str) -> Result<Self, DeltaTableError> {
        let datetime =
            DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(date_string)?);
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
    /// Options may be passed in the HashMap or set as environment variables.
    ///
    /// [s3_storage_options] describes the available options for the AWS or S3-compliant backend.
    /// [dynamodb_lock::DynamoDbLockClient] describes additional options for the AWS atomic rename client.
    /// [azure_storage_options] describes the available options for the Azure backend.
    /// [gcp_storage_options] describes the available options for the Google Cloud Platform backend.
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
    pub fn build_storage(self) -> Result<Arc<DeltaObjectStore>, DeltaTableError> {
        let (storage, prefix) = match self.options.storage_backend {
            Some(storage) => storage,
            None => get_storage_backend(
                &self.options.table_uri,
                self.storage_options,
                self.allow_http,
            )?,
        };
        let object_store = Arc::new(DeltaObjectStore::new(&prefix, storage));
        Ok(object_store)
    }

    /// Build the delta Table from specified options.
    ///
    /// This will not load the log, i.e. the table is not initialized. To get an initialized
    /// table use the `load` function
    pub fn build(self) -> Result<DeltaTable, DeltaTableError> {
        let (storage, prefix) = match self.options.storage_backend {
            Some(storage) => storage,
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
        let object_store = Arc::new(DeltaObjectStore::new(&prefix, storage));
        Ok(DeltaTable::new(object_store, config))
    }

    /// finally load the table
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
    /// ## Azure
    ///   * az://<container>/<path>
    ///   * abfs(s)://<container>/<path>
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
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Returns this [`StorageUrl`] as a string
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Returns the type of storage the URl refers to
    pub fn service_type(&self) -> StorageService {
        match self.url.scheme() {
            "file" => StorageService::Local,
            "az" | "abfs" | "abfss" | "adls2" | "azure" | "wasb" => StorageService::Azure,
            // TODO is s3a permissible?
            "s3" | "s3a" => StorageService::S3,
            "gs" => StorageService::GCS,
            _ => StorageService::Unknown,
        }
    }
}

impl AsRef<str> for StorageUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl AsRef<Url> for StorageUrl {
    fn as_ref(&self) -> &Url {
        &self.url
    }
}

impl std::fmt::Display for StorageUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

/// Create a new storage backend used in Delta table
fn get_storage_backend(
    table_uri: impl AsRef<str>,
    _options: Option<HashMap<String, String>>,
    allow_http: Option<bool>,
) -> ObjectStoreResult<(Arc<DynObjectStore>, Path)> {
    let storage_url = StorageUrl::parse(table_uri)?;
    match storage_url.service_type() {
        StorageService::Local => Ok((Arc::new(FileStorageBackend::new()), storage_url.prefix)),
        #[cfg(any(feature = "s3", feature = "s3-rustls"))]
        StorageService::S3 => {
            let url: &Url = storage_url.as_ref();
            let bucket_name = url.host_str().ok_or(ObjectStoreError::NotImplemented)?;
            let mut builder = get_s3_builder_from_options(_options.unwrap_or_default())
                .with_bucket_name(bucket_name);
            if let Some(allow) = allow_http {
                builder = builder.with_allow_http(allow);
            }
            Ok((Arc::new(builder.build()?), storage_url.prefix))
        }
        #[cfg(feature = "azure")]
        StorageService::Azure => {
            let url: &Url = storage_url.as_ref();
            // TODO we have to differentiate ...
            let container_name = url.host_str().ok_or(ObjectStoreError::NotImplemented)?;
            let mut builder = get_azure_builder_from_options(_options.unwrap_or_default())
                .with_container_name(container_name);
            if let Some(allow) = allow_http {
                builder = builder.with_allow_http(allow);
            }
            Ok((Arc::new(builder.build()?), storage_url.prefix))
        }
        #[cfg(feature = "gcs")]
        StorageService::GCS => {
            let url: &Url = storage_url.as_ref();
            let bucket_name = url.host_str().ok_or(ObjectStoreError::NotImplemented)?;
            let builder = get_gcp_builder_from_options(_options.unwrap_or_default())
                .with_bucket_name(bucket_name);
            Ok((Arc::new(builder.build()?), storage_url.prefix))
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

/// Generate a new AmazonS3Builder instance from a map of options
#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub fn get_s3_builder_from_options(options: HashMap<String, String>) -> AmazonS3Builder {
    let mut builder = AmazonS3Builder::new();

    if let Some(endpoint) = str_option(&options, s3_storage_options::AWS_ENDPOINT_URL) {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(region) = str_option(&options, s3_storage_options::AWS_REGION) {
        builder = builder.with_region(region);
    }
    if let Some(access_key_id) = str_option(&options, s3_storage_options::AWS_ACCESS_KEY_ID) {
        builder = builder.with_access_key_id(access_key_id);
    }
    if let Some(secret_access_key) = str_option(&options, s3_storage_options::AWS_SECRET_ACCESS_KEY)
    {
        builder = builder.with_secret_access_key(secret_access_key);
    }
    if let Some(session_token) = str_option(&options, s3_storage_options::AWS_SESSION_TOKEN) {
        builder = builder.with_token(session_token);
    }
    // TODO AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN are not configurable on the builder, but picked
    // up by the build function if set on the environment. If we have them in the map, should we set them in the env?
    // In the default case, always instance credentials are used.
    builder
}

/// Storage option keys to use when creating azure storage backend.
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
pub mod azure_storage_options {
    ///The ADLS Gen2 Access Key
    pub const AZURE_STORAGE_ACCOUNT_KEY: &str = "AZURE_STORAGE_ACCOUNT_KEY";
    ///The name of storage account
    pub const AZURE_STORAGE_ACCOUNT_NAME: &str = "AZURE_STORAGE_ACCOUNT_NAME";
    /// Connection string for connecting to azure storage account
    pub const AZURE_STORAGE_CONNECTION_STRING: &str = "AZURE_STORAGE_CONNECTION_STRING";
    /// Service principal id
    pub const AZURE_STORAGE_CLIENT_ID: &str = "AZURE_STORAGE_CLIENT_ID";
    /// Service principal secret
    pub const AZURE_STORAGE_CLIENT_SECRET: &str = "AZURE_STORAGE_CLIENT_SECRET";
    /// ID for Azure (AAD) tenant where service principal is registered.
    pub const AZURE_STORAGE_TENANT_ID: &str = "AZURE_STORAGE_TENANT_ID";
    /// Connect to a Azurite storage emulator instance
    pub const AZURE_STORAGE_USE_EMULATOR: &str = "AZURE_STORAGE_USE_EMULATOR";
}

/// Generate a new MicrosoftAzureBuilder instance from a map of options
#[cfg(feature = "azure")]
pub fn get_azure_builder_from_options(options: HashMap<String, String>) -> MicrosoftAzureBuilder {
    let mut builder = MicrosoftAzureBuilder::new();

    if let Some(account) = str_option(&options, azure_storage_options::AZURE_STORAGE_ACCOUNT_NAME) {
        builder = builder.with_account(account);
    }
    if let Some(account_key) =
        str_option(&options, azure_storage_options::AZURE_STORAGE_ACCOUNT_KEY)
    {
        builder = builder.with_access_key(account_key);
    }
    if let (Some(client_id), Some(client_secret), Some(tenant_id)) = (
        str_option(&options, azure_storage_options::AZURE_STORAGE_CLIENT_ID),
        str_option(&options, azure_storage_options::AZURE_STORAGE_CLIENT_SECRET),
        str_option(&options, azure_storage_options::AZURE_STORAGE_TENANT_ID),
    ) {
        builder = builder.with_client_secret_authorization(client_id, client_secret, tenant_id);
    }
    if let Some(_emulator) = str_option(&options, azure_storage_options::AZURE_STORAGE_USE_EMULATOR)
    {
        builder = builder.with_use_emulator(true).with_allow_http(true);
    }
    builder
}

/// Storage option keys to use when creating gcp storage backend.
/// The same key should be used whether passing a key in the hashmap or setting it as an environment variable.
pub mod gcp_storage_options {
    ///Path to the service account json file
    pub const SERVICE_ACCOUNT: &str = "SERVICE_ACCOUNT";
}

/// Generate a new GoogleCloudStorageBuilder instance from a map of options
#[cfg(feature = "gcs")]
pub fn get_gcp_builder_from_options(options: HashMap<String, String>) -> GoogleCloudStorageBuilder {
    let mut builder = GoogleCloudStorageBuilder::new();

    if let Some(account) = str_option(&options, gcp_storage_options::SERVICE_ACCOUNT) {
        builder = builder.with_service_account_path(account);
    }
    builder
}

pub(crate) fn str_option(map: &HashMap<String, String>, key: &str) -> Option<String> {
    map.get(key)
        .map_or_else(|| std::env::var(key).ok(), |v| Some(v.to_owned()))
}
