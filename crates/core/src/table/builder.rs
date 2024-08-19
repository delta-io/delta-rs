//! Create or load DeltaTables

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, Utc};
use object_store::DynObjectStore;
use serde::{Deserialize, Serialize};
use tracing::debug;
use url::Url;

use super::DeltaTable;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::LogStoreRef;
use crate::storage::{factories, IORuntime, StorageOptions};

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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
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
#[derive(Debug, Serialize, Deserialize, Clone)]
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
    /// Control the number of records to read / process from the commit / checkpoint files
    /// when processing record batches.
    pub log_batch_size: usize,
    #[serde(skip_serializing, skip_deserializing)]
    /// When a runtime handler is provided, all IO tasks are spawn in that handle
    pub io_runtime: Option<IORuntime>,
}

impl Default for DeltaTableConfig {
    fn default() -> Self {
        Self {
            require_tombstones: true,
            require_files: true,
            log_buffer_size: num_cpus::get() * 4,
            log_batch_size: 1024,
            io_runtime: None,
        }
    }
}

impl PartialEq for DeltaTableConfig {
    fn eq(&self, other: &Self) -> bool {
        self.require_tombstones == other.require_tombstones
            && self.require_files == other.require_files
            && self.log_buffer_size == other.log_buffer_size
            && self.log_batch_size == other.log_batch_size
    }
}

/// builder for configuring a delta table load.
#[derive(Debug)]
pub struct DeltaTableBuilder {
    /// table root uri
    table_uri: String,
    /// backend to access storage system
    storage_backend: Option<(Arc<DynObjectStore>, Url)>,
    /// specify the version we are going to load: a time stamp, a version, or just the newest
    /// available version
    version: DeltaVersion,
    storage_options: Option<HashMap<String, String>>,
    #[allow(unused_variables)]
    allow_http: Option<bool>,
    table_config: DeltaTableConfig,
}

impl DeltaTableBuilder {
    /// Creates `DeltaTableBuilder` from table uri
    ///
    /// Can panic on an invalid URI
    ///
    /// ```rust
    /// # use deltalake_core::table::builder::*;
    /// let builder = DeltaTableBuilder::from_uri("../test/tests/data/delta-0.8.0");
    /// assert!(true);
    /// ```
    pub fn from_uri(table_uri: impl AsRef<str>) -> Self {
        let url = ensure_table_uri(&table_uri).expect("The specified table_uri is not valid");
        DeltaTableBuilder::from_valid_uri(url).expect("Failed to create valid builder")
    }

    /// Creates `DeltaTableBuilder` from verified table uri.
    ///
    /// ```rust
    /// # use deltalake_core::table::builder::*;
    /// let builder = DeltaTableBuilder::from_valid_uri("memory:///");
    /// assert!(builder.is_ok(), "Builder failed with {builder:?}");
    /// ```
    pub fn from_valid_uri(table_uri: impl AsRef<str>) -> DeltaResult<Self> {
        if let Ok(url) = Url::parse(table_uri.as_ref()) {
            if url.scheme() == "file" {
                let path = url.to_file_path().map_err(|_| {
                    DeltaTableError::InvalidTableLocation(table_uri.as_ref().to_string())
                })?;
                ensure_file_location_exists(path)?;
            }
        } else {
            ensure_file_location_exists(PathBuf::from(table_uri.as_ref()))?;
        }

        let url = ensure_table_uri(&table_uri)?;
        debug!("creating table builder with {url}");

        Ok(Self {
            table_uri: url.into(),
            storage_backend: None,
            version: DeltaVersion::default(),
            storage_options: None,
            allow_http: None,
            table_config: DeltaTableConfig::default(),
        })
    }

    /// Sets `require_tombstones=false` to the builder
    pub fn without_tombstones(mut self) -> Self {
        self.table_config.require_tombstones = false;
        self
    }

    /// Sets `require_files=false` to the builder
    pub fn without_files(mut self) -> Self {
        self.table_config.require_files = false;
        self
    }

    /// Sets `version` to the builder
    pub fn with_version(mut self, version: i64) -> Self {
        self.version = DeltaVersion::Version(version);
        self
    }

    /// Sets `log_buffer_size` to the builder
    pub fn with_log_buffer_size(mut self, log_buffer_size: usize) -> DeltaResult<Self> {
        if log_buffer_size == 0 {
            return Err(DeltaTableError::Generic(String::from(
                "Log buffer size should be positive",
            )));
        }
        self.table_config.log_buffer_size = log_buffer_size;
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
        self.version = DeltaVersion::Timestamp(timestamp);
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
        self.storage_backend = Some((storage, location));
        self
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables. See documentation of
    /// underlying object store implementation for details. Trailing slash will be trimmed in
    /// the option's value to avoid failures. Trimming will only be done if one or more of below
    /// conditions are met:
    /// - key ends with `_URL` (e.g., `ENDPOINT_URL`, `S3_URL`, `JDBC_URL`, etc.)
    /// - value starts with `http://`` or `https://` (e.g., `http://localhost:8000/`)
    ///
    /// - [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants)
    /// - [S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants)
    /// - [Google options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants)
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(
            storage_options
                .clone()
                .into_iter()
                .map(|(k, v)| {
                    let needs_trim = v.starts_with("http://")
                        || v.starts_with("https://")
                        || k.to_lowercase().ends_with("_url");
                    if needs_trim {
                        (k.to_owned(), v.trim_end_matches('/').to_owned())
                    } else {
                        (k, v)
                    }
                })
                .collect(),
        );
        self
    }

    /// Allows unsecure connections via http.
    ///
    /// This setting is most useful for testing / development when connecting to emulated services.
    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = Some(allow_http);
        self
    }

    /// Provide a custom runtime handle or runtime config
    pub fn with_io_runtime(mut self, io_runtime: IORuntime) -> Self {
        self.table_config.io_runtime = Some(io_runtime);
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
    pub fn build_storage(&self) -> DeltaResult<LogStoreRef> {
        debug!("build_storage() with {}", self.table_uri);
        let location = Url::parse(&self.table_uri).map_err(|_| {
            DeltaTableError::NotATable(format!("Could not turn {} into a URL", self.table_uri))
        })?;

        if let Some((store, _url)) = self.storage_backend.as_ref() {
            debug!("Loading a logstore with a custom store: {store:?}");
            crate::logstore::logstore_with(
                store.clone(),
                location,
                self.storage_options(),
                self.table_config.io_runtime.clone(),
            )
        } else {
            // If there has been no backend defined just default to the normal logstore look up
            debug!("Loading a logstore based off the location: {location:?}");
            crate::logstore::logstore_for(
                location,
                self.storage_options(),
                self.table_config.io_runtime.clone(),
            )
        }
    }

    /// Build the [`DeltaTable`] from specified options.
    ///
    /// This will not load the log, i.e. the table is not initialized. To get an initialized
    /// table use the `load` function
    pub fn build(self) -> DeltaResult<DeltaTable> {
        Ok(DeltaTable::new(self.build_storage()?, self.table_config))
    }

    /// Build the [`DeltaTable`] and load its state
    pub async fn load(self) -> DeltaResult<DeltaTable> {
        let version = self.version;
        let mut table = self.build()?;
        match version {
            DeltaVersion::Newest => table.load().await?,
            DeltaVersion::Version(v) => table.load_version(v).await?,
            DeltaVersion::Timestamp(ts) => table.load_with_datetime(ts).await?,
        }
        Ok(table)
    }
}

enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// Utility function to figure out whether string representation of the path
/// is either local path or some kind or URL.
///
/// Will return an error if the path is not valid.
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();
    let known_schemes: Vec<_> = factories()
        .iter()
        .map(|v| v.key().scheme().to_owned())
        .collect();

    if let Ok(url) = Url::parse(table_uri) {
        let scheme = url.scheme().to_string();
        if url.scheme() == "file" {
            Ok(UriType::LocalPath(url.to_file_path().map_err(|err| {
                let msg = format!("Invalid table location: {}\nError: {:?}", table_uri, err);
                DeltaTableError::InvalidTableLocation(msg)
            })?))
        // NOTE this check is required to support absolute windows paths which may properly parse as url
        } else if known_schemes.contains(&scheme) {
            Ok(UriType::Url(url))
        // NOTE this check is required to support absolute windows paths which may properly parse as url
        // we assume here that a single character scheme is a windows drive letter
        } else if scheme.len() == 1 {
            Ok(UriType::LocalPath(PathBuf::from(table_uri)))
        } else {
            Err(DeltaTableError::InvalidTableLocation(format!(
                "Unknown scheme: {}",
                scheme
            )))
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

/// Validate that the given [PathBuf] does exist, otherwise return a
/// [DeltaTableError::InvalidTableLocation]
fn ensure_file_location_exists(path: PathBuf) -> DeltaResult<()> {
    if !path.exists() {
        let msg = format!(
            "Local path \"{}\" does not exist or you don't have access!",
            path.as_path().display(),
        );
        return Err(DeltaTableError::InvalidTableLocation(msg));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DefaultObjectStoreFactory;

    #[test]
    fn test_ensure_table_uri() {
        factories().insert(
            Url::parse("s3://").unwrap(),
            Arc::new(DefaultObjectStoreFactory::default()),
        );

        // parse an existing relative directory
        let uri = ensure_table_uri(".");
        assert!(uri.is_ok());
        let uri = ensure_table_uri("s3://container/path");
        assert!(uri.is_ok());
        #[cfg(not(windows))]
        {
            let uri = ensure_table_uri("file:///tmp/nonexistent/some/path");
            assert!(uri.is_ok());
        }
        let uri = ensure_table_uri("./nonexistent");
        assert!(uri.is_ok());
        let file_path = std::path::Path::new("./nonexistent");
        std::fs::remove_dir(file_path).unwrap();

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
        let tmp_dir = tempfile::tempdir().unwrap();
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
        std::fs::remove_dir_all(std::path::Path::new("_tmp")).unwrap();
    }

    #[test]
    fn test_ensure_table_uri_url() {
        // Urls should round trips as-is
        let expected = Url::parse("memory:///test/tests/data/delta-0.8.0").unwrap();
        let url = ensure_table_uri(&expected).unwrap();
        assert_eq!(expected, url);

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();
        let path = tmp_path.join("data/delta-0.8.0");
        let expected = Url::from_directory_path(path).unwrap();
        let url = ensure_table_uri(&expected).unwrap();
        assert_eq!(expected.as_str().trim_end_matches('/'), url.as_str());
    }

    #[test]
    fn test_invalid_uri() {
        // Urls should round trips as-is
        DeltaTableBuilder::from_valid_uri("this://is.nonsense")
            .expect_err("this should be an error");
    }

    #[test]
    fn test_writer_storage_opts_url_trim() {
        let cases = [
            // Trim Case 1 - Key indicating a url
            ("SOMETHING_URL", "something://else/", "something://else"),
            // Trim Case 2 - Value https url ending with slash
            (
                "SOMETHING",
                "http://something:port/",
                "http://something:port",
            ),
            // Trim Case 3 - Value https url ending with slash
            (
                "SOMETHING",
                "https://something:port/",
                "https://something:port",
            ),
            // No Trim Case 4 - JDBC MySQL url with slash
            (
                "SOME_JDBC_PREFIX",
                "jdbc:mysql://mysql.db.server:3306/",
                "jdbc:mysql://mysql.db.server:3306/",
            ),
            // No Trim Case 5 - S3A file system link
            ("SOME_S3_LINK", "s3a://bucket-name/", "s3a://bucket-name/"),
            // No Trim Case 6 - Not a url but ending with slash
            ("SOME_RANDOM_STRING", "a1b2c3d4e5f#/", "a1b2c3d4e5f#/"),
            // No Trim Case 7 - Some value not a url
            (
                "SOME_VALUE",
                "/ This is some value 123 /",
                "/ This is some value 123 /",
            ),
        ];
        for (key, val, expected) in cases {
            let table_uri = Url::parse("memory:///test/tests/data/delta-0.8.0").unwrap();
            let mut storage_opts = HashMap::<String, String>::new();
            storage_opts.insert(key.to_owned(), val.to_owned());

            let table = DeltaTableBuilder::from_uri(table_uri).with_storage_options(storage_opts);
            let found_opts = table.storage_options();
            assert_eq!(expected, found_opts.0.get(key).unwrap());
        }
    }
}
