//! Create or load DeltaTables

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, Utc};
use deltalake_derive::DeltaConfig;
use object_store::DynObjectStore;
use serde::{Deserialize, Serialize};
use tracing::debug;
use url::Url;

use super::normalize_table_url;
use crate::logstore::storage::IORuntime;
use crate::logstore::{LogStoreRef, StorageConfig, object_store_factories};
use crate::{DeltaResult, DeltaTable, DeltaTableError};

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
#[derive(Debug, Serialize, Deserialize, Clone, DeltaConfig)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableConfig {
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
    #[delta(skip)]
    /// When a runtime handler is provided, all IO tasks are spawn in that handle
    pub io_runtime: Option<IORuntime>,
}

impl Default for DeltaTableConfig {
    fn default() -> Self {
        Self {
            require_files: true,
            log_buffer_size: num_cpus::get() * 4,
            log_batch_size: 1024,
            io_runtime: None,
        }
    }
}

impl PartialEq for DeltaTableConfig {
    fn eq(&self, other: &Self) -> bool {
        self.require_files == other.require_files
            && self.log_buffer_size == other.log_buffer_size
            && self.log_batch_size == other.log_batch_size
    }
}

/// builder for configuring a delta table load.
#[derive(Debug)]
pub struct DeltaTableBuilder {
    /// table root uri
    table_url: Url,
    /// backend to access storage system
    storage_backend: Option<(Arc<DynObjectStore>, Url)>,
    /// specify the version we are going to load: a time stamp, a version, or just the newest
    /// available version
    version: DeltaVersion,
    storage_options: Option<HashMap<String, String>>,
    allow_http: Option<bool>,
    table_config: DeltaTableConfig,
}

impl DeltaTableBuilder {
    /// Creates `DeltaTableBuilder` from table URL
    ///
    /// ```rust
    /// # use deltalake_core::table::builder::*;
    /// # use url::Url;
    /// let url = Url::parse("memory:///test").unwrap();
    /// let builder = DeltaTableBuilder::from_url(url);
    /// ```
    pub fn from_url(table_url: Url) -> DeltaResult<Self> {
        // We cannot trust that a [Url] has had it's .. segments canonicalized out of the path
        // See <https://github.com/servo/rust-url/issues/1086>
        let table_url = Url::parse(table_url.as_str()).map_err(|_| {
            DeltaTableError::NotATable(
                "Received path segments that could not be canonicalized".into(),
            )
        })?;

        if table_url.scheme() == "file" {
            let path = table_url.to_file_path().map_err(|_| {
                DeltaTableError::InvalidTableLocation(table_url.as_str().to_string())
            })?;
            ensure_file_location_exists(path)?;
        }

        debug!("creating table builder with {table_url}");

        Ok(Self {
            table_url,
            storage_backend: None,
            version: DeltaVersion::default(),
            storage_options: None,
            allow_http: None,
            table_config: DeltaTableConfig::default(),
        })
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
    /// If a backend is not provided then it is derived from `location`.
    ///
    /// # Arguments
    ///
    /// * `root_storage` - A shared reference to an [`ObjectStore`](object_store::ObjectStore) with
    ///   "/" pointing at the root of the object store.
    /// * `location` - A url corresponding to the storage location of the delta table.
    pub fn with_storage_backend(
        mut self,
        root_storage: Arc<DynObjectStore>,
        location: Url,
    ) -> Self {
        self.storage_backend = Some((root_storage, location));
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

    /// Allows insecure connections via http.
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
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut storage_options = self.storage_options.clone().unwrap_or_default();
        if let Some(allow) = self.allow_http {
            storage_options.insert(
                "allow_http".into(),
                if allow { "true" } else { "false" }.into(),
            );
        };
        storage_options
    }

    /// Build a delta storage backend for the given config
    pub fn build_storage(&self) -> DeltaResult<LogStoreRef> {
        debug!("build_storage() with {}", self.table_url);

        let mut storage_config = StorageConfig::parse_options(self.storage_options())?;
        if let Some(io_runtime) = self.table_config.io_runtime.clone() {
            storage_config = storage_config.with_io_runtime(io_runtime);
        }

        if let Some((store, _url)) = self.storage_backend.as_ref() {
            debug!("Loading a logstore with a custom store: {store:?}");
            crate::logstore::logstore_with(store.clone(), &self.table_url, storage_config)
        } else {
            // If there has been no backend defined just default to the normal logstore look up
            debug!(
                "Loading a logstore based off the location: {:?}",
                self.table_url
            );
            crate::logstore::logstore_for(&self.table_url, storage_config)
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

/// Expand tilde (~) in path to home directory
fn expand_tilde_path(path: &str) -> DeltaResult<PathBuf> {
    if path.starts_with("~/") || path == "~" {
        let home_dir = dirs::home_dir().ok_or_else(|| {
            DeltaTableError::InvalidTableLocation(
                "Could not determine home directory for tilde expansion".to_string(),
            )
        })?;

        if path == "~" {
            Ok(home_dir)
        } else {
            let relative_path = &path[2..];
            Ok(home_dir.join(relative_path))
        }
    } else {
        Ok(PathBuf::from(path))
    }
}

/// Utility function to figure out whether string representation of the path
/// is either local path or some kind or URL.
///
/// Will return an error if the path is not valid.
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();
    let known_schemes: Vec<_> = object_store_factories()
        .iter()
        .map(|v| v.key().scheme().to_owned())
        .collect();

    match Url::parse(table_uri) {
        Ok(url) => {
            let scheme = url.scheme().to_string();
            if url.scheme() == "file" {
                Ok(UriType::LocalPath(url.to_file_path().map_err(|err| {
                    let msg = format!("Invalid table location: {table_uri}\nError: {err:?}");
                    DeltaTableError::InvalidTableLocation(msg)
                })?))
            // NOTE this check is required to support absolute windows paths which may properly parse as url
            } else if known_schemes.contains(&scheme) {
                Ok(UriType::Url(url))
            // NOTE this check is required to support absolute windows paths which may properly parse as url
            // we assume here that a single character scheme is a windows drive letter
            } else if scheme.len() == 1 {
                Ok(UriType::LocalPath(expand_tilde_path(table_uri)?))
            } else {
                Err(DeltaTableError::InvalidTableLocation(format!(
                    "Unknown scheme: {scheme}. Known schemes: {}",
                    known_schemes.join(",")
                )))
            }
        }
        Err(url_error) => {
            match url_error {
                // The RelativeUrlWithoutBase error _usually_ means this function has been called
                // with a file path looking thing.
                url::ParseError::RelativeUrlWithoutBase => {
                    Ok(UriType::LocalPath(expand_tilde_path(table_uri)?))
                }
                // All other parse errors are likely an actually broken URL that should not be
                // interpreted as anything but
                _others => Err(DeltaTableError::InvalidTableLocation(format!(
                    "Could not parse {table_uri} as a URL: {url_error}"
                ))),
            }
        }
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
/// Parse a table URI to a URL without creating directories.
/// This is useful for opening existing tables where we don't want to create directories.
pub fn parse_table_uri(table_uri: impl AsRef<str>) -> DeltaResult<Url> {
    let table_uri = table_uri.as_ref();

    let uri_type: UriType = resolve_uri_type(table_uri)?;

    let mut url = match uri_type {
        UriType::LocalPath(path) => {
            let path = std::fs::canonicalize(path).map_err(|err| {
                let msg = format!("Invalid table location: {table_uri}\nError: {err:?}");
                DeltaTableError::InvalidTableLocation(msg)
            })?;
            Url::from_directory_path(path).map_err(|_| {
                let msg = format!(
                    "Could not construct a URL from the canonical path: {table_uri}.\n\
                    Something must be very wrong with the table path.",
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

/// Will return an error if the location is not valid. For example,
/// Creates directories for local paths if they don't exist.
pub fn ensure_table_uri(table_uri: impl AsRef<str>) -> DeltaResult<Url> {
    let table_uri = table_uri.as_ref();

    let uri_type: UriType = resolve_uri_type(table_uri)?;

    // If it is a local path, we need to create it if it does not exist.
    let url = match uri_type {
        UriType::LocalPath(path) => {
            if !path.exists() {
                std::fs::create_dir_all(&path).map_err(|err| {
                    let msg =
                        format!("Could not create local directory: {table_uri}\nError: {err:?}");
                    DeltaTableError::InvalidTableLocation(msg)
                })?;
            }
            let path = std::fs::canonicalize(path).map_err(|err| {
                let msg = format!("Invalid table location: {table_uri}\nError: {err:?}");
                DeltaTableError::InvalidTableLocation(msg)
            })?;
            Url::from_directory_path(path).map_err(|_| {
                let msg = format!(
                    "Could not construct a URL from the canonical path: {table_uri}.\n\
                    Something must be very wrong with the table path.",
                );
                DeltaTableError::InvalidTableLocation(msg)
            })?
        }
        UriType::Url(url) => url,
    };

    // We should always be normalizing the table URL because trailing or redundant slashes can be
    // load bearing with [Url] and this helps ensure that a [Url] always meets our internal
    // expectations of path segments and join-ability.
    Ok(normalize_table_url(&url))
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
    use crate::logstore::factories::DefaultObjectStoreFactory;

    #[test]
    fn test_ensure_table_uri() {
        object_store_factories().insert(
            Url::parse("s3://").unwrap(),
            Arc::new(DefaultObjectStoreFactory::default()),
        );

        // parse an existing relative directory
        let uri = ensure_table_uri(".");
        assert!(uri.is_ok());
        let uri = ensure_table_uri("s3://container/path");
        assert!(uri.is_ok());
        assert_eq!(Url::parse("s3://container/path/").unwrap(), uri.unwrap());
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
                    "s3://tests/data/delta-0.8.0/",
                    "memory://",
                    "s3://bucket/my%20table/", // Doesn't double-encode
                ];
            } else {
                let roundtrip_cases = &[
                    "s3://tests/data/delta-0.8.0/",
                    "memory://",
                    "file:///",
                    "s3://bucket/my%20table/", // Doesn't double-encode
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
                "s3://tests/data/delta-0.8.0/",
            ),
            ("s3://bucket/my table", "s3://bucket/my%20table/"),
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
            ("c://", "file:///C:/"),
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
            assert_eq!(expected.as_str(), uri.as_str());
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
        let expected = Url::parse("memory:///test/tests/data/delta-0.8.0/").unwrap();
        let url = ensure_table_uri(&expected).unwrap();
        assert_eq!(expected, url);

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();
        let path = tmp_path.join("data/delta-0.8.0");
        let expected = Url::from_directory_path(path).unwrap();
        let url = ensure_table_uri(&expected).unwrap();
        assert_eq!(expected.as_str(), url.as_str());
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

            let table = DeltaTableBuilder::from_url(table_uri)
                .unwrap()
                .with_storage_options(storage_opts);
            let found_opts = table.storage_options();
            assert_eq!(expected, found_opts.get(key).unwrap());
        }
    }

    #[test]
    fn test_expand_tilde_path() {
        let home_dir = dirs::home_dir().expect("Should have home directory");

        let result = expand_tilde_path("~").unwrap();
        assert_eq!(result, home_dir);

        let result = expand_tilde_path("~/test/path").unwrap();
        assert_eq!(result, home_dir.join("test/path"));

        let result = expand_tilde_path("/absolute/path").unwrap();
        assert_eq!(result, PathBuf::from("/absolute/path"));

        let result = expand_tilde_path("relative/path").unwrap();
        assert_eq!(result, PathBuf::from("relative/path"));

        let result = expand_tilde_path("~other").unwrap();
        assert_eq!(result, PathBuf::from("~other"));
    }

    #[test]
    fn test_resolve_uri_type_with_tilde() {
        let home_dir = dirs::home_dir().expect("Should have home directory");

        match resolve_uri_type("~/test/path").unwrap() {
            UriType::LocalPath(path) => {
                assert_eq!(path, home_dir.join("test/path"));
            }
            _ => panic!("Expected LocalPath"),
        }

        match resolve_uri_type("~").unwrap() {
            UriType::LocalPath(path) => {
                assert_eq!(path, home_dir);
            }
            _ => panic!("Expected LocalPath"),
        }

        match resolve_uri_type("regular/path").unwrap() {
            UriType::LocalPath(path) => {
                assert_eq!(path, PathBuf::from("regular/path"));
            }
            _ => panic!("Expected LocalPath"),
        }
    }

    #[test]
    fn test_invalid_url_but_invalid_file_path_too() -> DeltaResult<()> {
        for wrong in &["s3://arn:aws:s3:::something", "hdfs://"] {
            let result = ensure_table_uri(wrong);
            assert!(
                result.is_err(),
                "Expected {wrong} parsed into {result:#?} to return an error because I gave it something URLish"
            );
        }
        Ok(())
    }

    #[test]
    fn test_ensure_table_uri_with_tilde() {
        let home_dir = dirs::home_dir().expect("Should have home directory");

        let test_dir = home_dir.join("delta_test_temp");
        std::fs::create_dir_all(&test_dir).ok();

        let tilde_path = "~/delta_test_temp";
        let result = ensure_table_uri(tilde_path);
        assert!(
            result.is_ok(),
            "ensure_table_uri should work with tilde paths"
        );

        let url = result.unwrap();
        assert!(!url.as_str().contains("~"));

        #[cfg(windows)]
        {
            let home_dir_normalized = home_dir.to_string_lossy().replace('\\', "/");
            assert!(url.as_str().contains(&home_dir_normalized));
        }

        #[cfg(not(windows))]
        {
            assert!(url.as_str().contains(home_dir.to_string_lossy().as_ref()));
        }

        std::fs::remove_dir_all(&test_dir).ok();
    }
}
