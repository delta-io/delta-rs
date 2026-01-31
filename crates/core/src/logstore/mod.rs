//! # DeltaLake storage system
//!
//! Interacting with storage systems is a crucial part of any table format.
//! On one had the storage abstractions need to provide certain guarantees
//! (e.g. atomic rename, ...) and meet certain assumptions (e.g. sorted list results)
//! on the other hand can we exploit our knowledge about the general file layout
//! and access patterns to optimize our operations in terms of cost and performance.
//!
//! Two distinct phases are involved in querying a Delta table:
//! - **Metadata**: Fetching metadata about the table, such as schema, partitioning, and statistics.
//! - **Data**: Reading and processing data files based on the metadata.
//!
//! When writing to a table, we see the same phases, just in inverse order:
//! - **Data**: Writing data files that should become part of the table.
//! - **Metadata**: Updating table metadata to incorporate updates.
//!
//! Two main abstractions govern the file operations [`LogStore`] and [`ObjectStore`].
//!
//! [`LogStore`]s are scoped to individual tables and are responsible for maintaining proper
//! behaviours and ensuring consistency during the metadata phase. The correctness is predicated
//! on the atomicity and durability guarantees of the implementation of this interface.
//!
//! - Atomic visibility: Partial writes must not be visible to readers.
//! - Mutual exclusion: Only one writer must be able to write to a specific log file.
//! - Consistent listing: Once a file has been written, any future list files operation must return
//!   the underlying file system entry must immediately.
//!
//! <div class="warning">
//!
//! While most object stores today provide the required guarantees, the specific
//! locking mechanics are a table level responsibility. Specific implementations may
//! decide to refer to a central catalog or other mechanisms for coordination.
//!
//! </div>
//!
//! [`ObjectStore`]s are responsible for direct interactions with storage systems. Either
//! during the data phase, where additional requirements are imposed on the storage system,
//! or by specific LogStore implementations for their internal object store interactions.
//!
//! ## Managing LogStores and ObjectStores.
//!
//! Aside from very basic implementations (i.e. in-memory and local file system) we rely
//! on external integrations to provide [`ObjectStore`] and/or [`LogStore`] implementations.
//!
//! At runtime, deltalake needs to produce appropriate [`ObjectStore`]s to access the files
//! discovered in a table. This is done via
//!
//! ## Configuration
//!
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
#[cfg(feature = "datafusion")]
use datafusion::datasource::object_store::ObjectStoreUrl;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::log_segment::LogSegment;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::{AsAny, Engine};
use futures::StreamExt;
use object_store::ObjectStoreScheme;
use object_store::{Error as ObjectStoreError, ObjectStore, path::Path};
use regex::Regex;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tokio::runtime::RuntimeFlavor;
use tracing::*;
use url::Url;
use uuid::Uuid;

use crate::kernel::transaction::TransactionError;
use crate::kernel::{Action, spawn_blocking_with_span};
use crate::table::normalize_table_url;
use crate::{DeltaResult, DeltaTableError};

pub use self::config::StorageConfig;
pub use self::factories::{
    LogStoreFactory, LogStoreFactoryRegistry, ObjectStoreFactory, ObjectStoreFactoryRegistry,
    logstore_factories, object_store_factories, store_for,
};
pub use self::storage::utils::commit_uri_from_version;
pub use self::storage::{
    DefaultObjectStoreRegistry, DeltaIOStorageBackend, IORuntime, ObjectStoreRef,
    ObjectStoreRegistry, ObjectStoreRetryExt,
};
/// Convenience re-export of the object store crate
pub use ::object_store;

pub mod config;
pub(crate) mod default_logstore;
pub(crate) mod factories;
pub(crate) mod storage;

/// Internal trait to handle object store configuration and initialization.
trait LogStoreFactoryExt {
    /// Create a new log store with the given options.
    ///
    /// ## Parameters
    ///
    /// - `root_store`: and instance of [`ObjectStoreRef`] with no prefix o.a. applied.
    ///   I.e. pointing to the root of the object store.
    /// - `location`: The location of the delta table (where the `_delta_log` directory is).
    /// - `options`: The options for the log store.
    fn with_options_internal(
        &self,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<LogStoreRef>;
}

impl<T: LogStoreFactory + ?Sized> LogStoreFactoryExt for T {
    fn with_options_internal(
        &self,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<LogStoreRef> {
        let prefixed_store = options.decorate_store(root_store.clone(), location)?;
        let log_store =
            self.with_options(Arc::new(prefixed_store), root_store, location, options)?;
        Ok(log_store)
    }
}

impl<T: LogStoreFactory> LogStoreFactoryExt for Arc<T> {
    fn with_options_internal(
        &self,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<LogStoreRef> {
        T::with_options_internal(self, root_store, location, options)
    }
}

/// Return the [DefaultLogStore] implementation with the provided configuration options
pub fn default_logstore(
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    location: &Url,
    options: &StorageConfig,
) -> Arc<dyn LogStore> {
    Arc::new(default_logstore::DefaultLogStore::new(
        prefixed_store,
        root_store,
        LogStoreConfig::new(location, options.clone()),
    ))
}

/// Sharable reference to [`LogStore`]
pub type LogStoreRef = Arc<dyn LogStore>;

static DELTA_LOG_PATH: LazyLock<Path> = LazyLock::new(|| Path::from("_delta_log"));

pub(crate) static DELTA_LOG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d{20})\.(json|checkpoint(\.\d+)?\.parquet)$").unwrap());

/// Return the [LogStoreRef] for the provided [Url] location
///
/// This will use the built-in process global [crate::storage::ObjectStoreRegistry] by default
///
/// ```rust
/// # use deltalake_core::logstore::*;
/// # use std::collections::HashMap;
/// # use url::Url;
/// let location = Url::parse("memory:///").expect("Failed to make location");
/// let storage_config = StorageConfig::default();
/// let logstore = logstore_for(&location, storage_config).expect("Failed to get a logstore");
/// ```
pub fn logstore_for(location: &Url, storage_config: StorageConfig) -> DeltaResult<LogStoreRef> {
    // turn location into scheme
    let scheme = Url::parse(&format!("{}://", location.scheme()))
        .map_err(|_| DeltaTableError::InvalidTableLocation(location.clone().into()))?;

    if let Some(entry) = object_store_factories().get(&scheme) {
        debug!("Found a storage provider for {scheme} ({location})");
        let (root_store, _prefix) = entry.value().parse_url_opts(location, &storage_config)?;
        return logstore_with(root_store, location, storage_config);
    }

    Err(DeltaTableError::InvalidTableLocation(location.to_string()))
}

/// Return the [LogStoreRef] using the given [ObjectStoreRef]
pub fn logstore_with(
    root_store: ObjectStoreRef,
    location: &Url,
    storage_config: StorageConfig,
) -> DeltaResult<LogStoreRef> {
    let scheme = Url::parse(&format!("{}://", location.scheme()))
        .map_err(|_| DeltaTableError::InvalidTableLocation(location.clone().into()))?;

    if let Some(factory) = logstore_factories().get(&scheme) {
        debug!("Found a logstore provider for {scheme}");
        return factory
            .value()
            .with_options_internal(root_store, location, &storage_config);
    }

    error!("Could not find a logstore for the scheme {scheme}");
    Err(DeltaTableError::InvalidTableLocation(location.to_string()))
}

/// Holder whether it's tmp_commit path or commit bytes
#[derive(Clone)]
pub enum CommitOrBytes {
    /// Path of the tmp commit, to be used by logstores which use CopyIfNotExists
    TmpCommit(Path),
    /// Bytes of the log, to be used by logstoers which use Conditional Put
    LogBytes(Bytes),
}

/// Configuration parameters for a log store
#[derive(Debug, Clone)]
pub struct LogStoreConfig {
    /// url corresponding to the storage location.
    location: Url,
    // Options used for configuring backend storage
    options: StorageConfig,
}

impl LogStoreConfig {
    pub fn new(location: &Url, options: StorageConfig) -> Self {
        let location = normalize_table_url(location);
        Self { location, options }
    }

    pub fn location(&self) -> &Url {
        &self.location
    }

    pub fn options(&self) -> &StorageConfig {
        &self.options
    }

    pub fn decorate_store<T: ObjectStore + Clone>(
        &self,
        store: T,
        table_root: Option<&url::Url>,
    ) -> DeltaResult<Box<dyn ObjectStore>> {
        let table_url = table_root.unwrap_or(&self.location);
        self.options.decorate_store(store, table_url)
    }

    pub fn object_store_factory(&self) -> ObjectStoreFactoryRegistry {
        self::factories::object_store_factories()
    }
}

/// Trait for critical operations required to read and write commit entries in Delta logs.
///
/// The correctness is predicated on the atomicity and durability guarantees of
/// the implementation of this interface. Specifically,
///
/// - Atomic visibility: Any commit created via `write_commit_entry` must become visible atomically.
/// - Mutual exclusion: Only one writer must be able to create a commit for a specific version.
/// - Consistent listing: Once a commit entry for version `v` has been written, any future call to
///   `get_latest_version` must return a version >= `v`, i.e. the underlying file system entry must
///   become visible immediately.
#[async_trait::async_trait]
pub trait LogStore: Send + Sync + AsAny {
    /// Return the name of this LogStore implementation
    fn name(&self) -> String;

    /// Trigger sync operation on log store to.
    async fn refresh(&self) -> DeltaResult<()> {
        Ok(())
    }

    /// Read data for commit entry with the given version.
    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>>;

    /// Write list of actions as delta commit entry for given version.
    ///
    /// This operation can be retried with a higher version in case the write
    /// fails with [`TransactionError::VersionAlreadyExists`].
    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError>;

    /// Abort the commit entry for the given version.
    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError>;

    /// Find latest version currently stored in the delta log.
    async fn get_latest_version(&self, start_version: i64) -> DeltaResult<i64>;

    /// Get object store, can pass operation_id for object stores linked to an operation
    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore>;

    fn root_object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore>;

    fn engine(&self, operation_id: Option<Uuid>) -> Arc<dyn Engine> {
        let store = self.root_object_store(operation_id);
        get_engine(store)
    }

    /// [Path] to Delta log
    fn to_uri(&self, location: &Path) -> String {
        let root = &self.config().location;
        to_uri(root, location)
    }

    /// Get fully qualified uri for table root
    fn root_url(&self) -> &Url {
        &self.config().location
    }

    /// [Path] to Delta log
    fn log_path(&self) -> &Path {
        &DELTA_LOG_PATH
    }

    /// Generate the appropriate [Url] to use for executing an operation.
    ///
    ///  This can be useful for branching LogStore implementations such as LakeFS which may return
    ///  something other than the base URL.
    fn transaction_url(&self, _operation_id: Option<Uuid>) -> DeltaResult<Url> {
        Ok(self.config().location().clone())
    }

    /// Check if the location is a delta table location
    async fn is_delta_table_location(&self) -> DeltaResult<bool> {
        let object_store = self.object_store(None);
        let dummy_url = Url::parse("http://example.com").unwrap();
        let log_path = Path::from("_delta_log");

        let mut stream = object_store.list(Some(&log_path));
        while let Some(res) = stream.next().await {
            match res {
                Ok(meta) => {
                    let file_url = dummy_url.join(meta.location.as_ref()).unwrap();
                    if let Ok(Some(parsed_path)) = ParsedLogPath::try_from(file_url)
                        && matches!(
                            parsed_path.file_type,
                            LogPathFileType::Commit
                                | LogPathFileType::SinglePartCheckpoint
                                | LogPathFileType::UuidCheckpoint
                                | LogPathFileType::MultiPartCheckpoint { .. }
                                | LogPathFileType::CompactedCommit { .. }
                        )
                    {
                        return Ok(true);
                    }
                    continue;
                }
                Err(ObjectStoreError::NotFound { .. }) => return Ok(false),
                Err(err) => return Err(err.into()),
            }
        }

        Ok(false)
    }

    /// Get configuration representing configured log store.
    fn config(&self) -> &LogStoreConfig;

    #[cfg(feature = "datafusion")]
    /// Generate a unique enough url to identify the store in datafusion.
    /// The DF object store registry only cares about the scheme and the host of the url for
    /// registering/fetching. In our case the scheme is hard-coded to "delta-rs", so to get a unique
    /// host we convert the location from this `LogStore` to a valid name, combining the
    /// original scheme, host and path with invalid characters replaced.
    ///
    /// This is a legacy/migration helper for delta-rs DataFusion integrations that use a
    /// synthetic per-table `delta-rs://...` URL mapping to a table-scoped (prefixed) object store.
    /// It will not work correctly with fully-qualified file URLs (e.g. shallow clones).
    fn object_store_url(&self) -> ObjectStoreUrl {
        crate::logstore::object_store_url(&self.config().location)
    }
}

/// Extension trait for LogStore to handle some internal invariants.
pub(crate) trait LogStoreExt: LogStore {
    /// The fully qualified table URL
    ///
    /// The paths is guaranteed to end with a slash,
    /// so that it can be used as a prefix for other paths.
    fn table_root_url(&self) -> Url {
        let mut base = self.config().location.clone();
        if !base.path().ends_with("/") {
            base.set_path(&format!("{}/", base.path()));
        }
        base
    }

    /// The fully qualified table log URL
    ///
    /// The paths is guaranteed to end with a slash,
    /// so that it can be used as a prefix for other paths.
    fn log_root_url(&self) -> Url {
        self.table_root_url().join("_delta_log/").unwrap()
    }
}

impl<T: LogStore + ?Sized> LogStoreExt for T {}

#[async_trait::async_trait]
impl<T: LogStore + ?Sized> LogStore for Arc<T> {
    fn name(&self) -> String {
        T::name(self)
    }

    async fn refresh(&self) -> DeltaResult<()> {
        T::refresh(self).await
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        T::read_commit_entry(self, version).await
    }

    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        T::write_commit_entry(self, version, commit_or_bytes, operation_id).await
    }

    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        T::abort_commit_entry(self, version, commit_or_bytes, operation_id).await
    }

    async fn get_latest_version(&self, start_version: i64) -> DeltaResult<i64> {
        T::get_latest_version(self, start_version).await
    }

    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        T::object_store(self, operation_id)
    }

    fn root_object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        T::root_object_store(self, operation_id)
    }

    fn engine(&self, operation_id: Option<Uuid>) -> Arc<dyn Engine> {
        T::engine(self, operation_id)
    }

    fn to_uri(&self, location: &Path) -> String {
        T::to_uri(self, location)
    }

    fn root_url(&self) -> &Url {
        T::root_url(self)
    }

    fn log_path(&self) -> &Path {
        T::log_path(self)
    }

    async fn is_delta_table_location(&self) -> DeltaResult<bool> {
        T::is_delta_table_location(self).await
    }

    fn config(&self) -> &LogStoreConfig {
        T::config(self)
    }

    #[cfg(feature = "datafusion")]
    fn object_store_url(&self) -> ObjectStoreUrl {
        T::object_store_url(self)
    }
}

pub(crate) fn get_engine(store: Arc<dyn ObjectStore>) -> Arc<dyn Engine> {
    let handle = tokio::runtime::Handle::current();
    match handle.runtime_flavor() {
        RuntimeFlavor::MultiThread => Arc::new(DefaultEngine::new_with_executor(
            store,
            Arc::new(TokioMultiThreadExecutor::new(handle)),
        )),
        RuntimeFlavor::CurrentThread => Arc::new(DefaultEngine::new_with_executor(
            store,
            Arc::new(TokioBackgroundExecutor::new()),
        )),
        _ => panic!("unsupported runtime flavor"),
    }
}

#[cfg(feature = "datafusion")]
fn object_store_url(location: &Url) -> ObjectStoreUrl {
    use object_store::path::DELIMITER;

    // azure storage urls encode the container as user in the url
    let user_at = match location.username() {
        u if !u.is_empty() => format!("{u}@"),
        _ => "".to_string(),
    };

    ObjectStoreUrl::parse(format!(
        "delta-rs://{}{}-{}{}",
        user_at,
        location.scheme(),
        location.host_str().unwrap_or("-"),
        location.path().replace(DELIMITER, "-").replace(':', "-")
    ))
    .expect("Invalid object store url.")
}

/// Parse the path from a URL accounting for special case witjh S3
// TODO: find out why this is necessary
pub(crate) fn object_store_path(table_root: &Url) -> DeltaResult<Path> {
    Ok(match ObjectStoreScheme::parse(table_root) {
        Ok((_, path)) => path,
        _ => Path::parse(table_root.path())?,
    })
}

/// Join the given `root` [Url] with the [Path] to produce a URI (String) of the two together.
///
/// This is largely a convenience function to help with the nuances of empty [Path] and file [Url]s
pub fn to_uri(root: &Url, location: &Path) -> String {
    match root.scheme() {
        "file" => {
            #[cfg(windows)]
            let uri = format!(
                "{}/{}",
                root.as_ref().trim_end_matches('/'),
                location.as_ref()
            )
            .replace("file:///", "");
            #[cfg(unix)]
            let uri = format!(
                "{}/{}",
                root.as_ref().trim_end_matches('/'),
                location.as_ref()
            )
            .replace("file://", "");
            uri
        }
        _ => {
            if location.as_ref().is_empty() || location.as_ref() == "/" {
                root.as_ref().to_string()
            } else {
                root.join(location.as_ref())
                    .expect("Somehow failed to join on a Url!")
                    .to_string()
            }
        }
    }
}

/// Reads a commit and gets list of actions
pub fn get_actions(
    version: i64,
    commit_log_bytes: &bytes::Bytes,
) -> Result<Vec<Action>, DeltaTableError> {
    debug!("parsing commit with version {version}...");
    Deserializer::from_slice(commit_log_bytes)
        .into_iter::<Action>()
        .map(|result| {
            result.map_err(|e| {
                let line = format!("Error at line {}, column {}", e.line(), e.column());
                DeltaTableError::InvalidJsonLog {
                    json_err: e,
                    line,
                    version,
                }
            })
        })
        .collect()
}

impl std::fmt::Debug for dyn LogStore + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name(), self.root_url())
    }
}

impl Serialize for LogStoreConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.location.to_string())?;
        seq.serialize_element(&self.options.raw)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for LogStoreConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LogStoreConfigVisitor {}

        impl<'de> Visitor<'de> for LogStoreConfigVisitor {
            type Value = LogStoreConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct LogStoreConfig")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let location_str: String = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let options: HashMap<String, String> = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let location = Url::parse(&location_str).map_err(A::Error::custom)?;
                Ok(LogStoreConfig {
                    location,
                    options: StorageConfig::parse_options(options).map_err(A::Error::custom)?,
                })
            }
        }

        deserializer.deserialize_seq(LogStoreConfigVisitor {})
    }
}

/// Extract version from a file name in the delta log
pub fn extract_version_from_filename(name: &str) -> Option<i64> {
    DELTA_LOG_REGEX
        .captures(name)
        .map(|captures| captures.get(1).unwrap().as_str().parse().unwrap())
}

/// Default implementation for retrieving the latest version
pub async fn get_latest_version(
    log_store: &dyn LogStore,
    current_version: i64,
) -> DeltaResult<i64> {
    let current_version = if current_version < 0 {
        0
    } else {
        current_version
    };

    let storage = log_store.engine(None).storage_handler();
    let log_root = log_store.log_root_url();

    let segment = spawn_blocking_with_span(move || {
        LogSegment::for_table_changes(storage.as_ref(), log_root, current_version as u64, None)
    })
    .await
    .map_err(|e| DeltaTableError::Generic(e.to_string()))?
    .map_err(|e| {
        if e.to_string()
            .contains(&format!("to have version {current_version}"))
        {
            DeltaTableError::InvalidVersion(current_version)
        } else {
            DeltaTableError::Generic(e.to_string())
        }
    })?;

    Ok(segment.end_version as i64)
}

/// Read delta log for a specific version
#[instrument(skip(storage), fields(version = version, path = %commit_uri_from_version(version)))]
pub async fn read_commit_entry(
    storage: &dyn ObjectStore,
    version: i64,
) -> DeltaResult<Option<Bytes>> {
    let commit_uri = commit_uri_from_version(version);
    match storage.get(&commit_uri).await {
        Ok(res) => {
            let bytes = res.bytes().await?;
            debug!(size = bytes.len(), "commit entry read successfully");
            Ok(Some(bytes))
        }
        Err(ObjectStoreError::NotFound { .. }) => {
            debug!("commit entry not found");
            Ok(None)
        }
        Err(err) => {
            error!(error = %err, version = version, "failed to read commit entry");
            Err(err.into())
        }
    }
}

/// Default implementation for writing a commit entry
#[instrument(skip(storage), fields(version = version, tmp_path = %tmp_commit, commit_path = %commit_uri_from_version(version)))]
pub async fn write_commit_entry(
    storage: &dyn ObjectStore,
    version: i64,
    tmp_commit: &Path,
) -> Result<(), TransactionError> {
    // move temporary commit file to delta log directory
    // rely on storage to fail if the file already exists -
    storage
        .rename_if_not_exists(tmp_commit, &commit_uri_from_version(version))
        .await
        .map_err(|err| -> TransactionError {
            match err {
                ObjectStoreError::AlreadyExists { .. } => {
                    warn!("commit entry already exists");
                    TransactionError::VersionAlreadyExists(version)
                }
                _ => {
                    error!(error = %err, "failed to write commit entry");
                    TransactionError::from(err)
                }
            }
        })?;
    debug!("commit entry written successfully");
    Ok(())
}

/// Default implementation for aborting a commit entry
#[instrument(skip(storage), fields(version = _version, tmp_path = %tmp_commit))]
pub async fn abort_commit_entry(
    storage: &dyn ObjectStore,
    _version: i64,
    tmp_commit: &Path,
) -> Result<(), TransactionError> {
    storage.delete_with_retries(tmp_commit, 15).await?;
    debug!("commit entry aborted successfully");
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_logstore_config_ctor() {
        let location = Url::parse("nonexistent://table/bar").unwrap();
        let config = LogStoreConfig::new(&location, StorageConfig::default());
        assert_eq!(config.location().to_string(), "nonexistent://table/bar/");
        assert_eq!(
            config.location().join("_delta_log/").unwrap(),
            Url::parse("nonexistent://table/bar/_delta_log/").unwrap()
        );
    }

    #[test]
    fn logstore_with_invalid_url() {
        let location = Url::parse("nonexistent://table").unwrap();

        let store = logstore_for(&location, StorageConfig::default());
        assert!(store.is_err());
    }

    #[test]
    fn logstore_with_memory() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(&location, StorageConfig::default());
        assert!(store.is_ok());
    }

    #[test]
    fn logstore_with_memory_and_rt() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(
            &location,
            StorageConfig::default().with_io_runtime(IORuntime::default()),
        );
        assert!(store.is_ok());
    }

    #[test]
    fn test_logstore_ext() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(&location, StorageConfig::default()).unwrap();
        let table_url = store.table_root_url();
        assert!(table_url.path().ends_with('/'));
        let log_url = store.log_root_url();
        assert!(log_url.path().ends_with("_delta_log/"));
    }

    #[tokio::test]
    async fn test_is_location_a_table() {
        use object_store::path::Path;
        use object_store::{PutOptions, PutPayload};
        let location = Url::parse("memory:///table").unwrap();
        let store =
            logstore_for(&location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(
            !store
                .is_delta_table_location()
                .await
                .expect("Failed to look at table")
        );

        // Let's put a failed commit into the directory and then see if it's still considered a
        // delta table (it shouldn't be).
        let payload = PutPayload::from_static(b"test-drivin");
        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/_commit_failed.tmp"),
                payload,
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");
        assert!(
            !store
                .is_delta_table_location()
                .await
                .expect("Failed to look at table")
        );
    }

    #[tokio::test]
    async fn test_is_location_a_table_commit() {
        use object_store::path::Path;
        use object_store::{PutOptions, PutPayload};
        let location = Url::parse("memory:///table").unwrap();
        let store =
            logstore_for(&location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(
            !store
                .is_delta_table_location()
                .await
                .expect("Failed to identify table")
        );

        // Save a commit to the transaction log
        let payload = PutPayload::from_static(b"test");
        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/00000000000000000000.json"),
                payload,
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");
        // The table should be considered a delta table
        assert!(
            store
                .is_delta_table_location()
                .await
                .expect("Failed to identify table")
        );
    }

    #[tokio::test]
    async fn test_is_location_a_table_checkpoint() {
        use object_store::path::Path;
        use object_store::{PutOptions, PutPayload};
        let location = Url::parse("memory:///table").unwrap();
        let store =
            logstore_for(&location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(
            !store
                .is_delta_table_location()
                .await
                .expect("Failed to identify table")
        );

        // Save a "checkpoint" file to the transaction log directory
        let payload = PutPayload::from_static(b"test");
        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/00000000000000000000.checkpoint.parquet"),
                payload,
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");
        // The table should be considered a delta table
        assert!(
            store
                .is_delta_table_location()
                .await
                .expect("Failed to identify table")
        );
    }

    #[tokio::test]
    async fn test_is_location_a_table_crc() {
        use object_store::path::Path;
        use object_store::{PutOptions, PutPayload};
        let location = Url::parse("memory:///table").unwrap();
        let store =
            logstore_for(&location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(
            !store
                .is_delta_table_location()
                .await
                .expect("Failed to identify table")
        );

        // Save .crc files to the transaction log directory (all 3 formats)
        let payload = PutPayload::from_static(b"test");

        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/.00000000000000000000.crc.crc"),
                payload.clone(),
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");

        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/.00000000000000000000.json.crc"),
                payload.clone(),
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");

        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/00000000000000000000.crc"),
                payload.clone(),
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");

        // Now add a commit
        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/00000000000000000000.json"),
                payload.clone(),
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");

        // The table should be considered a delta table
        assert!(
            store
                .is_delta_table_location()
                .await
                .expect("Failed to identify table")
        );
    }

    /// Collect list stream
    pub(crate) async fn flatten_list_stream(
        storage: &object_store::DynObjectStore,
        prefix: Option<&Path>,
    ) -> object_store::Result<Vec<Path>> {
        storage
            .list(prefix)
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
    }
}

#[cfg(all(test, feature = "datafusion"))]
mod datafusion_tests {
    use super::*;
    use url::Url;

    #[tokio::test]
    async fn test_unique_object_store_url() {
        for (location_1, location_2) in [
            // Same scheme, no host, different path
            ("file:///path/to/table_1", "file:///path/to/table_2"),
            // Different scheme/host, same path
            ("s3://my_bucket/path/to/table_1", "file:///path/to/table_1"),
            // Same scheme, different host, same path
            ("s3://bucket_1/table_1", "s3://bucket_2/table_1"),
            // Azure urls should encode the container
            (
                "abfss://container1@host/table_1",
                "abfss://container2@host/table_1",
            ),
        ] {
            let url_1 = Url::parse(location_1).unwrap();
            let url_2 = Url::parse(location_2).unwrap();

            assert_ne!(
                object_store_url(&url_1).as_str(),
                object_store_url(&url_2).as_str(),
            );
        }
    }

    #[tokio::test]
    async fn test_get_actions_malformed_json() {
        let malformed_json = bytes::Bytes::from(
            r#"{"add": {"path": "test.parquet", "partitionValues": {}, "size": 100, "modificationTime": 1234567890, "dataChange": true}}
{"invalid json without closing brace"#,
        );

        let result = get_actions(0, &malformed_json);

        match result {
            Err(DeltaTableError::InvalidJsonLog {
                line,
                version,
                json_err,
            }) => {
                assert_eq!(version, 0);
                assert!(line.contains("line 2"));
                assert!(json_err.is_eof());
            }
            other => panic!("Expected InvalidJsonLog error, got {:?}", other),
        }
    }
}
