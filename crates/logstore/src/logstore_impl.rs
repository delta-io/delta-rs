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
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::log_segment::LogSegment;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::{AsAny, Engine};
use deltalake_types::Action;
use futures::StreamExt;
use object_store::ObjectStoreScheme;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use regex::Regex;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tokio::runtime::RuntimeFlavor;
use tracing::*;
use url::Url;
use uuid::Uuid;

use tokio::task::spawn_blocking;
use tracing::dispatcher;
use tracing::Span;

use crate::config::StorageConfig;
use crate::default_logstore::DefaultLogStore;
use crate::error::{LogStoreError, LogStoreResult};
use crate::factories::{
    logstore_factories, object_store_factories, store_for, LogStoreFactory,
    LogStoreFactoryRegistry, ObjectStoreFactory, ObjectStoreFactoryRegistry,
};
use crate::storage::utils::commit_uri_from_version;
use crate::storage::{
    DefaultObjectStoreRegistry, DeltaIOStorageBackend, IORuntime, ObjectStoreRef,
    ObjectStoreRegistry, ObjectStoreRetryExt,
};

/// Helper function to spawn blocking tasks with tracing span context
pub(crate) fn spawn_blocking_with_span<F, R>(f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // Capture the current dispatcher and span
    let dispatch = dispatcher::get_default(|d| d.clone());
    let span = Span::current();

    spawn_blocking(move || {
        dispatcher::with_default(&dispatch, || {
            let _enter = span.enter();
            f()
        })
    })
}

/// Internal trait to handle object store configuration and initialization.
trait LogStoreFactoryExt {
    /// Create a new log store with the given options.
    ///
    /// ## Parameters
    ///
    /// - `root_store`: and instance of [`ObjectStoreRef`] with no prefix o.a. applied.
    ///   I.e. pointing to the root of the onject store.
    /// - `location`: The location of the delta table (where the `_delta_log` directory is).
    /// - `options`: The options for the log store.
    fn with_options_internal(
        &self,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> LogStoreResult<LogStoreRef>;
}

impl<T: LogStoreFactory + ?Sized> LogStoreFactoryExt for T {
    fn with_options_internal(
        &self,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> LogStoreResult<LogStoreRef> {
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
    ) -> LogStoreResult<LogStoreRef> {
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
    Arc::new(DefaultLogStore::new(
        prefixed_store,
        root_store,
        LogStoreConfig {
            location: location.clone(),
            options: options.clone(),
        },
    ))
}

/// Sharable reference to [`LogStore`]
pub type LogStoreRef = Arc<dyn LogStore>;

pub static DELTA_LOG_PATH: LazyLock<Path> = LazyLock::new(|| Path::from("_delta_log"));

pub static DELTA_LOG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d{20})\.(json|checkpoint(\.\d+)?\.parquet)$").unwrap());

/// Return the [LogStoreRef] for the provided [Url] location
///
/// This will use the built-in process global [crate::storage::ObjectStoreRegistry] by default
///
/// ```rust
/// # use deltalake_logstore::*;
/// # use std::collections::HashMap;
/// # use url::Url;
/// let location = Url::parse("memory:///").expect("Failed to make location");
/// let storage_config = StorageConfig::default();
/// let logstore = logstore_for(location, storage_config).expect("Failed to get a logstore");
/// ```
pub fn logstore_for(location: Url, storage_config: StorageConfig) -> LogStoreResult<LogStoreRef> {
    // turn location into scheme
    let scheme = Url::parse(&format!("{}://", location.scheme()))
        .map_err(|_| LogStoreError::InvalidTableLocation(location.to_string()))?;

    if let Some(entry) = object_store_factories().get(&scheme) {
        debug!("Found a storage provider for {scheme} ({location})");
        let (root_store, _prefix) = entry.value().parse_url_opts(&location, &storage_config)?;
        return logstore_with(root_store, location, storage_config);
    }

    Err(LogStoreError::InvalidTableLocation(location.to_string()))
}

/// Return the [LogStoreRef] using the given [ObjectStoreRef]
pub fn logstore_with(
    root_store: ObjectStoreRef,
    location: Url,
    storage_config: StorageConfig,
) -> LogStoreResult<LogStoreRef> {
    let scheme = Url::parse(&format!("{}://", location.scheme()))
        .map_err(|_| LogStoreError::InvalidTableLocation(location.to_string()))?;

    if let Some(factory) = logstore_factories().get(&scheme) {
        debug!("Found a logstore provider for {scheme}");
        return factory
            .value()
            .with_options_internal(root_store, &location, &storage_config);
    }

    error!("Could not find a logstore for the scheme {scheme}");
    Err(LogStoreError::InvalidTableLocation(location.to_string()))
}

/// Holder whether it's tmp_commit path or commit bytes
#[derive(Clone)]
pub enum CommitOrBytes {
    /// Path of the tmp commit, to be used by logstores which use CopyIfNotExists
    TmpCommit(Path),
    /// Bytes of the log, to be used by logstoers which use Conditional Put
    LogBytes(Bytes),
}

/// The next commit that's available from underlying storage
///
#[derive(Debug)]
pub enum PeekCommit {
    /// The next commit version and associated actions
    New(i64, Vec<Action>),
    /// Provided DeltaVersion is up to date
    UpToDate,
}

/// Configuration parameters for a log store
#[derive(Debug, Clone)]
pub struct LogStoreConfig {
    /// url corresponding to the storage location.
    pub location: Url,
    // Options used for configuring backend storage
    pub options: StorageConfig,
}

impl LogStoreConfig {
    pub fn decorate_store<T: ObjectStore + Clone>(
        &self,
        store: T,
        table_root: Option<&url::Url>,
    ) -> LogStoreResult<Box<dyn ObjectStore>> {
        let table_url = table_root.unwrap_or(&self.location);
        self.options.decorate_store(store, table_url)
    }

    pub fn object_store_factory(&self) -> ObjectStoreFactoryRegistry {
        crate::factories::object_store_factories()
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
    async fn refresh(&self) -> LogStoreResult<()> {
        Ok(())
    }

    /// Read data for commit entry with the given version.
    async fn read_commit_entry(&self, version: i64) -> LogStoreResult<Option<Bytes>>;

    /// Write list of actions as delta commit entry for given version.
    ///
    /// This operation can be retried with a higher version in case the write
    /// fails with [`LogStoreError::VersionAlreadyExists`].
    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), LogStoreError>;

    /// Abort the commit entry for the given version.
    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), LogStoreError>;

    /// Find latest version currently stored in the delta log.
    async fn get_latest_version(&self, start_version: i64) -> LogStoreResult<i64>;

    /// Get the actions for the next commit
    async fn peek_next_commit(&self, current_version: i64) -> LogStoreResult<PeekCommit> {
        let next_version = current_version + 1;
        let commit_log_bytes = match self.read_commit_entry(next_version).await {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => return Ok(PeekCommit::UpToDate),
            Err(err) => Err(err),
        }?;

        let actions = crate::get_actions(next_version, &commit_log_bytes)?;
        Ok(PeekCommit::New(next_version, actions))
    }

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
    fn root_uri(&self) -> String {
        self.to_uri(&Path::from(""))
    }

    /// [Path] to Delta log
    fn log_path(&self) -> &Path {
        &DELTA_LOG_PATH
    }

    #[deprecated(
        since = "0.1.0",
        note = "DO NOT USE: Just a stop gap to support lakefs during kernel migration"
    )]
    fn transaction_url(&self, _operation_id: Uuid, base: &Url) -> LogStoreResult<Url> {
        Ok(base.clone())
    }

    /// Check if the location is a delta table location
    async fn is_delta_table_location(&self) -> LogStoreResult<bool> {
        let object_store = self.object_store(None);
        let dummy_url = Url::parse("http://example.com").unwrap();
        let log_path = Path::from("_delta_log");

        let mut stream = object_store.list(Some(&log_path));
        while let Some(res) = stream.next().await {
            match res {
                Ok(meta) => {
                    let file_url = dummy_url.join(meta.location.as_ref()).unwrap();
                    if let Ok(Some(parsed_path)) = ParsedLogPath::try_from(file_url) {
                        if matches!(
                            parsed_path.file_type,
                            LogPathFileType::Commit
                                | LogPathFileType::SinglePartCheckpoint
                                | LogPathFileType::UuidCheckpoint(_)
                                | LogPathFileType::MultiPartCheckpoint { .. }
                                | LogPathFileType::CompactedCommit { .. }
                        ) {
                            return Ok(true);
                        }
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
}

/// Extension trait for LogStore to handle some internal invariants.
pub trait LogStoreExt: LogStore {
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

    async fn refresh(&self) -> LogStoreResult<()> {
        T::refresh(self).await
    }

    async fn read_commit_entry(&self, version: i64) -> LogStoreResult<Option<Bytes>> {
        T::read_commit_entry(self, version).await
    }

    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), LogStoreError> {
        T::write_commit_entry(self, version, commit_or_bytes, operation_id).await
    }

    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), LogStoreError> {
        T::abort_commit_entry(self, version, commit_or_bytes, operation_id).await
    }

    async fn get_latest_version(&self, start_version: i64) -> LogStoreResult<i64> {
        T::get_latest_version(self, start_version).await
    }

    async fn peek_next_commit(&self, current_version: i64) -> LogStoreResult<PeekCommit> {
        T::peek_next_commit(self, current_version).await
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

    fn root_uri(&self) -> String {
        T::root_uri(self)
    }

    fn log_path(&self) -> &Path {
        T::log_path(self)
    }

    async fn is_delta_table_location(&self) -> LogStoreResult<bool> {
        T::is_delta_table_location(self).await
    }

    fn config(&self) -> &LogStoreConfig {
        T::config(self)
    }
}

pub(crate) fn get_engine(store: Arc<dyn ObjectStore>) -> Arc<dyn Engine> {
    let handle = tokio::runtime::Handle::current();
    match handle.runtime_flavor() {
        RuntimeFlavor::MultiThread => Arc::new(DefaultEngine::new(
            store,
            Arc::new(TokioMultiThreadExecutor::new(handle)),
        )),
        RuntimeFlavor::CurrentThread => Arc::new(DefaultEngine::new(
            store,
            Arc::new(TokioBackgroundExecutor::new()),
        )),
        _ => panic!("unsupported runtime flavor"),
    }
}

/// Parse the path from a URL accounting for special case witjh S3
// TODO: find out why this is necessary
pub(crate) fn object_store_path(table_root: &Url) -> LogStoreResult<Path> {
    Ok(match ObjectStoreScheme::parse(table_root) {
        Ok((_, path)) => path,
        _ => Path::parse(table_root.path()).map_err(|e| LogStoreError::InvalidPath {
            path: table_root.path().to_string(),
            source: Box::new(e),
        })?,
    })
}

/// TODO
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
                format!("{}/{}", root.as_ref(), location.as_ref())
            }
        }
    }
}

/// Reads a commit and gets list of actions
pub fn get_actions(
    version: i64,
    commit_log_bytes: &bytes::Bytes,
) -> Result<Vec<deltalake_types::Action>, LogStoreError> {
    debug!("parsing commit with version {version}...");
    Deserializer::from_slice(commit_log_bytes)
        .into_iter::<deltalake_types::Action>()
        .map(|result| {
            result.map_err(|e| {
                let line = format!("Error at line {}, column {}", e.line(), e.column());
                LogStoreError::InvalidJsonLog {
                    json_err: e,
                    line,
                    version,
                }
            })
        })
        .collect()
}

// TODO: maybe a bit of a hack, required to `#[derive(Debug)]` for the operation builders
impl std::fmt::Debug for dyn LogStore + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name(), self.root_uri())
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
) -> LogStoreResult<i64> {
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
    .map_err(|e| LogStoreError::Generic {
        source: Box::new(e),
    })?
    .map_err(|e| {
        if e.to_string()
            .contains(&format!("to have version {current_version}"))
        {
            LogStoreError::InvalidVersion(current_version)
        } else {
            LogStoreError::Generic {
                source: Box::new(e),
            }
        }
    })?;

    Ok(segment.end_version as i64)
}

/// Read delta log for a specific version
#[instrument(skip(storage), fields(version = version, path = %commit_uri_from_version(version)))]
pub async fn read_commit_entry(
    storage: &dyn ObjectStore,
    version: i64,
) -> LogStoreResult<Option<Bytes>> {
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
) -> Result<(), LogStoreError> {
    // move temporary commit file to delta log directory
    // rely on storage to fail if the file already exists -
    storage
        .rename_if_not_exists(tmp_commit, &commit_uri_from_version(version))
        .await
        .map_err(|err| -> LogStoreError {
            match err {
                ObjectStoreError::AlreadyExists { .. } => {
                    warn!("commit entry already exists");
                    LogStoreError::VersionAlreadyExists(version)
                }
                _ => {
                    error!(error = %err, "failed to write commit entry");
                    LogStoreError::from(err)
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
) -> Result<(), LogStoreError> {
    storage.delete_with_retries(tmp_commit, 15).await?;
    debug!("commit entry aborted successfully");
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use futures::TryStreamExt;

    use super::*;

    #[test]
    fn logstore_with_invalid_url() {
        let location = Url::parse("nonexistent://table").unwrap();

        let store = logstore_for(location, StorageConfig::default());
        assert!(store.is_err());
    }

    #[test]
    fn logstore_with_memory() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(location, StorageConfig::default());
        assert!(store.is_ok());
    }

    #[test]
    fn logstore_with_memory_and_rt() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(
            location,
            StorageConfig::default().with_io_runtime(IORuntime::default()),
        );
        assert!(store.is_ok());
    }

    #[test]
    fn test_logstore_ext() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(location, StorageConfig::default()).unwrap();
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
            logstore_for(location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(!store
            .is_delta_table_location()
            .await
            .expect("Failed to look at table"));

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
        assert!(!store
            .is_delta_table_location()
            .await
            .expect("Failed to look at table"));
    }

    #[tokio::test]
    async fn test_is_location_a_table_commit() {
        use object_store::path::Path;
        use object_store::{PutOptions, PutPayload};
        let location = Url::parse("memory:///table").unwrap();
        let store =
            logstore_for(location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(!store
            .is_delta_table_location()
            .await
            .expect("Failed to identify table"));

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
        assert!(store
            .is_delta_table_location()
            .await
            .expect("Failed to identify table"));
    }

    #[tokio::test]
    async fn test_is_location_a_table_checkpoint() {
        use object_store::path::Path;
        use object_store::{PutOptions, PutPayload};
        let location = Url::parse("memory:///table").unwrap();
        let store =
            logstore_for(location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(!store
            .is_delta_table_location()
            .await
            .expect("Failed to identify table"));

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
        assert!(store
            .is_delta_table_location()
            .await
            .expect("Failed to identify table"));
    }

    #[tokio::test]
    async fn test_is_location_a_table_crc() {
        use object_store::path::Path;
        use object_store::{PutOptions, PutPayload};
        let location = Url::parse("memory:///table").unwrap();
        let store =
            logstore_for(location, StorageConfig::default()).expect("Failed to get logstore");
        assert!(!store
            .is_delta_table_location()
            .await
            .expect("Failed to identify table"));

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
        assert!(store
            .is_delta_table_location()
            .await
            .expect("Failed to identify table"));
    }

    #[tokio::test]
    async fn test_get_actions_malformed_json() {
        use super::*;

        let malformed_json = bytes::Bytes::from(
            r#"{"add": {"path": "test.parquet", "partitionValues": {}, "size": 100, "modificationTime": 1234567890, "dataChange": true}}
{"invalid json without closing brace"#,
        );

        let result = get_actions(0, &malformed_json);

        match result {
            Err(LogStoreError::InvalidJsonLog {
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
