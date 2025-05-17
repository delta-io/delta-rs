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
use std::cmp::{max, min};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
#[cfg(feature = "datafusion")]
use datafusion::datasource::object_store::ObjectStoreUrl;
use delta_kernel::AsAny;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use regex::Regex;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};
use url::Url;
use uuid::Uuid;

use crate::kernel::log_segment::PathExt;
use crate::kernel::transaction::TransactionError;
use crate::kernel::Action;
use crate::protocol::{get_last_checkpoint, ProtocolError};
use crate::{DeltaResult, DeltaTableError};

pub use self::config::StorageConfig;
pub use self::factories::{
    logstore_factories, object_store_factories, store_for, LogStoreFactory,
    LogStoreFactoryRegistry, ObjectStoreFactory, ObjectStoreFactoryRegistry,
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
    fn with_options_internal(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>>;
}

impl<T: LogStoreFactory + ?Sized> LogStoreFactoryExt for T {
    fn with_options_internal(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        let store = options.decorate_store(store, location)?;
        self.with_options(Arc::new(store), location, options)
    }
}

impl<T: LogStoreFactory> LogStoreFactoryExt for Arc<T> {
    fn with_options_internal(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        T::with_options_internal(self, store, location, options)
    }
}

/// Return the [DefaultLogStore] implementation with the provided configuration options
pub fn default_logstore(
    store: ObjectStoreRef,
    location: &Url,
    options: &StorageConfig,
) -> Arc<dyn LogStore> {
    Arc::new(default_logstore::DefaultLogStore::new(
        store,
        LogStoreConfig {
            location: location.clone(),
            options: options.clone(),
        },
    ))
}

/// Sharable reference to [`LogStore`]
pub type LogStoreRef = Arc<dyn LogStore>;

static DELTA_LOG_PATH: LazyLock<Path> = LazyLock::new(|| Path::from("_delta_log"));

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
/// let logstore = logstore_for(location, storage_config).expect("Failed to get a logstore");
/// ```
pub fn logstore_for(location: Url, storage_config: StorageConfig) -> DeltaResult<LogStoreRef> {
    // turn location into scheme
    let scheme = Url::parse(&format!("{}://", location.scheme()))
        .map_err(|_| DeltaTableError::InvalidTableLocation(location.clone().into()))?;

    if let Some(entry) = object_store_factories().get(&scheme) {
        debug!("Found a storage provider for {scheme} ({location})");
        let (store, _prefix) = entry.value().parse_url_opts(
            &location,
            &storage_config.raw,
            &storage_config.retry,
            storage_config.runtime.clone().map(|rt| rt.get_handle()),
        )?;
        return logstore_with(store, location, storage_config);
    }

    Err(DeltaTableError::InvalidTableLocation(location.into()))
}

/// Return the [LogStoreRef] using the given [ObjectStoreRef]
pub fn logstore_with(
    store: ObjectStoreRef,
    location: Url,
    storage_config: StorageConfig,
) -> DeltaResult<LogStoreRef> {
    let scheme = Url::parse(&format!("{}://", location.scheme()))
        .map_err(|_| DeltaTableError::InvalidTableLocation(location.clone().into()))?;

    if let Some(factory) = logstore_factories().get(&scheme) {
        debug!("Found a logstore provider for {scheme}");
        return factory
            .value()
            .with_options_internal(store, &location, &storage_config);
    }

    error!("Could not find a logstore for the scheme {scheme}");
    Err(DeltaTableError::InvalidTableLocation(
        location.clone().into(),
    ))
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

    /// Find earliest version currently stored in the delta log.
    async fn get_earliest_version(&self, start_version: i64) -> DeltaResult<i64>;

    /// Get the list of actions for the next commit
    async fn peek_next_commit(&self, current_version: i64) -> DeltaResult<PeekCommit> {
        let next_version = current_version + 1;
        let commit_log_bytes = match self.read_commit_entry(next_version).await {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => return Ok(PeekCommit::UpToDate),
            Err(err) => Err(err),
        }?;

        let actions = crate::logstore::get_actions(next_version, commit_log_bytes).await;
        Ok(PeekCommit::New(next_version, actions?))
    }

    /// Get object store, can pass operation_id for object stores linked to an operation
    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore>;

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

    /// Check if the location is a delta table location
    async fn is_delta_table_location(&self) -> DeltaResult<bool> {
        let object_store = self.object_store(None);
        let mut stream = object_store.list(Some(self.log_path()));
        while let Some(res) = stream.next().await {
            match res {
                Ok(meta) => {
                    // Valid but optional files.
                    if meta.location.is_crc_file()
                        || meta.location.is_last_checkpoint_file()
                        || meta.location.is_last_vacuum_info_file()
                        || meta.location.is_deletion_vector_file()
                    {
                        continue;
                    }
                    let is_valid =
                        meta.location.is_commit_file() || meta.location.is_checkpoint_file();
                    if !is_valid {
                        warn!(
                            "Expected a valid delta file. Found {}",
                            meta.location.filename().unwrap_or("<empty>")
                        )
                    }
                    return Ok(is_valid);
                }
                Err(ObjectStoreError::NotFound { .. }) => return Ok(false),
                Err(err) => return Err(err.into()),
            }
        }

        Ok(false)
    }

    #[cfg(feature = "datafusion")]
    /// Generate a unique enough url to identify the store in datafusion.
    /// The DF object store registry only cares about the scheme and the host of the url for
    /// registering/fetching. In our case the scheme is hard-coded to "delta-rs", so to get a unique
    /// host we convert the location from this `LogStore` to a valid name, combining the
    /// original scheme, host and path with invalid characters replaced.
    fn object_store_url(&self) -> ObjectStoreUrl {
        crate::logstore::object_store_url(&self.config().location)
    }

    /// Get configuration representing configured log store.
    fn config(&self) -> &LogStoreConfig;
}

#[cfg(feature = "datafusion")]
fn object_store_url(location: &Url) -> ObjectStoreUrl {
    use object_store::path::DELIMITER;
    ObjectStoreUrl::parse(format!(
        "delta-rs://{}-{}{}",
        location.scheme(),
        location.host_str().unwrap_or("-"),
        location.path().replace(DELIMITER, "-").replace(':', "-")
    ))
    .expect("Invalid object store url.")
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
pub async fn get_actions(
    version: i64,
    commit_log_bytes: bytes::Bytes,
) -> Result<Vec<Action>, DeltaTableError> {
    debug!("parsing commit with version {version}...");
    let reader = BufReader::new(Cursor::new(commit_log_bytes));

    let mut actions = Vec::new();
    for re_line in reader.lines() {
        let line = re_line?;
        let lstr = line.as_str();
        let action = serde_json::from_str(lstr).map_err(|e| DeltaTableError::InvalidJsonLog {
            json_err: e,
            line,
            version,
        })?;
        actions.push(action);
    }
    Ok(actions)
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

static DELTA_LOG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d{20})\.(json|checkpoint).*$").unwrap());

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
    let version_start = match get_last_checkpoint(log_store).await {
        Ok(last_check_point) => last_check_point.version,
        Err(ProtocolError::CheckpointNotFound) => -1, // no checkpoint
        Err(e) => return Err(DeltaTableError::from(e)),
    };

    debug!("latest checkpoint version: {version_start}");

    let version_start = max(current_version, version_start);

    // list files to find max version
    let version = async {
        let mut max_version: i64 = version_start;
        let prefix = log_store.log_path();
        let offset_path = commit_uri_from_version(max_version);
        let object_store = log_store.object_store(None);
        let mut files = object_store.list_with_offset(Some(prefix), &offset_path);
        let mut empty_stream = true;

        while let Some(obj_meta) = files.next().await {
            let obj_meta = obj_meta?;
            let location_path: &Path = &obj_meta.location;
            let part_count = location_path.prefix_match(prefix).unwrap().count();
            if part_count > 1 {
                // Per the spec, ignore any files in subdirectories.
                // Spark may create these as uncommited transactions which we don't want
                //
                // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries
                // "Delta files are stored as JSON in a directory at the *root* of the table
                // named _delta_log, and ... make up the log of all changes that have occurred to a table."
                continue;
            }
            if let Some(log_version) = extract_version_from_filename(obj_meta.location.as_ref()) {
                max_version = max(max_version, log_version);
                // also cache timestamp for version, for faster time-travel
                // TODO: temporarily disabled because `version_timestamp` is not available in the [`LogStore`]
                // self.version_timestamp
                //     .insert(log_version, obj_meta.last_modified.timestamp());
            }
            empty_stream = false;
        }

        if max_version < 0 {
            return Err(DeltaTableError::not_a_table(log_store.root_uri()));
        }

        // This implies no files were fetched during list_offset so either the starting_version is the latest
        // or starting_version is invalid, so we use current_version -1, and do one more try.
        if empty_stream {
            let obj_meta = object_store
                .head(&commit_uri_from_version(max_version))
                .await;
            if obj_meta.is_err() {
                return Box::pin(get_latest_version(log_store, -1)).await;
            }
        }

        Ok::<i64, DeltaTableError>(max_version)
    }
    .await?;

    Ok(version)
}

/// Default implementation for retrieving the earliest version
pub async fn get_earliest_version(
    log_store: &dyn LogStore,
    current_version: i64,
) -> DeltaResult<i64> {
    let version_start = match get_last_checkpoint(log_store).await {
        Ok(last_check_point) => last_check_point.version,
        Err(ProtocolError::CheckpointNotFound) => {
            // no checkpoint so start from current_version
            current_version
        }
        Err(e) => {
            return Err(DeltaTableError::from(e));
        }
    };

    // list files to find min version
    let version = async {
        let mut min_version: i64 = version_start;
        let prefix = Some(log_store.log_path());
        let offset_path = commit_uri_from_version(version_start);
        let object_store = log_store.object_store(None);

        // Manually filter until we can provide direction in https://github.com/apache/arrow-rs/issues/6274
        let mut files = object_store
            .list(prefix)
            .try_filter(move |f| futures::future::ready(f.location < offset_path))
            .boxed();

        while let Some(obj_meta) = files.next().await {
            let obj_meta = obj_meta?;
            if let Some(log_version) = extract_version_from_filename(obj_meta.location.as_ref()) {
                min_version = min(min_version, log_version);
            }
        }

        if min_version < 0 {
            return Err(DeltaTableError::not_a_table(log_store.root_uri()));
        }

        Ok::<i64, DeltaTableError>(min_version)
    }
    .await?;
    Ok(version)
}

/// Read delta log for a specific version
pub async fn read_commit_entry(
    storage: &dyn ObjectStore,
    version: i64,
) -> DeltaResult<Option<Bytes>> {
    let commit_uri = commit_uri_from_version(version);
    match storage.get(&commit_uri).await {
        Ok(res) => Ok(Some(res.bytes().await?)),
        Err(ObjectStoreError::NotFound { .. }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Default implementation for writing a commit entry
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
                    TransactionError::VersionAlreadyExists(version)
                }
                _ => TransactionError::from(err),
            }
        })?;
    Ok(())
}

/// Default implementation for aborting a commit entry
pub async fn abort_commit_entry(
    storage: &dyn ObjectStore,
    _version: i64,
    tmp_commit: &Path,
) -> Result<(), TransactionError> {
    storage.delete_with_retries(tmp_commit, 15).await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    type Opts = HashMap<String, String>;

    #[test]
    fn logstore_with_invalid_url() {
        let location = Url::parse("nonexistent://table").unwrap();

        let store = logstore_for(location, StorageConfig::default());
        assert!(store.is_err());
    }

    #[tokio::test]
    async fn logstore_with_memory() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(location, StorageConfig::default());
        assert!(store.is_ok());
    }

    #[tokio::test]
    async fn logstore_with_memory_and_rt() {
        let location = Url::parse("memory:///table").unwrap();
        let store = logstore_for(
            location,
            StorageConfig::default().with_io_runtime(IORuntime::default()),
        );
        assert!(store.is_ok());
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
                &Path::from("_delta_log/0.json"),
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
                &Path::from("_delta_log/0.checkpoint.parquet"),
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
                &Path::from("_delta_log/.0.crc.crc"),
                payload.clone(),
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");

        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/.0.json.crc"),
                payload.clone(),
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");

        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/0.crc"),
                payload.clone(),
                PutOptions::default(),
            )
            .await
            .expect("Failed to put");

        // Now add a commit
        let _put = store
            .object_store(None)
            .put_opts(
                &Path::from("_delta_log/0.json"),
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

    /// <https://github.com/delta-io/delta-rs/issues/3297>:w
    #[tokio::test]
    async fn test_peek_with_invalid_json() -> DeltaResult<()> {
        use crate::logstore::object_store::memory::InMemory;
        let memory_store = Arc::new(InMemory::new());
        let log_path = Path::from("delta-table/_delta_log/00000000000000000001.json");

        let log_content = r#"{invalid_json"#;

        memory_store
            .put(&log_path, log_content.into())
            .await
            .expect("Failed to write log file");

        let table_uri = "memory:///delta-table";

        let table = crate::DeltaTableBuilder::from_valid_uri(table_uri)
            .unwrap()
            .with_storage_backend(memory_store, Url::parse(table_uri).unwrap())
            .build()?;

        let result = table.log_store().peek_next_commit(0).await;
        assert!(result.is_err());
        Ok(())
    }

    /// Collect list stream
    pub async fn flatten_list_stream(
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
        ] {
            let url_1 = Url::parse(location_1).unwrap();
            let url_2 = Url::parse(location_2).unwrap();

            assert_ne!(
                object_store_url(&url_1).as_str(),
                object_store_url(&url_2).as_str(),
            );
        }
    }
}
