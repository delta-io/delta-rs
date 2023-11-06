//! Delta log store.
use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use std::{cmp::max, sync::Arc};

use crate::{
    errors::DeltaResult,
    operations::transaction::TransactionError,
    protocol::{get_last_checkpoint, ProtocolError},
    storage::{commit_uri_from_version, DeltaObjectStore, ObjectStoreRef},
    DeltaTableError,
};
use bytes::Bytes;
use log::debug;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};

pub mod default_logstore;

/// Sharable reference to [`LogStore`]
pub type LogStoreRef = Arc<dyn LogStore>;

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
pub trait LogStore: Sync + Send {
    /// Read data for commit entry with the given version.
    /// TODO: return the actual commit data, i.e. Vec<Action>, instead?
    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Bytes>;

    /// Write list of actions as delta commit entry for given version.
    ///
    /// This operation can be retried with a higher version in case the write
    /// fails with `TransactionError::VersionAlreadyExists`.
    async fn write_commit_entry(
        &self,
        version: i64,
        tmp_commit: &Path,
    ) -> Result<(), TransactionError>;

    /// Find latest version currently stored in the delta log.
    async fn get_latest_version(&self, start_version: i64) -> DeltaResult<i64>;

    /// Get underlying object store.
    fn object_store(&self) -> ObjectStoreRef;
}

// TODO: maybe a bit of a hack, required to `#[derive(Debug)]` for the operation builders
impl std::fmt::Debug for dyn LogStore + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.object_store().fmt(f)
    }
}

lazy_static! {
    static ref DELTA_LOG_REGEX: Regex = Regex::new(r"(\d{20})\.(json|checkpoint).*$").unwrap();
}

/// Extract version from a file name in the delta log
pub fn extract_version_from_filename(name: &str) -> Option<i64> {
    DELTA_LOG_REGEX
        .captures(name)
        .map(|captures| captures.get(1).unwrap().as_str().parse().unwrap())
}

async fn get_latest_version(storage: &ObjectStoreRef, current_version: i64) -> DeltaResult<i64> {
    let version_start = match get_last_checkpoint(storage).await {
        Ok(last_check_point) => last_check_point.version,
        Err(ProtocolError::CheckpointNotFound) => {
            // no checkpoint
            -1
        }
        Err(e) => {
            return Err(DeltaTableError::from(e));
        }
    };

    debug!("latest checkpoint version: {version_start}");

    let version_start = max(current_version, version_start);

    // list files to find max version
    let version = async {
        let mut max_version: i64 = version_start;
        let prefix = Some(storage.log_path());
        let offset_path = commit_uri_from_version(max_version);
        let mut files = storage.list_with_offset(prefix, &offset_path).await?;

        while let Some(obj_meta) = files.next().await {
            let obj_meta = obj_meta?;
            if let Some(log_version) = extract_version_from_filename(obj_meta.location.as_ref()) {
                max_version = max(max_version, log_version);
                // also cache timestamp for version, for faster time-travel
                // TODO: temporarily disabled because `version_timestamp` is not available in the [`LogStore`]
                // self.version_timestamp
                //     .insert(log_version, obj_meta.last_modified.timestamp());
            }
        }

        if max_version < 0 {
            return Err(DeltaTableError::not_a_table(storage.root_uri()));
        }

        Ok::<i64, DeltaTableError>(max_version)
    }
    .await?;
    Ok(version)
}

async fn read_commit_entry(storage: &dyn ObjectStore, version: i64) -> DeltaResult<Bytes> {
    let commit_uri = commit_uri_from_version(version);
    let data = storage.get(&commit_uri).await?.bytes().await?;
    Ok(data)
}

async fn write_commit_entry(
    storage: &DeltaObjectStore,
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
