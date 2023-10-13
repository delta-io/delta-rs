//! Delta log store.
use crate::errors::DeltaResult;
use bytes::Bytes;

use crate::protocol::Action;

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
pub trait LogStore {
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
        actions: Vec<Action>,
    ) -> DeltaResult<()>;

    /// Find latest version currently stored in the delta log.
    async fn get_latest_version(&self) -> DeltaResult<i64>;
}
