pub mod dynamodb;

use std::fmt::Debug;

use crate::dynamodb::DynamoError;
use thiserror::Error;

/// A lock that has been successfully acquired
#[derive(Clone, Debug)]
pub struct LockItem {
    /// The name of the owner that owns this lock.
    pub owner_name: String,
    /// Current version number of the lock in DynamoDB. This is what tells the lock client
    /// when the lock is stale.
    pub record_version_number: String,
    /// The amount of time (in seconds) that the owner has this lock for.
    /// If lease_duration is None then the lock is non-expirable.
    pub lease_duration: Option<u64>,
    /// Tells whether or not the lock was marked as released when loaded from DynamoDB.
    pub is_released: bool,
    /// Optional data associated with this lock.
    pub data: Option<String>,
    /// The last time this lock was updated or retrieved.
    pub lookup_time: u128,
    /// Tells whether this lock was acquired by expiring existing one.
    pub acquired_expired_lock: bool,
    /// If true then this lock could not be acquired.
    pub is_non_acquirable: bool,
}

/// Abstraction over a distributive lock provider
#[async_trait::async_trait]
pub trait LockClient: Send + Sync + Debug {
    /// Attempts to acquire lock. If successful, returns the lock.
    /// Otherwise returns [`Option::None`] which is retryable action.
    /// Visit implementation docs for more details.
    async fn try_acquire_lock(&self, data: &str) -> Result<Option<LockItem>, DistributedLockError>;

    /// Returns current lock from DynamoDB (if any).
    async fn get_lock(&self) -> Result<Option<LockItem>, DistributedLockError>;

    /// Update data in the upstream lock of the current user still has it.
    /// The returned lock will have a new `rvn` so it'll increase the lease duration
    /// as this method is usually called when the work with a lock is extended.
    async fn update_data(&self, lock: &LockItem) -> Result<LockItem, DistributedLockError>;

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    async fn release_lock(&self, lock: &LockItem) -> Result<bool, DistributedLockError>;
}

pub const DEFAULT_MAX_RETRY_ACQUIRE_LOCK_ATTEMPTS: u32 = 10_000;

#[derive(Error, Debug)]
pub enum DistributedLockError {
    /// Error returned by `acquire_lock` which indicates that the lock could
    /// not be acquired for more that returned number of seconds.
    #[error("Could not acquire lock for {0} sec")]
    TimedOut(u64),

    /// Error returned which indicates that the lock could
    /// not be acquired because the `is_non_acquirable` is set to `true`.
    /// Usually this is done intentionally outside of a locking client.
    ///
    /// The example could be the dropping of a table. For example external service acquires the lock
    /// to drop (or drop/create etc., something that modifies the delta log completely) a table.
    /// The dangerous part here is that the concurrent delta workers will still perform the write
    /// whenever the lock is available, because it effectively locks the rename operation. However
    /// if the `is_non_acquirable` is set, then the `NonAcquirableLock` is returned which prohibits
    /// the delta-rs to continue the write.
    #[error("The existing lock is non-acquirable")]
    NonAcquirableLock,

    /// DynamoDB specific errors
    #[error("Error returned by DynamoDB locking service: {0}")]
    DynamoDB(#[from] DynamoError),
}
