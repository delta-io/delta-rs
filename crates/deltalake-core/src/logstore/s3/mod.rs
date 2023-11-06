//! Log store implementation leveraging DynamoDb.
//! Implementation uses DynamoDb to guarantee atomic writes of delta log entries
//! when the underlying object storage does not support atomic `put_if_absent`
//! or `rename_if_absent` operations, as is the case for S3.

mod errors;
pub mod lock_client;

use std::time::SystemTime;

use bytes::Bytes;
use object_store::path::Path;
use url::Url;

use crate::logstore::DELTA_LOG_PATH;
use crate::{
    operations::transaction::TransactionError,
    storage::{config::StorageOptions, s3::S3StorageOptions, ObjectStoreRef},
    DeltaResult, DeltaTableError,
};
use errors::LockClientError;

use self::lock_client::{DynamoDbLockClient, UpdateLogEntryResult};

use super::{LogStore, LogStoreConfig};

const STORE_NAME: &str = "DeltaS3ObjectStore";
const MAX_REPAIR_RETRIES: i64 = 3;

/// [`LogStore`] implementation backed by DynamoDb
pub struct S3DynamoDbLogStore {
    pub(crate) storage: ObjectStoreRef,
    lock_client: DynamoDbLockClient,
    config: LogStoreConfig,
    table_path: String,
}

impl S3DynamoDbLogStore {
    /// Create log store
    pub fn try_new(
        location: Url,
        options: impl Into<StorageOptions> + Clone,
        s3_options: &S3StorageOptions,
        object_store: ObjectStoreRef,
    ) -> DeltaResult<Self> {
        let lock_client = DynamoDbLockClient::try_new(&s3_options).map_err(|err| {
            DeltaTableError::ObjectStore {
                source: object_store::Error::Generic {
                    store: STORE_NAME,
                    source: err.into(),
                },
            }
        })?;
        let table_path = super::to_uri(&location, &Path::from(""));
        Ok(Self {
            storage: object_store,
            lock_client,
            config: LogStoreConfig {
                location,
                options: options.into(),
            },
            table_path,
        })
    }

    /// Attempt to repair an incomplete log entry by moving the temporary commit file
    /// to `N.json` and update the associated log entry to mark it as completed.
    pub async fn repair_entry(
        &self,
        entry: &CommitEntry,
    ) -> Result<RepairLogEntryResult, TransactionError> {
        // java does this, do we need it?
        if entry.complete {
            return Ok(RepairLogEntryResult::AlreadyCompleted);
        }
        for retry in 0..=MAX_REPAIR_RETRIES {
            match super::write_commit_entry(self.storage.as_ref(), entry.version, &entry.temp_path)
                .await
            {
                Ok(()) => {
                    return self.try_complete_entry(entry, true).await;
                }
                // `N.json` has already been moved, complete the entry in DynamoDb just in case
                Err(TransactionError::VersionAlreadyExists(_)) => {
                    return self.try_complete_entry(entry, false).await;
                }
                Err(err) if retry == MAX_REPAIR_RETRIES => return Err(err),
                Err(err) => log::debug!(
                    "retry #{retry} on log entry {entry:?} failed to move commit: '{err}'"
                ),
            }
        }
        unreachable!("for loop yields Ok or Err in body when retry = MAX_REPAIR_RETRIES")
    }

    /// Update an incomplete log entry to completed.
    async fn try_complete_entry(
        &self,
        entry: &CommitEntry,
        copy_performed: bool,
    ) -> Result<RepairLogEntryResult, TransactionError> {
        for retry in 0..=MAX_REPAIR_RETRIES {
            match self
                .lock_client
                .update_commit_entry(entry.version, &self.table_path)
                .await
                .map_err(|err| TransactionError::LogStoreError {
                    msg: format!(
                        "unable to complete entry for '{}': failure to write to DynamoDb",
                        entry.version
                    ),
                    source: Box::new(err),
                }) {
                Ok(x) => return Ok(Self::map_retry_result(x, copy_performed)),
                Err(err) if retry == MAX_REPAIR_RETRIES => return Err(err.into()),
                Err(err) => log::debug!(
                    "retry #{retry} on log entry {entry:?} failed to update lock db: '{err}'"
                ),
            }
        }
        unreachable!("for loop yields Ok or Err in body when retyr = MAX_REPAIR_RETRIES")
    }

    fn map_retry_result(
        result: UpdateLogEntryResult,
        copy_performed: bool,
    ) -> RepairLogEntryResult {
        match result {
            UpdateLogEntryResult::UpdatePerformed if copy_performed => {
                RepairLogEntryResult::MovedFileAndFixedEntry
            }
            UpdateLogEntryResult::UpdatePerformed => RepairLogEntryResult::FixedEntry,
            UpdateLogEntryResult::AlreadyCompleted if copy_performed => {
                RepairLogEntryResult::MovedFile
            }
            UpdateLogEntryResult::AlreadyCompleted => RepairLogEntryResult::AlreadyCompleted,
        }
    }
}

#[async_trait::async_trait]
impl LogStore for S3DynamoDbLogStore {
    fn root_uri(&self) -> String {
        self.table_path.clone()
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        let entry = self
            .lock_client
            .get_commit_entry(&self.table_path, version)
            .await;
        if let Ok(Some(entry)) = entry {
            self.repair_entry(&entry).await?;
        }
        super::read_commit_entry(self.storage.as_ref(), version).await
    }

    /// Tries to commit a prepared commit file. Returns [DeltaTableError::VersionAlreadyExists]
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    async fn write_commit_entry(
        &self,
        version: i64,
        tmp_commit: &Path,
    ) -> Result<(), TransactionError> {
        let entry = CommitEntry::new(version, tmp_commit.clone());
        // create log entry in dynamo db: complete = false, no expireTime
        self.lock_client
            .put_commit_entry(&self.table_path, &entry)
            .await
            .map_err(|err| match err {
                LockClientError::VersionAlreadyExists { version, .. } => {
                    TransactionError::VersionAlreadyExists(version)
                }
                LockClientError::ProvisionedThroughputExceeded => todo!(),
                err => TransactionError::LogStoreError {
                    msg: "dynamodb client failed to write log entry".to_owned(),
                    source: Box::new(err),
                },
            })?;
        // `repair_entry` performs the exact steps required to finalize the commit, but contains
        // retry logic and more robust error handling under the assumption that any other client
        // could attempt to concurrently repair that very same entry. In fact, the original writer
        // of the commit is just one delta client competing to perform the repair operation, as any
        // other client could see the incomplete commit to immediately trigger a repair.
        self.repair_entry(&entry).await?;
        Ok(())
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        let entry = self
            .lock_client
            .get_latest_entry(&self.table_path)
            .await
            .map_err(|err| DeltaTableError::GenericError {
                source: Box::new(err),
            })?;
        // when there is a latest entry in DynamoDb, we can avoid the file listing in S3.
        if let Some(entry) = entry {
            self.repair_entry(&entry).await?;
            Ok(entry.version)
        } else {
            super::get_latest_version(self, current_version).await
        }
    }

    fn object_store(&self) -> ObjectStoreRef {
        self.storage.clone()
    }

    fn to_uri(&self, location: &Path) -> String {
        super::to_uri(&self.config.location, location)
    }

    #[cfg(feature = "datafusion")]
    fn object_store_url(&self) -> datafusion::execution::object_store::ObjectStoreUrl {
        super::object_store_url(&self.config.location)
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

/// Representation of a log entry stored in DynamoDb
/// dynamo db item consists of:
/// - tablePath: String - part of primary key, configured in [`DeltaObjectStore`]
/// - fileName: String - commit version.json (part of primary key), stored as i64 here
/// - tempPath: String - name of temporary file containing commit info
/// - complete: bool - operation completed, i.e. atomic rename from `tempPath` to `fileName` succeeded
/// - expireTime: Option<SystemTime> - epoch seconds at which this external commit entry is safe to be deleted
#[derive(Debug, PartialEq)]
pub struct CommitEntry {
    /// Commit version, stored as file name (e.g., 00000N.json) in dynamodb (relative to `_delta_log/`
    pub version: i64,
    /// Path to temp file for this commit, relative to the `_delta_log
    pub temp_path: Path,
    /// true if delta json file is successfully copied to its destination location, else false
    pub complete: bool,
    /// If complete = true, epoch seconds at which this external commit entry is safe to be deleted
    pub expire_time: Option<SystemTime>,
}

impl CommitEntry {
    /// Create a new log entry for the given version.
    /// Initial log entry state is incomplete.
    pub fn new(version: i64, temp_path: Path) -> CommitEntry {
        Self {
            version,
            temp_path,
            complete: false,
            expire_time: None,
        }
    }
}

/// Represents the possible outcomes of calling `DynamoDbLockClient::repair_entry()`.
#[derive(Debug, PartialEq)]
pub enum RepairLogEntryResult {
    /// Both repair tasks where executed successfully.
    MovedFileAndFixedEntry,
    /// The database entry has been rewritten, but the file was already moved.
    FixedEntry,
    /// Moved file, but the database entry was alrady updated.
    MovedFile,
    /// Both parts of the repair process where already carried.
    AlreadyCompleted,
}

/// Represents the possible, positive outcomes of calling `DynamoDbClient::try_create_lock_table()`
#[derive(Debug, PartialEq)]
pub enum CreateLockTableResult {
    /// Table created successfully.
    TableCreated,
    /// Table was not created because it already exists.
    /// Does not imply that the table has the correct schema.
    TableAlreadyExists,
}
