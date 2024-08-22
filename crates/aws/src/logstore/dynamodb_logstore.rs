//! Log store implementation leveraging DynamoDb.
//! Implementation uses DynamoDb to guarantee atomic writes of delta log entries
//! when the underlying object storage does not support atomic `put_if_absent`
//! or `rename_if_absent` operations, as is the case for S3.

use crate::errors::LockClientError;
use crate::storage::S3StorageOptions;
use crate::{constants, CommitEntry, DynamoDbLockClient, UpdateLogEntryResult};

use bytes::Bytes;
use deltalake_core::{ObjectStoreError, Path};
use tracing::{debug, error, warn};
use url::Url;

use deltalake_core::logstore::*;
use deltalake_core::{
    operations::transaction::TransactionError,
    storage::{ObjectStoreRef, StorageOptions},
    DeltaResult, DeltaTableError,
};

const STORE_NAME: &str = "DeltaS3ObjectStore";
const MAX_REPAIR_RETRIES: i64 = 3;

/// [`LogStore`] implementation backed by DynamoDb
pub struct S3DynamoDbLogStore {
    pub(crate) storage: ObjectStoreRef,
    lock_client: DynamoDbLockClient,
    config: LogStoreConfig,
    table_path: String,
}

impl std::fmt::Debug for S3DynamoDbLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "S3DynamoDbLogStore({})", self.table_path)
    }
}

impl S3DynamoDbLogStore {
    /// Create log store
    pub fn try_new(
        location: Url,
        options: impl Into<StorageOptions> + Clone,
        s3_options: &S3StorageOptions,
        object_store: ObjectStoreRef,
    ) -> DeltaResult<Self> {
        let lock_client = DynamoDbLockClient::try_new(
            &s3_options.sdk_config,
            s3_options
                .extra_opts
                .get(constants::LOCK_TABLE_KEY_NAME)
                .cloned(),
            s3_options
                .extra_opts
                .get(constants::BILLING_MODE_KEY_NAME)
                .cloned(),
            s3_options
                .extra_opts
                .get(constants::MAX_ELAPSED_REQUEST_TIME_KEY_NAME)
                .cloned(),
            s3_options.dynamodb_endpoint.clone(),
        )
        .map_err(|err| DeltaTableError::ObjectStore {
            source: ObjectStoreError::Generic {
                store: STORE_NAME,
                source: Box::new(err),
            },
        })?;
        let table_path = to_uri(&location, &Path::from(""));
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
            match write_commit_entry(&self.storage, entry.version, &entry.temp_path).await {
                Ok(()) => {
                    debug!("Successfully committed entry for version {}", entry.version);
                    return self.try_complete_entry(entry, true).await;
                }
                // `N.json` has already been moved, complete the entry in DynamoDb just in case
                Err(TransactionError::ObjectStore {
                    source: ObjectStoreError::NotFound { .. },
                }) => {
                    warn!("It looks like the {}.json has already been moved, we got 404 from ObjectStorage.", entry.version);
                    return self.try_complete_entry(entry, false).await;
                }
                Err(err) if retry == MAX_REPAIR_RETRIES => return Err(err),
                Err(err) => {
                    debug!("retry #{retry} on log entry {entry:?} failed to move commit: '{err}'")
                }
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
        debug!("try_complete_entry for {:?}, {}", entry, copy_performed);
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
                Err(err) if retry == MAX_REPAIR_RETRIES => return Err(err),
                Err(err) => error!(
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
    fn name(&self) -> String {
        "S3DynamoDbLogStore".into()
    }

    fn root_uri(&self) -> String {
        self.table_path.clone()
    }

    async fn refresh(&self) -> DeltaResult<()> {
        let entry = self
            .lock_client
            .get_latest_entry(&self.table_path)
            .await
            .map_err(|err| DeltaTableError::GenericError {
                source: Box::new(err),
            })?;
        if let Some(entry) = entry {
            self.repair_entry(&entry).await?;
        }
        Ok(())
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        let entry = self
            .lock_client
            .get_commit_entry(&self.table_path, version)
            .await;
        if let Ok(Some(entry)) = entry {
            self.repair_entry(&entry).await?;
        }
        read_commit_entry(&self.storage, version).await
    }

    /// Tries to commit a prepared commit file. Returns [DeltaTableError::VersionAlreadyExists]
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        let tmp_commit = match commit_or_bytes {
            CommitOrBytes::TmpCommit(tmp_commit) => tmp_commit,
            _ => unreachable!(), // S3DynamoDBLogstore should never get Bytes
        };
        let entry = CommitEntry::new(version, tmp_commit.clone());
        debug!("Writing commit entry for {self:?}: {entry:?}");
        // create log entry in dynamo db: complete = false, no expireTime
        self.lock_client
            .put_commit_entry(&self.table_path, &entry)
            .await
            .map_err(|err| match err {
                LockClientError::VersionAlreadyExists { version, .. } => {
                    warn!("LockClientError::VersionAlreadyExists({version})");
                    TransactionError::VersionAlreadyExists(version)
                }
                LockClientError::ProvisionedThroughputExceeded => todo!(
                    "deltalake-aws does not yet handle DynamoDB provisioned throughput errors"
                ),
                LockClientError::LockTableNotFound => {
                    let table_name = self.lock_client.get_lock_table_name();
                    error!("Lock table '{table_name}' not found");
                    TransactionError::LogStoreError {
                        msg: format!("lock table '{table_name}' not found"),
                        source: Box::new(err),
                    }
                }
                err => {
                    error!("dynamodb client failed to write log entry: {err:?}");
                    TransactionError::LogStoreError {
                        msg: "dynamodb client failed to write log entry".to_owned(),
                        source: Box::new(err),
                    }
                }
            })?;
        // `repair_entry` performs the exact steps required to finalize the commit, but contains
        // retry logic and more robust error handling under the assumption that any other client
        // could attempt to concurrently repair that very same entry. In fact, the original writer
        // of the commit is just one delta client competing to perform the repair operation, as any
        // other client could see the incomplete commit to immediately trigger a repair.
        self.repair_entry(&entry).await?;
        Ok(())
    }

    /// Tries to abort an entry by first deleting the commit log entry, then deleting the temp commit file
    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        let tmp_commit = match commit_or_bytes {
            CommitOrBytes::TmpCommit(tmp_commit) => tmp_commit,
            _ => unreachable!(), // S3DynamoDBLogstore should never get Bytes
        };
        self.lock_client
            .delete_commit_entry(version, &self.table_path)
            .await
            .map_err(|err| match err {
                LockClientError::ProvisionedThroughputExceeded => todo!(
                    "deltalake-aws does not yet handle DynamoDB provisioned throughput errors"
                ),
                LockClientError::VersionAlreadyCompleted { version, .. } => {
                    error!("Trying to abort a completed commit");
                    TransactionError::LogStoreError {
                        msg: format!("trying to abort a completed log entry: {}", version),
                        source: Box::new(err),
                    }
                }
                err => TransactionError::LogStoreError {
                    msg: "dynamodb client failed to delete log entry".to_owned(),
                    source: Box::new(err),
                },
            })?;

        abort_commit_entry(&self.storage, version, &tmp_commit).await?;
        Ok(())
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        debug!("Retrieving latest version of {self:?} at v{current_version}");
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
            get_latest_version(self, current_version).await
        }
    }

    fn object_store(&self) -> ObjectStoreRef {
        self.storage.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
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
