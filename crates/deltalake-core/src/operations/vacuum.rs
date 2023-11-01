//! Vacuum a Delta table
//!
//! Run the Vacuum command on the Delta Table: delete files no longer referenced by a Delta table and are older than the retention threshold.
//! We do not recommend that you set a retention interval shorter than 7 days, because old snapshots
//! and uncommitted files can still be in use by concurrent readers or writers to the table.
//!
//! If vacuum cleans up active files, concurrent readers can fail or, worse, tables can be
//! corrupted when vacuum deletes files that have not yet been committed.
//! If `retention_period` is not set then the `configuration.deletedFileRetentionDuration` of
//! delta table is used or if that's missing too, then the default value of 7 days otherwise.
//!
//! When you run vacuum then you cannot use time travel to a version older than
//! the specified retention period.
//!
//! Warning: Vacuum does not support partitioned tables on Windows. This is due
//! to Windows not using unix style paths. See #682
//!
//! # Example
//! ```rust ignore
//! let mut table = open_table("../path/to/table")?;
//! let (table, metrics) = VacuumBuilder::new(table.object_store(). table.state).await?;
//! ````

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use object_store::Error;
use object_store::{path::Path, ObjectStore};
use serde::Serialize;
use serde_json::{Map, Value};

use super::transaction::commit;
use crate::crate_version;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::protocol::{Action, DeltaOperation}; // Txn CommitInfo
use crate::storage::DeltaObjectStore;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;

/// Errors that can occur during vacuum
#[derive(thiserror::Error, Debug)]
enum VacuumError {
    /// Error returned when Vacuum retention period is below the safe threshold
    #[error(
        "Invalid retention period, minimum retention for vacuum is configured to be greater than {} hours, got {} hours", .min, .provided
    )]
    InvalidVacuumRetentionPeriod {
        /// User provided retention on vacuum call
        provided: i64,
        /// Minimal retention configured in delta table config
        min: i64,
    },

    /// Error returned
    #[error(transparent)]
    DeltaTable(#[from] DeltaTableError),
}

impl From<VacuumError> for DeltaTableError {
    fn from(err: VacuumError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}

/// A source of time
pub trait Clock: Debug + Send + Sync {
    /// get the current time in milliseconds since epoch
    fn current_timestamp_millis(&self) -> i64;
}

#[derive(Debug)]
/// Vacuum a Delta table with the given options
/// See this module's documentation for more information
pub struct VacuumBuilder {
    /// A snapshot of the to-be-vacuumed table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    store: Arc<DeltaObjectStore>,
    /// Period of stale files allowed.
    retention_period: Option<Duration>,
    /// Validate the retention period is not below the retention period configured in the table
    enforce_retention_duration: bool,
    /// Don't delete the files. Just determine which files can be deleted
    dry_run: bool,
    /// Override the source of time
    clock: Option<Arc<dyn Clock>>,
}

/// Details for the Vacuum operation including which files were
#[derive(Debug)]
pub struct VacuumMetrics {
    /// Was this a dry run
    pub dry_run: bool,
    /// Files deleted successfully
    pub files_deleted: Vec<String>,
}

/// Details for the Vacuum start operation for the transaction log
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VacuumStartOperationMetrics {
    /// The number of files that will be deleted
    pub num_files_to_delete: i64,
    /// Size of the data to be deleted in bytes
    pub size_of_data_to_delete: i64,
}

/// Details for the Vacuum End operation for the transaction log
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VacuumEndOperationMetrics {
    /// The number of actually deleted files
    pub num_deleted_files: i64,
    /// The number of actually vacuumed directories
    pub num_vacuumed_directories: i64,
}

/// Methods to specify various vacuum options and to execute the operation
impl VacuumBuilder {
    /// Create a new [`VacuumBuilder`]
    pub fn new(store: Arc<DeltaObjectStore>, snapshot: DeltaTableState) -> Self {
        VacuumBuilder {
            snapshot,
            store,
            retention_period: None,
            enforce_retention_duration: true,
            dry_run: false,
            clock: None,
        }
    }

    /// Override the default rention period for which files are deleted.
    pub fn with_retention_period(mut self, retention_period: Duration) -> Self {
        self.retention_period = Some(retention_period);
        self
    }

    /// Only determine which files should be deleted
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Check if the specified retention period is less than the table's minimum
    pub fn with_enforce_retention_duration(mut self, enforce: bool) -> Self {
        self.enforce_retention_duration = enforce;
        self
    }

    /// add a time source for testing
    #[doc(hidden)]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Determine which files can be deleted. Does not actually peform the deletion
    async fn create_vacuum_plan(&self) -> Result<VacuumPlan, VacuumError> {
        let min_retention = Duration::milliseconds(self.snapshot.tombstone_retention_millis());
        let retention_period = self.retention_period.unwrap_or(min_retention);
        let enforce_retention_duration = self.enforce_retention_duration;

        if enforce_retention_duration && retention_period < min_retention {
            return Err(VacuumError::InvalidVacuumRetentionPeriod {
                provided: retention_period.num_hours(),
                min: min_retention.num_hours(),
            });
        }

        let now_millis = match &self.clock {
            Some(clock) => clock.current_timestamp_millis(),
            None => Utc::now().timestamp_millis(),
        };

        let expired_tombstones = get_stale_files(&self.snapshot, retention_period, now_millis);
        let valid_files = self.snapshot.file_paths_iter().collect::<HashSet<Path>>();

        let mut files_to_delete = vec![];
        let mut file_sizes = vec![];
        let mut all_files = self.store.list(None).await.map_err(DeltaTableError::from)?;

        let partition_columns = &self
            .snapshot
            .current_metadata()
            .ok_or(DeltaTableError::NoMetadata)?
            .partition_columns;

        while let Some(obj_meta) = all_files.next().await {
            // TODO should we allow NotFound here in case we have a temporary commit file in the list
            let obj_meta = obj_meta.map_err(DeltaTableError::from)?;
            if valid_files.contains(&obj_meta.location) // file is still being tracked in table
            || !expired_tombstones.contains(obj_meta.location.as_ref()) // file is not an expired tombstone
            || is_hidden_directory(partition_columns, &obj_meta.location)?
            {
                continue;
            }

            files_to_delete.push(obj_meta.location);
            file_sizes.push(obj_meta.size as i64);
        }

        Ok(VacuumPlan {
            files_to_delete,
            file_sizes,
            retention_check_enabled: enforce_retention_duration,
            default_retention_millis: min_retention.num_milliseconds(),
            specified_retention_millis: Some(retention_period.num_milliseconds()),
        })
    }
}

impl std::future::IntoFuture for VacuumBuilder {
    type Output = DeltaResult<(DeltaTable, VacuumMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let plan = this.create_vacuum_plan().await?;
            if this.dry_run {
                return Ok((
                    DeltaTable::new_with_state(this.store, this.snapshot),
                    VacuumMetrics {
                        files_deleted: plan.files_to_delete.iter().map(|f| f.to_string()).collect(),
                        dry_run: true,
                    },
                ));
            }

            let metrics = plan.execute(&this.store, &this.snapshot).await?;
            Ok((
                DeltaTable::new_with_state(this.store, this.snapshot),
                metrics,
            ))
        })
    }
}

/// Encapsulate which files are to be deleted and the parameters used to make that decision
struct VacuumPlan {
    /// What files are to be deleted
    pub files_to_delete: Vec<Path>,
    /// Size of each file which to delete
    pub file_sizes: Vec<i64>,
    /// If retention check is enabled
    pub retention_check_enabled: bool,
    /// Default retention in milliseconds
    pub default_retention_millis: i64,
    /// Overrided retention in milliseconds
    pub specified_retention_millis: Option<i64>,
}

impl VacuumPlan {
    /// Execute the vacuum plan and delete files from underlying storage
    pub async fn execute(
        self,
        store: &DeltaObjectStore,
        snapshot: &DeltaTableState,
    ) -> Result<VacuumMetrics, DeltaTableError> {
        if self.files_to_delete.is_empty() {
            return Ok(VacuumMetrics {
                dry_run: false,
                files_deleted: Vec::new(),
            });
        }

        let start_operation = DeltaOperation::VacuumStart {
            retention_check_enabled: self.retention_check_enabled,
            specified_retention_millis: self.specified_retention_millis,
            default_retention_millis: self.default_retention_millis,
        };

        let end_operation = DeltaOperation::VacuumEnd {
            status: String::from("COMPLETED"), // Maybe this should be FAILED when vacuum has error during the files, not sure how to check for this
        };

        let start_metrics = serde_json::to_value(VacuumStartOperationMetrics {
            num_files_to_delete: self.files_to_delete.len() as i64,
            size_of_data_to_delete: self.file_sizes.iter().sum(),
        });

        // Begin VACUUM START COMMIT
        let mut commit_info = start_operation.get_commit_info();
        let mut extra_info = Map::<String, Value>::new();

        commit_info.timestamp = Some(Utc::now().timestamp_millis());
        extra_info.insert(
            "clientVersion".to_string(),
            Value::String(format!("delta-rs.{}", crate_version())),
        );
        if let Ok(map) = start_metrics {
            extra_info.insert("operationMetrics".to_owned(), map);
        }
        commit_info.info = extra_info;

        let start_actions = vec![Action::commitInfo(commit_info)];

        commit(store, &start_actions, start_operation, snapshot, None).await?;
        // Finish VACUUM START COMMIT

        let locations = futures::stream::iter(self.files_to_delete)
            .map(Result::Ok)
            .boxed();

        let files_deleted = store
            .delete_stream(locations)
            .map(|res| match res {
                Ok(path) => Ok(path.to_string()),
                Err(Error::NotFound { path, .. }) => Ok(path),
                Err(err) => Err(err),
            })
            .try_collect::<Vec<_>>()
            .await?;

        // Create end metadata
        let end_metrics = serde_json::to_value(VacuumEndOperationMetrics {
            num_deleted_files: files_deleted.len() as i64,
            num_vacuumed_directories: 0, // Set to zero since we only remove files not dirs
        });

        // Begin VACUUM END COMMIT
        let mut commit_info = end_operation.get_commit_info();
        let mut extra_info = Map::<String, Value>::new();

        commit_info.timestamp = Some(Utc::now().timestamp_millis());
        extra_info.insert(
            "clientVersion".to_string(),
            Value::String(format!("delta-rs.{}", crate_version())),
        );
        if let Ok(map) = end_metrics {
            extra_info.insert("operationMetrics".to_owned(), map);
        }
        commit_info.info = extra_info;

        let end_actions = vec![Action::commitInfo(commit_info)];

        commit(store, &end_actions, end_operation, snapshot, None).await?;
        // Finish VACUUM END COMMIT

        Ok(VacuumMetrics {
            files_deleted,
            dry_run: false,
        })
    }
}

/// Whether a path should be hidden for delta-related file operations, such as Vacuum.
/// Names of the form partitionCol=[value] are partition directories, and should be
/// deleted even if they'd normally be hidden. The _db_index directory contains (bloom filter)
/// indexes and these must be deleted when the data they are tied to is deleted.
fn is_hidden_directory(partition_columns: &[String], path: &Path) -> Result<bool, DeltaTableError> {
    let path_name = path.to_string();
    Ok((path_name.starts_with('.') || path_name.starts_with('_'))
        && !path_name.starts_with("_delta_index")
        && !path_name.starts_with("_change_data")
        && !partition_columns
            .iter()
            .any(|partition_column| path_name.starts_with(partition_column)))
}

/// List files no longer referenced by a Delta table and are older than the retention threshold.
fn get_stale_files(
    snapshot: &DeltaTableState,
    retention_period: Duration,
    now_timestamp_millis: i64,
) -> HashSet<&str> {
    let tombstone_retention_timestamp = now_timestamp_millis - retention_period.num_milliseconds();
    snapshot
        .all_tombstones()
        .iter()
        .filter(|tombstone| {
            // if the file has a creation time before the `tombstone_retention_timestamp`
            // then it's considered as a stale file
            tombstone.deletion_timestamp.unwrap_or(0) < tombstone_retention_timestamp
        })
        .map(|tombstone| tombstone.path.as_str())
        .collect::<HashSet<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_table;
    use std::time::SystemTime;

    #[tokio::test]
    async fn vacuum_delta_8_0_table() {
        let table = open_table("./tests/data/delta-0.8.0").await.unwrap();

        let result = VacuumBuilder::new(table.object_store(), table.state.clone())
            .with_retention_period(Duration::hours(1))
            .with_dry_run(true)
            .await;

        assert!(result.is_err());

        let table = open_table("./tests/data/delta-0.8.0").await.unwrap();
        let (table, result) = VacuumBuilder::new(table.object_store(), table.state)
            .with_retention_period(Duration::hours(0))
            .with_dry_run(true)
            .with_enforce_retention_duration(false)
            .await
            .unwrap();
        // do not enforce retention duration check with 0 hour will purge all files
        assert_eq!(
            result.files_deleted,
            vec!["part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"]
        );

        let (table, result) = VacuumBuilder::new(table.object_store(), table.state)
            .with_retention_period(Duration::hours(169))
            .with_dry_run(true)
            .await
            .unwrap();

        assert_eq!(
            result.files_deleted,
            vec!["part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"]
        );

        let retention_hours = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            / 3600;
        let empty: Vec<String> = Vec::new();
        let (_table, result) = VacuumBuilder::new(table.object_store(), table.state)
            .with_retention_period(Duration::hours(retention_hours as i64))
            .with_dry_run(true)
            .await
            .unwrap();

        assert_eq!(result.files_deleted, empty);
    }
}
