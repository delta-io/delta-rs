//! Vacuum a Delta table
//!
//! Run the Vacuum command on the Delta Table: delete files no longer referenced by a Delta table and are older than the retention threshold.
//! We do not recommend that you set a retention interval shorter than 7 days, because old snapshots
//! and uncommitted files can still be in use by concurrent readers or writers to the table.
//! If vacuum cleans up active files, concurrent readers can fail or, worse, tables can be
//! corrupted when vacuum deletes files that have not yet been committed.
//! If `retention_hours` is not set then the `configuration.deletedFileRetentionDuration` of
//! delta table is used or if that's missing too, then the default value of 7 days otherwise.

use crate::action::DeltaOperation;
use crate::delta::extract_rel_path;
use crate::{DeltaDataTypeLong, DeltaDataTypeVersion, DeltaTable, DeltaTableError};
use chrono::{Duration, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

/// A source of time
pub trait Clock: Debug {
    /// get the current time in milliseconds since epoch
    fn current_timestamp_millis(&self) -> i64;
}

#[derive(Debug)]
/// Vacuum a Delta table with the given options
///
/// TODO: Talk about retention period and default retention_period..
pub struct Vacuum {
    /// Period of stale files allowed.
    pub retention_period: Option<Duration>,
    /// Validate the retention period is not below the retention period configured in the table
    pub enforce_retention_duration: bool,
    /// Don't delete the files. Just determine which files can be deleted
    pub dry_run: bool,
    /// Should metrics be committed to the log
    pub commit_to_log: bool,
    /// Override the source for the current time
    pub clock: Option<Arc<dyn Clock>>,
}

impl Default for Vacuum {
    fn default() -> Self {
        Vacuum {
            retention_period: None,
            enforce_retention_duration: true,
            dry_run: false,
            clock: None,
            commit_to_log: true,
        }
    }
}

/// Encapsulate which files are to be deleted and the parameters used to make that decision
pub struct VacuumPlan {
    /// What files are to be deleted
    pub files_to_delete: Vec<String>,
    /// What was the version of Deltatable when this plan was created
    pub read_version: DeltaDataTypeVersion,
    /// Original parameters passed
    pub params: Vacuum,
    /// The minimum retention period read from the table
    pub min_retention: Duration,
}

/// Details for the Vacuum operation including which files were
#[derive(Debug)]
pub struct VacuumMetrics {
    /// Was this a dry run
    pub dry_run: bool,
    /// Files deleted successfully
    pub files_deleted: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// Start Metrics that are committed to the delta log
pub(crate) struct VacuumStartMetrics {
    num_files_to_delete: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// End Metrics that are committed to the delta log
pub(crate) struct VacuumEndMetrics {
    // How many files directories were deleted
    num_deleted_files: usize,
}

/// Errors that can occur during vacuum
#[derive(thiserror::Error, Debug)]
pub enum VacuumError {
    /// Error returned when Vacuum retention period is below the safe threshold
    #[error(
        "Invalid retention period, minimum retention for vacuum is configured to be greater than {} hours, got {} hours", .min, .provided
    )]
    InvalidVacuumRetentionPeriod {
        /// User provided retention on vacuum call
        provided: DeltaDataTypeLong,
        /// Minimal retention configured in delta table config
        min: DeltaDataTypeLong,
    },

    /// Error returned
    #[error(transparent)]
    DeltaTable(#[from] DeltaTableError),
}

// TODO: Check if Start and End are performed for Dry Runs...
//Vacuum Start
/*  |227723 |2022-06-01 01:12:45|7135346068736757|blajda@hotmail.com|VACUUM START|{retentionCheckEnabled -> false, specifiedRetentionMillis -> 259200000, defaultRetentionMillis -> 604800000}|null|{1188643012922703}|0601-010729-sj6s5qz|227722     |SnapshotIsolation|true         |{numFilesToDelete -> 99740} */
//Vacuum End
/* |227728 |2022-06-01 01:14:51|7135346068736757|blajda@hotmail.com|VACUUM END  |{status -> COMPLETED}                                                                                       |null|{1188643012922703}|0601-010729-sj6s5qz|227727     |SnapshotIsolation|true         |{numDeletedFiles -> 99740, numVacuumedDirectories -> 90}                                                                                                                                                                                   |null        |Databricks-Runtime/10.4.x-scala2.12| */

/*                                                                                                                                                                                                                           |isBindAppend|
|296933 |2022-07-01 15:37:36|7135346068736757|blajda@hotmail.com|VACUUM END  |{status -> COMPLETED}                                               |null|{1188643012922703}|0601-010729-sj6s5qz|296932     |SnapshotIsolation|true         |{numDeletedFiles -> 56325, numVacuumedDirectories -> 118}|null        |Databricks-Runtime/10.4.x-scala2.12|
|296932 |2022-07-01 15:37:33|null            |null              |null        |null                                                                |null|null              |null               |null       |null             |null         |null                                                     |null        |null                               |
|296931 |2022-07-01 15:37:02|null            |null              |null        |null                                                                |null|null              |null               |null       |null             |null         |null                                                     |null        |null                               |
|296930 |2022-07-01 15:36:31|null            |null              |null        |null                                                                |null|null              |null               |null       |null             |null         |null                                                     |null        |null                               |
|296929 |2022-07-01 15:36:21|7135346068736757|blajda@hotmail.com|VACUUM START|{retentionCheckEnabled -> true, defaultRetentionMillis -> 604800000}|null|{1188643012922703}|0601-010729-sj6s5qz|296928     |SnapshotIsolation|true         |{numFilesToDelete -> 56325}                              |null        |Databricks-Runtime/10.4.x-scala2.12|
*/

/// List files no longer referenced by a Delta table and are older than the retention threshold.
pub(crate) fn get_stale_files(
    table: &DeltaTable,
    retention_period: Duration,
    now_timestamp_millis: i64,
) -> Result<HashSet<&str>, VacuumError> {
    let tombstone_retention_timestamp = now_timestamp_millis - retention_period.num_milliseconds();

    Ok(table
        .state
        .all_tombstones()
        .iter()
        .filter(|tombstone| {
            // if the file has a creation time before the `tombstone_retention_timestamp`
            // then it's considered as a stale file
            tombstone.deletion_timestamp.unwrap_or(0) < tombstone_retention_timestamp
        })
        .map(|tombstone| tombstone.path.as_str())
        .collect::<HashSet<_>>())
}

/// Whether a path should be hidden for delta-related file operations, such as Vacuum.
/// Names of the form partitionCol=[value] are partition directories, and should be
/// deleted even if they'd normally be hidden. The _db_index directory contains (bloom filter)
/// indexes and these must be deleted when the data they are tied to is deleted.
pub(crate) fn is_hidden_directory(
    table: &DeltaTable,
    path_name: &str,
) -> Result<bool, DeltaTableError> {
    Ok((path_name.starts_with('.') || path_name.starts_with('_'))
        && !path_name.starts_with("_delta_index")
        && !path_name.starts_with("_change_data")
        && !table
            .state
            .current_metadata()
            .ok_or(DeltaTableError::NoMetadata)?
            .partition_columns
            .iter()
            .any(|partition_column| path_name.starts_with(partition_column)))
}

impl VacuumPlan {
    /// Execute the vacuum plan and delete files from underlying storage
    pub async fn execute(self, table: &mut DeltaTable) -> Result<VacuumMetrics, VacuumError> {
        if self.files_to_delete.is_empty() {
            return Ok(VacuumMetrics {
                dry_run: false,
                files_deleted: Vec::new(),
            });
        }

        // Commit to the table that a vacuum is occuring
        if self.params.commit_to_log {
            let mut start_transaction = table.create_transaction(None);
            let mut start_metadata = Map::new();
            let start_metrics = VacuumStartMetrics {
                num_files_to_delete: self.files_to_delete.len(),
            };
            start_metadata.insert("readVersion".to_owned(), self.read_version.into());
            start_metadata.insert(
                "operationMetrics".to_owned(),
                serde_json::to_value(start_metrics)
                    .expect("VacuumStartMetrics should be serializable"),
            );

            start_transaction
                .commit(
                    Some(DeltaOperation::VacuumStart {
                        retention_check_enabled: self.params.enforce_retention_duration,
                        specified_retention_millis: self
                            .params
                            .retention_period
                            .map(|dur| dur.num_milliseconds()),
                        default_retention_millis: self.min_retention.num_milliseconds(),
                    }),
                    Some(start_metadata),
                )
                .await?;
        }

        // Delete the files
        // TODO: Modify delete_objs to returns which deletes failed and which ones passed
        let files_deleted = match table.storage.delete_objs(&self.files_to_delete).await {
            Ok(_) => Ok(self.files_to_delete),
            Err(err) => Err(VacuumError::from(DeltaTableError::StorageError {
                source: err,
            })),
        }?;

        // Commit to the table that the vacuum has finished. Even if an error occurred
        if self.params.commit_to_log {
            let mut end_transaction = table.create_transaction(None);
            let mut end_metadata = Map::new();
            let end_metrics = VacuumEndMetrics {
                num_deleted_files: files_deleted.len(),
            };
            end_metadata.insert(
                "operationMetrics".to_owned(),
                serde_json::to_value(end_metrics).expect("VacuumEndMetrics should be serializable"),
            );

            // TODO: Having the end_transaction fail isn't really a big deal...
            // TODO: Determine what kind of statues are accepted.
            end_transaction
                .commit(
                    Some(DeltaOperation::VacuumEnd {
                        status: "COMPLETED".to_string(),
                    }),
                    Some(end_metadata),
                )
                .await?;
        }

        Ok(VacuumMetrics {
            files_deleted,
            dry_run: false,
        })
    }
}

/// Determine which files can be deleted. Does not actually peform the deletion
pub async fn create_vacuum_plan(
    table: &DeltaTable,
    params: Vacuum,
) -> Result<VacuumPlan, VacuumError> {
    let read_version = table.version();
    let min_retention = Duration::milliseconds(table.state.tombstone_retention_millis());
    let retention_period = params.retention_period.unwrap_or(min_retention);
    let enforce_retention_duration = params.enforce_retention_duration;

    if enforce_retention_duration && retention_period < min_retention {
        return Err(VacuumError::InvalidVacuumRetentionPeriod {
            provided: retention_period.num_hours(),
            min: min_retention.num_hours(),
        });
    }

    let now_millis = match &params.clock {
        Some(clock) => clock.current_timestamp_millis(),
        _ => Utc::now().timestamp_millis(),
    };

    let expired_tombstones = get_stale_files(table, retention_period, now_millis)?;
    let valid_files = table.get_file_set();

    let mut files_to_delete = vec![];
    let mut all_files = table
        .storage
        .list_objs(&table.table_uri)
        .await
        .map_err(DeltaTableError::from)?;

    while let Some(obj_meta) = all_files.next().await {
        let obj_meta = obj_meta.map_err(DeltaTableError::from)?;
        let rel_path = extract_rel_path(&table.table_uri, &obj_meta.path)?;

        if valid_files.contains(rel_path) // file is still being tracked in table
            || !expired_tombstones.contains(rel_path) // file is not an expired tombstone
            || is_hidden_directory(table, rel_path)?
        {
            continue;
        }

        files_to_delete.push(obj_meta.path);
    }

    Ok(VacuumPlan {
        files_to_delete,
        read_version,
        params,
        min_retention,
    })
}

/// Methods to specify various vacuum options and to execute the operation
impl Vacuum {
    /// Override the default rention period for which files are deleted.
    pub fn with_retention_period(mut self, retention_period: Duration) -> Self {
        self.retention_period = Some(retention_period);
        self
    }

    /// Only determine which files should be deleted
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Check if the specified retention period is less than the table's minimum
    pub fn enforce_retention_duration(mut self, enforce: bool) -> Self {
        self.enforce_retention_duration = enforce;
        self
    }

    /// Should vacuum start and vacuum end statistics be committed to the log
    pub fn commit_to_log(mut self, commit: bool) -> Self {
        self.commit_to_log = commit;
        self
    }

    /// Perform the vacuum. Returns metrics on which files were deleted
    pub async fn execute(self, table: &mut DeltaTable) -> Result<VacuumMetrics, VacuumError> {
        let dry_run = self.dry_run;
        let plan = create_vacuum_plan(table, self).await?;
        if dry_run {
            return Ok(VacuumMetrics {
                files_deleted: plan.files_to_delete,
                dry_run: true,
            });
        }

        return plan.execute(table).await;
    }
}
