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
use tracing::log::*;

use super::{CustomExecuteHandler, Operation};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::logstore::{LogStore, LogStoreRef};
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaTable, DeltaTableConfig};

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

/// Type of Vacuum operation to perform
#[derive(Debug, Default, Clone, PartialEq)]
pub enum VacuumMode {
    /// The `lite` mode will only remove files which are referenced in the `_delta_log` associated
    /// with `remove` action
    #[default]
    Lite,
    /// A `full` mode vacuum will remove _all_ data files no longer actively referenced in the
    /// `_delta_log` table. For example, if parquet files exist in the table directory but are no
    /// longer mentioned as `add` actions in the transaction log, then this mode will scan storage
    /// and remove those files.
    Full,
}

/// Vacuum a Delta table with the given options
/// See this module's documentation for more information
pub struct VacuumBuilder {
    /// A snapshot of the to-be-vacuumed table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Period of stale files allowed.
    retention_period: Option<Duration>,
    /// Validate the retention period is not below the retention period configured in the table
    enforce_retention_duration: bool,
    /// Keep files associated with particular versions
    keep_versions: Option<Vec<i64>>,
    /// Don't delete the files. Just determine which files can be deleted
    dry_run: bool,
    /// Mode of vacuum that should be run
    mode: VacuumMode,
    /// Override the source of time
    clock: Option<Arc<dyn Clock>>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation<()> for VacuumBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

/// Details for the Vacuum operation including which files were
#[derive(Debug, Default)]
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
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        VacuumBuilder {
            snapshot,
            log_store,
            retention_period: None,
            enforce_retention_duration: true,
            keep_versions: None,
            dry_run: false,
            mode: VacuumMode::Lite,
            clock: None,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Override the default retention period for which files are deleted.
    pub fn with_retention_period(mut self, retention_period: Duration) -> Self {
        self.retention_period = Some(retention_period);
        self
    }

    /// Specify table versions that we want to keep for time travel.
    /// This will prevent deletion of files required by these versions.
    pub fn with_keep_versions(mut self, versions: &[i64]) -> Self {
        warn!("Using experimental API VacuumBuilder::with_keep_versions");
        self.keep_versions = Some(versions.to_vec());
        self
    }

    /// Override the default vacuum mode (lite)
    pub fn with_mode(mut self, mode: VacuumMode) -> Self {
        self.mode = mode;
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

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    /// Determine which files can be deleted. Does not actually perform the deletion
    async fn create_vacuum_plan(&self) -> Result<VacuumPlan, VacuumError> {
        if self.mode == VacuumMode::Full {
            info!("Vacuum configured to run with 'VacuumMode::Full'. It will scan for orphaned parquet files in the Delta table directory and remove those as well!");
        }

        let min_retention = Duration::milliseconds(
            self.snapshot
                .table_config()
                .deleted_file_retention_duration()
                .as_millis() as i64,
        );
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

        let keep_files = match &self.keep_versions {
            Some(versions) if !versions.is_empty() => {
                let mut sorted_versions = versions.clone();
                sorted_versions.sort();
                let mut keep_files: HashSet<String> = HashSet::new();
                let mut state = DeltaTableState::try_new(
                    &self.log_store,
                    DeltaTableConfig::default(),
                    Some(versions[0]),
                )
                .await?;
                for version in sorted_versions {
                    state.update(&self.log_store, Some(version)).await?;
                    let files: Vec<String> = state
                        .file_paths_iter()
                        .map(|path| path.to_string())
                        .collect();
                    debug!("keep version:{}\n, {:#?}", version, files);
                    keep_files.extend(files);
                }

                keep_files
            }
            _ => HashSet::new(),
        };

        let expired_tombstones = get_stale_files(
            &self.snapshot,
            retention_period,
            now_millis,
            &self.log_store,
        )
        .await?;
        let valid_files = self.snapshot.file_paths_iter().collect::<HashSet<Path>>();

        let mut files_to_delete = vec![];
        let mut file_sizes = vec![];
        let object_store = self.log_store.object_store(None);
        let mut all_files = object_store.list(None);
        let partition_columns = self.snapshot.metadata().partition_columns();

        while let Some(obj_meta) = all_files.next().await {
            // TODO should we allow NotFound here in case we have a temporary commit file in the list
            let obj_meta = obj_meta.map_err(DeltaTableError::from)?;
            // file is still being tracked in table
            if valid_files.contains(&obj_meta.location) {
                continue;
            }
            // file is associated with a version that we are keeping
            if keep_files.contains(&obj_meta.location.to_string()) {
                debug!(
                    "The file {:?} is in a version specified to be kept by the user, skipping",
                    &obj_meta.location
                );
                continue;
            }
            if is_hidden_directory(partition_columns, &obj_meta.location)? {
                continue;
            }
            // file is not an expired tombstone _and_ this is a "Lite" vacuum
            // If the file is not an expired tombstone and we have gotten to here with a
            // VacuumMode::Full then it should be added to the deletion plan
            if !expired_tombstones.contains(obj_meta.location.as_ref()) {
                if self.mode == VacuumMode::Lite {
                    debug!("The file {:?} was not referenced in a log file, but VacuumMode::Lite means it will not be vacuumed", &obj_meta.location);
                    continue;
                } else {
                    debug!("The file {:?} was not referenced in a log file, but VacuumMode::Full means it *will be vacuumed*", &obj_meta.location);
                }
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
            if !&this.snapshot.load_config().require_files {
                return Err(DeltaTableError::NotInitializedWithFiles("VACUUM".into()));
            }

            let plan = this.create_vacuum_plan().await?;
            if this.dry_run {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, this.snapshot),
                    VacuumMetrics {
                        files_deleted: plan.files_to_delete.iter().map(|f| f.to_string()).collect(),
                        dry_run: true,
                    },
                ));
            }
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let result = plan
                .execute(
                    this.log_store.clone(),
                    &this.snapshot,
                    this.commit_properties.clone(),
                    operation_id,
                    this.get_custom_execute_handler(),
                )
                .await?;

            this.post_execute(operation_id).await?;

            Ok(match result {
                Some((snapshot, metrics)) => (
                    DeltaTable::new_with_state(this.log_store, snapshot),
                    metrics,
                ),
                None => (
                    DeltaTable::new_with_state(this.log_store, this.snapshot),
                    Default::default(),
                ),
            })
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
    /// Overridden retention in milliseconds
    pub specified_retention_millis: Option<i64>,
}

impl VacuumPlan {
    /// Execute the vacuum plan and delete files from underlying storage
    pub async fn execute(
        self,
        store: LogStoreRef,
        snapshot: &DeltaTableState,
        mut commit_properties: CommitProperties,
        operation_id: uuid::Uuid,
        handle: Option<Arc<dyn CustomExecuteHandler>>,
    ) -> Result<Option<(DeltaTableState, VacuumMetrics)>, DeltaTableError> {
        if self.files_to_delete.is_empty() {
            return Ok(None);
        }

        let start_operation = DeltaOperation::VacuumStart {
            retention_check_enabled: self.retention_check_enabled,
            specified_retention_millis: self.specified_retention_millis,
            default_retention_millis: self.default_retention_millis,
        };

        let end_operation = DeltaOperation::VacuumEnd {
            status: String::from("COMPLETED"), // Maybe this should be FAILED when vacuum has error during the files, not sure how to check for this
        };

        let start_metrics = VacuumStartOperationMetrics {
            num_files_to_delete: self.files_to_delete.len() as i64,
            size_of_data_to_delete: self.file_sizes.iter().sum(),
        };

        // Begin VACUUM START COMMIT
        let mut start_props = CommitProperties::default();
        start_props.app_metadata = commit_properties.app_metadata.clone();
        start_props.app_metadata.insert(
            "operationMetrics".to_owned(),
            serde_json::to_value(start_metrics)?,
        );

        let last_commit = CommitBuilder::from(start_props)
            .with_operation_id(operation_id)
            .with_post_commit_hook_handler(handle.clone())
            .build(Some(snapshot), store.clone(), start_operation)
            .await?;
        // Finish VACUUM START COMMIT

        let locations = futures::stream::iter(self.files_to_delete)
            .map(Result::Ok)
            .boxed();

        let files_deleted = store
            .object_store(Some(operation_id))
            .delete_stream(locations)
            .map(|res| match res {
                Ok(path) => Ok(path.to_string()),
                Err(Error::NotFound { path, .. }) => Ok(path),
                Err(err) => Err(err),
            })
            .try_collect::<Vec<_>>()
            .await?;

        // Create end metadata
        let end_metrics = VacuumEndOperationMetrics {
            num_deleted_files: files_deleted.len() as i64,
            num_vacuumed_directories: 0, // Set to zero since we only remove files not dirs
        };

        // Begin VACUUM END COMMIT
        commit_properties.app_metadata.insert(
            "operationMetrics".to_owned(),
            serde_json::to_value(end_metrics)?,
        );
        let last_commit = CommitBuilder::from(commit_properties)
            .with_operation_id(operation_id)
            .with_post_commit_hook_handler(handle)
            .build(Some(&last_commit.snapshot), store.clone(), end_operation)
            .await?;
        // Finish VACUUM END COMMIT

        Ok(Some((
            last_commit.snapshot,
            VacuumMetrics {
                files_deleted,
                dry_run: false,
            },
        )))
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
async fn get_stale_files(
    snapshot: &DeltaTableState,
    retention_period: Duration,
    now_timestamp_millis: i64,
    store: &dyn LogStore,
) -> DeltaResult<HashSet<String>> {
    let tombstone_retention_timestamp = now_timestamp_millis - retention_period.num_milliseconds();
    Ok(snapshot
        .all_tombstones(store)
        .await?
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|tombstone| {
            // if the file has a creation time before the `tombstone_retention_timestamp`
            // then it's considered as a stale file
            tombstone.deletion_timestamp.unwrap_or(0) < tombstone_retention_timestamp
        })
        .map(|tombstone| tombstone.path)
        .collect::<HashSet<_>>())
}

#[cfg(test)]
mod tests {
    use object_store::{local::LocalFileSystem, memory::InMemory, GetResult, PutPayload};

    use super::*;
    use crate::{checkpoints::create_checkpoint, open_table};
    use std::{io::Read, time::SystemTime};

    #[tokio::test]
    async fn test_vacuum_full() -> DeltaResult<()> {
        let table = open_table("../test/tests/data/simple_commit").await?;

        let (_table, result) = VacuumBuilder::new(table.log_store(), table.snapshot()?.clone())
            .with_retention_period(Duration::hours(0))
            .with_dry_run(true)
            .with_mode(VacuumMode::Lite)
            .with_enforce_retention_duration(false)
            .await?;
        // When running lite, this table with superfluous parquet files should not have anything to
        // delete
        assert!(result.files_deleted.is_empty());

        let (_table, result) = VacuumBuilder::new(table.log_store(), table.snapshot()?.clone())
            .with_retention_period(Duration::hours(0))
            .with_dry_run(true)
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .await?;
        let mut files_deleted = result.files_deleted.clone();
        files_deleted.sort();
        // When running with full, these superfluous parquet files which are not actually
        // referenced in the _delta_log commits should be considered for the
        // low-orbit ion-cannon
        assert_eq!(
            files_deleted,
            vec![
                "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet",
                "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
                "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
                "part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet",
            ]
        );
        Ok(())
    }

    /// This test simply ensures that with_keep_versions invocation of [VacuumBuilder] removes
    /// fewer files than a full vacuum.
    #[tokio::test]
    async fn test_vacuum_keep_version_sanity_check() -> DeltaResult<()> {
        let table_loc = "../test/tests/data/simple_table";
        let table = open_table(table_loc).await?;
        let versions_to_keep = vec![3];

        // First, vacuum without keeping any particular versions
        let (_table, result) = VacuumBuilder::new(table.log_store(), table.snapshot()?.clone())
            .with_retention_period(Duration::hours(0))
            .with_dry_run(true)
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .await?;

        // Our simple_table has 32 data files in it which could be vacuumed.
        assert_eq!(32, result.files_deleted.len());

        // Next, vacuum with specific versions retained
        let (_table, result) = VacuumBuilder::new(table.log_store(), table.snapshot()?.clone())
            .with_retention_period(Duration::hours(0))
            .with_keep_versions(&versions_to_keep)
            .with_dry_run(true)
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .await?;
        assert_ne!(
            32,
            result.files_deleted.len(),
            "with_keep_versions should have fewer files deleted than a full vacuum"
        );

        Ok(())
    }

    /// This test ensures that with_keep_versions invocations retain files which are removed within
    /// the context of the kept ranges
    #[tokio::test]
    async fn test_vacuum_keep_version_add_removes() -> DeltaResult<()> {
        let table_loc = "../test/tests/data/simple_table";
        let table = open_table(table_loc).await?;
        let versions_to_keep = vec![2, 3];

        // First, vacuum without keeping any particular versions
        let (_table, result) = VacuumBuilder::new(table.log_store(), table.snapshot()?.clone())
            .with_retention_period(Duration::hours(0))
            .with_dry_run(true)
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .await?;

        // Our simple_table has 32 data files in it which could be vacuumed.
        assert_eq!(32, result.files_deleted.len());

        // Next, vacuum with specific versions retained
        let (_table, result) = VacuumBuilder::new(table.log_store(), table.snapshot()?.clone())
            .with_retention_period(Duration::hours(0))
            .with_keep_versions(&versions_to_keep)
            .with_dry_run(true)
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .await?;
        assert_ne!(
            32,
            result.files_deleted.len(),
            "with_keep_versions should have fewer files deleted than a full vacuum"
        );

        let kept_files = vec![
            // Adds from v3
            "part-00000-f17fcbf5-e0dc-40ba-adae-ce66d1fcaef6-c000.snappy.parquet",
            "part-00001-bb70d2ba-c196-4df2-9c85-f34969ad3aa9-c000.snappy.parquet",
            // Removes from v3, these were add in v2
            "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet",
            "part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet",
        ];

        for kept in kept_files {
            assert!(
                !result.files_deleted.contains(&kept.to_string()),
                "files_deleted contains something which should be kept!: {:#?} {kept}",
                result.files_deleted
            )
        }
        Ok(())
    }

    // This test will do some table operations after executing a vacuum with versions to ensure
    // that the table is still functional, can be read, checkpointed, etc.
    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_vacuum_keep_version_validity() -> DeltaResult<()> {
        use datafusion::prelude::SessionContext;
        use object_store::GetResultPayload;
        let store = InMemory::new();
        let source = LocalFileSystem::new_with_prefix("../test/tests/data/simple_table")?;
        let mut stream = source.list(None);

        while let Some(Ok(entity)) = stream.next().await {
            let mut contents = vec![];
            match source.get(&entity.location).await?.payload {
                GetResultPayload::File(mut fd, _path) => {
                    fd.read_to_end(&mut contents)?;
                }
                _ => panic!("We should only be dealing in files!"),
            }
            let content = bytes::Bytes::from(contents);
            store
                .put(&entity.location, PutPayload::from_bytes(content))
                .await?;
        }

        let mut table = crate::DeltaTableBuilder::from_valid_uri("memory:///")?
            .with_storage_backend(Arc::new(store), url::Url::parse("memory:///").unwrap())
            .build()?;
        table.load().await?;

        let (mut table, result) = VacuumBuilder::new(table.log_store(), table.snapshot()?.clone())
            .with_retention_period(Duration::hours(0))
            .with_keep_versions(&[2, 3])
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .await?;
        // Our simple_table has 32 data files in it, and we shouldn't have deleted them all!
        assert_ne!(32, result.files_deleted.len());

        // Can we checkpoint it?
        create_checkpoint(&table, None).await?;
        table.load().await?;
        assert_eq!(Some(6), table.version());

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))?;
        let batches = ctx.sql("SELECT * FROM test").await?.collect().await?;

        Ok(())
    }

    #[tokio::test]
    async fn vacuum_delta_8_0_table() -> DeltaResult<()> {
        let table = open_table("../test/tests/data/delta-0.8.0").await.unwrap();

        let result = VacuumBuilder::new(table.log_store(), table.snapshot().unwrap().clone())
            .with_retention_period(Duration::hours(1))
            .with_dry_run(true)
            .await;

        assert!(result.is_err());

        let table = open_table("../test/tests/data/delta-0.8.0").await.unwrap();

        let (table, result) =
            VacuumBuilder::new(table.log_store(), table.snapshot().unwrap().clone())
                .with_retention_period(Duration::hours(0))
                .with_dry_run(true)
                .with_enforce_retention_duration(false)
                .await?;
        // do not enforce retention duration check with 0 hour will purge all files
        assert_eq!(
            result.files_deleted,
            vec!["part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"]
        );

        let (table, result) =
            VacuumBuilder::new(table.log_store(), table.snapshot().unwrap().clone())
                .with_retention_period(Duration::hours(169))
                .with_dry_run(true)
                .await?;

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
        let (_table, result) =
            VacuumBuilder::new(table.log_store(), table.snapshot().unwrap().clone())
                .with_retention_period(Duration::hours(retention_hours as i64))
                .with_dry_run(true)
                .await?;

        assert_eq!(result.files_deleted, empty);
        Ok(())
    }
}
