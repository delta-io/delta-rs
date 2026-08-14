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
//! let mut table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
//! let (table, metrics) = VacuumBuilder::new(table.object_store(). table.state).await?;
//! ````

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use chrono::{Duration, Utc};
use futures::channel::mpsc;
use futures::future::{BoxFuture, ready};
use futures::{SinkExt, Stream, StreamExt, TryStreamExt, stream};
use object_store::{Error, ObjectStore, path::Path};
use serde::Serialize;
use tracing::*;

use super::{CustomExecuteHandler, Operation};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    ActiveAddOptions, AddStatsPolicy, EagerSnapshot, TombstoneView, Version, resolve_snapshot,
};
use crate::logstore::{LogStore, LogStoreRef};
use crate::protocol::DeltaOperation;
use crate::table::config::TablePropertiesExt as _;
use crate::table::state::DeltaTableState;
use crate::{DeltaTable, DeltaTableConfig};

const DEFAULT_VACUUM_LIST_CONCURRENCY: usize = 10;

/// Default scan concurrency from env (`DELTARS_VACUUM_LIST_CONCURRENCY`) or built-in default.
///
/// Cached process-wide. Prefer [`VacuumBuilder::with_scan_concurrency`] for per-operation control.
fn default_vacuum_list_concurrency() -> usize {
    static LIST_CONCURRENCY: OnceLock<usize> = OnceLock::new();
    *LIST_CONCURRENCY.get_or_init(|| {
        std::env::var("DELTARS_VACUUM_LIST_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(DEFAULT_VACUUM_LIST_CONCURRENCY)
    })
}

fn resolve_scan_concurrency(override_value: Option<usize>) -> usize {
    override_value
        .filter(|&value| value > 0)
        .unwrap_or_else(default_vacuum_list_concurrency)
}

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

    /// Failed while collecting orphaned files during the full-mode scan.
    #[error("Failed to scan for orphaned files: {0}")]
    OrphanScanError(String),
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
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Period of stale files allowed.
    retention_period: Option<Duration>,
    /// Validate the retention period is not below the retention period configured in the table
    enforce_retention_duration: bool,
    /// Keep files associated with particular versions
    keep_versions: Option<Vec<Version>>,
    /// Don't delete the files. Just determine which files can be deleted
    dry_run: bool,
    /// Mode of vacuum that should be run
    mode: VacuumMode,
    /// Max concurrent object-store LIST operations during full-mode scan.
    ///
    /// `None` uses [`default_vacuum_list_concurrency`] (env or built-in default).
    scan_concurrency: Option<usize>,
    /// By default, true. If true, it will scan the files parallelizing
    /// through prefix-expansion path, otherwise, if false it will use
    /// flat `list(None)` full-mode scan.
    parallel_scan: bool,
    /// Override the source of time
    clock: Option<Arc<dyn Clock>>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for VacuumBuilder {
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
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        VacuumBuilder {
            snapshot,
            log_store,
            retention_period: None,
            enforce_retention_duration: true,
            keep_versions: None,
            dry_run: false,
            mode: VacuumMode::Lite,
            scan_concurrency: None,
            parallel_scan: true,
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

    /// Set the maximum number of concurrent object-store LIST operations used
    /// during full-mode vacuum scanning (partition prefix expansion and leaf
    /// orphan listing).
    ///
    /// Higher values can speed up scans on high-latency object stores but may
    /// trigger throttling (e.g. HTTP 429/503). LIST retries/backoff are handled
    /// by the underlying object store's [`object_store::RetryConfig`], not by
    /// vacuum itself—if a LIST ultimately fails, the vacuum operation fails.
    ///
    /// When unset, concurrency is taken from the `DELTARS_VACUUM_LIST_CONCURRENCY`
    /// environment variable if set to a positive integer, otherwise defaults to 10.
    /// Values of `0` are ignored and fall back to that default resolution.
    pub fn with_scan_concurrency(mut self, concurrency: usize) -> Self {
        self.scan_concurrency = Some(concurrency);
        self
    }

    /// Flag to enable/disable the parallel multi-level partition scan used by full-mode vacuum.
    ///
    /// By default, tables with more than one partition column use hierarchical
    /// prefix expansion and concurrent leaf LIST calls. Calling this with `false` forces the
    /// flat `list(None)` scan path instead.
    pub fn parallel_scan(mut self, parallel_scan: bool) -> Self {
        self.parallel_scan = parallel_scan;
        self
    }

    /// Specify table versions that we want to keep for time travel.
    /// This will prevent deletion of files required by these versions.
    pub fn with_keep_versions(mut self, versions: &[Version]) -> Self {
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
    async fn create_vacuum_plan(
        &self,
        snapshot: &EagerSnapshot,
    ) -> Result<VacuumPlan, VacuumError> {
        if self.mode == VacuumMode::Full {
            info!(
                "Vacuum configured to run with 'VacuumMode::Full'. It will scan for orphaned parquet files in the Delta table directory and remove those as well!"
            );
        }

        let min_retention = Duration::milliseconds(
            snapshot
                .table_properties()
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
            Some(versions) => {
                let mut sorted_versions = versions.clone();
                sorted_versions.sort();
                let mut sorted_versions = sorted_versions.into_iter();
                match sorted_versions.next() {
                    Some(initial_version) => {
                        let mut keep_files: HashSet<String> = HashSet::new();
                        let mut state = DeltaTableState::try_new(
                            &self.log_store,
                            DeltaTableConfig::default(),
                            Some(initial_version),
                        )
                        .await?;
                        let mut record_keep_files = |version: Version, state: &DeltaTableState| {
                            let files: Vec<String> = state
                                .log_data()
                                .into_iter()
                                .map(|add| add.object_store_path())
                                .map(|path| path.to_string())
                                .collect();
                            debug!("keep version:{version}\n, {files:#?}");
                            keep_files.extend(files);
                        };

                        record_keep_files(initial_version, &state);
                        for version in sorted_versions {
                            state.update(&self.log_store, Some(version)).await?;
                            record_keep_files(version, &state);
                        }

                        keep_files
                    }
                    None => HashSet::new(),
                }
            }
            _ => HashSet::new(),
        };

        let mut file_count = 0;

        let tombstone_retention_timestamp = now_millis - retention_period.num_milliseconds();
        let (expired_tombstones, tombstone_path_sets) = if self.mode == VacuumMode::Full {
            collect_full_mode_tombstones(snapshot, tombstone_retention_timestamp, &self.log_store)
                .await?
        } else {
            (
                get_stale_files(snapshot, retention_period, now_millis, &self.log_store).await?,
                TombstonePathSets::default(),
            )
        };
        let valid_files: HashSet<_> = snapshot
            .snapshot()
            .active_adds(
                self.log_store.as_ref(),
                ActiveAddOptions {
                    predicate: None,
                    stats: AddStatsPolicy::None,
                },
            )
            .map_ok(|f| f.object_store_path())
            .try_collect()
            .await?;

        let partition_columns = snapshot.metadata().partition_columns();

        let mut files_to_delete = vec![];
        let mut file_sizes = vec![];

        // VacuumMode::Lite file set
        // Expired tombstones are *always deleted (*unless in keep list)
        for tombs in expired_tombstones.iter() {
            let path = Path::from(tombs.path().to_string());
            if ok_to_delete(&path, &valid_files, &keep_files, partition_columns)? {
                files_to_delete.push(path);
                file_sizes.push(tombs.size().unwrap_or(0));
            }
        }

        if self.mode == VacuumMode::Full {
            let object_store = self.log_store.object_store(None);

            if self.parallel_scan && should_try_parallel_vacuum(partition_columns) {
                let valid_files = Arc::new(valid_files);
                let keep_files = Arc::new(keep_files);
                let tombstone_path_sets = Arc::new(tombstone_path_sets);
                let partition_columns: Arc<Vec<String>> = Arc::new(partition_columns.to_vec());
                let retention_millis = retention_period.num_milliseconds();
                let scan_concurrency = resolve_scan_concurrency(self.scan_concurrency);
                // Stop before the last partition column so leaf LIST targets are
                // prefix paths of depth n-1 (e.g. date=…/), not the final fan-out
                // (e.g. date=…/part=…/).
                let partition_depth = partition_columns.len() - 1;

                let parallel_span = info_span!(
                    "list_files_parallel",
                    operation = "vacuum",
                    partition_depth,
                    scan_concurrency,
                );
                async {
                    // Walk n-1 delimiter levels, streaming leaf prefixes as they
                    // are discovered so leaf LIST can start before the full
                    // prefix set is materialised. Intermediate-level objects are
                    // reported via callback.
                    let expand_scanned = Arc::new(AtomicUsize::new(0));
                    let leaf_scanned = Arc::new(AtomicUsize::new(0));

                    // Expand runs in a spawned task; buffer intermediate orphans
                    // until the leaf pipeline finishes (usually few/none).
                    let intermediate_orphans =
                        Arc::new(std::sync::Mutex::new(Vec::<(Path, i64)>::new()));
                    let intermediate_orphans_cb = Arc::clone(&intermediate_orphans);

                    expand_partition_prefixes(
                        object_store.clone(),
                        partition_depth,
                        Arc::clone(&valid_files),
                        Arc::clone(&keep_files),
                        Arc::clone(&partition_columns),
                        Arc::clone(&tombstone_path_sets),
                        now_millis,
                        retention_millis,
                        Arc::clone(&expand_scanned),
                        scan_concurrency,
                        move |path, size| {
                            intermediate_orphans_cb
                                .lock()
                                .map_err(|_| {
                                    VacuumError::OrphanScanError("Failed to lock mutex".to_string())
                                })?
                                .push((path, size));
                            Ok(())
                        },
                    )
                    .map(|prefix_res| {
                        let store = object_store.clone();
                        let valid_files = Arc::clone(&valid_files);
                        let keep_files = Arc::clone(&keep_files);
                        let tombstone_path_sets = Arc::clone(&tombstone_path_sets);
                        let partition_columns = Arc::clone(&partition_columns);
                        let scanned = Arc::clone(&leaf_scanned);
                        // Lazy leaf-list stream per prefix. LIST runs on poll;
                        // concurrency comes from try_flatten_unordered below.
                        prefix_res.map(|prefix| {
                            list_orphans_under_prefix(
                                store,
                                prefix,
                                valid_files,
                                keep_files,
                                partition_columns,
                                tombstone_path_sets,
                                now_millis,
                                retention_millis,
                                scanned,
                            )
                        })
                    })
                    .try_flatten_unordered(scan_concurrency)
                    .try_for_each(|(path, size)| {
                        files_to_delete.push(path);
                        file_sizes.push(size);
                        ready(Ok::<_, DeltaTableError>(()))
                    })
                    .await?;

                    let mut intermediate = intermediate_orphans.lock().map_err(|_| {
                        VacuumError::OrphanScanError("Failed to lock mutex".to_string())
                    })?;

                    for (path, size) in intermediate.drain(..) {
                        files_to_delete.push(path);
                        file_sizes.push(size);
                    }

                    file_count += expand_scanned.load(Ordering::Relaxed);
                    file_count += leaf_scanned.load(Ordering::Relaxed);

                    Ok::<_, VacuumError>(())
                }
                .instrument(parallel_span)
                .await?;
            } else {
                let list_span = info_span!("list_files", operation = "vacuum");
                let mut all_files = list_span.in_scope(|| object_store.list(None));

                while let Some(obj_meta) = all_files.next().await {
                    // TODO should we allow NotFound here in case we have a temporary commit file in the list
                    let obj_meta = obj_meta.map_err(DeltaTableError::from)?;
                    if let Some((path, size)) = consider_orphan_for_deletion(
                        &obj_meta,
                        &valid_files,
                        &keep_files,
                        partition_columns,
                        &tombstone_path_sets,
                        now_millis,
                        retention_period.num_milliseconds(),
                    )? {
                        files_to_delete.push(path);
                        file_sizes.push(size);
                        file_count += 1;
                    }
                }
            }
        }
        info!(
            files_scanned = file_count,
            files_to_delete = files_to_delete.len(),
            "vacuum file listing completed"
        );

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
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), true, None).await?;
            let plan = this.create_vacuum_plan(&snapshot).await?;

            if this.dry_run {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot)),
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
                    &snapshot,
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
                    DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot)),
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
        snapshot: &EagerSnapshot,
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

#[derive(Debug, Default, PartialEq, Eq)]
struct TombstonePathSets {
    expired_tombstone_paths: HashSet<Path>,
    all_tombstone_paths: HashSet<Path>,
}

impl TombstonePathSets {
    fn record(&mut self, path: Path, is_expired: bool) {
        if is_expired {
            self.expired_tombstone_paths.insert(path.clone());
        }
        self.all_tombstone_paths.insert(path);
    }
}

/// Whether a path should be hidden for delta-related file operations, such as Vacuum.
/// Names of the form partitionCol=[value] are partition directories, and should be
/// deleted even if they'd normally be hidden. The _db_index directory contains (bloom filter)
/// indexes and these must be deleted when the data they are tied to is deleted.
fn is_hidden_directory(partition_columns: &[String], path: &Path) -> Result<bool, DeltaTableError> {
    let path_name = path.as_ref();
    Ok((path_name.starts_with('.') || path_name.starts_with('_'))
        && !path_name.starts_with("_delta_index")
        && !path_name.starts_with("_change_data")
        && !partition_columns
            .iter()
            .any(|partition_column| path_name.starts_with(partition_column)))
}

/// Returns true if the file at `location` is a candidate for deletion.
/// A file should NOT be deleted if it is still tracked in the table,
/// associated with a kept version, or is a hidden directory.
fn ok_to_delete(
    location: &Path,
    valid_files: &HashSet<Path>,
    keep_files: &HashSet<String>,
    partition_columns: &[String],
) -> Result<bool, DeltaTableError> {
    Ok(
        !(valid_files.contains(location) // file is still being tracked in table
        || keep_files.contains(&location.to_string()) // file is associated with a version that we are keeping
        || is_hidden_directory(partition_columns, location)?),
    )
}

/// Decide whether a listed object is an orphan eligible for full-mode vacuum.
///
/// Returns `Some((path, size))` when the object should be deleted, or `None`
/// when it is still referenced, kept, hidden, recently tombstoned, or too new.
fn consider_orphan_for_deletion(
    obj_meta: &object_store::ObjectMeta,
    valid_files: &HashSet<Path>,
    keep_files: &HashSet<String>,
    partition_columns: &[String],
    tombstone_path_sets: &TombstonePathSets,
    now_millis: i64,
    retention_millis: i64,
) -> Result<Option<(Path, i64)>, DeltaTableError> {
    if tombstone_path_sets
        .expired_tombstone_paths
        .contains(&obj_meta.location)
    {
        debug!(
            "The file {:?} is already queued as an expired tombstone",
            &obj_meta.location,
        );
        return Ok(None);
    }

    if !ok_to_delete(
        &obj_meta.location,
        valid_files,
        keep_files,
        partition_columns,
    )? {
        return Ok(None);
    }

    if tombstone_path_sets
        .all_tombstone_paths
        .contains(&obj_meta.location)
    {
        debug!(
            "The file {:?} has a recent tombstone, keeping it until tombstone retention expires",
            &obj_meta.location,
        );
        return Ok(None);
    }

    // At this point the path is untracked by the Delta log, so full mode falls back
    // to physical object age to protect recent concurrent-writer output.
    let file_age_millis = now_millis - obj_meta.last_modified.timestamp_millis();
    if file_age_millis < retention_millis {
        debug!(
            "The file {:?} is an untracked recent file, protecting it from vacuum",
            &obj_meta.location,
        );
        return Ok(None);
    }

    debug!(
        "The file {:?} is an untracked stale orphan and will be vacuumed in full mode",
        &obj_meta.location
    );
    Ok(Some((obj_meta.location.clone(), obj_meta.size as i64)))
}

/// Root prefixes that should not be expanded during full-mode partition walks.
fn is_skippable_root_prefix(path: &Path) -> bool {
    let path_name = path.as_ref();
    path_name == "_delta_log" || path_name.starts_with("_delta_log/") || path_name.starts_with('.')
}

/// Walk `partition_depth` delimiter levels and stream leaf partition prefixes.
///
/// Non-final levels still keep an in-memory prefix frontier (required for BFS).
/// Delimiter results are folded as they complete (no per-level `Vec<ListResult>`).
/// At the final level each child prefix is yielded immediately.
/// Intermediate-level orphans are reported via `on_intermediate_orphan`.
/// Every listed object increments `scanned`.
fn expand_partition_prefixes(
    store: Arc<dyn ObjectStore>,
    partition_depth: usize,
    valid_files: Arc<HashSet<Path>>,
    keep_files: Arc<HashSet<String>>,
    partition_columns: Arc<Vec<String>>,
    tombstone_path_sets: Arc<TombstonePathSets>,
    now_millis: i64,
    retention_millis: i64,
    scanned: Arc<AtomicUsize>,
    scan_concurrency: usize,
    mut on_intermediate_orphan: impl FnMut(Path, i64) -> Result<(), DeltaTableError> + Send + 'static,
) -> impl Stream<Item = Result<Path, DeltaTableError>> {
    let (mut tx, rx) = mpsc::channel(scan_concurrency.saturating_mul(4).max(16));

    tokio::spawn(async move {
        let mut frontier: Vec<Option<Path>> = vec![None];

        for level in 0..partition_depth {
            let is_final_level = level + 1 == partition_depth;
            let current = std::mem::take(&mut frontier);
            let mut next_frontier = Vec::new();

            let mut listings = stream::iter(current)
                .map(|prefix| {
                    let store = store.clone();
                    let prefix_label = prefix
                        .as_ref()
                        .map(|p| p.as_ref())
                        .unwrap_or("")
                        .to_string();
                    async move {
                        store
                            .list_with_delimiter(prefix.as_ref())
                            .instrument(info_span!(
                                "list_with_delimiter",
                                operation = "vacuum",
                                level,
                                prefix = prefix_label,
                            ))
                            .await
                            .map_err(DeltaTableError::from)
                    }
                })
                .buffer_unordered(scan_concurrency);

            loop {
                match listings.try_next().await {
                    Ok(Some(listing)) => {
                        for obj_meta in listing.objects {
                            scanned.fetch_add(1, Ordering::Relaxed);
                            match consider_orphan_for_deletion(
                                &obj_meta,
                                valid_files.as_ref(),
                                keep_files.as_ref(),
                                partition_columns.as_ref(),
                                tombstone_path_sets.as_ref(),
                                now_millis,
                                retention_millis,
                            ) {
                                Ok(Some((path, size))) => {
                                    if let Err(e) = on_intermediate_orphan(path, size) {
                                        let _ = tx.send(Err(e)).await;
                                        return;
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }

                        for child in listing.common_prefixes {
                            if level == 0 && is_skippable_root_prefix(&child) {
                                continue;
                            }
                            if is_final_level {
                                if tx.send(Ok(child)).await.is_err() {
                                    return; // consumer dropped
                                }
                            } else {
                                next_frontier.push(Some(child));
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }

            frontier = next_frontier;
        }
    });

    rx
}

/// List `prefix` and yield orphans eligible for full-mode vacuum.
///
/// Each successfully listed object increments `scanned` (objects observed,
/// not just orphans). Only objects that pass `consider_orphan_for_deletion`
/// are yielded. Inputs are owned/`Arc` so the stream is `'static` and can be
/// driven from concurrent `buffer_unordered` tasks.
fn list_orphans_under_prefix(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    valid_files: Arc<HashSet<Path>>,
    keep_files: Arc<HashSet<String>>,
    partition_columns: Arc<Vec<String>>,
    tombstone_path_sets: Arc<TombstonePathSets>,
    now_millis: i64,
    retention_millis: i64,
    scanned: Arc<AtomicUsize>,
) -> impl Stream<Item = Result<(Path, i64), DeltaTableError>> {
    let span = info_span!(
        "list_files",
        operation = "vacuum",
        mode = "parallel",
        prefix = prefix.to_string(),
    );
    store
        .list(Some(&prefix))
        .map_err(DeltaTableError::from)
        .try_filter_map(move |obj_meta| {
            let _guard = span.enter();
            // Count every listed object for progress diagnostics.
            scanned.fetch_add(1, Ordering::Relaxed);
            // TODO should we allow NotFound here in case we have a temporary commit file in the list
            let result = consider_orphan_for_deletion(
                &obj_meta,
                valid_files.as_ref(),
                keep_files.as_ref(),
                partition_columns.as_ref(),
                tombstone_path_sets.as_ref(),
                now_millis,
                retention_millis,
            );
            ready(result)
        })
}

async fn collect_full_mode_tombstones(
    snapshot: &EagerSnapshot,
    tombstone_retention_timestamp: i64,
    store: &dyn LogStore,
) -> DeltaResult<(Vec<TombstoneView>, TombstonePathSets)> {
    snapshot
        .snapshot()
        .active_tombstones(store)
        .try_fold(
            (Vec::new(), TombstonePathSets::default()),
            |(mut expired_tombstones, mut tombstone_path_sets), tombstone| {
                let is_expired = is_tombstone_expired(&tombstone, tombstone_retention_timestamp);
                let path = Path::from(tombstone.path().to_string());
                tombstone_path_sets.record(path, is_expired);
                if is_expired {
                    expired_tombstones.push(tombstone);
                }
                ready(Ok((expired_tombstones, tombstone_path_sets)))
            },
        )
        .await
}

/// List files no longer referenced by a Delta table and are older than the retention threshold.
async fn get_stale_files(
    snapshot: &EagerSnapshot,
    retention_period: Duration,
    now_timestamp_millis: i64,
    store: &dyn LogStore,
) -> DeltaResult<Vec<TombstoneView>> {
    let tombstone_retention_timestamp = now_timestamp_millis - retention_period.num_milliseconds();
    snapshot
        .snapshot()
        .active_tombstones(store)
        .try_filter(|tombstone| {
            ready(is_tombstone_expired(
                tombstone,
                tombstone_retention_timestamp,
            ))
        })
        .try_collect::<Vec<_>>()
        .await
}

fn is_tombstone_expired(tombstone: &TombstoneView, tombstone_retention_timestamp: i64) -> bool {
    tombstone.deletion_timestamp().unwrap_or(0) < tombstone_retention_timestamp
}

fn should_try_parallel_vacuum(partition_columns: &[String]) -> bool {
    // Single-level partitions: flat list(None) is cheaper than expand-to-leaves
    // when cardinality is high and leaves are tiny. Multi-level: expand to n-1.
    partition_columns.len() > 1
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt as _, PutPayload, local::LocalFileSystem, memory::InMemory};
    use serde_json::json;

    use super::*;
    use crate::kernel::Action;
    use crate::kernel::transaction::CommitBuilder;
    use crate::protocol::SaveMode;
    use crate::writer::test_utils::create_initialized_table;
    use crate::writer::{DeltaWriter, JsonWriter};
    use crate::{ensure_table_uri, open_table};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{
        fs::{FileTimes, OpenOptions},
        io::Read,
        time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
    };
    use url::Url;

    #[tokio::test]
    async fn test_vacuum_full() -> DeltaResult<()> {
        let table_path = Path::new("../test/tests/data/simple_commit");
        let table_uri =
            Url::from_directory_path(std::fs::canonicalize(table_path).unwrap()).unwrap();
        let table = open_table(table_uri).await?;

        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::hours(0))
                .with_dry_run(true)
                .with_mode(VacuumMode::Lite)
                .with_enforce_retention_duration(false)
                .await?;
        // When running lite, this table with superfluous parquet files should not have anything to
        // delete
        assert!(result.files_deleted.is_empty());

        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
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
        let table_uri = ensure_table_uri(table_loc).unwrap();
        let table = open_table(table_uri).await?;
        let versions_to_keep = vec![3];

        // First, vacuum without keeping any particular versions
        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::hours(0))
                .with_dry_run(true)
                .with_mode(VacuumMode::Full)
                .with_enforce_retention_duration(false)
                .await?;

        // Our simple_table has 32 data files in it which could be vacuumed.
        assert_eq!(32, result.files_deleted.len());

        // Next, vacuum with specific versions retained
        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
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
        let table_uri = ensure_table_uri(table_loc).unwrap();
        let table = open_table(table_uri).await?;
        let versions_to_keep = vec![2, 3];

        // First, vacuum without keeping any particular versions
        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::hours(0))
                .with_dry_run(true)
                .with_mode(VacuumMode::Full)
                .with_enforce_retention_duration(false)
                .await?;

        // Our simple_table has 32 data files in it which could be vacuumed.
        assert_eq!(32, result.files_deleted.len());

        // Next, vacuum with specific versions retained
        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
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

    #[tokio::test]
    async fn test_vacuum_keep_versions_descending_order() -> DeltaResult<()> {
        let table_loc = "../test/tests/data/simple_table";
        let table_uri = ensure_table_uri(table_loc).unwrap();
        let table = open_table(table_uri).await?;

        let (_table, ascending_result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::hours(0))
                .with_keep_versions(&[0, 1, 2, 3])
                .with_dry_run(true)
                .with_mode(VacuumMode::Full)
                .with_enforce_retention_duration(false)
                .await?;

        let (_table, descending_result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::hours(0))
                .with_keep_versions(&[3, 2, 1, 0])
                .with_dry_run(true)
                .with_mode(VacuumMode::Full)
                .with_enforce_retention_duration(false)
                .await?;

        let mut ascending_files = ascending_result.files_deleted;
        ascending_files.sort();
        let mut descending_files = descending_result.files_deleted;
        descending_files.sort();

        assert_eq!(descending_files, ascending_files);
        Ok(())
    }

    // This test will do some table operations after executing a vacuum with versions to ensure
    // that the table is still functional, can be read, checkpointed, etc.
    #[cfg(feature = "datafusion")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vacuum_keep_version_validity() {
        use datafusion::prelude::SessionContext;
        use object_store::GetResultPayload;
        let store = InMemory::new();
        let source = LocalFileSystem::new_with_prefix("../test/tests/data/simple_table").unwrap();
        let mut stream = source.list(None);

        while let Some(Ok(entity)) = stream.next().await {
            let mut contents = vec![];
            match source.get(&entity.location).await.unwrap().payload {
                GetResultPayload::File(mut fd, _path) => {
                    fd.read_to_end(&mut contents).unwrap();
                }
                _ => panic!("We should only be dealing in files!"),
            }
            let content = bytes::Bytes::from(contents);
            store
                .put(&entity.location, PutPayload::from_bytes(content))
                .await
                .unwrap();
        }

        let table_url = url::Url::parse("memory:///").unwrap();
        let mut table = crate::DeltaTableBuilder::from_url(table_url.clone())
            .unwrap()
            .with_storage_backend(Arc::new(store), table_url)
            .build()
            .unwrap();
        table.load().await.unwrap();

        let (mut table, result) = VacuumBuilder::new(
            table.log_store(),
            Some(table.snapshot().unwrap().snapshot.clone()),
        )
        .with_retention_period(Duration::hours(0))
        .with_keep_versions(&[2, 3])
        .with_mode(VacuumMode::Full)
        .with_enforce_retention_duration(false)
        .await
        .unwrap();
        // Our simple_table has 32 data files in it, and we shouldn't have deleted them all!
        assert_ne!(32, result.files_deleted.len());

        // Can we checkpoint it?
        crate::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        table.load().await.unwrap();
        assert_eq!(Some(6), table.version());

        let ctx = SessionContext::new();
        table.update_datafusion_session(&ctx.state()).unwrap();
        ctx.register_table("test", table.table_provider().await.unwrap())
            .unwrap();
        let _batches = ctx
            .sql("SELECT * FROM test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn vacuum_delta_8_0_table() -> DeltaResult<()> {
        let table_path = Path::new("../test/tests/data/delta-0.8.0");
        let table_uri =
            Url::from_directory_path(std::fs::canonicalize(table_path).unwrap()).unwrap();
        let table = open_table(table_uri).await.unwrap();

        let result = VacuumBuilder::new(
            table.log_store(),
            Some(table.snapshot().unwrap().snapshot.clone()),
        )
        .with_retention_period(Duration::hours(1))
        .with_dry_run(true)
        .await;

        assert!(result.is_err());

        let table_path = Path::new("../test/tests/data/delta-0.8.0");
        let table_uri =
            Url::from_directory_path(std::fs::canonicalize(table_path).unwrap()).unwrap();
        let table = open_table(table_uri).await.unwrap();

        let (table, result) = VacuumBuilder::new(
            table.log_store(),
            Some(table.snapshot().unwrap().snapshot.clone()),
        )
        .with_retention_period(Duration::hours(0))
        .with_dry_run(true)
        .with_enforce_retention_duration(false)
        .await?;
        // do not enforce retention duration check with 0 hour will purge all files
        assert_eq!(
            result.files_deleted,
            vec!["part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"]
        );

        let (table, result) = VacuumBuilder::new(
            table.log_store(),
            Some(table.snapshot().unwrap().snapshot.clone()),
        )
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
        let (_table, result) = VacuumBuilder::new(
            table.log_store(),
            Some(table.snapshot().unwrap().snapshot.clone()),
        )
        .with_retention_period(Duration::hours(retention_hours as i64))
        .with_dry_run(true)
        .await?;

        assert_eq!(result.files_deleted, empty);
        Ok(())
    }

    /// Mock clock for testing time-dependent vacuum behavior
    #[derive(Debug, Clone)]
    struct MockClock {
        timestamp_millis: i64,
    }

    impl MockClock {
        fn new(timestamp_millis: i64) -> Self {
            Self { timestamp_millis }
        }
    }

    impl Clock for MockClock {
        fn current_timestamp_millis(&self) -> i64 {
            self.timestamp_millis
        }
    }

    fn set_last_modified(path: &Path, last_modified: SystemTime) {
        let file = OpenOptions::new().write(true).open(path).unwrap();
        let times = FileTimes::new()
            .set_accessed(last_modified)
            .set_modified(last_modified);
        file.set_times(times).unwrap();
    }

    #[tokio::test]
    async fn test_vacuum_full_recent_tombstones_are_not_treated_as_orphans() -> DeltaResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();
        let mut table = create_initialized_table(table_path, &[]).await;
        let current_time = SystemTime::now();
        let current_time_millis =
            current_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let stale_time = current_time - StdDuration::from_secs(10);
        let recent_time = current_time - StdDuration::from_secs(1);
        let original_data = json!({
            "id": "A",
            "value": 1,
            "modified": "2021-02-01"
        });
        let replacement_data = json!({
            "id": "B",
            "value": 2,
            "modified": "2021-02-02"
        });

        let mut writer = JsonWriter::for_table(&table)?;
        writer.write(vec![original_data]).await?;
        writer.flush_and_commit(&mut table).await?;

        let tombstoned_paths: Vec<_> = table
            .snapshot()?
            .log_data()
            .into_iter()
            .map(|add| add.object_store_path().to_string())
            .collect();
        assert_eq!(tombstoned_paths.len(), 1);
        let recent_tombstone_path = tombstoned_paths[0].clone();
        set_last_modified(&temp_dir.path().join(&recent_tombstone_path), stale_time);

        let stale_orphan_path = "orphan-old.parquet";
        std::fs::write(temp_dir.path().join(stale_orphan_path), b"stale orphan").unwrap();
        set_last_modified(&temp_dir.path().join(stale_orphan_path), stale_time);

        let remove_actions = table
            .snapshot()?
            .snapshot()
            .file_views(&table.log_store(), None)
            .map_ok(|file| {
                let mut remove = file.remove_action(true);
                remove.deletion_timestamp = Some(current_time_millis);
                Action::Remove(remove)
            })
            .try_collect::<Vec<_>>()
            .await?;
        let mut overwrite_writer = JsonWriter::for_table(&table)?;
        overwrite_writer.write(vec![replacement_data]).await?;
        let add_actions = overwrite_writer.flush().await?.into_iter().map(Action::Add);
        let mut actions = remove_actions;
        actions.extend(add_actions);
        let operation = DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            partition_by: None,
            predicate: None,
        };
        CommitBuilder::default()
            .with_actions(actions)
            .build(
                Some(table.snapshot()?),
                table.log_store().clone(),
                operation,
            )
            .await?;
        table.update_state().await?;

        let recent_orphan_path = "orphan-recent.parquet";
        std::fs::write(temp_dir.path().join(recent_orphan_path), b"recent orphan").unwrap();
        set_last_modified(&temp_dir.path().join(recent_orphan_path), recent_time);

        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::seconds(5))
                .with_dry_run(true)
                .with_mode(VacuumMode::Full)
                .with_enforce_retention_duration(false)
                .with_clock(Arc::new(MockClock::new(current_time_millis)))
                .await?;

        assert!(
            !result.files_deleted.contains(&recent_tombstone_path),
            "recent tombstone was treated like an orphan: {:?}",
            result.files_deleted
        );
        assert!(
            result
                .files_deleted
                .contains(&stale_orphan_path.to_string()),
            "stale orphan should still be vacuum eligible: {:?}",
            result.files_deleted
        );
        assert!(
            !result
                .files_deleted
                .contains(&recent_orphan_path.to_string()),
            "recent orphan should still be protected: {:?}",
            result.files_deleted
        );

        Ok(())
    }

    /// Test that recently written uncommitted files are protected from deletion in Full mode
    /// This tests the fix for the race condition where concurrent writer's files could be deleted
    #[tokio::test]
    async fn test_vacuum_full_protects_recent_uncommitted_files() -> DeltaResult<()> {
        use chrono::DateTime;
        use object_store::GetResultPayload;

        let store = InMemory::new();
        let source = LocalFileSystem::new_with_prefix("../test/tests/data/simple_table").unwrap();
        let mut stream = source.list(None);

        while let Some(Ok(entity)) = stream.next().await {
            let mut contents = vec![];
            match source.get(&entity.location).await.unwrap().payload {
                GetResultPayload::File(mut fd, _path) => {
                    fd.read_to_end(&mut contents).unwrap();
                }
                _ => panic!("We should only be dealing in files!"),
            }
            let content = bytes::Bytes::from(contents);
            store
                .put(&entity.location, PutPayload::from_bytes(content))
                .await
                .unwrap();
        }

        // Add a "recently written" orphaned file that simulates an uncommitted file
        let recent_file_path = object_store::path::Path::from("uncommitted-recent.parquet");
        store
            .put(
                &recent_file_path,
                PutPayload::from_bytes(bytes::Bytes::from("test data")),
            )
            .await
            .unwrap();

        let table_url = url::Url::parse("memory:///").unwrap();
        let mut table = crate::DeltaTableBuilder::from_url(table_url.clone())
            .unwrap()
            .with_storage_backend(Arc::new(store), table_url)
            .build()
            .unwrap();
        table.load().await.unwrap();

        // Set current time to 10 days after epoch
        let current_time = DateTime::from_timestamp(10 * 24 * 3600, 0)
            .unwrap()
            .timestamp_millis();
        let mock_clock = Arc::new(MockClock::new(current_time));

        // Run vacuum with 7-day retention in Full mode
        // The recent file should NOT be deleted because it's too new
        let (_table, result) = VacuumBuilder::new(
            table.log_store(),
            Some(table.snapshot().unwrap().snapshot.clone()),
        )
        .with_retention_period(Duration::days(7))
        .with_dry_run(true)
        .with_mode(VacuumMode::Full)
        .with_enforce_retention_duration(false)
        .with_clock(mock_clock)
        .await
        .unwrap();

        // The recent uncommitted file should NOT be in the deletion list
        assert!(
            !result.files_deleted.contains(&recent_file_path.to_string()),
            "Recent uncommitted file should be protected from deletion, but found in deletion list: {:?}",
            result.files_deleted
        );

        Ok(())
    }

    #[test]
    fn test_should_try_parallel_vacuum() {
        assert!(!should_try_parallel_vacuum(&[]));
        assert!(!should_try_parallel_vacuum(&["modified".to_string()]));
        assert!(should_try_parallel_vacuum(&[
            "modified".to_string(),
            "id".to_string()
        ]));
        assert!(should_try_parallel_vacuum(&[
            "a".to_string(),
            "b".to_string(),
            "c".to_string()
        ]));
    }

    #[test]
    fn test_is_skippable_root_prefix() {
        assert!(is_skippable_root_prefix(&object_store::path::Path::from(
            "_delta_log"
        )));
        assert!(is_skippable_root_prefix(&object_store::path::Path::from(
            "_delta_log/00000000000000000000.json"
        )));
        assert!(is_skippable_root_prefix(&object_store::path::Path::from(
            ".hidden"
        )));
        assert!(!is_skippable_root_prefix(&object_store::path::Path::from(
            "modified=2021-02-01"
        )));
        assert!(!is_skippable_root_prefix(&object_store::path::Path::from(
            "regular_folder"
        )));
    }

    #[test]
    fn test_tombstone_path_sets_record() {
        let mut sets = TombstonePathSets::default();
        let expired_path = object_store::path::Path::from("expired.parquet");
        let recent_path = object_store::path::Path::from("recent.parquet");

        sets.record(expired_path.clone(), true);
        sets.record(recent_path.clone(), false);

        assert!(sets.expired_tombstone_paths.contains(&expired_path));
        assert!(!sets.expired_tombstone_paths.contains(&recent_path));
        assert!(sets.all_tombstone_paths.contains(&expired_path));
        assert!(sets.all_tombstone_paths.contains(&recent_path));
    }

    /// The `DELTARS_VACUUM_LIST_CONCURRENCY` env var is only read once, on the
    /// first call, because the value is cached in a process-wide `OnceLock`.
    /// Since no other test sets this env var, every call in this test binary
    /// (regardless of ordering) is guaranteed to resolve to the built-in
    /// default, which is what this test asserts.
    #[test]
    fn test_default_vacuum_list_concurrency() {
        assert!(std::env::var("DELTARS_VACUUM_LIST_CONCURRENCY").is_err());
        assert_eq!(
            default_vacuum_list_concurrency(),
            DEFAULT_VACUUM_LIST_CONCURRENCY
        );
        // Calling it again must return the same cached value.
        assert_eq!(
            default_vacuum_list_concurrency(),
            default_vacuum_list_concurrency()
        );
    }

    #[test]
    fn test_resolve_scan_concurrency() {
        // Explicit positive overrides win.
        assert_eq!(resolve_scan_concurrency(Some(1)), 1);
        assert_eq!(resolve_scan_concurrency(Some(7)), 7);
        assert_eq!(resolve_scan_concurrency(Some(1000)), 1000);

        // None and zero fall back to the process default.
        assert_eq!(
            resolve_scan_concurrency(None),
            default_vacuum_list_concurrency()
        );
        assert_eq!(
            resolve_scan_concurrency(Some(0)),
            default_vacuum_list_concurrency()
        );
    }

    #[tokio::test]
    async fn test_with_scan_concurrency_builder() -> DeltaResult<()> {
        let table_path = std::path::Path::new("../test/tests/data/simple_commit");
        let table_uri =
            Url::from_directory_path(std::fs::canonicalize(table_path).unwrap()).unwrap();
        let table = open_table(table_uri).await?;

        let builder =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()));
        assert_eq!(builder.scan_concurrency, None);
        assert_eq!(
            resolve_scan_concurrency(builder.scan_concurrency),
            default_vacuum_list_concurrency()
        );

        let builder = builder.with_scan_concurrency(25);
        assert_eq!(builder.scan_concurrency, Some(25));
        assert_eq!(resolve_scan_concurrency(builder.scan_concurrency), 25);

        // Zero is stored but ignored at resolve time.
        let builder = builder.with_scan_concurrency(0);
        assert_eq!(builder.scan_concurrency, Some(0));
        assert_eq!(
            resolve_scan_concurrency(builder.scan_concurrency),
            default_vacuum_list_concurrency()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_parallel_scan_builder() -> DeltaResult<()> {
        let table_path = std::path::Path::new("../test/tests/data/simple_commit");
        let table_uri =
            Url::from_directory_path(std::fs::canonicalize(table_path).unwrap()).unwrap();
        let table = open_table(table_uri).await?;

        let multi_level = ["date".to_string(), "group".to_string()];
        assert!(should_try_parallel_vacuum(&multi_level));

        let builder =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()));
        // Parallel scan is enabled by default for multi-level tables.
        assert!(builder.parallel_scan);
        let use_parallel = builder.parallel_scan && should_try_parallel_vacuum(&multi_level);
        assert!(use_parallel);

        let builder = builder.parallel_scan(false);
        assert!(!builder.parallel_scan);
        let use_parallel = builder.parallel_scan && should_try_parallel_vacuum(&multi_level);
        assert!(!use_parallel);

        Ok(())
    }

    fn make_object_meta(
        path: &str,
        last_modified_millis: i64,
        size: u64,
    ) -> object_store::ObjectMeta {
        object_store::ObjectMeta {
            location: object_store::path::Path::from(path),
            last_modified: chrono::DateTime::from_timestamp_millis(last_modified_millis).unwrap(),
            size,
            e_tag: None,
            version: None,
        }
    }

    /// Directly exercises every branch of `consider_orphan_for_deletion` with
    /// hand-crafted inputs, independent of any full vacuum run.
    #[test]
    fn test_consider_orphan_for_deletion_branches() {
        let partition_columns = vec!["modified".to_string()];
        let now_millis: i64 = 1_000_000;
        let retention_millis: i64 = 1_000;

        let valid_files: HashSet<object_store::path::Path> =
            [object_store::path::Path::from("valid.parquet")]
                .into_iter()
                .collect();
        let keep_files: HashSet<String> = ["kept.parquet".to_string()].into_iter().collect();

        let mut tombstone_path_sets = TombstonePathSets::default();
        tombstone_path_sets.record(
            object_store::path::Path::from("expired-tombstone.parquet"),
            true,
        );
        tombstone_path_sets.record(
            object_store::path::Path::from("recent-tombstone.parquet"),
            false,
        );

        // 1. Already queued as an expired tombstone -> None, regardless of age.
        let meta = make_object_meta("expired-tombstone.parquet", now_millis, 10);
        assert_eq!(
            consider_orphan_for_deletion(
                &meta,
                &valid_files,
                &keep_files,
                &partition_columns,
                &tombstone_path_sets,
                now_millis,
                retention_millis,
            )
            .unwrap(),
            None
        );

        // 2. Still tracked as a valid file -> not ok to delete -> None.
        let meta = make_object_meta("valid.parquet", 0, 10);
        assert_eq!(
            consider_orphan_for_deletion(
                &meta,
                &valid_files,
                &keep_files,
                &partition_columns,
                &tombstone_path_sets,
                now_millis,
                retention_millis,
            )
            .unwrap(),
            None
        );

        // 3. Associated with a version being kept -> not ok to delete -> None.
        let meta = make_object_meta("kept.parquet", 0, 10);
        assert_eq!(
            consider_orphan_for_deletion(
                &meta,
                &valid_files,
                &keep_files,
                &partition_columns,
                &tombstone_path_sets,
                now_millis,
                retention_millis,
            )
            .unwrap(),
            None
        );

        // 4. Hidden directory entry (not a partition column, not _delta_index /
        //    _change_data) -> not ok to delete -> None.
        let meta = make_object_meta("_staging/file.parquet", 0, 10);
        assert_eq!(
            consider_orphan_for_deletion(
                &meta,
                &valid_files,
                &keep_files,
                &partition_columns,
                &tombstone_path_sets,
                now_millis,
                retention_millis,
            )
            .unwrap(),
            None
        );

        // 5. Has a recent (non-expired) tombstone -> keep until it expires -> None.
        let meta = make_object_meta("recent-tombstone.parquet", 0, 10);
        assert_eq!(
            consider_orphan_for_deletion(
                &meta,
                &valid_files,
                &keep_files,
                &partition_columns,
                &tombstone_path_sets,
                now_millis,
                retention_millis,
            )
            .unwrap(),
            None
        );

        // 6. Untracked but too recent (age < retention) -> protected -> None.
        let meta = make_object_meta(
            "recent-orphan.parquet",
            now_millis - retention_millis + 1,
            10,
        );
        assert_eq!(
            consider_orphan_for_deletion(
                &meta,
                &valid_files,
                &keep_files,
                &partition_columns,
                &tombstone_path_sets,
                now_millis,
                retention_millis,
            )
            .unwrap(),
            None
        );

        // 7. Untracked and stale -> eligible for deletion -> Some((path, size)).
        let meta = make_object_meta(
            "stale-orphan.parquet",
            now_millis - retention_millis - 1,
            42,
        );
        assert_eq!(
            consider_orphan_for_deletion(
                &meta,
                &valid_files,
                &keep_files,
                &partition_columns,
                &tombstone_path_sets,
                now_millis,
                retention_millis,
            )
            .unwrap(),
            Some((object_store::path::Path::from("stale-orphan.parquet"), 42))
        );
    }

    /// Directly exercises `expand_partition_prefixes` at depth 1: root-level
    /// orphans are collected immediately, `_delta_log` is skipped, and the
    /// remaining top-level partition directories are returned as leaf prefixes.
    #[tokio::test]
    async fn test_expand_partition_prefixes_root_orphans_and_skips() -> DeltaResult<()> {
        let store = InMemory::new();
        store
            .put(
                &object_store::path::Path::from("orphan-root.parquet"),
                PutPayload::from_static(b"root orphan"),
            )
            .await?;
        store
            .put(
                &object_store::path::Path::from("_delta_log/00000000000000000000.json"),
                PutPayload::from_static(b"{}"),
            )
            .await?;
        store
            .put(
                &object_store::path::Path::from("a=1/file.parquet"),
                PutPayload::from_static(b"a1"),
            )
            .await?;
        store
            .put(
                &object_store::path::Path::from("a=2/file.parquet"),
                PutPayload::from_static(b"a2"),
            )
            .await?;

        let valid_files = HashSet::new();
        let keep_files = HashSet::new();
        let partition_columns = vec!["a".to_string(), "b".to_string()];
        let tombstone_path_sets = TombstonePathSets::default();

        let scanned = Arc::new(AtomicUsize::new(0));
        let orphans = Arc::new(std::sync::Mutex::new(Vec::new()));
        let orphans_cb = Arc::clone(&orphans);

        let mut leaf_prefixes = expand_partition_prefixes(
            Arc::new(store),
            1,
            Arc::new(valid_files),
            Arc::new(keep_files),
            Arc::new(partition_columns),
            Arc::new(tombstone_path_sets),
            i64::MAX / 2,
            0,
            Arc::clone(&scanned),
            DEFAULT_VACUUM_LIST_CONCURRENCY,
            move |path, size| {
                orphans_cb.lock().unwrap().push((path, size));
                Ok(())
            },
        )
        .try_collect::<Vec<_>>()
        .await?;

        leaf_prefixes.sort();
        assert_eq!(
            leaf_prefixes,
            vec![
                object_store::path::Path::from("a=1"),
                object_store::path::Path::from("a=2"),
            ]
        );
        assert_eq!(
            orphans.lock().unwrap().clone(),
            vec![(object_store::path::Path::from("orphan-root.parquet"), 11)]
        );
        // Only the root-level object is scanned by this function; nested
        // objects under "a=1/" and "a=2/" are left for the leaf listing step.
        assert_eq!(scanned.load(Ordering::Relaxed), 1);

        Ok(())
    }

    /// Directly exercises `expand_partition_prefixes` recursing across two
    /// delimiter levels (depth 2), including an orphan found at the
    /// intermediate ("a=1/") level.
    #[tokio::test]
    async fn test_expand_partition_prefixes_recurses_multiple_levels() -> DeltaResult<()> {
        let store = InMemory::new();
        store
            .put(
                &object_store::path::Path::from("a=1/orphan-mid.parquet"),
                PutPayload::from_static(b"mid orphan"),
            )
            .await?;
        store
            .put(
                &object_store::path::Path::from("a=1/b=1/file.parquet"),
                PutPayload::from_static(b"leaf"),
            )
            .await?;
        store
            .put(
                &object_store::path::Path::from("a=2/b=1/file.parquet"),
                PutPayload::from_static(b"leaf"),
            )
            .await?;

        let valid_files = HashSet::new();
        let keep_files = HashSet::new();
        let partition_columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let tombstone_path_sets = TombstonePathSets::default();

        let scanned = Arc::new(AtomicUsize::new(0));
        let orphans = Arc::new(std::sync::Mutex::new(Vec::new()));
        let orphans_cb = Arc::clone(&orphans);

        let mut leaf_prefixes = expand_partition_prefixes(
            Arc::new(store),
            2,
            Arc::new(valid_files),
            Arc::new(keep_files),
            Arc::new(partition_columns),
            Arc::new(tombstone_path_sets),
            i64::MAX / 2,
            0,
            Arc::clone(&scanned),
            DEFAULT_VACUUM_LIST_CONCURRENCY,
            move |path, size| {
                orphans_cb.lock().unwrap().push((path, size));
                Ok(())
            },
        )
        .try_collect::<Vec<_>>()
        .await?;

        leaf_prefixes.sort();
        assert_eq!(
            leaf_prefixes,
            vec![
                object_store::path::Path::from("a=1/b=1"),
                object_store::path::Path::from("a=2/b=1"),
            ]
        );
        assert_eq!(
            orphans.lock().unwrap().clone(),
            vec![(object_store::path::Path::from("a=1/orphan-mid.parquet"), 10)]
        );

        Ok(())
    }

    /// Directly exercises `list_orphans_under_prefix`, verifying that valid,
    /// kept, and tombstoned files are excluded while untracked files --
    /// including one nested under a `_`-prefixed subdirectory, which
    /// `is_hidden_directory` does not special-case for non-root locations --
    /// are returned as orphans.
    #[tokio::test]
    async fn test_list_orphans_under_prefix_orphan_detection() -> DeltaResult<()> {
        let store = InMemory::new();
        for (path, contents) in [
            ("leaf/valid.parquet", "valid"),
            ("leaf/kept.parquet", "kept"),
            ("leaf/_hidden/file.parquet", "hidden"),
            ("leaf/expired-tombstone.parquet", "expired"),
            ("leaf/recent-tombstone.parquet", "recent"),
            ("leaf/orphan.parquet", "orphan!"),
        ] {
            store
                .put(
                    &object_store::path::Path::from(path),
                    PutPayload::from_bytes(bytes::Bytes::from_static(contents.as_bytes())),
                )
                .await?;
        }

        let valid_files: HashSet<object_store::path::Path> =
            [object_store::path::Path::from("leaf/valid.parquet")]
                .into_iter()
                .collect();
        let keep_files: HashSet<String> = ["leaf/kept.parquet".to_string()].into_iter().collect();
        let partition_columns = vec!["modified".to_string()];

        let mut tombstone_path_sets = TombstonePathSets::default();
        tombstone_path_sets.record(
            object_store::path::Path::from("leaf/expired-tombstone.parquet"),
            true,
        );
        tombstone_path_sets.record(
            object_store::path::Path::from("leaf/recent-tombstone.parquet"),
            false,
        );

        // retention_millis = 0 so the untracked orphan is always considered stale.
        let scanned = Arc::new(AtomicUsize::new(0));
        let mut orphans = list_orphans_under_prefix(
            Arc::new(store),
            object_store::path::Path::from("leaf"),
            Arc::new(valid_files),
            Arc::new(keep_files),
            Arc::new(partition_columns),
            Arc::new(tombstone_path_sets),
            i64::MAX / 2,
            0,
            Arc::clone(&scanned),
        )
        .try_collect::<Vec<_>>()
        .await?;

        assert_eq!(scanned.load(Ordering::Relaxed), 6);
        orphans.sort();
        assert_eq!(
            orphans,
            vec![
                (
                    object_store::path::Path::from("leaf/_hidden/file.parquet"),
                    6
                ),
                (object_store::path::Path::from("leaf/orphan.parquet"), 7),
            ]
        );

        Ok(())
    }

    /// Directly exercises `list_orphans_under_prefix` protecting an untracked
    /// file that is younger than the retention period.
    #[tokio::test]
    async fn test_list_orphans_under_prefix_protects_recent_files() -> DeltaResult<()> {
        let store = InMemory::new();
        store
            .put(
                &object_store::path::Path::from("leaf/recent-orphan.parquet"),
                PutPayload::from_static(b"recent"),
            )
            .await?;

        let valid_files = HashSet::new();
        let keep_files = HashSet::new();
        let partition_columns = vec!["modified".to_string()];
        let tombstone_path_sets = TombstonePathSets::default();

        let now_millis = Utc::now().timestamp_millis();
        // Retention far larger than any possible elapsed time since the put above.
        let retention_millis = 24 * 60 * 60 * 1000;

        let scanned = Arc::new(AtomicUsize::new(0));
        let orphans = list_orphans_under_prefix(
            Arc::new(store),
            object_store::path::Path::from("leaf"),
            Arc::new(valid_files),
            Arc::new(keep_files),
            Arc::new(partition_columns),
            Arc::new(tombstone_path_sets),
            now_millis,
            retention_millis,
            Arc::clone(&scanned),
        )
        .try_collect::<Vec<_>>()
        .await?;

        assert_eq!(scanned.load(Ordering::Relaxed), 1);
        assert!(
            orphans.is_empty(),
            "recent untracked file should be protected: {orphans:?}"
        );

        Ok(())
    }

    /// Shared multi-level full-vacuum fixture: 2 partition columns, tracked data
    /// file, stale orphans at root/mid/leaf, and one recent protected orphan.
    async fn multi_level_full_vacuum_fixture() -> DeltaResult<(
        tempfile::TempDir,
        crate::DeltaTable,
        i64,
        Vec<String>,
        String,
        String,
        String,
        String,
    )> {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let mut table = create_initialized_table(table_path, &partition_cols).await;

        let current_time = SystemTime::now();
        let current_time_millis =
            current_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let stale_time = current_time - StdDuration::from_secs(10);
        let recent_time = current_time - StdDuration::from_secs(1);

        let mut writer = JsonWriter::for_table(&table)?;
        writer
            .write(vec![
                json!({"id": "A", "value": 1, "modified": "2021-02-01"}),
            ])
            .await?;
        writer.flush_and_commit(&mut table).await?;

        let valid_paths: Vec<String> = table
            .snapshot()?
            .log_data()
            .into_iter()
            .map(|add| add.object_store_path().to_string())
            .collect();
        assert_eq!(valid_paths.len(), 1);

        let root_orphan = "orphan-root.parquet".to_string();
        std::fs::write(temp_dir.path().join(&root_orphan), b"root orphan").unwrap();
        set_last_modified(&temp_dir.path().join(&root_orphan), stale_time);

        let modified_prefix = "modified=2021-02-01";
        let mid_orphan = format!("{modified_prefix}/orphan-mid.parquet");
        std::fs::write(temp_dir.path().join(&mid_orphan), b"mid orphan").unwrap();
        set_last_modified(&temp_dir.path().join(&mid_orphan), stale_time);

        let deep_dir = format!("{modified_prefix}/id=A");
        let deep_orphan = format!("{deep_dir}/orphan-deep.parquet");
        std::fs::write(temp_dir.path().join(&deep_orphan), b"deep orphan").unwrap();
        set_last_modified(&temp_dir.path().join(&deep_orphan), stale_time);

        let recent_orphan = format!("{deep_dir}/orphan-recent.parquet");
        std::fs::write(temp_dir.path().join(&recent_orphan), b"recent orphan").unwrap();
        set_last_modified(&temp_dir.path().join(&recent_orphan), recent_time);

        Ok((
            temp_dir,
            table,
            current_time_millis,
            valid_paths,
            root_orphan,
            mid_orphan,
            deep_orphan,
            recent_orphan,
        ))
    }

    fn assert_multi_level_vacuum_result(
        result: &VacuumMetrics,
        root_orphan: &str,
        mid_orphan: &str,
        deep_orphan: &str,
        recent_orphan: &str,
        valid_paths: &[String],
    ) {
        for expected in [root_orphan, mid_orphan, deep_orphan] {
            assert!(
                result.files_deleted.contains(&expected.to_string()),
                "expected {expected} to be vacuumed: {:?}",
                result.files_deleted
            );
        }
        assert!(
            !result.files_deleted.contains(&recent_orphan.to_string()),
            "recent orphan should be protected: {:?}",
            result.files_deleted
        );
        for valid in valid_paths {
            assert!(
                !result.files_deleted.contains(valid),
                "valid tracked file should never be vacuumed: {valid}"
            );
        }
    }

    /// Exercises the hierarchical parallel-listing path used for multi-level
    /// partitioned tables (`should_try_parallel_vacuum` / `expand_partition_prefixes`
    /// / `list_orphans_under_prefix`), covering orphans found at the table root
    /// (intermediate level), directly under the first partition level, and nested
    /// under the leaf partition directory.
    #[tokio::test]
    async fn test_vacuum_full_parallel_multi_level_partitions() -> DeltaResult<()> {
        let (
            _temp_dir,
            table,
            current_time_millis,
            valid_paths,
            root_orphan,
            mid_orphan,
            deep_orphan,
            recent_orphan,
        ) = multi_level_full_vacuum_fixture().await?;

        assert!(!should_try_parallel_vacuum(&[]));
        assert!(should_try_parallel_vacuum(&[
            "modified".to_string(),
            "id".to_string()
        ]));

        let (_table, result) =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::seconds(5))
                .with_dry_run(true)
                .with_mode(VacuumMode::Full)
                .with_enforce_retention_duration(false)
                .with_clock(Arc::new(MockClock::new(current_time_millis)))
                .await?;

        assert_multi_level_vacuum_result(
            &result,
            &root_orphan,
            &mid_orphan,
            &deep_orphan,
            &recent_orphan,
            &valid_paths,
        );
        Ok(())
    }

    /// Same multi-level fixture as the parallel test, but with
    /// [`VacuumBuilder::parallel_scan`] forcing the flat `list(None)`
    /// path. Results must match the parallel path.
    #[tokio::test]
    async fn test_vacuum_full_flat_scan_with_disable_parallel_scan() -> DeltaResult<()> {
        let (
            _temp_dir,
            table,
            current_time_millis,
            valid_paths,
            root_orphan,
            mid_orphan,
            deep_orphan,
            recent_orphan,
        ) = multi_level_full_vacuum_fixture().await?;

        let builder =
            VacuumBuilder::new(table.log_store(), Some(table.snapshot()?.snapshot.clone()))
                .with_retention_period(Duration::seconds(5))
                .with_dry_run(true)
                .with_mode(VacuumMode::Full)
                .with_enforce_retention_duration(false)
                .parallel_scan(false)
                .with_clock(Arc::new(MockClock::new(current_time_millis)));
        assert!(!builder.parallel_scan);

        let (_table, result) = builder.await?;

        assert_multi_level_vacuum_result(
            &result,
            &root_orphan,
            &mid_orphan,
            &deep_orphan,
            &recent_orphan,
            &valid_paths,
        );
        Ok(())
    }

    /// Parallel and flat full-mode scans on the same multi-level table must
    /// select the same delete set.
    #[tokio::test]
    async fn test_vacuum_full_parallel_and_flat_agree() -> DeltaResult<()> {
        let (
            _temp_dir,
            table,
            current_time_millis,
            _valid_paths,
            _root_orphan,
            _mid_orphan,
            _deep_orphan,
            _recent_orphan,
        ) = multi_level_full_vacuum_fixture().await?;

        let clock: Arc<dyn Clock> = Arc::new(MockClock::new(current_time_millis));
        let snapshot = table.snapshot()?.snapshot.clone();

        let (_t1, parallel) = VacuumBuilder::new(table.log_store(), Some(snapshot.clone()))
            .with_retention_period(Duration::seconds(5))
            .with_dry_run(true)
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .with_clock(Arc::clone(&clock))
            .await?;

        let (_t2, flat) = VacuumBuilder::new(table.log_store(), Some(snapshot))
            .with_retention_period(Duration::seconds(5))
            .with_dry_run(true)
            .with_mode(VacuumMode::Full)
            .with_enforce_retention_duration(false)
            .parallel_scan(false)
            .with_clock(clock)
            .await?;

        let mut parallel_files = parallel.files_deleted;
        let mut flat_files = flat.files_deleted;
        parallel_files.sort();
        flat_files.sort();
        assert_eq!(
            parallel_files, flat_files,
            "parallel and flat full scans must agree on delete set"
        );
        Ok(())
    }
}
