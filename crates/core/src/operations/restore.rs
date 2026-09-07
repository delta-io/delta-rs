//! Perform restore of delta table to a specified version or datetime
//!
//! Algorithm:
//! 1) Read the latest state snapshot of the table.
//! 2) Read table state for version or datetime to restore
//! 3) Compute files available in state for restoring (files were removed by some commit)
//!    but missed in the latest. Add these files into commit as AddFile action.
//! 4) Compute files available in the latest state snapshot (files were added after version to restore)
//!    but missed in the state to restore. Add these files into commit as RemoveFile action.
//! 5) If ignore_missing_files option is false (default value) check availability of AddFile
//!    in file system.
//! 6) Commit Protocol, all RemoveFile and AddFile actions
//!    into delta log using `LogStore::write_commit_entry` (commit will be failed in case of parallel transaction)
//!    TODO: comment is outdated
//! 7) If table was modified in parallel then ignore restore and raise exception.
//!
//! # Example
//! ```rust ignore
//! let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
//! let (table, metrics) = RestoreBuilder::new(table.object_store(), table.state).with_version_to_restore(1).await?;
//! ````

use std::cmp::max;
use std::collections::HashSet;
use std::ops::BitXor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use serde::Serialize;
use uuid::Uuid;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    Action, ActiveAddOptions, Add, AddStatsPolicy, EagerSnapshot, LogicalFileView,
    ProtocolExt as _, ProtocolInner, Remove, Snapshot, Version, resolve_snapshot,
};
use crate::logstore::{LogStore, LogStoreRef};
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableConfig, DeltaTableError, ObjectStoreError};

/// Errors that can occur during restore
#[derive(thiserror::Error, Debug)]
enum RestoreError {
    #[error("Either the version or datetime should be provided for restore")]
    InvalidRestoreParameter,

    #[error("Version to restore {0} should be less then last available version {1}.")]
    TooLargeRestoreVersion(Version, Version),

    #[error("Find missing file {0} when restore.")]
    MissingDataFile(String),
}

impl From<RestoreError> for DeltaTableError {
    fn from(err: RestoreError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}

/// Metrics from Restore
#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreMetrics {
    /// Number of files removed
    pub num_removed_file: usize,
    /// Number of files restored
    pub num_restored_file: usize,
}

/// Restore a Delta table with given version
/// See this module's documentation for more information
pub struct RestoreBuilder {
    /// A snapshot of the to-be-restored table's state
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Version to restore
    version_to_restore: Option<Version>,
    /// Datetime to restore
    datetime_to_restore: Option<DateTime<Utc>>,
    /// Ignore missing files
    ignore_missing_files: bool,
    /// Protocol downgrade allowed
    protocol_downgrade_allowed: bool,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for RestoreBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl RestoreBuilder {
    /// Create a new [`RestoreBuilder`]
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            version_to_restore: None,
            datetime_to_restore: None,
            ignore_missing_files: false,
            protocol_downgrade_allowed: false,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Set the version to restore
    pub fn with_version_to_restore(mut self, version: Version) -> Self {
        self.version_to_restore = Some(version);
        self
    }

    /// Set the datetime to restore
    pub fn with_datetime_to_restore(mut self, datetime: DateTime<Utc>) -> Self {
        self.datetime_to_restore = Some(datetime);
        self
    }

    /// Set whether to ignore missing files which delete manually or by vacuum.
    /// If true, continue to run when encountering missing files.
    pub fn with_ignore_missing_files(mut self, ignore_missing_files: bool) -> Self {
        self.ignore_missing_files = ignore_missing_files;
        self
    }

    /// Set whether allow to downgrade protocol
    pub fn with_protocol_downgrade_allowed(mut self, protocol_downgrade_allowed: bool) -> Self {
        self.protocol_downgrade_allowed = protocol_downgrade_allowed;
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
}

async fn plan_restore_file_changes(
    log_store: &dyn LogStore,
    current: &Snapshot,
    target: &Snapshot,
    deletion_timestamp: i64,
) -> DeltaResult<(Vec<Add>, Vec<Remove>)> {
    let target_files: Vec<_> = target
        .active_adds(
            log_store,
            ActiveAddOptions {
                predicate: None,
                stats: AddStatsPolicy::RawJson,
            },
        )
        .try_collect()
        .await?;
    let current_files: Vec<_> = current
        .active_adds(
            log_store,
            ActiveAddOptions {
                predicate: None,
                stats: AddStatsPolicy::None,
            },
        )
        .try_collect()
        .await?;
    let target_keys = HashSet::<_>::from_iter(target_files.iter().map(restore_file_key));
    let current_keys = HashSet::<_>::from_iter(current_files.iter().map(restore_file_key));

    let files_to_add = target_files
        .iter()
        .filter(|file| !current_keys.contains(&restore_file_key(file)))
        .map(|file| {
            let mut add = file.to_add();
            add.data_change = true;
            add
        })
        .collect();
    let files_to_remove = current_files
        .iter()
        .filter(|file| !target_keys.contains(&restore_file_key(file)))
        .map(|file| {
            let add = file.to_add();
            let mut remove = file.remove_action(true);
            remove.deletion_timestamp = Some(deletion_timestamp);
            remove.tags = add.tags;
            remove.base_row_id = add.base_row_id;
            remove.default_row_commit_version = add.default_row_commit_version;
            remove
        })
        .collect();

    Ok((files_to_add, files_to_remove))
}

fn restore_file_key(file: &LogicalFileView) -> (Path, Option<String>) {
    let deletion_vector_id =
        file.deletion_vector_descriptor()
            .map(|deletion_vector| match deletion_vector.offset {
                Some(offset) => format!(
                    "{}{}@{offset}",
                    deletion_vector.storage_type, deletion_vector.path_or_inline_dv
                ),
                None => format!(
                    "{}{}",
                    deletion_vector.storage_type, deletion_vector.path_or_inline_dv
                ),
            });
    (file.object_store_path(), deletion_vector_id)
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    log_store: LogStoreRef,
    snapshot: EagerSnapshot,
    version_to_restore: Option<Version>,
    datetime_to_restore: Option<DateTime<Utc>>,
    ignore_missing_files: bool,
    protocol_downgrade_allowed: bool,
    mut commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
    operation_id: Uuid,
) -> DeltaResult<(RestoreMetrics, DeltaTableState)> {
    if !(version_to_restore
        .is_none()
        .bitxor(datetime_to_restore.is_none()))
    {
        return Err(DeltaTableError::from(RestoreError::InvalidRestoreParameter));
    }
    let mut table = DeltaTable::new(log_store.clone(), DeltaTableConfig::default());

    match datetime_to_restore {
        Some(datetime) => {
            table.load_with_datetime(datetime).await?;
        }
        None => {
            table.load_version(version_to_restore.unwrap()).await?;
        }
    }

    let current_snapshot: &Snapshot = snapshot.snapshot();
    let snapshot_restored = table.snapshot()?;
    let target_snapshot: &Snapshot = snapshot_restored.snapshot().snapshot();
    let version = target_snapshot.version();

    if version >= current_snapshot.version() {
        return Err(DeltaTableError::from(RestoreError::TooLargeRestoreVersion(
            version,
            current_snapshot.version(),
        )));
    }

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let (files_to_add, files_to_remove) = plan_restore_file_changes(
        log_store.as_ref(),
        current_snapshot,
        target_snapshot,
        deletion_timestamp,
    )
    .await?;
    let metadata_restored_version = target_snapshot.metadata();

    if !ignore_missing_files {
        check_files_available(log_store.object_store(None).as_ref(), &files_to_add).await?;
    }

    let metrics = RestoreMetrics {
        num_removed_file: files_to_remove.len(),
        num_restored_file: files_to_add.len(),
    };

    let mut actions = vec![];
    let protocol = if protocol_downgrade_allowed {
        ProtocolInner {
            min_reader_version: target_snapshot.protocol().min_reader_version(),
            min_writer_version: target_snapshot.protocol().min_writer_version(),
            writer_features: if current_snapshot.protocol().min_writer_version() < 7 {
                None
            } else {
                target_snapshot.protocol().writer_features_set()
            },
            reader_features: if current_snapshot.protocol().min_reader_version() < 3 {
                None
            } else {
                target_snapshot.protocol().reader_features_set()
            },
        }
    } else {
        ProtocolInner {
            min_reader_version: max(
                target_snapshot.protocol().min_reader_version(),
                current_snapshot.protocol().min_reader_version(),
            ),
            min_writer_version: max(
                target_snapshot.protocol().min_writer_version(),
                current_snapshot.protocol().min_writer_version(),
            ),
            writer_features: current_snapshot.protocol().writer_features_set(),
            reader_features: current_snapshot.protocol().reader_features_set(),
        }
    };
    commit_properties
        .app_metadata
        .insert("readVersion".to_owned(), current_snapshot.version().into());
    commit_properties.app_metadata.insert(
        "operationMetrics".to_owned(),
        serde_json::to_value(&metrics)?,
    );

    actions.push(Action::Protocol(protocol.as_kernel()));
    actions.extend(files_to_add.into_iter().map(Action::Add));
    actions.extend(files_to_remove.into_iter().map(Action::Remove));
    // Add the metadata from the restored version to undo e.g. constraint or field metadata changes
    actions.push(Action::Metadata(metadata_restored_version.clone()));

    let operation = DeltaOperation::Restore {
        version: version_to_restore,
        datetime: datetime_to_restore.map(|time| -> i64 { time.timestamp_millis() }),
    };

    let commit = CommitBuilder::from(commit_properties)
        .with_actions(actions)
        .with_max_retries(0)
        .with_operation_id(operation_id)
        .with_post_commit_hook_handler(custom_execute_handler)
        .build(Some(&snapshot), log_store.clone(), operation)
        .await?;

    Ok((metrics, commit.snapshot()))
}

async fn check_files_available(
    object_store: &dyn ObjectStore,
    files: &Vec<Add>,
) -> DeltaResult<()> {
    for file in files {
        let file_path = Path::parse(file.path.clone())?;
        match object_store.head(&file_path).await {
            Ok(_) => {}
            Err(ObjectStoreError::NotFound { .. }) => {
                return Err(DeltaTableError::from(RestoreError::MissingDataFile(
                    file.path.clone(),
                )));
            }
            Err(e) => return Err(DeltaTableError::from(e)),
        }
    }
    Ok(())
}

impl std::future::IntoFuture for RestoreBuilder {
    type Output = DeltaResult<(DeltaTable, RestoreMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), true, None).await?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let handle = this.custom_execute_handler.take();
            let (metrics, new_state) = execute(
                this.log_store.clone(),
                snapshot,
                this.version_to_restore,
                this.datetime_to_restore,
                this.ignore_missing_files,
                this.protocol_downgrade_allowed,
                this.commit_properties.clone(),
                handle.clone(),
                operation_id,
            )
            .await?;

            if let Some(handler) = handle {
                handler.post_execute(&this.log_store, operation_id).await?;
            }

            Ok((
                DeltaTable::new_with_state(this.log_store, new_state),
                metrics,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::kernel::{
        DataType, DeletionVectorDescriptor, PrimitiveType, Snapshot, StorageType, StructField,
    };
    use crate::protocol::SaveMode;
    #[cfg(feature = "datafusion")]
    use crate::writer::test_utils::{create_bare_table, get_record_batch};
    use crate::{DeltaResult, TableProperty};

    async fn commit_actions(table: &DeltaTable, actions: Vec<Action>) -> DeltaResult<()> {
        CommitBuilder::default()
            .with_actions(actions)
            .build(
                Some(table.snapshot()?),
                table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?;
        Ok(())
    }

    async fn metadata_rich_restore_table() -> DeltaResult<(DeltaTable, Add, Add)> {
        let mut target_add = crate::test_utils::make_test_add(
            "part=a/metadata-rich.parquet",
            &[("part", "a")],
            1_725_000_000_000,
        );
        target_add.size = 1234;
        target_add.stats = Some(
            r#"{"numRecords":5,"minValues":{"id":1},"maxValues":{"id":5},"nullCount":{"id":0}}"#
                .to_string(),
        );
        target_add.tags = Some(HashMap::from([
            ("source".to_string(), Some("restore-target".to_string())),
            ("nullable-tag".to_string(), None),
        ]));
        target_add.deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: StorageType::Inline,
            path_or_inline_dv: "AAAA".to_string(),
            offset: None,
            size_in_bytes: 0,
            cardinality: 2,
        });
        target_add.base_row_id = Some(41);
        target_add.default_row_commit_version = Some(3);
        target_add.clustering_provider = Some("liquid".to_string());

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(vec![
                StructField::new(
                    "id".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    false,
                ),
                StructField::new(
                    "part".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    false,
                ),
            ])
            .with_partition_columns(["part"])
            .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
            .with_actions([Action::Add(target_add.clone())])
            .await?;

        let remove_target = Remove {
            path: target_add.path.clone(),
            data_change: true,
            deletion_timestamp: Some(1_725_000_001_000),
            extended_file_metadata: Some(true),
            partition_values: Some(target_add.partition_values.clone()),
            size: Some(target_add.size),
            tags: target_add.tags.clone(),
            deletion_vector: target_add.deletion_vector.clone(),
            base_row_id: target_add.base_row_id,
            default_row_commit_version: target_add.default_row_commit_version,
        };
        let mut current_add = crate::test_utils::make_test_add(
            target_add.path.clone(),
            &[("part", "a")],
            1_725_000_002_000,
        );
        current_add.size = target_add.size;
        current_add.stats = Some(
            r#"{"numRecords":5,"minValues":{"id":1},"maxValues":{"id":5},"nullCount":{"id":0}}"#
                .to_string(),
        );
        current_add.tags = Some(HashMap::from([
            ("source".to_string(), Some("restore-current".to_string())),
            ("nullable-tag".to_string(), None),
        ]));
        current_add.deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: StorageType::Inline,
            path_or_inline_dv: "BBBB".to_string(),
            offset: None,
            size_in_bytes: 0,
            cardinality: 1,
        });
        current_add.base_row_id = Some(84);
        current_add.default_row_commit_version = Some(4);
        commit_actions(
            &table,
            vec![
                Action::Remove(remove_target),
                Action::Add(current_add.clone()),
            ],
        )
        .await?;

        Ok((table, target_add, current_add))
    }

    fn no_stats_config(require_files: bool) -> DeltaTableConfig {
        DeltaTableConfig {
            require_files,
            skip_stats: true,
            ..Default::default()
        }
    }

    fn normalize_adds(mut adds: Vec<Add>) -> DeltaResult<Vec<serde_json::Value>> {
        adds.sort_by(|left, right| left.path.cmp(&right.path));
        adds.into_iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn normalize_removes(mut removes: Vec<Remove>) -> DeltaResult<Vec<serde_json::Value>> {
        removes.sort_by(|left, right| left.path.cmp(&right.path));
        removes
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn restore_plan_lazy_eager_parity_preserves_complete_actions() -> DeltaResult<()> {
        let (table, target_add, current_add) = metadata_rich_restore_table().await?;
        let log_store = table.log_store();
        let eager_current =
            EagerSnapshot::try_new(log_store.as_ref(), no_stats_config(true), None).await?;
        let eager_target =
            EagerSnapshot::try_new(log_store.as_ref(), no_stats_config(true), Some(0)).await?;
        let lazy_current =
            Snapshot::try_new(log_store.as_ref(), no_stats_config(false), None).await?;
        let lazy_target =
            Snapshot::try_new(log_store.as_ref(), no_stats_config(false), Some(0)).await?;
        let deletion_timestamp = 1_725_000_003_000;

        assert!(!lazy_current.has_materialized_files_for_test());
        assert!(!lazy_target.has_materialized_files_for_test());
        let (eager_adds, eager_removes) = plan_restore_file_changes(
            log_store.as_ref(),
            eager_current.snapshot(),
            eager_target.snapshot(),
            deletion_timestamp,
        )
        .await?;
        let (lazy_adds, lazy_removes) = plan_restore_file_changes(
            log_store.as_ref(),
            &lazy_current,
            &lazy_target,
            deletion_timestamp,
        )
        .await?;

        let lazy_adds = normalize_adds(lazy_adds)?;
        assert_eq!(normalize_adds(eager_adds)?, lazy_adds);
        assert_eq!(lazy_adds, vec![serde_json::to_value(target_add)?]);
        assert_eq!(
            normalize_removes(eager_removes)?,
            normalize_removes(lazy_removes.clone())?
        );
        let expected_remove = Remove {
            path: current_add.path,
            data_change: true,
            deletion_timestamp: Some(deletion_timestamp),
            extended_file_metadata: Some(true),
            partition_values: Some(current_add.partition_values),
            size: Some(current_add.size),
            tags: current_add.tags,
            deletion_vector: current_add.deletion_vector,
            base_row_id: current_add.base_row_id,
            default_row_commit_version: current_add.default_row_commit_version,
        };
        assert_eq!(
            normalize_removes(lazy_removes)?,
            vec![serde_json::to_value(expected_remove)?]
        );
        assert!(!lazy_current.has_materialized_files_for_test());
        assert!(!lazy_target.has_materialized_files_for_test());

        Ok(())
    }

    #[tokio::test]
    async fn restore_plan_to_metadata_only_version_removes_current_file() -> DeltaResult<()> {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                false,
            )])
            .await?;
        commit_actions(
            &table,
            vec![Action::Add(crate::test_utils::make_test_add(
                "current.parquet",
                &[],
                1_725_000_000_000,
            ))],
        )
        .await?;
        let log_store = table.log_store();
        let current = Snapshot::try_new(log_store.as_ref(), no_stats_config(false), None).await?;
        let target = Snapshot::try_new(log_store.as_ref(), no_stats_config(false), Some(0)).await?;

        assert_eq!(target.version(), 0);
        assert_eq!(current.version(), 1);
        assert!(!current.has_materialized_files_for_test());
        assert!(!target.has_materialized_files_for_test());
        let (adds, removes) =
            plan_restore_file_changes(log_store.as_ref(), &current, &target, 123).await?;

        assert!(adds.is_empty());
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].path, "current.parquet");
        assert_eq!(removes[0].deletion_timestamp, Some(123));
        assert!(!current.has_materialized_files_for_test());
        assert!(!target.has_materialized_files_for_test());

        Ok(())
    }

    #[tokio::test]
    async fn restore_file_key_preserves_valid_dv_identity() -> DeltaResult<()> {
        // Use the Delta protocol examples with '@' in the inline data and UUID path.
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-descriptor-schema
        let inline = DeletionVectorDescriptor {
            storage_type: StorageType::Inline,
            path_or_inline_dv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".into(),
            offset: None,
            size_in_bytes: 40,
            cardinality: 6,
        };
        let persisted = DeletionVectorDescriptor {
            storage_type: StorageType::UuidRelativePath,
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".into(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        };
        let later = DeletionVectorDescriptor {
            offset: Some(42),
            ..persisted.clone()
        };
        let cases = [
            (None, None),
            (
                Some(inline),
                Some("iwi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L"),
            ),
            (Some(persisted), Some("uab^-aqEH.-t@S}K{vb[*k^@4")),
            (Some(later), Some("uab^-aqEH.-t@S}K{vb[*k^@42")),
        ];
        for (deletion_vector, expected_id) in cases {
            let mut add = crate::test_utils::make_test_add("file.parquet", &[], 0);
            add.stats = Some(r#"{"numRecords":30}"#.into());
            add.deletion_vector = deletion_vector;
            let table = DeltaTable::new_in_memory()
                .create()
                .with_columns([StructField::new("id", DataType::INTEGER, false)])
                .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
                .with_actions([Action::Add(add)])
                .await?;
            let log_store = table.log_store();
            let snapshot =
                Snapshot::try_new(log_store.as_ref(), no_stats_config(false), None).await?;
            let files: Vec<_> = snapshot
                .active_adds(
                    log_store.as_ref(),
                    ActiveAddOptions {
                        predicate: None,
                        stats: AddStatsPolicy::None,
                    },
                )
                .try_collect()
                .await?;
            assert_eq!(files.len(), 1);

            let key = restore_file_key(&files[0]);
            assert_eq!(key.0, Path::parse("file.parquet")?);
            assert_eq!(key.1.as_deref(), expected_id);
        }
        Ok(())
    }

    /// Verify that restore respects constraints that were added/removed in previous version_to_restore
    /// <https://github.com/delta-io/delta-rs/issues/3352>
    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_simple_restore_constraints() -> DeltaResult<()> {
        use crate::table::config::TablePropertiesExt as _;

        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;
        let first_v = table.version().unwrap();

        let constraint = table
            .add_constraint()
            .with_constraint("my_custom_constraint", "value < 100")
            .await;
        let table = constraint.expect("Failed to add constraint to table");

        let constraints = table
            .state
            .as_ref()
            .unwrap()
            .table_config()
            .get_constraints();
        assert!(constraints.len() == 1);
        assert_eq!(constraints[0].name, "my_custom_constraint");

        let (table, _metrics) = table.restore().with_version_to_restore(first_v).await?;
        assert_ne!(table.version(), Some(first_v));

        let constraints = table.state.unwrap().table_config().get_constraints();
        assert!(constraints.is_empty());

        Ok(())
    }
}
