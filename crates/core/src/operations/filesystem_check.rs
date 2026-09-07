//! Audit the Delta Table for active files that do not exist in the underlying filesystem and remove them.
//!
//! Active files are ones that have an add action in the log, but no corresponding remove action.
//! This operation creates a new transaction containing a remove action for each of the missing files.
//!
//! This can be used to repair tables where a data file has been deleted accidentally or
//! purposefully, if the file was corrupted.
//!
//! # Example
//! ```rust ignore
//! let mut table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
//! let (table, metrics) = FileSystemCheckBuilder::new(table.object_store(), table.state).await?;
//! ````

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use tracing::*;
use url::{ParseError, Url};
use uuid::Uuid;

use super::CustomExecuteHandler;
use super::Operation;
use crate::DeltaTable;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{Action, Add, Remove};
use crate::kernel::{ActiveAddOptions, AddStatsPolicy, EagerSnapshot, Snapshot, resolve_snapshot};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;

/// Audit the Delta Table's active files with the underlying file system.
/// See this module's documentation for more information
pub struct FileSystemCheckBuilder {
    /// A snapshot of the to-be-checked table's state
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Don't remove actions to the table log. Just determine which files can be removed
    dry_run: bool,
    /// Commit properties and configuration
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

/// Details of the FSCK operation including which files were removed from the log
#[derive(Debug, Serialize)]
pub struct FileSystemCheckMetrics {
    /// Was this a dry run
    pub dry_run: bool,
    /// Files that were removed successfully
    #[serde(
        serialize_with = "serialize_vec_string",
        deserialize_with = "deserialize_vec_string"
    )]
    pub files_removed: Vec<String>,
}

struct FileSystemCheckPlan {
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Files that no longer exists in undlying ObjectStore but have active add actions
    pub files_to_remove: Vec<Add>,
}

// Custom serialization function that serializes metric details as a string
fn serialize_vec_string<S>(value: &Vec<String>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let json_string = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&json_string)
}

// Custom deserialization that parses a JSON string into MetricDetails
#[expect(dead_code)]
fn deserialize_vec_string<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(DeError::custom)
}

fn is_absolute_path(path: &str) -> DeltaResult<bool> {
    match Url::parse(path) {
        Ok(_) => Ok(true),
        Err(ParseError::RelativeUrlWithoutBase) => Ok(false),
        Err(_) => Err(DeltaTableError::Generic(format!(
            "Unable to parse path: {path}"
        ))),
    }
}

impl super::Operation for FileSystemCheckBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl FileSystemCheckBuilder {
    /// Create a new [`FileSystemCheckBuilder`]
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        FileSystemCheckBuilder {
            snapshot,
            log_store,
            dry_run: false,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Only determine which add actions should be removed. A dry run will not commit actions to the Delta log
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Additional information to write to the commit
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    async fn create_fsck_plan(&self, snapshot: &Snapshot) -> DeltaResult<FileSystemCheckPlan> {
        let mut files_relative: HashMap<String, Add> = HashMap::new();
        let log_store = self.log_store.clone();
        let mut file_stream = snapshot
            .active_adds(
                log_store.as_ref(),
                ActiveAddOptions {
                    predicate: None,
                    stats: AddStatsPolicy::None,
                },
            )
            .map_ok(|f| f.to_add());
        while let Some(active) = file_stream.next().await {
            let active = active?;
            if is_absolute_path(&active.path)? {
                return Err(DeltaTableError::Generic(
                    "Filesystem check does not support absolute paths".to_string(),
                ));
            } else {
                files_relative.insert(active.path.clone(), active);
            }
        }

        let object_store = log_store.object_store(None);
        let list_span = info_span!("list_files", operation = "filesystem_check");
        let mut files = list_span.in_scope(|| object_store.list(None));

        let mut file_count = 0;
        while let Some(result) = files.next().await {
            let file = result?;
            file_count += 1;
            files_relative.remove(file.location.as_ref());

            if files_relative.is_empty() {
                break;
            }
        }
        info!(
            files_scanned = file_count,
            missing_files = files_relative.len(),
            "filesystem check listing completed"
        );

        let files_to_remove: Vec<Add> = files_relative
            .into_values()
            .map(|file| file.to_owned())
            .collect();

        Ok(FileSystemCheckPlan {
            files_to_remove,
            log_store,
        })
    }
}

impl FileSystemCheckPlan {
    pub async fn execute(
        self,
        snapshot: &EagerSnapshot,
        mut commit_properties: CommitProperties,
        operation_id: Uuid,
        handle: Option<Arc<dyn CustomExecuteHandler>>,
    ) -> DeltaResult<FileSystemCheckMetrics> {
        let mut actions = Vec::with_capacity(self.files_to_remove.len());
        let mut removed_file_paths = Vec::with_capacity(self.files_to_remove.len());

        for file in self.files_to_remove {
            let deletion_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let deletion_time = deletion_time.as_millis() as i64;
            removed_file_paths.push(file.path.clone());
            actions.push(Action::Remove(Remove {
                path: file.path,
                deletion_timestamp: Some(deletion_time),
                data_change: true,
                extended_file_metadata: None,
                partition_values: Some(file.partition_values),
                size: Some(file.size),
                deletion_vector: file.deletion_vector,
                tags: file.tags,
                base_row_id: file.base_row_id,
                default_row_commit_version: file.default_row_commit_version,
            }));
        }
        let metrics = FileSystemCheckMetrics {
            dry_run: false,
            files_removed: removed_file_paths,
        };

        commit_properties
            .app_metadata
            .insert("readVersion".to_owned(), snapshot.version().into());
        commit_properties.app_metadata.insert(
            "operationMetrics".to_owned(),
            serde_json::to_value(&metrics)?,
        );

        CommitBuilder::from(commit_properties)
            .with_operation_id(operation_id)
            .with_post_commit_hook_handler(handle)
            .with_actions(actions)
            .build(
                Some(snapshot),
                self.log_store.clone(),
                DeltaOperation::FileSystemCheck {},
            )
            .await?;

        Ok(metrics)
    }
}

impl std::future::IntoFuture for FileSystemCheckBuilder {
    type Output = DeltaResult<(DeltaTable, FileSystemCheckMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), true, None).await?;

            let plan = this.create_fsck_plan(snapshot.snapshot()).await?;
            if this.dry_run {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot)),
                    FileSystemCheckMetrics {
                        files_removed: plan.files_to_remove.into_iter().map(|f| f.path).collect(),
                        dry_run: true,
                    },
                ));
            }
            if plan.files_to_remove.is_empty() {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot)),
                    FileSystemCheckMetrics {
                        dry_run: false,
                        files_removed: Vec::new(),
                    },
                ));
            };
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let metrics = plan
                .execute(
                    &snapshot,
                    this.commit_properties.clone(),
                    operation_id,
                    this.get_custom_execute_handler(),
                )
                .await?;

            this.post_execute(operation_id).await?;

            let mut table =
                DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot));
            table.update_state().await?;
            Ok((table, metrics))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use object_store::{ObjectStoreExt as _, PutPayload};

    use super::*;
    use crate::kernel::{
        DataType, DeletionVectorDescriptor, EagerSnapshot, PrimitiveType, Snapshot, StorageType,
        StructField,
    };
    use crate::{DeltaTableConfig, TableProperty};

    async fn metadata_rich_missing_file_table() -> DeltaResult<(DeltaTable, Add)> {
        let mut source_add = crate::test_utils::make_test_add(
            "part=a/metadata-rich.parquet",
            &[("part", "a")],
            1_725_000_000_000,
        );
        source_add.size = 1234;
        source_add.stats = Some(
            r#"{"numRecords":5,"minValues":{"id":1},"maxValues":{"id":5},"nullCount":{"id":0}}"#
                .to_string(),
        );
        source_add.tags = Some(HashMap::from([
            ("source".to_string(), Some("metadata-rich".to_string())),
            ("nullable-tag".to_string(), None),
        ]));
        source_add.deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: StorageType::Inline,
            path_or_inline_dv: "AAAA".to_string(),
            offset: None,
            size_in_bytes: 0,
            cardinality: 2,
        });
        source_add.base_row_id = Some(41);
        source_add.default_row_commit_version = Some(3);
        source_add.clustering_provider = Some("liquid".to_string());

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
            .with_actions([Action::Add(source_add.clone())])
            .await?;

        Ok((table, source_add))
    }

    fn normalize_adds(mut adds: Vec<Add>) -> DeltaResult<Vec<serde_json::Value>> {
        adds.sort_by(|left, right| left.path.cmp(&right.path));
        adds.into_iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn fsck_plan_lazy_eager_parity_preserves_remove_metadata() -> DeltaResult<()> {
        let (table, source_add) = metadata_rich_missing_file_table().await?;
        let log_store = table.log_store();
        let eager = EagerSnapshot::try_new(
            log_store.as_ref(),
            DeltaTableConfig {
                skip_stats: true,
                ..Default::default()
            },
            None,
        )
        .await?;
        let lazy = Snapshot::try_new(
            log_store.as_ref(),
            DeltaTableConfig {
                require_files: false,
                skip_stats: true,
                ..Default::default()
            },
            None,
        )
        .await?;
        let builder = FileSystemCheckBuilder::new(log_store.clone(), None);

        assert!(!lazy.has_materialized_files_for_test());
        let eager_plan = builder.create_fsck_plan(eager.snapshot()).await?;
        let lazy_plan = builder.create_fsck_plan(&lazy).await?;

        let lazy_files = normalize_adds(lazy_plan.files_to_remove.clone())?;
        assert_eq!(normalize_adds(eager_plan.files_to_remove)?, lazy_files);
        let mut expected = source_add;
        expected.stats = None;
        assert_eq!(lazy_files, vec![serde_json::to_value(expected)?]);
        assert!(!lazy.has_materialized_files_for_test());

        Ok(())
    }

    #[tokio::test]
    async fn fsck_removes_missing_deletion_vector_logical_file() -> DeltaResult<()> {
        let (table, source_add) = metadata_rich_missing_file_table().await?;
        let log_store = table.log_store();

        let (table, metrics) = table.filesystem_check().await?;

        assert_eq!(metrics.files_removed, vec![source_add.path]);
        let active_files: Vec<_> = table
            .snapshot()?
            .snapshot()
            .snapshot()
            .active_adds(
                log_store.as_ref(),
                ActiveAddOptions {
                    predicate: None,
                    stats: AddStatsPolicy::None,
                },
            )
            .try_collect()
            .await?;
        assert!(active_files.is_empty());

        Ok(())
    }

    const FSCK_PATH_CASES: &[(&str, &str)] = &[
        (
            "partition=a/file with spaces.parquet",
            "partition=a/file%20with%20spaces.parquet",
        ),
        (
            "partition=a/file%20with%20spaces.parquet",
            "partition=a/file%2520with%2520spaces.parquet",
        ),
    ];

    async fn fsck_path_table(physical_path: &str, wire_path: &str) -> DeltaResult<DeltaTable> {
        let add = crate::test_utils::make_test_add(physical_path, &[("partition", "a")], 0);
        // Check the encoded path that Add writes to the log.
        assert_eq!(serde_json::to_value(&add)?["path"], wire_path);
        DeltaTable::new_in_memory()
            .create()
            .with_columns([
                StructField::new("id", DataType::INTEGER, false),
                StructField::new("partition", DataType::STRING, false),
            ])
            .with_partition_columns(["partition"])
            .with_actions([Action::Add(add)])
            .await
    }

    #[tokio::test]
    async fn fsck_preserves_present_files_with_spaces_or_literal_percent_sequences()
    -> DeltaResult<()> {
        for &(physical_path, wire_path) in FSCK_PATH_CASES {
            let table = fsck_path_table(physical_path, wire_path).await?;
            let version = table.snapshot()?.version();
            // Use parse to preserve literal percent signs in the object path.
            table
                .object_store()
                .put(
                    &object_store::path::Path::parse(physical_path)?,
                    PutPayload::from_static(b"data"),
                )
                .await?;

            let (table, metrics) = table.filesystem_check().await?;

            assert!(
                metrics.files_removed.is_empty(),
                "{physical_path}: {metrics:?}"
            );
            assert_eq!(table.snapshot()?.version(), version);
        }
        Ok(())
    }

    #[tokio::test]
    async fn fsck_removes_missing_files_with_spaces_or_literal_percent_sequences() -> DeltaResult<()>
    {
        for &(physical_path, wire_path) in FSCK_PATH_CASES {
            let table = fsck_path_table(physical_path, wire_path).await?;
            let version = table.snapshot()?.version();
            let log_store = table.log_store();

            let (table, metrics) = table.filesystem_check().with_dry_run(true).await?;
            assert!(metrics.dry_run);
            assert_eq!(metrics.files_removed, vec![physical_path]);
            assert_eq!(table.snapshot()?.version(), version);

            let (table, metrics) = table.filesystem_check().await?;
            assert!(!metrics.dry_run);
            assert_eq!(metrics.files_removed, vec![physical_path]);
            assert_eq!(table.snapshot()?.version(), version + 1);

            let snapshot = Snapshot::try_new(
                log_store.as_ref(),
                DeltaTableConfig {
                    require_files: false,
                    ..Default::default()
                },
                None,
            )
            .await?;
            let active_files: Vec<_> = snapshot
                .active_adds(
                    log_store.as_ref(),
                    ActiveAddOptions {
                        predicate: None,
                        stats: AddStatsPolicy::None,
                    },
                )
                .try_collect()
                .await?;
            assert!(active_files.is_empty(), "{physical_path}");

            // Read the JSON path before Remove deserialization decodes it.
            let commit = log_store
                .read_commit_entry(version + 1)
                .await?
                .expect("expected the FSCK commit");
            let actions = serde_json::Deserializer::from_slice(&commit)
                .into_iter::<serde_json::Value>()
                .collect::<Result<Vec<_>, _>>()?;
            let removes: Vec<_> = actions
                .iter()
                .filter_map(|action| action.get("remove"))
                .collect();
            assert_eq!(removes.len(), 1);
            assert_eq!(removes[0]["path"], wire_path);
        }
        Ok(())
    }

    #[test]
    fn absolute_path() {
        assert!(
            !is_absolute_path(
                "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet"
            )
            .unwrap()
        );
        assert!(
            !is_absolute_path(
                "x=9/y=9.9/part-00007-3c50fba1-4264-446c-9c67-d8e24a1ccf83.c000.snappy.parquet"
            )
            .unwrap()
        );

        assert!(is_absolute_path("abfss://container@account_name.blob.core.windows.net/full/part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet").unwrap());
        assert!(is_absolute_path("file:///C:/my_table/windows.parquet").unwrap());
        assert!(is_absolute_path("file:///home/my_table/unix.parquet").unwrap());
        assert!(is_absolute_path("s3://container/path/file.parquet").unwrap());
        assert!(is_absolute_path("gs://container/path/file.parquet").unwrap());
        assert!(is_absolute_path("scheme://table/file.parquet").unwrap());
    }
}
