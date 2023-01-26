//! Audit the Delta Table for active files that do not exist in the underlying filesystem and remove them
//!
//! # Example
//! ```rust ignore
//! let mut table = open_table("../path/to/table")?;
//! let (table, metrics) = FileSystemCheckBuilder::new(table.object_store(). table.state).await?;
//! ````
use crate::action::{Action, Add, DeltaOperation, Remove};
use crate::operations::transaction::commit;
use crate::storage::DeltaObjectStore;
use crate::table_state::DeltaTableState;
use crate::DeltaDataTypeVersion;
use crate::{DeltaDataTypeLong, DeltaResult, DeltaTable, DeltaTableError};
use futures::future::BoxFuture;
pub use object_store::path::Path;
use object_store::Error as ObjectStoreError;
use object_store::ObjectStore;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Audit the Delta Table's active files with the underlying file system.
/// See this module's documentaiton for more information
#[derive(Debug)]
pub struct FileSystemCheckBuilder {
    /// A snapshot of the to-be-checked table's state
    state: DeltaTableState,
    /// Delta object store for handling data files
    store: Arc<DeltaObjectStore>,
    /// Don't remove actions to the table log. Just determine which files can be removed
    dry_run: bool,
}

/// Details of the FSCK operation including which files were removed from the log
#[derive(Debug)]
pub struct FileSystemCheckMetrics {
    /// Was this a dry run
    pub dry_run: bool,
    /// Files that wrere removed successfully
    pub files_removed: Vec<String>,
}

struct FileSystemCheckPlan {
    /// Version of the snapshot provided
    version: DeltaDataTypeVersion,
    /// Delta object store for handling data files
    store: Arc<DeltaObjectStore>,
    /// Files that no longer exists in undlying ObjectStore but have active add actions
    pub files_to_remove: Vec<Add>,
}

impl FileSystemCheckBuilder {
    /// Create a new [`FileSystemCheckBuilder`]
    pub fn new(store: Arc<DeltaObjectStore>, state: DeltaTableState) -> Self {
        FileSystemCheckBuilder {
            state,
            store,
            dry_run: false,
        }
    }

    /// Only determine which add actions should be removed
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    async fn create_fsck_plan(&self) -> DeltaResult<FileSystemCheckPlan> {
        let mut files_to_remove = Vec::new();
        let version = self.state.version();
        let store = self.store.clone();

        for active in self.state.files() {
            let res = self.store.head(&Path::from(active.path.as_str())).await;
            if let Err(ObjectStoreError::NotFound { path: _, source: _ }) = res {
                files_to_remove.push(active.to_owned());
            } else {
                res.map_err(DeltaTableError::from)?;
            }
        }

        Ok(FileSystemCheckPlan {
            files_to_remove,
            version,
            store,
        })
    }
}

impl FileSystemCheckPlan {
    pub async fn execute(self) -> DeltaResult<FileSystemCheckMetrics> {
        if self.files_to_remove.is_empty() {
            return Ok(FileSystemCheckMetrics {
                dry_run: false,
                files_removed: Vec::new(),
            });
        }

        let mut actions = Vec::new();
        let mut removed_file_paths = Vec::new();
        let version = self.version;
        let store = &self.store;

        for file in self.files_to_remove {
            let deletion_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let deletion_time = deletion_time.as_millis() as DeltaDataTypeLong;
            removed_file_paths.push(file.path.clone());
            actions.push(Action::remove(Remove {
                path: file.path,
                deletion_timestamp: Some(deletion_time),
                data_change: true,
                extended_file_metadata: None,
                partition_values: Some(file.partition_values),
                size: Some(file.size),
                tags: file.tags,
            }));
        }

        if !actions.is_empty() {
            commit(
                store,
                version + 1,
                actions,
                DeltaOperation::FileSystemCheck {},
                None,
            )
            .await?;
        }

        Ok(FileSystemCheckMetrics {
            dry_run: false,
            files_removed: removed_file_paths,
        })
    }
}

impl std::future::IntoFuture for FileSystemCheckBuilder {
    type Output = DeltaResult<(DeltaTable, FileSystemCheckMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let plan = this.create_fsck_plan().await?;
            if this.dry_run {
                return Ok((
                    DeltaTable::new_with_state(this.store, this.state),
                    FileSystemCheckMetrics {
                        files_removed: plan
                            .files_to_remove
                            .iter()
                            .map(|f| f.path.clone())
                            .collect(),
                        dry_run: true,
                    },
                ));
            }

            let metrics = plan.execute().await?;
            let mut table = DeltaTable::new_with_state(this.store, this.state);
            table.update().await?;
            Ok((table, metrics))
        })
    }
}
