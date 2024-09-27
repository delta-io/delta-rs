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
//! let mut table = open_table("../path/to/table")?;
//! let (table, metrics) = FileSystemCheckBuilder::new(table.object_store(), table.state).await?;
//! ````

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use futures::future::BoxFuture;
use futures::StreamExt;
pub use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;
use url::{ParseError, Url};

use super::transaction::{CommitBuilder, CommitProperties};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Action, Add, Remove};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;

/// Audit the Delta Table's active files with the underlying file system.
/// See this module's documentation for more information
#[derive(Debug)]
pub struct FileSystemCheckBuilder {
    /// A snapshot of the to-be-checked table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Don't remove actions to the table log. Just determine which files can be removed
    dry_run: bool,
    /// Commit properties and configuration
    commit_properties: CommitProperties,
}

/// Details of the FSCK operation including which files were removed from the log
#[derive(Debug, Serialize)]
pub struct FileSystemCheckMetrics {
    /// Was this a dry run
    pub dry_run: bool,
    /// Files that wrere removed successfully
    pub files_removed: Vec<String>,
}

struct FileSystemCheckPlan {
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Files that no longer exists in undlying ObjectStore but have active add actions
    pub files_to_remove: Vec<Add>,
}

fn is_absolute_path(path: &str) -> DeltaResult<bool> {
    match Url::parse(path) {
        Ok(_) => Ok(true),
        Err(ParseError::RelativeUrlWithoutBase) => Ok(false),
        Err(_) => Err(DeltaTableError::Generic(format!(
            "Unable to parse path: {}",
            &path
        ))),
    }
}

impl super::Operation<()> for FileSystemCheckBuilder {}

impl FileSystemCheckBuilder {
    /// Create a new [`FileSystemCheckBuilder`]
    pub fn new(log_store: LogStoreRef, state: DeltaTableState) -> Self {
        FileSystemCheckBuilder {
            snapshot: state,
            log_store,
            dry_run: false,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Only determine which add actions should be removed. A dry run will not commit actions to the Delta log
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Additonal information to write to the commit
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    async fn create_fsck_plan(&self) -> DeltaResult<FileSystemCheckPlan> {
        let mut files_relative: HashMap<String, Add> =
            HashMap::with_capacity(self.snapshot.files_count());
        let log_store = self.log_store.clone();

        for active in self.snapshot.file_actions_iter()? {
            if is_absolute_path(&active.path)? {
                return Err(DeltaTableError::Generic(
                    "Filesystem check does not support absolute paths".to_string(),
                ));
            } else {
                files_relative.insert(active.path.clone(), active);
            }
        }

        let object_store = log_store.object_store();
        let mut files = object_store.list(None);
        while let Some(result) = files.next().await {
            let file = result?;
            files_relative.remove(file.location.as_ref());

            if files_relative.is_empty() {
                break;
            }
        }

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
        snapshot: &DeltaTableState,
        mut commit_properties: CommitProperties,
    ) -> DeltaResult<FileSystemCheckMetrics> {
        if self.files_to_remove.is_empty() {
            return Ok(FileSystemCheckMetrics {
                dry_run: false,
                files_removed: Vec::new(),
            });
        }

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
                deletion_vector: None,
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
            let plan = this.create_fsck_plan().await?;
            if this.dry_run {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, this.snapshot),
                    FileSystemCheckMetrics {
                        files_removed: plan.files_to_remove.into_iter().map(|f| f.path).collect(),
                        dry_run: true,
                    },
                ));
            }

            let metrics = plan.execute(&this.snapshot, this.commit_properties).await?;
            let mut table = DeltaTable::new_with_state(this.log_store, this.snapshot);
            table.update().await?;
            Ok((table, metrics))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_path() {
        assert!(!is_absolute_path(
            "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet"
        )
        .unwrap());
        assert!(!is_absolute_path(
            "x=9/y=9.9/part-00007-3c50fba1-4264-446c-9c67-d8e24a1ccf83.c000.snappy.parquet"
        )
        .unwrap());

        assert!(is_absolute_path("abfss://container@account_name.blob.core.windows.net/full/part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet").unwrap());
        assert!(is_absolute_path("file:///C:/my_table/windows.parquet").unwrap());
        assert!(is_absolute_path("file:///home/my_table/unix.parquet").unwrap());
        assert!(is_absolute_path("s3://container/path/file.parquet").unwrap());
        assert!(is_absolute_path("gs://container/path/file.parquet").unwrap());
        assert!(is_absolute_path("scheme://table/file.parquet").unwrap());
    }
}
