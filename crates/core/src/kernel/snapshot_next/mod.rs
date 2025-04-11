//! Snapshot of a Delta Table at a specific version.

use std::sync::Arc;

use arrow_array::RecordBatch;
use delta_kernel::actions::visitors::SetTransactionMap;
use delta_kernel::actions::{Metadata, Protocol, SetTransaction};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::Schema;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{ExpressionRef, Version};
use iterators::{LogicalFileView, LogicalFileViewIterator};
use url::Url;

use crate::kernel::actions::CommitInfo;
use crate::{DeltaResult, DeltaTableError};

pub use eager::EagerSnapshot;
pub use lazy::LazySnapshot;

mod cache;
mod eager;
mod iterators;
mod lazy;

// TODO: avoid repetitive parsing of json stats

#[derive(thiserror::Error, Debug)]
enum SnapshotError {
    #[error("Tried accessing file data at snapshot initialized with no files.")]
    FilesNotInitialized,
}

impl From<SnapshotError> for DeltaTableError {
    fn from(e: SnapshotError) -> Self {
        match &e {
            SnapshotError::FilesNotInitialized => DeltaTableError::generic(e),
        }
    }
}

/// Helper trait to extract individual values from a `StructData`.
pub trait StructDataExt {
    fn get(&self, key: &str) -> Option<&Scalar>;
}

impl StructDataExt for StructData {
    fn get(&self, key: &str) -> Option<&Scalar> {
        self.fields()
            .iter()
            .zip(self.values().iter())
            .find(|(k, _)| k.name() == key)
            .map(|(_, v)| v)
    }
}

/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
pub trait Snapshot {
    /// Location where the Delta Table (metadata) is stored.
    fn table_root(&self) -> &Url;

    /// Version of this `Snapshot` in the table.
    fn version(&self) -> Version;

    /// Table [`Schema`] at this `Snapshot`s version.
    fn schema(&self) -> Arc<Schema>;

    /// Table [`Metadata`] at this `Snapshot`s version.
    ///
    /// Metadata contains information about the table, such as the table name,
    /// the schema, the partition columns, the configuration, etc.
    fn metadata(&self) -> &Metadata;

    /// Table [`Protocol`] at this `Snapshot`s version.
    ///
    /// The protocol indicates the min reader / writer version required to
    /// read / write the table. For modern readers / writers, the reader /
    /// writer features active in the table are also available.
    fn protocol(&self) -> &Protocol;

    /// Get the [`TableProperties`] for this [`Snapshot`].
    fn table_properties(&self) -> &TableProperties;

    fn logical_file_schema(&self) -> Schema {
        scan_row_schema()
    }

    /// Get all logical files present in the current snapshot.
    ///
    /// # Parameters
    /// - `predicate`: An optional predicate to filter the files based on file statistics.
    ///
    /// # Returns
    /// An iterator of [`RecordBatch`]es, where each batch contains logical file data.
    fn logical_files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>> + '_>>;

    fn logical_files_view(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<LogicalFileView>> + '_>> {
        #[allow(deprecated)]
        Ok(Box::new(LogicalFileViewIterator::new(
            self.logical_files(predicate)?,
        )))
    }

    /// Get all tombstones in the table.
    ///
    /// Remove Actions (tombstones) are records that indicate that a file has been deleted.
    /// They are returned mostly for the purposes of VACUUM operations.
    ///
    /// # Returns
    /// An iterator of [`RecordBatch`]es, where each batch contains remove action data.
    fn tombstones(&self) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>>;

    /// Scan the Delta Log to obtain the latest transaction for all applications
    ///
    /// This method requires a full scan of the log to find all transactions.
    /// When a specific application id is requested, it is much more efficient to use
    /// [`application_transaction`](Self::application_transaction) instead.
    fn application_transactions(&self) -> DeltaResult<SetTransactionMap>;

    /// Scan the Delta Log for the latest transaction entry for a specific application.
    ///
    /// Initiates a log scan, but terminates as soon as the transaction
    /// for the given application is found.
    ///
    /// # Parameters
    /// - `app_id`: The application id for which to fetch the transaction.
    ///
    /// # Returns
    /// The latest transaction for the given application id, if it exists.
    fn application_transaction(&self, app_id: &str) -> DeltaResult<Option<SetTransaction>>;

    /// Get commit info for the table.
    ///
    /// The [`CommitInfo`]s are returned in descending order of version
    /// with the most recent commit first starting from the `start_version`.
    ///
    /// [`CommitInfo`]s are read on a best-effort basis. If the action
    /// for a version is not available or cannot be parsed, it is skipped.
    ///
    /// # Parameters
    /// - `start_version`: The version from which to start fetching commit info.
    ///   Defaults to the latest version.
    /// - `limit`: The maximum number of commit infos to fetch.
    ///
    /// # Returns
    /// An iterator of commit info tuples. The first element of the tuple is the version
    /// of the commit, the second element is the corresponding commit info.
    // TODO(roeap): this is currently using our commit info, we should be using
    // the definition form kernel, once handling over there matured.
    fn commit_infos(
        &self,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Box<dyn Iterator<Item = (Version, CommitInfo)>>>;

    /// Update the snapshot to a specific version.
    ///
    /// The target version must be greater then the current version of the snapshot.
    ///
    /// # Parameters
    /// - `target_version`: The version to update the snapshot to. Defaults to latest.
    ///
    /// # Returns
    /// A boolean indicating if the snapshot was updated.
    fn update(&mut self, target_version: Option<Version>) -> DeltaResult<bool>;
}

impl<T: Snapshot> Snapshot for Box<T> {
    fn table_root(&self) -> &Url {
        self.as_ref().table_root()
    }

    fn version(&self) -> Version {
        self.as_ref().version()
    }

    fn schema(&self) -> Arc<Schema> {
        self.as_ref().schema()
    }

    fn metadata(&self) -> &Metadata {
        self.as_ref().metadata()
    }

    fn protocol(&self) -> &Protocol {
        self.as_ref().protocol()
    }

    fn table_properties(&self) -> &TableProperties {
        self.as_ref().table_properties()
    }

    fn logical_files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>> + '_>> {
        self.as_ref().logical_files(predicate)
    }

    fn tombstones(&self) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        self.as_ref().tombstones()
    }

    fn application_transactions(&self) -> DeltaResult<SetTransactionMap> {
        self.as_ref().application_transactions()
    }

    fn application_transaction(&self, app_id: &str) -> DeltaResult<Option<SetTransaction>> {
        self.as_ref().application_transaction(app_id)
    }

    fn commit_infos(
        &self,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Box<dyn Iterator<Item = (Version, CommitInfo)>>> {
        self.as_ref().commit_infos(start_version, limit)
    }

    fn update(&mut self, target_version: Option<Version>) -> DeltaResult<bool> {
        self.as_mut().update(target_version)
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use delta_kernel::Table;
    use deltalake_test::utils::*;

    use super::*;

    fn get_lazy(
        ctx: &IntegrationContext,
        table: TestTables,
        version: Option<Version>,
    ) -> TestResult<Pin<Box<dyn Future<Output = TestResult<Box<dyn Snapshot>>>>>> {
        let store = ctx.table_builder(table).build_storage()?.object_store(None);
        let table = Table::try_from_uri("memory:///")?;
        Ok(Box::pin(async move {
            Ok(Box::new(LazySnapshot::try_new(table, store, version).await?) as Box<dyn Snapshot>)
        }))
    }

    fn get_eager(
        ctx: &IntegrationContext,
        table: TestTables,
        version: Option<Version>,
    ) -> TestResult<Pin<Box<dyn Future<Output = TestResult<Box<dyn Snapshot>>>>>> {
        let store = ctx.table_builder(table).build_storage()?.object_store(None);
        let config = Default::default();
        Ok(Box::pin(async move {
            Ok(
                Box::new(EagerSnapshot::try_new("memory:///", store, config, version).await?)
                    as Box<dyn Snapshot>,
            )
        }))
    }

    #[tokio::test]
    async fn test_snapshots() -> TestResult {
        let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;
        context.load_table(TestTables::Checkpoints).await?;
        context.load_table(TestTables::Simple).await?;
        context.load_table(TestTables::SimpleWithCheckpoint).await?;
        context.load_table(TestTables::WithDvSmall).await?;

        test_snapshot(&context, get_lazy).await?;
        test_snapshot(&context, get_eager).await?;

        Ok(())
    }

    // NOTE: test needs to be async, so that we can pick up the runtime from the context
    async fn test_snapshot<F>(ctx: &IntegrationContext, get_snapshot: F) -> TestResult<()>
    where
        F: Fn(
            &IntegrationContext,
            TestTables,
            Option<Version>,
        ) -> TestResult<Pin<Box<dyn Future<Output = TestResult<Box<dyn Snapshot>>>>>>,
    {
        for version in 0..=12 {
            let snapshot = get_snapshot(ctx, TestTables::Checkpoints, Some(version))?.await?;
            assert_eq!(snapshot.version(), version);

            test_commit_infos(snapshot.as_ref())?;
            test_logical_files(snapshot.as_ref())?;
            test_logical_files_view(snapshot.as_ref())?;
        }

        let mut snapshot = get_snapshot(ctx, TestTables::Checkpoints, Some(0))?.await?;
        for version in 1..=12 {
            snapshot.update(Some(version))?;
            assert_eq!(snapshot.version(), version);

            test_commit_infos(snapshot.as_ref())?;
            test_logical_files(snapshot.as_ref())?;
            test_logical_files_view(snapshot.as_ref())?;
        }

        Ok(())
    }

    fn test_logical_files(snapshot: &dyn Snapshot) -> TestResult<()> {
        let logical_files = snapshot
            .logical_files(None)?
            .collect::<Result<Vec<_>, _>>()?;
        let num_files = logical_files
            .iter()
            .map(|b| b.num_rows() as i64)
            .sum::<i64>();
        assert_eq!((num_files as u64), snapshot.version());
        Ok(())
    }

    fn test_logical_files_view(snapshot: &dyn Snapshot) -> TestResult<()> {
        let num_files_view = snapshot
            .logical_files_view(None)?
            .map(|f| f.unwrap().path().to_string())
            .count() as u64;
        assert_eq!(num_files_view, snapshot.version());
        Ok(())
    }

    fn test_commit_infos(snapshot: &dyn Snapshot) -> TestResult<()> {
        let commit_infos = snapshot.commit_infos(None, Some(100))?.collect::<Vec<_>>();
        assert_eq!((commit_infos.len() as u64), snapshot.version() + 1);
        assert_eq!(commit_infos.first().unwrap().0, snapshot.version());
        Ok(())
    }
}
