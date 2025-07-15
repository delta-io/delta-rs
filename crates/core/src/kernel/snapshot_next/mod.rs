//! Snapshot of a Delta Table at a specific version.

use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::Schema;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{Engine, PredicateRef, Version};
use futures::{StreamExt, TryStreamExt};
use iterators::LogicalFileView;
use itertools::Itertools;
use object_store::ObjectStore;
use stream::{SendableRBStream, SendableViewStream};
use url::Url;
use uuid::Uuid;

use crate::kernel::actions::CommitInfo;
use crate::logstore::{LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableError};

pub use eager::EagerSnapshot;
pub use lazy::LazySnapshot;

mod arrow_ext;
mod eager;
mod iterators;
mod lazy;
mod legacy;
mod stream;

static SCAN_ROW_SCHEMA_ARROW: LazyLock<ArrowSchemaRef> =
    LazyLock::new(|| Arc::new(scan_row_schema().as_ref().try_into_arrow().unwrap()));

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

pub(crate) struct LogStoreHandler {
    log_store: Arc<dyn LogStore>,
    operation_id: Option<Uuid>,
}

impl LogStoreHandler {
    pub fn new(log_store: Arc<dyn LogStore>, operation_id: Option<Uuid>) -> Self {
        Self {
            log_store,
            operation_id,
        }
    }

    pub(crate) fn log_store(&self) -> &Arc<dyn LogStore> {
        &self.log_store
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.log_store.object_store(self.operation_id)
    }

    pub(crate) fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.log_store.root_object_store(self.operation_id)
    }

    pub(crate) fn engine(&self) -> Arc<dyn Engine> {
        self.log_store.engine(self.operation_id)
    }

    pub(crate) fn table_url(&self) -> DeltaResult<Url> {
        if let Some(op_id) = self.operation_id {
            #[allow(deprecated)]
            self.log_store
                .transaction_url(op_id, &self.log_store.table_root_url())
        } else {
            Ok(self.log_store.table_root_url())
        }
    }
}

/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
#[async_trait::async_trait]
pub trait Snapshot: Send + Sync + 'static {
    /// Version of this `Snapshot` in the table.
    fn version(&self) -> Version;

    /// Table [`Schema`] at this `Snapshot`s version.
    fn schema(&self) -> ArrowSchemaRef;

    /// Get the [`TableProperties`] for this [`Snapshot`].
    fn table_properties(&self) -> &TableProperties;

    fn logical_file_schema(&self) -> ArrowSchemaRef {
        SCAN_ROW_SCHEMA_ARROW.clone()
    }

    /// Get all logical files present in the current snapshot.
    ///
    /// # Parameters
    /// - `predicate`: An optional predicate to filter the files based on file statistics.
    ///
    /// # Returns
    /// A stream of [`RecordBatch`]es, where each batch contains logical file data.
    fn logical_files(
        &self,
        log_store: &LogStoreHandler,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream;

    fn logical_files_view(
        &self,
        log_store: &LogStoreHandler,
        predicate: Option<PredicateRef>,
    ) -> SendableViewStream {
        self.logical_files(log_store, predicate)
            .map_ok(|files| {
                futures::stream::iter((0..files.num_rows()).into_iter().map(move |i| {
                    Ok(LogicalFileView {
                        files: files.clone(),
                        index: i,
                    })
                }))
            })
            .try_flatten()
            .boxed()
    }

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
    async fn application_transaction_version(
        &self,
        log_store: &LogStoreHandler,
        app_id: String,
    ) -> DeltaResult<Option<i64>>;

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
    async fn commit_infos(
        &self,
        log_store: &LogStoreHandler,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Vec<(Version, CommitInfo)>>;

    /// Update the snapshot to a specific version.
    ///
    /// The target version must be greater then the current version of the snapshot.
    ///
    /// # Parameters
    /// - `target_version`: The version to update the snapshot to. Defaults to latest.
    ///
    /// # Returns
    /// A boolean indicating if the snapshot was updated.
    async fn update(
        &mut self,
        log_store: &LogStoreHandler,
        target_version: Option<Version>,
    ) -> DeltaResult<bool>;
}

#[async_trait::async_trait]
impl<T: Snapshot> Snapshot for Box<T> {
    fn version(&self) -> Version {
        self.as_ref().version()
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.as_ref().schema()
    }

    fn table_properties(&self) -> &TableProperties {
        self.as_ref().table_properties()
    }

    fn logical_files(
        &self,
        log_store: &LogStoreHandler,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        self.as_ref().logical_files(log_store, predicate)
    }

    async fn application_transaction_version(
        &self,
        log_store: &LogStoreHandler,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        self.as_ref()
            .application_transaction_version(log_store, app_id)
            .await
    }

    async fn commit_infos(
        &self,
        log_store: &LogStoreHandler,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Vec<(Version, CommitInfo)>> {
        self.as_ref()
            .commit_infos(log_store, start_version, limit)
            .await
    }

    async fn update(
        &mut self,
        log_store: &LogStoreHandler,
        target_version: Option<Version>,
    ) -> DeltaResult<bool> {
        self.as_mut().update(log_store, target_version).await
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use crate::test_utils::*;
    use delta_kernel::Table;

    use super::*;

    fn get_lazy(
        table: TestTables,
        version: Option<Version>,
    ) -> TestResult<Pin<Box<dyn Future<Output = TestResult<(Box<dyn Snapshot>, LogStoreHandler)>>>>>
    {
        let log_store = table.table_builder().build_storage()?;
        let log_store = LogStoreHandler::new(log_store, None);
        Ok(Box::pin(async move {
            Ok((
                Box::new(LazySnapshot::try_new(&log_store, version).await?) as Box<dyn Snapshot>,
                log_store,
            ))
        }))
    }

    fn get_eager(
        table: TestTables,
        version: Option<Version>,
    ) -> TestResult<Pin<Box<dyn Future<Output = TestResult<(Box<dyn Snapshot>, LogStoreHandler)>>>>>
    {
        let log_store = table.table_builder().build_storage()?;
        let log_store = LogStoreHandler::new(log_store, None);
        let config = Default::default();
        Ok(Box::pin(async move {
            Ok((
                Box::new(EagerSnapshot::try_new(&log_store, config, version, None).await?)
                    as Box<dyn Snapshot>,
                log_store,
            ))
        }))
    }

    #[tokio::test]
    async fn test_snapshots() -> TestResult {
        test_snapshot(get_lazy).await?;
        test_snapshot(get_eager).await?;

        Ok(())
    }

    // NOTE: test needs to be async, so that we can pick up the runtime from the context
    async fn test_snapshot<F>(get_snapshot: F) -> TestResult<()>
    where
        F: Fn(
            TestTables,
            Option<Version>,
        ) -> TestResult<
            Pin<Box<dyn Future<Output = TestResult<(Box<dyn Snapshot>, LogStoreHandler)>>>>,
        >,
    {
        for version in 0..=12 {
            let (snapshot, log_store) =
                get_snapshot(TestTables::Checkpoints, Some(version))?.await?;
            assert_eq!(snapshot.version(), version);

            test_commit_infos(&log_store, snapshot.as_ref()).await?;
            test_logical_files(&log_store, snapshot.as_ref()).await?;
            test_logical_files_view(&log_store, snapshot.as_ref()).await?;
        }

        let (mut snapshot, log_store) = get_snapshot(TestTables::Checkpoints, Some(0))?.await?;
        for version in 1..=12 {
            snapshot.update(&log_store, Some(version)).await?;
            assert_eq!(snapshot.version(), version);

            test_commit_infos(&log_store, snapshot.as_ref()).await?;
            test_logical_files(&log_store, snapshot.as_ref()).await?;
            test_logical_files_view(&log_store, snapshot.as_ref()).await?;
        }

        Ok(())
    }

    async fn test_logical_files(
        log_store: &LogStoreHandler,
        snapshot: &dyn Snapshot,
    ) -> TestResult<()> {
        let logical_files = snapshot
            .logical_files(log_store, None)
            .try_collect::<Vec<_>>()
            .await?;
        let num_files = logical_files
            .iter()
            .map(|b| b.num_rows() as i64)
            .sum::<i64>();
        assert_eq!((num_files as u64), snapshot.version());
        Ok(())
    }

    async fn test_logical_files_view(
        log_store: &LogStoreHandler,
        snapshot: &dyn Snapshot,
    ) -> TestResult<()> {
        let num_files_view = snapshot
            .logical_files_view(log_store, None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .count() as u64;
        assert_eq!(num_files_view, snapshot.version());
        Ok(())
    }

    async fn test_commit_infos(
        log_store: &LogStoreHandler,
        snapshot: &dyn Snapshot,
    ) -> TestResult<()> {
        let commit_infos = snapshot.commit_infos(log_store, None, Some(100)).await?;
        assert_eq!((commit_infos.len() as u64), snapshot.version() + 1);
        assert_eq!(commit_infos.first().unwrap().0, snapshot.version());
        Ok(())
    }
}
