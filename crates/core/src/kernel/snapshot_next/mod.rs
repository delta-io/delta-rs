//! Snapshot of a Delta Table at a specific version.

use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch, StructArray};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use delta_kernel::actions::visitors::SetTransactionMap;
use delta_kernel::actions::{
    get_log_add_schema, get_log_schema, Metadata, Protocol, SetTransaction, ADD_NAME, REMOVE_NAME,
};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::apply_schema;
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::log_replay::scan_action_iter;
use delta_kernel::schema::{DataType, Schema};
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{EngineData, ExpressionRef, Version};
use iterators::{AddView, AddViewIterator};
use itertools::Itertools;
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
    fn schema(&self) -> &Schema;

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

    /// Get all currently active files in the table.
    ///
    /// # Parameters
    /// - `predicate`: An optional predicate to filter the files based on file statistics.
    ///
    /// # Returns
    /// An iterator of [`RecordBatch`]es, where each batch contains add action data.
    fn files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>>;

    fn logical_files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>>;

    fn files_view(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<AddView>>>> {
        Ok(Box::new(AddViewIterator::new(self.files(predicate)?)))
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

    fn schema(&self) -> &Schema {
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
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        self.as_ref().logical_files(predicate)
    }

    fn files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        self.as_ref().files(predicate)
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

fn replay_file_actions(
    snapshot: &LazySnapshot,
    predicate: impl Into<Option<ExpressionRef>>,
) -> DeltaResult<RecordBatch> {
    let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
    let checkpoint_read_schema = get_log_add_schema().clone();

    let curr_data = snapshot
        .inner
        ._log_segment()
        .replay(
            snapshot.engine_ref().as_ref(),
            commit_read_schema.clone(),
            checkpoint_read_schema.clone(),
            None,
        )?
        .map_ok(
            |(data, flag)| -> Result<(RecordBatch, bool), delta_kernel::Error> {
                Ok((ArrowEngineData::try_from_engine_data(data)?.into(), flag))
            },
        )
        .flatten()
        .collect::<Result<Vec<_>, _>>()?;

    scan_as_log_data(snapshot, curr_data, predicate)
}

// helper function to replay log data as stored using kernel log replay.
// The kernel replay usually emits a tuple of (data, selection) where data is the
// data is a re-ordered subset of the full data in the log which is relevant to the
// engine. this function leverages the replay, but applies the selection to the
// original data to get the final data.
fn scan_as_log_data(
    snapshot: &LazySnapshot,
    curr_data: impl IntoIterator<Item = (RecordBatch, bool)>,
    predicate: impl Into<Option<ExpressionRef>>,
) -> Result<RecordBatch, DeltaTableError> {
    let curr_data = curr_data.into_iter().collect::<Vec<_>>();
    let scan_iter = curr_data.clone().into_iter().map(|(data, flag)| {
        Ok((
            Box::new(ArrowEngineData::new(data.clone())) as Box<dyn EngineData>,
            flag,
        ))
    });

    let scan = snapshot
        .inner
        .as_ref()
        .clone()
        .into_scan_builder()
        .with_predicate(predicate)
        .build()?;

    let res = scan_action_iter(
        snapshot.engine_ref().as_ref(),
        scan_iter,
        scan.physical_predicate()
            .map(|p| (p, scan.schema().clone())),
    )
    .map(|res| {
        res.and_then(|(d, selection)| {
            Ok((
                RecordBatch::from(ArrowEngineData::try_from_engine_data(d)?),
                selection,
            ))
        })
    })
    .zip(curr_data.into_iter())
    .map(|(scan_res, (data_raw, _))| match scan_res {
        Ok((_, selection)) => {
            let data = filter_record_batch(&data_raw, &BooleanArray::from(selection))?;
            let dt: DataType = get_log_add_schema().as_ref().clone().into();
            let data: StructArray = data.project(&[0])?.into();
            apply_schema(&data, &dt)
        }
        Err(e) => Err(e),
    })
    .collect::<Result<Vec<_>, _>>()?;

    let schema_ref = Arc::new(get_log_add_schema().as_ref().try_into()?);
    Ok(concat_batches(&schema_ref, &res)?)
}

#[cfg(test)]
mod tests {
    use std::{future::Future, path::PathBuf, pin::Pin};

    use delta_kernel::Table;
    use deltalake_test::utils::*;

    use super::*;

    pub(super) fn get_dat_dir() -> PathBuf {
        let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut rep_root = d
            .parent()
            .and_then(|p| p.parent())
            .expect("valid directory")
            .to_path_buf();
        rep_root.push("dat/out/reader_tests/generated");
        rep_root
    }

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

            test_files(snapshot.as_ref())?;
            test_files_view(snapshot.as_ref())?;
            test_commit_infos(snapshot.as_ref())?;
        }

        let mut snapshot = get_snapshot(ctx, TestTables::Checkpoints, Some(0))?.await?;
        for version in 1..=12 {
            snapshot.update(Some(version))?;
            assert_eq!(snapshot.version(), version);

            test_files(snapshot.as_ref())?;
            test_files_view(snapshot.as_ref())?;
            test_commit_infos(snapshot.as_ref())?;
        }

        Ok(())
    }

    fn test_files(snapshot: &dyn Snapshot) -> TestResult<()> {
        let batches = snapshot.files(None)?.collect::<Result<Vec<_>, _>>()?;
        let num_files = batches.iter().map(|b| b.num_rows() as i64).sum::<i64>();
        assert_eq!((num_files as u64), snapshot.version());
        Ok(())
    }

    fn test_commit_infos(snapshot: &dyn Snapshot) -> TestResult<()> {
        let commit_infos = snapshot.commit_infos(None, Some(100))?.collect::<Vec<_>>();
        assert_eq!((commit_infos.len() as u64), snapshot.version() + 1);
        assert_eq!(commit_infos.first().unwrap().0, snapshot.version());
        Ok(())
    }

    fn test_files_view(snapshot: &dyn Snapshot) -> TestResult<()> {
        let num_files_view = snapshot
            .files_view(None)?
            .map(|f| f.unwrap().path().to_string())
            .count() as u64;
        assert_eq!(num_files_view, snapshot.version());
        Ok(())
    }
}
