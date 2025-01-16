//! Snapshot of a Delta Table at a specific version.
//!
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use arrow::compute::{concat_batches, filter_record_batch};
use arrow_arith::boolean::{and, is_null, not};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_cast::pretty::print_batches;
use chrono::{DateTime, Utc};
use delta_kernel::actions::set_transaction::{SetTransactionMap, SetTransactionScanner};
use delta_kernel::actions::visitors::{
    AddVisitor, CdcVisitor, MetadataVisitor, ProtocolVisitor, RemoveVisitor, SetTransactionVisitor,
};
use delta_kernel::actions::{
    get_log_add_schema, get_log_schema, ADD_NAME, CDC_NAME, METADATA_NAME, PROTOCOL_NAME,
    REMOVE_NAME, SET_TRANSACTION_NAME,
};
use delta_kernel::actions::{Metadata, Protocol, SetTransaction};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::{GetData, RowVisitor, TypedGetData as _};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::log_segment::LogSegment;
use delta_kernel::scan::log_replay::scan_action_iter;
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::{DataType, Schema, StructField, StructType};
use delta_kernel::snapshot::Snapshot as SnapshotInner;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{
    DeltaResult as KernelResult, Engine, EngineData, Expression, ExpressionHandler, ExpressionRef,
    Table, Version,
};
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use tracing::warn;
use url::Url;

use crate::kernel::scalars::ScalarExt;
use crate::kernel::{ActionType, ARROW_HANDLER};
use crate::storage::cache::CommitCacheObjectStore;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

type ReplayIter = Box<dyn Iterator<Item = KernelResult<(Box<dyn EngineData>, Vec<bool>)>>>;

type LocalFileSystem = CommitCacheObjectStore;

#[derive(thiserror::Error, Debug)]
enum SnapshotError {
    #[error("Snapshot not initialized for action type: {0}")]
    MissingData(String),
}

impl SnapshotError {
    fn missing_data(action: ActionType) -> Self {
        Self::MissingData(action.field_name_unckecked().to_string())
    }
}

impl From<SnapshotError> for DeltaTableError {
    fn from(e: SnapshotError) -> Self {
        match &e {
            SnapshotError::MissingData(_) => DeltaTableError::generic(e),
        }
    }
}

impl ActionType {
    pub(self) fn field_name_unckecked(&self) -> &'static str {
        match self {
            Self::Metadata => METADATA_NAME,
            Self::Protocol => PROTOCOL_NAME,
            Self::Remove => REMOVE_NAME,
            Self::Add => ADD_NAME,
            Self::Txn => SET_TRANSACTION_NAME,
            Self::Cdc => CDC_NAME,
            _ => panic!(),
        }
    }

    pub(self) fn field_name(&self) -> DeltaResult<&'static str> {
        let name = match self {
            Self::Metadata => METADATA_NAME,
            Self::Protocol => PROTOCOL_NAME,
            Self::Remove => REMOVE_NAME,
            Self::Add => ADD_NAME,
            Self::Txn => SET_TRANSACTION_NAME,
            Self::Cdc => CDC_NAME,
            _ => {
                return Err(DeltaTableError::generic(format!(
                    "unsupported action type: {self:?}"
                )))
            }
        };
        Ok(name)
    }
}

#[derive(Clone)]
pub struct Snapshot {
    inner: Arc<SnapshotInner>,
    engine: Arc<dyn Engine>,
}

impl Snapshot {
    /// Create a new [`Snapshot`] instance.
    pub fn new(inner: Arc<SnapshotInner>, engine: Arc<dyn Engine>) -> Self {
        Self { inner, engine }
    }

    /// Create a new [`Snapshot`] instance for a table.
    pub async fn try_new(
        table: Table,
        store: Arc<dyn ObjectStore>,
        version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        // TODO: how to deal with the dedicated IO runtime? Would this already be covered by the
        // object store implementation pass to this?
        let table_root = Path::from_url_path(table.location().path())?;
        let store_str = format!("{}", store);
        let is_local = store_str.starts_with("LocalFileSystem");
        let store = Arc::new(CommitCacheObjectStore::new(store));
        let handle = tokio::runtime::Handle::current();
        let engine: Arc<dyn Engine> = match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => Arc::new(DefaultEngine::new_with_opts(
                store,
                table_root,
                Arc::new(TokioMultiThreadExecutor::new(handle)),
                !is_local,
            )),
            tokio::runtime::RuntimeFlavor::CurrentThread => Arc::new(DefaultEngine::new_with_opts(
                store,
                table_root,
                Arc::new(TokioBackgroundExecutor::new()),
                !is_local,
            )),
            _ => return Err(DeltaTableError::generic("unsupported runtime flavor")),
        };

        let snapshot = table.snapshot(engine.as_ref(), version.into())?;
        Ok(Self::new(Arc::new(snapshot), engine))
    }

    pub(crate) fn engine_ref(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    pub fn table_root(&self) -> &Url {
        &self.inner.table_root()
    }

    pub fn version(&self) -> Version {
        self.inner.version()
    }

    pub fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    pub fn protocol(&self) -> &Protocol {
        self.inner.protocol()
    }

    pub fn metadata(&self) -> &Metadata {
        self.inner.metadata()
    }

    pub fn table_properties(&self) -> &TableProperties {
        &self.inner.table_properties()
    }

    /// Get the timestamp of the given version in miliscends since epoch.
    ///
    /// Extracts the timestamp from the commit file of the given version
    /// from the current log segment. If the commit file is not part of the
    /// current log segment, `None` is returned.
    pub fn version_timestamp(&self, version: Version) -> Option<i64> {
        self.inner
            ._log_segment()
            .ascending_commit_files
            .iter()
            .find(|f| f.version == version)
            .map(|f| f.location.last_modified)
    }

    /// read all active files from the log
    pub(crate) fn files(
        &self,
        predicate: Option<Arc<Expression>>,
    ) -> DeltaResult<impl Iterator<Item = Result<RecordBatch, delta_kernel::Error>>> {
        let scan = self
            .inner
            .clone()
            .scan_builder()
            .with_predicate(predicate)
            .build()?;
        Ok(scan.scan_data(self.engine.as_ref())?.map(|res| {
            res.and_then(|(data, mut predicate)| {
                let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
                if predicate.len() < batch.num_rows() {
                    predicate
                        .extend(std::iter::repeat(true).take(batch.num_rows() - predicate.len()));
                }
                Ok(filter_record_batch(&batch, &BooleanArray::from(predicate))?)
            })
        }))
    }

    pub(crate) fn tombstones(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        static META_PREDICATE: LazyLock<Option<ExpressionRef>> = LazyLock::new(|| {
            Some(Arc::new(
                Expression::column([REMOVE_NAME, "path"]).is_not_null(),
            ))
        });
        let read_schema = get_log_schema().project(&[REMOVE_NAME])?;
        Ok(self
            .inner
            ._log_segment()
            .replay(
                self.engine.as_ref(),
                read_schema.clone(),
                read_schema,
                META_PREDICATE.clone(),
            )?
            .map_ok(|(d, _)| Ok(RecordBatch::from(ArrowEngineData::try_from_engine_data(d)?)))
            .flatten())
    }

    /// Scan the Delta Log to obtain the latest transaction for all applications
    ///
    /// This method requires a full scan of the log to find all transactions.
    /// When a specific application id is requested, it is much more efficient to use
    /// [`application_transaction`](Self::application_transaction) instead.
    pub fn application_transactions(&self) -> DeltaResult<SetTransactionMap> {
        let scanner = SetTransactionScanner::new(self.inner.clone());
        Ok(scanner.application_transactions(self.engine.as_ref())?)
    }

    /// Scan the Delta Log for the latest transaction entry for a specific application.
    ///
    /// Initiates a log scan, but terminates as soon as the transaction
    /// for the given application is found.
    pub fn application_transaction(
        &self,
        app_id: impl AsRef<str>,
    ) -> DeltaResult<Option<SetTransaction>> {
        let scanner = SetTransactionScanner::new(self.inner.clone());
        Ok(scanner.application_transaction(self.engine.as_ref(), app_id.as_ref())?)
    }
}

#[derive(Clone)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    files: Option<RecordBatch>,
    predicate: Option<Arc<Expression>>,
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance tracking actions of the given types.
    ///
    /// Only actions supplied by `tracked_actions` will be loaded into memory.
    /// This is useful when only a subset of actions are needed. `Add` and `Remove` actions
    /// are treated specially. I.e. `Add` and `Remove` will be loaded as well.
    pub async fn try_new_with_actions(
        table_root: impl AsRef<str>,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: impl Into<Option<Version>>,
        tracked_actions: HashSet<ActionType>,
        predicate: Option<Arc<Expression>>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(Table::try_from_uri(table_root)?, store, version).await?;
        let files = config
            .require_files
            .then(|| -> DeltaResult<_> { Ok(replay_file_actions(&snapshot)?) })
            .transpose()?;
        Ok(Self {
            snapshot,
            files,
            predicate,
        })
    }

    pub fn version(&self) -> Version {
        self.snapshot.version()
    }

    pub fn schema(&self) -> &Schema {
        self.snapshot.schema()
    }

    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    pub fn table_properties(&self) -> &TableProperties {
        &self.snapshot.table_properties()
    }

    pub fn files(&self) -> DeltaResult<impl Iterator<Item = LogicalFileView>> {
        Ok(LogicalFileView {
            files: self
                .files
                .clone()
                .ok_or_else(|| SnapshotError::missing_data(ActionType::Add))?,
            index: 0,
        })
    }

    /// Get the number of files in the current snapshot
    pub fn files_count(&self) -> DeltaResult<usize> {
        Ok(self
            .files
            .as_ref()
            .map(|f| f.num_rows())
            .ok_or_else(|| SnapshotError::missing_data(ActionType::Add))?)
    }

    pub fn tombstones(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        self.snapshot.tombstones()
    }

    /// Scan the Delta Log to obtain the latest transaction for all applications
    ///
    /// This method requires a full scan of the log to find all transactions.
    /// When a specific application id is requested, it is much more efficient to use
    /// [`application_transaction`](Self::application_transaction) instead.
    pub fn application_transactions(&self) -> DeltaResult<SetTransactionMap> {
        self.snapshot.application_transactions()
    }

    /// Scan the Delta Log for the latest transaction entry for a specific application.
    ///
    /// Initiates a log scan, but terminates as soon as the transaction
    /// for the given application is found.
    pub fn application_transaction(
        &self,
        app_id: impl AsRef<str>,
    ) -> DeltaResult<Option<SetTransaction>> {
        self.snapshot.application_transaction(app_id)
    }

    pub(crate) fn update(&mut self) -> DeltaResult<()> {
        let state = self
            .files
            .as_ref()
            .ok_or(SnapshotError::missing_data(ActionType::Add))?
            .clone();

        let log_root = self.snapshot.table_root().join("_delta_log/").unwrap();
        let fs_client = self.snapshot.engine.get_file_system_client();
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_add_schema().clone();

        let segment =
            LogSegment::for_table_changes(fs_client.as_ref(), log_root, self.version() + 1, None)?;
        let slice_iter = segment
            .replay(
                self.snapshot.engine.as_ref(),
                commit_read_schema,
                checkpoint_read_schema,
                None,
            )?
            .chain(std::iter::once(Ok((
                Box::new(ArrowEngineData::from(state)) as Box<dyn EngineData>,
                false,
            ))));

        let res = scan_action_iter(self.snapshot.engine.as_ref(), slice_iter, None)
            .map(|res| {
                res.and_then(|(d, sel)| {
                    let batch = RecordBatch::from(ArrowEngineData::try_from_engine_data(d)?);
                    Ok(filter_record_batch(&batch, &BooleanArray::from(sel))?)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.files = Some(concat_batches(res[0].schema_ref(), &res)?);

        Ok(())
    }
}

fn replay_file_actions(snapshot: &Snapshot) -> DeltaResult<RecordBatch> {
    let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
    let checkpoint_read_schema = get_log_add_schema().clone();

    let curr_data = snapshot
        .inner
        ._log_segment()
        .replay(
            snapshot.engine.as_ref(),
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

    let scan_iter = curr_data.clone().into_iter().map(|(data, flag)| {
        Ok((
            Box::new(ArrowEngineData::new(data.clone())) as Box<dyn EngineData>,
            flag,
        ))
    });

    let res = scan_action_iter(snapshot.engine.as_ref(), scan_iter, None)
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
                Ok(data.project(&[0])?)
            }
            Err(e) => Err(e),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(concat_batches(res[0].schema_ref(), &res)?)
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

#[derive(Clone)]
pub struct LogicalFileView {
    files: RecordBatch,
    index: usize,
}

impl LogicalFileView {
    /// Path of the file.
    pub fn path(&self) -> &str {
        self.files.column(0).as_string::<i32>().value(self.index)
    }

    /// Size of the file in bytes.
    pub fn size(&self) -> i64 {
        self.files
            .column(1)
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Modification time of the file in milliseconds since epoch.
    pub fn modification_time(&self) -> i64 {
        self.files
            .column(2)
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Datetime of the last modification time of the file.
    pub fn modification_datetime(&self) -> DeltaResult<chrono::DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.modification_time()).ok_or(DeltaTableError::from(
            crate::protocol::ProtocolError::InvalidField(format!(
                "invalid modification_time: {:?}",
                self.modification_time()
            )),
        ))
    }

    pub fn stats(&self) -> Option<&str> {
        let col = self.files.column(3).as_string::<i32>();
        col.is_valid(self.index).then(|| col.value(self.index))
    }

    pub fn partition_values(&self) -> Option<StructData> {
        self.files
            .column_by_name("fileConstantValues")
            .and_then(|col| col.as_struct_opt())
            .and_then(|s| s.column_by_name("partitionValues"))
            .and_then(|arr| {
                arr.is_valid(self.index)
                    .then(|| match Scalar::from_array(arr, self.index) {
                        Some(Scalar::Struct(s)) => Some(s),
                        _ => None,
                    })
                    .flatten()
            })
    }
}

impl Iterator for LogicalFileView {
    type Item = LogicalFileView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.files.num_rows() {
            let file = LogicalFileView {
                files: self.files.clone(),
                index: self.index,
            };
            self.index += 1;
            Some(file)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deltalake_test::acceptance::{read_dat_case, TestCaseInfo};
    use deltalake_test::TestResult;
    use std::path::PathBuf;

    fn get_dat_dir() -> PathBuf {
        let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut rep_root = d
            .parent()
            .and_then(|p| p.parent())
            .expect("valid directory")
            .to_path_buf();
        rep_root.push("dat/out/reader_tests/generated");
        rep_root
    }

    async fn load_snapshot() -> TestResult<()> {
        // some comment
        let mut dat_dir = get_dat_dir();
        dat_dir.push("multi_partitioned");

        let dat_info: TestCaseInfo = read_dat_case(dat_dir)?;
        let table_info = dat_info.table_summary()?;

        let table = Table::try_from_uri(dat_info.table_root()?)?;

        let snapshot = Snapshot::try_new(
            table,
            Arc::new(object_store::local::LocalFileSystem::default()),
            None,
        )
        .await?;

        assert_eq!(snapshot.version(), table_info.version);
        assert_eq!(
            (
                snapshot.protocol().min_reader_version(),
                snapshot.protocol().min_writer_version()
            ),
            (table_info.min_reader_version, table_info.min_writer_version)
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_snapshot_multi() -> TestResult<()> {
        load_snapshot().await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn load_snapshot_current() -> TestResult<()> {
        load_snapshot().await
    }

    #[tokio::test]
    async fn load_eager_snapshot() -> TestResult<()> {
        let mut dat_dir = get_dat_dir();
        dat_dir.push("multi_partitioned");

        let dat_info: TestCaseInfo = read_dat_case(dat_dir)?;
        let table_info = dat_info.table_summary()?;

        let table = Table::try_from_uri(dat_info.table_root()?)?;

        let mut snapshot = EagerSnapshot::try_new_with_actions(
            table.location(),
            Arc::new(object_store::local::LocalFileSystem::default()),
            Default::default(),
            Some(1),
            Default::default(),
            None,
        )
        .await?;

        // assert_eq!(snapshot.version(), table_info.version);
        // assert_eq!(
        //     snapshot.protocol().min_reader_version(),
        //     table_info.min_reader_version
        // );

        snapshot.update()?;

        Ok(())
    }
}
