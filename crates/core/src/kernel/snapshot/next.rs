use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::{concat_batches, filter_record_batch};
use arrow_arith::boolean::{and, is_null, not};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, BooleanArray, RecordBatch};
use chrono::{DateTime, Utc};
use delta_kernel::actions::set_transaction::{SetTransactionMap, SetTransactionScanner};
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
use delta_kernel::scan::log_replay::scan_action_iter;
use delta_kernel::scan::{scan_row_schema, PhysicalPredicate};
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot as SnapshotInner;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{DeltaResult as KernelResult, Engine, EngineData, Expression, Table, Version};
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use tracing::warn;
use url::Url;

use crate::kernel::scalars::ScalarExt;
use crate::kernel::ActionType;
use crate::storage::cache::CommitCacheObjectStore;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

type ReplayIter = Box<dyn Iterator<Item = KernelResult<(Box<dyn EngineData>, Vec<bool>)>>>;

type LocalFileSystem = CommitCacheObjectStore;

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
        version: Option<i64>,
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

        let snapshot = table.snapshot(engine.as_ref(), version.map(|v| v as u64))?;
        Ok(Self::new(Arc::new(snapshot), engine))
    }

    pub(crate) fn engine_ref(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    pub fn table_root(&self) -> &Url {
        &self.inner.table_root()
    }

    pub fn version(&self) -> u64 {
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

    fn log_data(
        &self,
        types: &[ActionType],
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(RecordBatch, bool)>>> {
        let field_names = types
            .iter()
            .filter_map(|t| t.field_name().ok())
            .collect::<Vec<_>>();
        if field_names.len() != types.len() {
            warn!("skipping unsupported action types");
        }
        let log_schema = get_log_schema().project(&field_names)?;
        Ok(self
            .inner
            ._log_segment()
            .replay(
                self.engine.as_ref(),
                log_schema.clone(),
                log_schema.clone(),
                None,
            )?
            .map_ok(|(d, flag)| {
                Ok((
                    RecordBatch::from(ArrowEngineData::try_from_engine_data(d)?),
                    flag,
                ))
            })
            .flatten())
    }
}

#[derive(Clone)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    files: RecordBatch,
    actions: Option<RecordBatch>,
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
        version: Option<i64>,
        tracked_actions: HashSet<ActionType>,
        predicate: Option<Expression>,
    ) -> DeltaResult<Self> {
        let mut replay_actions = Vec::new();
        if config.require_files {
            replay_actions.push(ActionType::Add);
            replay_actions.push(ActionType::Remove);
        }
        replay_actions.extend(tracked_actions.into_iter().filter(|it| {
            !config.require_files || (it != &ActionType::Add && it != &ActionType::Remove)
        }));

        let snapshot = Snapshot::try_new(Table::try_from_uri(table_root)?, store, version).await?;

        let mut replay_data = Vec::new();
        let mut action_data = Vec::new();
        for slice in snapshot.log_data(&replay_actions)? {
            let (batch, flag) = slice?;

            let action_projection = replay_actions
                .iter()
                .filter_map(|t| {
                    (t != &ActionType::Add && t != &ActionType::Remove)
                        .then_some(
                            t.field_name()
                                .ok()
                                .and_then(|n| batch.schema_ref().index_of(n).ok()),
                        )
                        .flatten()
                })
                .collect_vec();

            if !action_projection.is_empty() {
                action_data.push(batch.project(&action_projection)?);
            }

            if config.require_files {
                let file_data = batch.project(&[0, 1])?;
                let file_data = filter_record_batch(
                    &file_data,
                    &not(&and(
                        &is_null(batch.column(0))?,
                        &is_null(batch.column(1))?,
                    )?)?,
                )?;
                replay_data.push(Ok((
                    Box::new(ArrowEngineData::from(file_data)) as Box<dyn EngineData>,
                    flag,
                )));
            }
        }

        let files_schema = Arc::new(get_log_add_schema().as_ref().try_into()?);
        let scan_schema = Arc::new((&scan_row_schema()).try_into()?);

        let files = if !replay_data.is_empty() {
            let (engine, action_iter) = (snapshot.engine_ref().as_ref(), replay_data.into_iter());
            let physical_predicate =
                predicate.and_then(|p| PhysicalPredicate::try_new(&p, snapshot.schema()).ok());

            let it: ReplayIter = match physical_predicate {
                Some(PhysicalPredicate::StaticSkipAll) => Box::new(std::iter::empty()),
                Some(PhysicalPredicate::Some(p, s)) => {
                    Box::new(scan_action_iter(engine, action_iter, Some((p, s))))
                }
                None | Some(PhysicalPredicate::None) => {
                    Box::new(scan_action_iter(engine, action_iter, None))
                }
            };

            let mut filtered = Vec::new();
            for res in it {
                let (batch, selection) = res?;
                let predicate = BooleanArray::from(selection);
                let data: RecordBatch = ArrowEngineData::try_from_engine_data(batch)?.into();
                filtered.push(filter_record_batch(&data, &predicate)?);
            }
            concat_batches(&scan_schema, &filtered)?
        } else {
            RecordBatch::new_empty(scan_schema.clone())
        };

        let actions = (!action_data.is_empty())
            .then(|| concat_batches(&files_schema, &action_data).ok())
            .flatten();

        Ok(Self {
            snapshot,
            files,
            actions,
        })
    }

    pub fn version(&self) -> u64 {
        self.snapshot.version()
    }

    pub fn schema(&self) -> &Schema {
        self.snapshot.schema()
    }

    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    pub fn metadata(&self) -> &delta_kernel::actions::Metadata {
        self.snapshot.metadata()
    }

    pub fn table_properties(&self) -> &TableProperties {
        &self.snapshot.table_properties()
    }

    pub fn files(&self) -> impl Iterator<Item = LogicalFileView> {
        LogicalFileView {
            files: self.files.clone(),
            index: 0,
        }
    }

    /// Get the number of files in the current snapshot
    pub fn files_count(&self) -> usize {
        self.files.num_rows()
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

    use arrow_cast::pretty::print_batches;
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
        // some comment
        let mut dat_dir = get_dat_dir();
        dat_dir.push("multi_partitioned");
        let dat_info: TestCaseInfo = read_dat_case(dat_dir)?;
        let table_info = dat_info.table_summary()?;

        let table = Table::try_from_uri(dat_info.table_root()?)?;

        let snapshot = EagerSnapshot::try_new_with_actions(
            table.location(),
            Arc::new(object_store::local::LocalFileSystem::default()),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await?;

        assert_eq!(snapshot.version(), table_info.version);
        assert_eq!(
            snapshot.protocol().min_reader_version(),
            table_info.min_reader_version
        );

        print_batches(&[snapshot.files])?;

        Ok(())
    }
}
