use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use ::serde::{Deserialize, Serialize};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow_arith::boolean::{and, is_null, not};
use arrow_array::{BooleanArray, RecordBatch};
use delta_kernel::actions::{
    get_log_add_schema, get_log_schema, Add, ADD_NAME, CDC_NAME, METADATA_NAME, PROTOCOL_NAME,
    REMOVE_NAME, SET_TRANSACTION_NAME,
};
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::{GetData, RowVisitor, TypedGetData as _};
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::log_replay::scan_action_iter;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::scan::{scan_row_schema, PhysicalPredicate, ScanBuilder, ScanData};
use delta_kernel::schema::{ColumnNamesAndTypes, DataType, Schema};
use delta_kernel::snapshot::Snapshot as SnapshotInner;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{
    DeltaResult as KernelResult, Engine, EngineData, Error, Expression, Table, Version,
};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use tracing::warn;
use url::Url;

use crate::kernel::ActionType;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

type ReplayIter = Box<dyn Iterator<Item = KernelResult<(Box<dyn EngineData>, Vec<bool>)>>>;

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
    pub fn new(inner: Arc<SnapshotInner>, engine: Arc<dyn Engine>) -> Self {
        Self { inner, engine }
    }

    pub async fn try_new(
        table: Table,
        store: Arc<dyn ObjectStore>,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        // let executor = Arc::new(TokioMultiThreadExecutor::new(
        //     config
        //         .io_runtime
        //         .map(|rt| rt.get_handle())
        //         .unwrap_or(tokio::runtime::Handle::current()),
        // ));
        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        let table_root = Path::from_url_path(table.location().path())?;
        let engine = DefaultEngine::new(store, table_root, executor);
        let snapshot = table.snapshot(&engine, version.map(|v| v as u64))?;
        Ok(Self::new(Arc::new(snapshot), Arc::new(engine)))
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

    pub fn metadata(&self) -> &delta_kernel::actions::Metadata {
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
        // let it = scan_action_iter(
        //     engine,
        //     self.replay_for_scan_data(engine)?,
        //     physical_predicate,
        // );
        // Ok(Some(it).into_iter().flatten())
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

    #[tokio::test(flavor = "multi_thread")]
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
            snapshot.protocol().min_reader_version(),
            table_info.min_reader_version
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
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
