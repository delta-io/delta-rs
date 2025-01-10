use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use ::serde::{Deserialize, Serialize};
use arrow_array::RecordBatch;
use delta_kernel::actions::{
    get_log_schema, ADD_NAME, CDC_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME,
};
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::{GetData, RowVisitor, TypedGetData as _};
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::schema::{ColumnNamesAndTypes, DataType};
use delta_kernel::snapshot::Snapshot as SnapshotInner;
use delta_kernel::{DeltaResult as KernelResult, Engine, Error, Table};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::sync::mpsc::channel;

use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

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
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        let table_root = Path::from_url_path(table.location().path())?;
        let engine = DefaultEngine::new(store, table_root, executor);
        let snapshot = table.snapshot(&engine, None)?;

        Ok(Self::new(Arc::new(snapshot), Arc::new(engine)))
    }

    pub fn protocol(&self) -> &Protocol {
        self.inner.protocol()
    }

    pub fn metadata(&self) -> &delta_kernel::actions::Metadata {
        self.inner.metadata()
    }

    pub(crate) fn replay_log(&self) -> DeltaResult<()> {
        let log_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let actions = self.inner._log_segment().replay(
            self.engine.as_ref(),
            log_schema.clone(),
            log_schema.clone(),
            None,
        )?;

        // let it = scan_action_iter(
        //     engine,
        //     self.replay_for_scan_data(engine)?,
        //     physical_predicate,
        // );
        // Ok(Some(it).into_iter().flatten())

        Ok(())
    }
}

enum Action {
    Metadata(delta_kernel::actions::Metadata),
    Protocol(delta_kernel::actions::Protocol),
    Remove(delta_kernel::actions::Remove),
    Add(delta_kernel::actions::Add),
    SetTransaction(delta_kernel::actions::SetTransaction),
    Cdc(delta_kernel::actions::Cdc),
}

static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| get_log_schema().leaves(None));

struct LogVisitor {
    actions: Vec<(Action, usize)>,
    offsets: HashMap<String, (usize, usize)>,
    previous_rows_seen: usize,
}

impl LogVisitor {
    fn new() -> LogVisitor {
        // Grab the start offset for each top-level column name, then compute the end offset by
        // skipping the rest of the leaves for that column.
        let mut offsets = HashMap::new();
        let mut it = NAMES_AND_TYPES.as_ref().0.iter().enumerate().peekable();
        while let Some((start, col)) = it.next() {
            let mut end = start + 1;
            while it.next_if(|(_, other)| col[0] == other[0]).is_some() {
                end += 1;
            }
            offsets.insert(col[0].clone(), (start, end));
        }
        LogVisitor {
            actions: vec![],
            offsets,
            previous_rows_seen: 0,
        }
    }
}

impl RowVisitor for LogVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        todo!()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        todo!()
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

    #[tokio::test(flavor = "multi_thread")]
    async fn load_snapshot() -> TestResult<()> {
        // some comment
        let mut dat_dir = get_dat_dir();
        dat_dir.push("basic_append");
        let dat_info: TestCaseInfo = read_dat_case(dat_dir)?;
        let table_info = dat_info.table_summary()?;

        let table = Table::try_from_uri(dat_info.table_root()?)?;

        let snapshot = Snapshot::try_new(
            table,
            Arc::new(object_store::local::LocalFileSystem::default()),
            Default::default(),
            None,
        )
        .await?;

        assert_eq!(
            snapshot.protocol().min_reader_version(),
            table_info.min_reader_version
        );

        // let table_root = object_store::path::Path::new("s3://delta-rs/test");
        // let store = object_store::ObjectStore::new(&table_root).unwrap();
        // let table = delta::DeltaTable::load(&store, &table_root).await.unwrap();
        // let snapshot = delta::Snapshot::try_new(table_root, table, store, Default::default(), None)
        //     .await
        //     .unwrap();
        // snapshot.replay_log().unwrap();
        Ok(())
    }
}
