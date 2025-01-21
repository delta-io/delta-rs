//! Snapshot of a Delta Table at a specific version.
//!
use std::io::{BufRead, BufReader, Cursor};
use std::sync::{Arc, LazyLock};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_select::filter::filter_record_batch;
use delta_kernel::actions::set_transaction::{SetTransactionMap, SetTransactionScanner};
use delta_kernel::actions::{get_log_schema, REMOVE_NAME};
use delta_kernel::actions::{Metadata, Protocol, SetTransaction};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::evaluate_expression;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::log_segment::LogSegment;
use delta_kernel::schema::{DataType, Schema};
use delta_kernel::snapshot::Snapshot as SnapshotInner;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{Engine, Expression, ExpressionHandler, ExpressionRef, Table, Version};
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use super::cache::CommitCacheObjectStore;
use super::{replay_file_actions, Snapshot};
use crate::kernel::{Action, CommitInfo, ARROW_HANDLER};
use crate::{DeltaResult, DeltaTableError};

// TODO: avoid repetitive parsing of json stats

#[derive(Clone)]
pub struct LazySnapshot {
    pub(super) inner: Arc<SnapshotInner>,
    engine: Arc<dyn Engine>,
}

impl Snapshot for LazySnapshot {
    fn table_root(&self) -> &Url {
        self.inner.table_root()
    }

    fn version(&self) -> Version {
        self.inner.version()
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn protocol(&self) -> &Protocol {
        self.inner.protocol()
    }

    fn metadata(&self) -> &Metadata {
        self.inner.metadata()
    }

    fn table_properties(&self) -> &TableProperties {
        self.inner.table_properties()
    }

    fn logical_files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        let scan = self
            .inner
            .clone()
            .scan_builder()
            .with_predicate(predicate)
            .build()?;
        Ok(Box::new(
            scan.scan_data(self.engine.as_ref())?
                .map(|res| {
                    res.and_then(|(data, predicate)| {
                        let batch: RecordBatch =
                            ArrowEngineData::try_from_engine_data(data)?.into();
                        Ok(filter_record_batch(&batch, &BooleanArray::from(predicate))?)
                    })
                })
                .map(|batch| batch.map_err(|e| e.into())),
        ))
    }

    fn files(
        &self,
        predicate: Option<Arc<Expression>>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        Ok(Box::new(std::iter::once(replay_file_actions(
            &self, predicate,
        ))))
    }

    fn tombstones(&self) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        static META_PREDICATE: LazyLock<ExpressionRef> =
            LazyLock::new(|| Arc::new(Expression::column([REMOVE_NAME, "path"]).is_not_null()));
        let read_schema = get_log_schema().project(&[REMOVE_NAME])?;
        Ok(Box::new(
            self.inner
                ._log_segment()
                .replay(
                    self.engine.as_ref(),
                    read_schema.clone(),
                    read_schema,
                    Some(META_PREDICATE.clone()),
                )?
                .map_ok(|(d, _)| {
                    let batch = RecordBatch::from(ArrowEngineData::try_from_engine_data(d)?);
                    let selection = evaluate_expression(
                        META_PREDICATE.as_ref(),
                        &batch,
                        Some(&DataType::BOOLEAN),
                    )?;
                    let filter = selection
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            DeltaTableError::generic("failed to downcast to BooleanArray")
                        })?;
                    Ok(filter_record_batch(&batch, filter)?)
                })
                .flatten(),
        ))
    }

    fn application_transactions(&self) -> DeltaResult<SetTransactionMap> {
        let scanner = SetTransactionScanner::new(self.inner.clone());
        Ok(scanner.application_transactions(self.engine.as_ref())?)
    }

    fn application_transaction(&self, app_id: &str) -> DeltaResult<Option<SetTransaction>> {
        let scanner = SetTransactionScanner::new(self.inner.clone());
        Ok(scanner.application_transaction(self.engine.as_ref(), app_id)?)
    }

    fn commit_infos(
        &self,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Box<dyn Iterator<Item = (Version, CommitInfo)>>> {
        // let start_version = start_version.into();
        let fs_client = self.engine.get_file_system_client();
        let end_version = start_version.unwrap_or_else(|| self.version());
        let start_version = limit
            .and_then(|limit| {
                if limit == 0 {
                    Some(end_version)
                } else {
                    Some(end_version.saturating_sub(limit as u64 - 1))
                }
            })
            .unwrap_or(0);

        let log_root = self.inner.table_root().join("_delta_log").unwrap();
        let mut log_segment = LogSegment::for_table_changes(
            fs_client.as_ref(),
            log_root,
            start_version,
            end_version,
        )?;
        log_segment.ascending_commit_files.reverse();
        let files = log_segment
            .ascending_commit_files
            .iter()
            .map(|commit_file| (commit_file.location.location.clone(), None))
            .collect_vec();

        Ok(Box::new(
            fs_client
                .read_files(files)?
                .zip(log_segment.ascending_commit_files.into_iter())
                .filter_map(|(data, path)| {
                    data.ok().and_then(|d| {
                        let reader = BufReader::new(Cursor::new(d));
                        for line in reader.lines() {
                            match line.and_then(|l| Ok(serde_json::from_str::<Action>(&l)?)) {
                                Ok(Action::CommitInfo(commit_info)) => {
                                    return Some((path.version, commit_info))
                                }
                                Err(_) => return None,
                                _ => continue,
                            };
                        }
                        None
                    })
                }),
        ))
    }

    fn update(&mut self, target_version: Option<Version>) -> DeltaResult<bool> {
        let mut snapshot = self.inner.as_ref().clone();
        let did_update = snapshot.update(target_version, self.engine_ref().as_ref())?;
        self.inner = Arc::new(snapshot);
        Ok(did_update)
    }
}

impl LazySnapshot {
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

    /// A shared reference to the engine used for interacting with the Delta Table.
    pub(crate) fn engine_ref(&self) -> &Arc<dyn Engine> {
        &self.engine
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
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::StructType;
    use deltalake_test::utils::*;
    use deltalake_test::TestResult;

    use super::*;

    async fn load_snapshot() -> TestResult<()> {
        let ctx = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;
        ctx.load_table(TestTables::Simple).await?;

        let store = ctx
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store(None);
        let table = Table::try_from_uri("memory:///")?;
        let snapshot = LazySnapshot::try_new(table, store, None).await?;

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema(), &expected);

        let infos = snapshot.commit_infos(None, None)?.collect_vec();
        assert_eq!(infos.len(), 5);

        let tombstones: Vec<_> = snapshot.tombstones()?.try_collect()?;
        let num_tombstones = tombstones.iter().map(|b| b.num_rows() as i64).sum::<i64>();
        assert_eq!(num_tombstones, 31);

        let expected = vec![
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
        ];
        let file_names: Vec<_> = snapshot
            .logical_files_view(None)?
            .map_ok(|f| f.path().to_owned())
            .try_collect()?;
        assert_eq!(file_names, expected);

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
}
