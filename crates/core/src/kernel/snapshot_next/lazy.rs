//! Snapshot of a Delta Table at a specific version.
//!
use std::io::{BufRead, BufReader, Cursor};
use std::sync::{Arc, LazyLock};

use arrow::array::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_select::filter::filter_record_batch;
use delta_kernel::actions::{get_log_schema, REMOVE_NAME, SIDECAR_NAME};
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::log_segment::LogSegment;
use delta_kernel::schema::{DataType, Schema};
use delta_kernel::snapshot::Snapshot as SnapshotInner;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{
    Engine, EvaluationHandler, Expression, ExpressionEvaluator, ExpressionRef, PredicateRef, Table,
    Version,
};
use futures::{StreamExt, TryFutureExt};
use itertools::Itertools;
use object_store::ObjectStore;
use tokio::task::spawn_blocking;
use url::Url;
use uuid::Uuid;

use super::arrow_ext::{ExpressionEvaluatorExt, ScanExt};
use super::stream::{RecordBatchReceiverStreamBuilder, SendableRBStream};
use super::{LogStoreHandler, Snapshot};
use crate::kernel::{Action, CommitInfo, ARROW_HANDLER};
use crate::logstore::{LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableError};

// TODO: avoid repetitive parsing of json stats

#[derive(Clone)]
pub struct LazySnapshot {
    pub(super) inner: Arc<SnapshotInner>,
    arrow_schema: ArrowSchemaRef,
}

#[async_trait::async_trait]
impl Snapshot for LazySnapshot {
    fn version(&self) -> Version {
        self.inner.version()
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_properties(&self) -> &TableProperties {
        self.inner.table_properties()
    }

    fn logical_files(
        &self,
        log_store: &LogStoreHandler,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        let scan = match self
            .inner
            .clone()
            .scan_builder()
            .with_predicate(predicate)
            .build()
        {
            Ok(scan) => scan,
            Err(err) => {
                return Box::pin(futures::stream::once(async {
                    Err(DeltaTableError::KernelError(err))
                }))
            }
        };

        // TODO: which capacity to choose?
        let mut builder = RecordBatchReceiverStreamBuilder::new(100);
        let tx = builder.tx();
        let engine = log_store.engine();

        builder.spawn_blocking(move || {
            let mut scan_iter = scan.scan_metadata_arrow(engine.as_ref())?;
            for res in scan_iter {
                let batch = res?.scan_files;
                if tx.blocking_send(Ok(batch)).is_err() {
                    break;
                }
            }
            Ok(())
        });

        builder.build()
    }

    async fn application_transaction_version(
        &self,
        log_store: &LogStoreHandler,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        let engine = log_store.engine();
        let inner = self.inner.clone();
        let version = spawn_blocking(move || inner.get_app_id_version(&app_id, engine.as_ref()))
            .await
            .map_err(|e| DeltaTableError::GenericError { source: e.into() })??;
        Ok(version)
    }

    async fn commit_infos(
        &self,
        log_store: &LogStoreHandler,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Vec<(Version, CommitInfo)>> {
        // let start_version = start_version.into();
        let storage = log_store.engine().storage_handler();
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

        let log_root = log_store.table_url()?.join("_delta_log/").unwrap();

        let fs_client = storage.clone();
        let mut log_segment = spawn_blocking(move || {
            LogSegment::for_table_changes(fs_client.as_ref(), log_root, start_version, end_version)
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        log_segment.ascending_commit_files.reverse();
        let files = log_segment
            .ascending_commit_files
            .iter()
            .map(|commit_file| (commit_file.location.location.clone(), None))
            .collect_vec();

        let res = spawn_blocking(move || {
            Ok::<_, DeltaTableError>(
                storage
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
                    })
                    .collect_vec(),
            )
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        Ok(res)
    }

    async fn update(
        &mut self,
        log_store: &LogStoreHandler,
        target_version: Option<Version>,
    ) -> DeltaResult<bool> {
        let current = self.inner.clone();
        let engine = log_store.engine();

        let snapshot = spawn_blocking(move || {
            SnapshotInner::try_new_from(current, engine.as_ref(), target_version)
        })
        .await
        .map_err(|e| DeltaTableError::GenericError { source: e.into() })??;

        let did_update = snapshot.version() != self.inner.version();
        self.inner = snapshot;

        Ok(did_update)
    }
}

impl LazySnapshot {
    /// Create a new [`Snapshot`] instance.
    pub(crate) fn new(inner: Arc<SnapshotInner>, arrow_schema: ArrowSchemaRef) -> Self {
        Self {
            inner,
            arrow_schema,
        }
    }

    /// Create a new [`Snapshot`] instance for a table.
    pub(crate) async fn try_new(
        log_store: &LogStoreHandler,
        version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        let version = version.into();

        let table_url = log_store.table_url()?;
        let engine = log_store.engine();

        let table = Table::try_from_uri(table_url)?;
        let task_engine = engine.clone();
        let snapshot = spawn_blocking(move || table.snapshot(task_engine.as_ref(), version))
            .await
            .map_err(|e| DeltaTableError::GenericError { source: e.into() })??;

        let arrow_schema = Arc::new(snapshot.schema().as_ref().try_into_arrow()?);

        Ok(Self::new(Arc::new(snapshot), arrow_schema))
    }

    /// Get the timestamp of the given version in miliscends since epoch.
    ///
    /// Extracts the timestamp from the commit file of the given version
    /// from the current log segment. If the commit file is not part of the
    /// current log segment, `None` is returned.
    pub fn version_timestamp(&self, version: Version) -> Option<i64> {
        self.inner
            .log_segment()
            .ascending_commit_files
            .iter()
            .find(|f| f.version == version)
            .map(|f| f.location.last_modified)
    }

    /// Get the tombstones of the given version.
    ///
    /// Extracts the tombstones from the commit file of the given version
    /// from the current log segment. If the commit file is not part of the
    /// current log segment, `None` is returned.
    pub(crate) async fn tombstones(
        &self,
        log_store: &LogStoreHandler,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        // TODO: spawn blocking
        static META_PREDICATE: LazyLock<ExpressionRef> = LazyLock::new(|| {
            Arc::new(Expression::from_pred(
                Expression::column([REMOVE_NAME, "path"]).is_not_null(),
            ))
        });
        static EVALUATOR: LazyLock<Arc<dyn ExpressionEvaluator>> = LazyLock::new(|| {
            ARROW_HANDLER.new_expression_evaluator(
                get_log_schema().project(&[REMOVE_NAME]).unwrap(),
                META_PREDICATE.as_ref().clone(),
                DataType::BOOLEAN,
            )
        });
        let read_schema = get_log_schema().project(&[REMOVE_NAME])?;
        let read_schema2 = get_log_schema().project(&[REMOVE_NAME, SIDECAR_NAME])?;
        let engine = log_store.engine();
        Ok(Box::new(
            self.inner
                .log_segment()
                .read_actions(engine.as_ref(), read_schema, read_schema2, None)?
                .map_ok(|(data)| {
                    let batch =
                        RecordBatch::from(ArrowEngineData::try_from_engine_data(data.actions)?);
                    let selection = EVALUATOR.evaluate_arrow(batch.clone())?;
                    let filter = selection.column(0).as_boolean_opt().ok_or_else(|| {
                        DeltaTableError::generic("failed to downcast to BooleanArray")
                    })?;
                    Ok(filter_record_batch(&batch, filter)?)
                })
                .flatten(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow_cast::pretty::print_batches;
    use delta_kernel::schema::StructType;
    use futures::TryStreamExt;

    use super::*;
    use crate::test_utils::*;

    async fn load_snapshot() -> TestResult<()> {
        let log_store = TestTables::Simple.table_builder().build_storage()?;
        let log_store = LogStoreHandler::new(log_store, None);
        let snapshot = LazySnapshot::try_new(&log_store, None).await?;

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        let expected_arrow_schema = (&expected).try_into_arrow()?;
        assert_eq!(snapshot.schema().as_ref(), &expected_arrow_schema);

        let infos = snapshot.commit_infos(&log_store, None, None).await?;
        assert_eq!(infos.len(), 5);

        let tombstones: Vec<_> = snapshot.tombstones(&log_store).await?.try_collect()?;
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
            .logical_files_view(&log_store, None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|f| f.path().to_owned())
            .collect();
        assert_eq!(file_names, expected);

        let logical_files: Vec<_> = snapshot
            .logical_files(&log_store, None)
            .try_collect()
            .await?;

        print_batches(&logical_files)?;

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
