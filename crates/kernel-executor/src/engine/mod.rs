//! [`DataFusionEngine`]: a kernel [`Engine`] whose handlers all route through
//! [`DataFusionPlanExecutor`].
//!
//! This is the layout of the kernel's `PlanBasedEngine`: each handler turns its call into an
//! [`Operation`] and hands it to the executor, so the executor is the one surface every read goes
//! through.
//!
//! `read_json_files` and `read_parquet_files` ignore their `predicate` argument. There is no plan
//! shape yet that puts a `Filter` node over a scan node, so the predicate has nowhere to go; the
//! kernel's own plan-based handlers ignore it for the same reason.
//!
//! [`Operation`]: delta_kernel::plans::Operation

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::TaskContext;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::plans::PlanExecutor;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};
use tokio::runtime::Handle;

use self::handlers::{PlanBasedJsonHandler, PlanBasedParquetHandler, PlanBasedStorageHandler};
use crate::DataFusionPlanExecutor;

mod handlers;

/// A kernel [`Engine`] backed by DataFusion, with every handler routed through the executor.
///
/// Build one per DataFusion session, the same way as [`DataFusionPlanExecutor`].
#[derive(Debug, Clone)]
pub struct DataFusionEngine {
    executor: Arc<DataFusionPlanExecutor>,
    evaluation: Arc<ArrowEvaluationHandler>,
    storage: Arc<PlanBasedStorageHandler>,
    json: Arc<PlanBasedJsonHandler>,
    parquet: Arc<PlanBasedParquetHandler>,
}

impl DataFusionEngine {
    /// Creates an engine from a DataFusion [`Session`], reusing its task context and the current
    /// tokio runtime handle.
    pub fn new_from_session(session: &dyn Session) -> Arc<Self> {
        Self::new(session.task_ctx(), Handle::current()).into()
    }

    /// Creates an engine from a [`TaskContext`], using the current tokio runtime handle. Useful
    /// inside physical operators where only the task context is available.
    pub fn new_from_context(ctx: Arc<TaskContext>) -> Arc<Self> {
        Self::new(ctx, Handle::current()).into()
    }

    /// Creates an engine from an explicit [`TaskContext`] and tokio runtime [`Handle`].
    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        Self::from_executor(Arc::new(DataFusionPlanExecutor::new(ctx, handle)))
    }

    /// Creates an engine over an existing executor.
    pub fn from_executor(executor: Arc<DataFusionPlanExecutor>) -> Self {
        Self {
            evaluation: Arc::new(ArrowEvaluationHandler),
            storage: Arc::new(PlanBasedStorageHandler {
                executor: executor.clone(),
            }),
            json: Arc::new(PlanBasedJsonHandler {
                executor: executor.clone(),
            }),
            parquet: Arc::new(PlanBasedParquetHandler {
                executor: executor.clone(),
            }),
            executor,
        }
    }

    /// The executor every handler routes through. [`Engine::plan_executor`] returns the same
    /// executor behind the trait.
    pub fn executor(&self) -> &Arc<DataFusionPlanExecutor> {
        &self.executor
    }
}

impl Engine for DataFusionEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        Some(self.executor.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use bytes::Bytes;
    use datafusion::arrow::array::{Array as _, Int64Array, RecordBatch};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::schema::{DataType, MetadataValue, SchemaRef, StructField, StructType};
    use delta_kernel::{
        EngineData, Error, FileDataReadResultIterator, FileMeta, FilteredEngineData,
    };
    use url::Url;

    use super::*;

    // === Shared helpers ===

    fn engine() -> DataFusionEngine {
        DataFusionEngine::new(SessionContext::new().task_ctx(), Handle::current())
    }

    /// Handlers block on the runtime, so they run off the test's worker threads.
    async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        tokio::task::spawn_blocking(f).await.unwrap()
    }

    fn file_meta(path: &Path) -> FileMeta {
        FileMeta {
            location: Url::from_file_path(path).unwrap(),
            last_modified: 0,
            size: std::fs::metadata(path).unwrap().len(),
        }
    }

    fn write_parquet(path: &Path, column: &str, values: &[i64]) -> FileMeta {
        let batch = RecordBatch::try_from_iter([(
            column,
            Arc::new(Int64Array::from(values.to_vec())) as _,
        )])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        file_meta(path)
    }

    fn write_json(path: &Path, lines: &[&str]) -> FileMeta {
        std::fs::write(path, lines.join("\n")).unwrap();
        file_meta(path)
    }

    fn x_schema() -> SchemaRef {
        Arc::new(StructType::try_new([StructField::nullable("x", DataType::LONG)]).unwrap())
    }

    /// The `x` values of each batch, one vector per batch.
    fn x_per_batch(batches: FileDataReadResultIterator) -> Vec<Vec<i64>> {
        batches
            .map(|batch| {
                let batch = ArrowEngineData::try_from_engine_data(batch.unwrap()).unwrap();
                batch
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    // === Tests ===

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_engine_exposes_its_executor_as_the_plan_executor() {
        let engine = engine();
        assert!(engine.plan_executor().is_some());
        assert!(
            engine
                .evaluation_handler()
                .as_ref()
                .any_ref()
                .is::<ArrowEvaluationHandler>()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn storage_calls_round_trip_through_the_executor() {
        let dir = tempfile::tempdir().unwrap();
        let url = Url::from_file_path(dir.path().join("blob.bin")).unwrap();
        let dir_url = Url::from_directory_path(dir.path()).unwrap();
        let storage = engine().storage_handler();
        blocking(move || {
            storage
                .put(&url, Bytes::from_static(b"kernel"), false)
                .unwrap();
            assert_eq!(storage.head(&url).unwrap().size, 6);

            let listed: Vec<_> = storage
                .list_from(&dir_url)
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].location, url);

            let bytes: Vec<_> = storage
                .read_files(vec![(url.clone(), Some(0..3))])
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(bytes, [Bytes::from_static(b"ker")]);

            storage.delete(&url).unwrap();
            assert!(matches!(storage.head(&url), Err(Error::FileNotFound(_))));
        })
        .await;
    }

    /// `JsonHandler` contract: batches arrive in file order and never span files.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_json_files_keeps_file_order_with_one_plan_per_file() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            write_json(&dir.path().join("a.json"), &[r#"{"x": 1}"#, r#"{"x": 2}"#]),
            write_json(&dir.path().join("b.json"), &[r#"{"x": 3}"#]),
            write_json(&dir.path().join("c.json"), &[r#"{"x": 4}"#]),
        ];
        let json = engine().json_handler();
        let per_batch =
            blocking(move || x_per_batch(json.read_json_files(&files, x_schema(), None).unwrap()))
                .await;
        assert_eq!(per_batch, [vec![1, 2], vec![3], vec![4]]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_json_files_reports_a_missing_file_as_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut missing = write_json(&dir.path().join("a.json"), &[r#"{"x": 1}"#]);
        missing.location = Url::from_file_path(dir.path().join("missing.json")).unwrap();
        let json = engine().json_handler();
        let results: Vec<_> = blocking(move || {
            json.read_json_files(&[missing], x_schema(), None)
                .unwrap()
                .map(|batch| batch.map(|_| ()))
                .collect()
        })
        .await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_parquet_files_matches_field_id_schemas_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_parquet(&dir.path().join("a.parquet"), "col-abc", &[1, 2, 3]);
        // A column-mapped table's physical schema: physical name plus a field id.
        let schema = Arc::new(
            StructType::try_new([StructField::nullable("col-abc", DataType::LONG)
                .with_metadata([("parquet.field.id", MetadataValue::Number(5))])])
            .unwrap(),
        );
        let parquet = engine().parquet_handler();
        let per_batch = blocking(move || {
            x_per_batch(parquet.read_parquet_files(&[file], schema, None).unwrap())
        })
        .await;
        assert_eq!(per_batch, [vec![1, 2, 3]]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_parquet_footer_goes_through_the_executor() {
        let dir = tempfile::tempdir().unwrap();
        let file = write_parquet(&dir.path().join("a.parquet"), "x", &[1]);
        let parquet = engine().parquet_handler();
        let footer = blocking(move || parquet.read_parquet_footer(&file).unwrap()).await;
        assert_eq!(footer.schema.fields().count(), 1);
        assert!(footer.schema.field("x").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_go_to_the_executors_file_format_handler() {
        let dir = tempfile::tempdir().unwrap();
        let json_url = Url::from_file_path(dir.path().join("out.json")).unwrap();
        let parquet_url = Url::from_file_path(dir.path().join("out.parquet")).unwrap();
        let engine = engine();
        blocking(move || {
            let batch =
                RecordBatch::try_from_iter([("x", Arc::new(Int64Array::from(vec![7, 8])) as _)])
                    .unwrap();
            let data = || Box::new(ArrowEngineData::new(batch.clone())) as Box<dyn EngineData>;

            let written = engine
                .json_handler()
                .write_json_file(
                    &json_url,
                    Box::new(std::iter::once(Ok(
                        FilteredEngineData::with_all_rows_selected(data()),
                    ))),
                    false,
                )
                .unwrap();
            let contents = std::fs::read_to_string(json_url.to_file_path().unwrap()).unwrap();
            assert_eq!(contents, "{\"x\":7}\n{\"x\":8}\n");
            assert_eq!(written, contents.len() as u64);

            engine
                .parquet_handler()
                .write_parquet_file(parquet_url.clone(), Box::new(std::iter::once(Ok(data()))))
                .unwrap();
            let file = file_meta(&parquet_url.to_file_path().unwrap());
            let per_batch = x_per_batch(
                engine
                    .parquet_handler()
                    .read_parquet_files(&[file], x_schema(), None)
                    .unwrap(),
            );
            assert_eq!(per_batch, [vec![7, 8]]);
        })
        .await;
    }
}
