use std::any::Any;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow_array::{BooleanArray, RecordBatchOptions};
use arrow_schema::{Fields, Schema};
use dashmap::DashMap;
use datafusion::common::HashMap;
use datafusion::common::config::ConfigOptions;
use datafusion::common::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, PlanProperties,
};
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, Statistics,
};
use delta_kernel::schema::{Schema as KernelSchema, SchemaRef as KernelSchemaRef};
use delta_kernel::{EvaluationHandler, ExpressionRef};
use futures::stream::Stream;
use itertools::Itertools;

use crate::cast_record_batch;
use crate::delta_datafusion::table_provider::next::KernelScanPlan;
use crate::kernel::ARROW_HANDLER;
use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt;

// TODO:
// - implement limit/fetch pushdown

/// Execution plan for scanning a Delta table based only on file metadata.
///
/// For certain queries (e.g. `COUNT(*)`) we may be able to satisfy the query
/// using only file metadata without reading any actual data files.
#[derive(Clone, Debug)]
pub(crate) struct DeltaScanMetaExec {
    scan_plan: Arc<KernelScanPlan>,
    /// Execution plan yielding the raw data read from data files.
    input: Vec<VecDeque<(String, usize)>>,
    /// Transforms to be applied to data eminating from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Deletion vectors for the table
    selection_vectors: Arc<DashMap<String, Vec<bool>>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Column name for the file id
    file_id_column: String,
    /// plan properties
    properties: PlanProperties,
}

impl DisplayAs for DeltaScanMetaExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO: actually implement formatting according to the type
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DeltaScanMetaExec: file_id_column={}",
                    self.file_id_column
                )
            }
        }
    }
}

impl DeltaScanMetaExec {
    pub(super) fn new(
        scan_plan: Arc<KernelScanPlan>,
        input: Vec<VecDeque<(String, usize)>>,
        transforms: Arc<HashMap<String, ExpressionRef>>,
        selection_vectors: Arc<DashMap<String, Vec<bool>>>,
        file_id_column: String,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(scan_plan.result_schema.clone()),
            Partitioning::UnknownPartitioning(input.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            scan_plan,
            input,
            transforms,
            selection_vectors,
            metrics,
            file_id_column,
            properties,
        }
    }
}

impl ExecutionPlan for DeltaScanMetaExec {
    fn name(&self) -> &'static str {
        "DeltaScanMetaExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "DeltaMetaScan does not support children".to_string(),
            ));
        }
        Ok(self.clone())
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let total_files = self.input.iter().map(|q| q.len()).sum::<usize>();
        let partition_size = total_files / target_partitions;
        if partition_size == 0 {
            return Ok(None);
        }
        let new_input: Vec<VecDeque<(String, usize)>> = self
            .input
            .iter()
            .flatten()
            .cloned()
            .chunks(partition_size)
            .into_iter()
            .map(|chunk| chunk.collect())
            .collect();

        Ok(Some(Arc::new(Self {
            input: new_input,
            ..self.clone()
        })))
    }

    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition >= self.input.len() {
            return Err(DataFusionError::Plan(format!(
                "DeltaScanMetaExec: invalid partition {}",
                partition
            )));
        }
        Ok(Box::pin(DeltaScanMetaStream {
            scan_plan: Arc::clone(&self.scan_plan),
            input: self.input[partition].clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            transforms: Arc::clone(&self.transforms),
            selection_vectors: Arc::clone(&self.selection_vectors),
            file_id_column: self.file_id_column.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // self.input.partition_statistics(None)
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn with_fetch(&self, _: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn partition_statistics(&self, _: Option<usize>) -> Result<Statistics> {
        // TODO: handle statistics conversion properly to leverage parquet plan statistics.
        // self.input.partition_statistics(partition)
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // TODO(roeap): this will likely not do much for column mapping enabled tables
        // since the default methods determines this based on existence of columns in child
        // schemas. In the case of column mapping all columns will have a different name.
        FilterDescription::from_children(parent_filters, &self.children())
    }
}

/// Stream of RecordBatches produced by scanning a Delta table.
///
/// The data returned by this stream represents the logical data caontained in the table.
/// This means all transformations according to the Delta protocol are applied. This includes:
/// - partition values
/// - column mapping to the logical schema
/// - deletion vectors
struct DeltaScanMetaStream {
    scan_plan: Arc<KernelScanPlan>,
    /// Input stream yielding raw data read from data files.
    input: VecDeque<(String, usize)>,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// Transforms to be applied to data read from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Selection vectors to be applied to data read from individual files
    selection_vectors: Arc<DashMap<String, Vec<bool>>>,
    /// Column name for the file id
    file_id_column: String,
}

impl DeltaScanMetaStream {
    /// Apply the per-file transformation to a RecordBatch.
    fn batch_project(&self, file_id: String, row_count: usize) -> Result<RecordBatch> {
        static EMPTY_SCHEMA: LazyLock<SchemaRef> =
            LazyLock::new(|| Arc::new(Schema::new(Fields::empty())));
        static EMPTY_KERNEL_SCHEMA: LazyLock<KernelSchemaRef> =
            LazyLock::new(|| Arc::new(KernelSchema::try_new(vec![]).unwrap()));

        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let batch = RecordBatch::try_new_with_options(
            EMPTY_SCHEMA.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )?;

        let batch = if let Some(selection) = self.selection_vectors.get(&file_id) {
            let missing = batch.num_rows() - selection.len();
            let filter = if missing > 0 {
                BooleanArray::from_iter(selection.iter().chain(std::iter::repeat_n(&true, missing)))
            } else {
                BooleanArray::from_iter(selection.iter())
            };
            filter_record_batch(&batch, &filter)?
        } else {
            batch
        };

        let Some(transform) = self.transforms.get(&file_id) else {
            return Ok(cast_record_batch(
                &batch,
                self.scan_plan.result_schema.clone(),
                true,
                true,
            )?);
        };

        let evaluator = ARROW_HANDLER
            .new_expression_evaluator(
                EMPTY_KERNEL_SCHEMA.clone(),
                transform.clone(),
                self.scan_plan.scan.logical_schema().clone().into(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let result = evaluator
            .evaluate_arrow(batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(cast_record_batch(
            &result,
            self.scan_plan.result_schema.clone(),
            true,
            true,
        )?)
    }
}

impl Stream for DeltaScanMetaStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = if let Some((file, row_count)) = self.input.pop_front() {
            Poll::Ready(Some(self.batch_project(file, row_count)))
        } else {
            Poll::Ready(None)
        };
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.input.len(), Some(self.input.len()))
    }
}

impl RecordBatchStream for DeltaScanMetaStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.scan_plan.result_schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        catalog::TableProvider,
        physical_plan::collect_partitioned,
        prelude::{col, lit},
    };

    use super::*;
    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::{DataFusionMixins, session::create_test_session},
        kernel::Snapshot,
        test_utils::{TestResult, open_fs_path},
    };

    #[tokio::test]
    async fn test_meta_only_scan() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let snapshot =
            Arc::new(Snapshot::try_new(&table.log_store(), Default::default(), None).await?);
        let session = Arc::new(create_test_session().into_inner());

        let scan = snapshot
            .scan(
                &session.state(),
                Some(&vec![]),
                &[col("letter").eq(lit("b"))],
                None,
            )
            .await?;

        let downcast = scan.as_any().downcast_ref::<DeltaScanMetaExec>();
        assert!(downcast.is_some());

        session.register_table("delta_table", snapshot).unwrap();

        let df = session
            .sql("SELECT COUNT(*) as count FROM delta_table WHERE letter = 'b'")
            .await
            .unwrap();
        let batches = df.collect().await?;
        let expected = vec![
            "+-------+",
            "| count |",
            "+-------+",
            "| 1     |",
            "+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        let df = session
            .sql("SELECT COUNT(*) as count FROM delta_table")
            .await
            .unwrap();
        let batches = df.collect().await?;
        let expected = vec![
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_only_scan_with_projection() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let snapshot =
            Arc::new(Snapshot::try_new(&table.log_store(), Default::default(), None).await?);
        let session = Arc::new(create_test_session().into_inner());

        let idx = snapshot.read_schema().index_of("data").unwrap();

        let scan = snapshot
            .scan(
                &session.state(),
                Some(&vec![idx]),
                &[col("letter").eq(lit("b"))],
                None,
            )
            .await?;

        let downcast = scan.as_any().downcast_ref::<DeltaScanMetaExec>();
        assert!(downcast.is_some());

        let expected = vec![
            "+----------+",
            "| data     |",
            "+----------+",
            "| f09f9888 |",
            "+----------+",
        ];

        let data = collect_partitioned(scan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_batches_sorted_eq!(&expected, &data);

        session
            .register_table("delta_table", snapshot.clone())
            .unwrap();
        let df = session
            .sql("SELECT data FROM delta_table WHERE letter = 'b'")
            .await
            .unwrap();
        let batches = df.collect().await?;
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
}
