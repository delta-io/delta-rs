use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayAccessor, AsArray, RecordBatch, StringArray};
use arrow::datatypes::{SchemaRef, UInt16Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::error::{DataFusionError, Result};
use datafusion::common::HashMap;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, PlanProperties};
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, Statistics,
};
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::{EvaluationHandler, ExpressionRef};
use futures::stream::{Stream, StreamExt};

use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt;
use crate::kernel::ARROW_HANDLER;

#[derive(Clone, Debug)]
pub struct DeltaScanExec {
    /// Output schema for processed data.
    logical_schema: SchemaRef,
    kernel_logical_schema: KernelSchemaRef,
    /// Execution plan yielding the raw data read from data files.
    input: Arc<dyn ExecutionPlan>,
    /// Transforms to be applied to data eminating from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Deletion vectors for the table
    selection_vectors: Arc<HashMap<String, Vec<bool>>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    file_id_column: String,
}

impl DisplayAs for DeltaScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO: actually implement formatting according to the type
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DeltaScanExec: ")
            }
        }
    }
}

impl DeltaScanExec {
    pub(crate) fn new(
        logical_schema: SchemaRef,
        kernel_logical_schema: KernelSchemaRef,
        input: Arc<dyn ExecutionPlan>,
        transforms: Arc<HashMap<String, ExpressionRef>>,
        selection_vectors: Arc<HashMap<String, Vec<bool>>>,
        file_id_column: String,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            logical_schema,
            kernel_logical_schema,
            input,
            transforms,
            selection_vectors,
            metrics,
            file_id_column,
        }
    }
}

impl ExecutionPlan for DeltaScanExec {
    fn name(&self) -> &'static str {
        "DeltaScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    // fn maintains_input_order(&self) -> Vec<bool> {
    //     // Tell optimizer this operator doesn't reorder its input
    //     vec![true]
    // }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "DeltaScan: wrong number of children {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            logical_schema: self.logical_schema.clone(),
            kernel_logical_schema: self.kernel_logical_schema.clone(),
            input: children[0].clone(),
            transforms: self.transforms.clone(),
            selection_vectors: self.selection_vectors.clone(),
            metrics: self.metrics.clone(),
            file_id_column: self.file_id_column.clone(),
        }))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(new_pq) = self.input.repartitioned(target_partitions, config)? {
            let mut new_plan = self.clone();
            new_plan.input = new_pq;
            Ok(Some(Arc::new(new_plan)))
        } else {
            Ok(None)
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(DeltaScanStream {
            schema: Arc::clone(&self.logical_schema),
            kernel_schema: Arc::clone(&self.kernel_logical_schema),
            input: self.input.execute(partition, context)?,
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
        self.input.partition_statistics(None)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if let Some(new_input) = self.input.with_fetch(limit) {
            let mut new_plan = self.clone();
            new_plan.input = new_input;
            Some(Arc::new(new_plan))
        } else {
            None
        }
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // TODO(roeap): this will likely not do much for column mapping enabled tables
        // since the default methods determines this b ased on existence of columns in child
        // schemas. In the case of column mapping all columns will have a different name.
        FilterDescription::from_children(parent_filters, &self.children())
    }
}

/// Stream of RecordBatches produced read from delta table.
///
/// The data returned by this stream represents the logical data caontained inn the table.
/// This means all transformations according to the delta protocol are applied.
struct DeltaScanStream {
    schema: SchemaRef,
    kernel_schema: KernelSchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    /// Transforms to be applied to data read from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Selection vectors to be applied to data read from individual files
    selection_vectors: Arc<HashMap<String, Vec<bool>>>,
    /// Column name for the file id
    file_id_column: String,
}

impl DeltaScanStream {
    fn batch_project(&self, mut batch: RecordBatch) -> Result<RecordBatch> {
        // Records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let (file_id, file_id_idx) = extract_file_id(&batch, &self.file_id_column)?;
        batch.remove_column(file_id_idx);

        let Some(transform) = self.transforms.get(&file_id) else {
            let batch = RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec())?;
            return Ok(batch);
        };

        let input_schema = Arc::new(
            batch
                .schema()
                .try_into_kernel()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let evaluator = ARROW_HANDLER.new_expression_evaluator(
            input_schema,
            transform.clone(),
            self.kernel_schema.clone().into(),
        );

        let result = evaluator
            .evaluate_arrow(batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(result)
    }
}

impl Stream for DeltaScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch_project(batch)),
            other => other,
        });
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for DeltaScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn extract_file_id(batch: &RecordBatch, file_id_column: &str) -> Result<(String, usize)> {
    let file_id_idx = batch
        .schema_ref()
        .fields()
        .iter()
        .position(|f| f.name() == file_id_column)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Expected column '{}' to be present in the input",
                file_id_column
            ))
        })?;

    let file_id = batch
        .column(file_id_idx)
        .as_dictionary::<UInt16Type>()
        .downcast_dict::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Expected file id column to be a dictionary of strings"
            ))
        })?
        .value(0)
        .to_string();

    Ok((file_id, file_id_idx))
}
