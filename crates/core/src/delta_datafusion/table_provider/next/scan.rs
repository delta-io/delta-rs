use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayAccessor, AsArray, RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{SchemaRef, UInt16Type};
use arrow_array::BooleanArray;
use dashmap::DashMap;
use datafusion::common::HashMap;
use datafusion::common::config::ConfigOptions;
use datafusion::common::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, PlanProperties};
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, Statistics,
};
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::{EvaluationHandler, ExpressionRef};
use futures::stream::{Stream, StreamExt};

use crate::cast_record_batch;
use crate::delta_datafusion::table_provider::next::KernelScanPlan;
use crate::kernel::ARROW_HANDLER;
use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt;

#[derive(Clone, Debug)]
pub struct DeltaScanExec {
    scan_plan: Arc<KernelScanPlan>,
    /// Execution plan yielding the raw data read from data files.
    input: Arc<dyn ExecutionPlan>,
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

impl DisplayAs for DeltaScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO: actually implement formatting according to the type
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DeltaScanExec: file_id_column={}", self.file_id_column)
            }
        }
    }
}

impl DeltaScanExec {
    pub(super) fn new(
        scan_plan: Arc<KernelScanPlan>,
        input: Arc<dyn ExecutionPlan>,
        transforms: Arc<HashMap<String, ExpressionRef>>,
        selection_vectors: Arc<DashMap<String, Vec<bool>>>,
        file_id_column: String,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(scan_plan.result_schema.clone()),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
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

impl ExecutionPlan for DeltaScanExec {
    fn name(&self) -> &'static str {
        "DeltaScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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
        Ok(Arc::new(Self::new(
            self.scan_plan.clone(),
            children[0].clone(),
            self.transforms.clone(),
            self.selection_vectors.clone(),
            self.file_id_column.clone(),
            self.metrics.clone(),
        )))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(input) = self.input.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(Self {
                input,
                ..self.clone()
            })))
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
            scan_plan: Arc::clone(&self.scan_plan),
            kernel_type: Arc::clone(self.scan_plan.scan.logical_schema()).into(),
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

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
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
struct DeltaScanStream {
    scan_plan: Arc<KernelScanPlan>,
    /// Kernel data type for the data after transformations
    kernel_type: KernelDataType,
    /// Input stream yielding raw data read from data files.
    input: SendableRecordBatchStream,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// Transforms to be applied to data read from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Selection vectors to be applied to data read from individual files
    selection_vectors: Arc<DashMap<String, Vec<bool>>>,
    /// Column name for the file id
    file_id_column: String,
}

impl DeltaScanStream {
    /// Apply the per-file transformation to a RecordBatch.
    fn batch_project(&mut self, mut batch: RecordBatch) -> Result<RecordBatch> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let (file_id, file_id_idx) = extract_file_id(&batch, &self.file_id_column)?;
        batch.remove_column(file_id_idx);

        let selection = if let Some(mut selection_vector) = self.selection_vectors.get_mut(&file_id)
        {
            if selection_vector.len() >= batch.num_rows() {
                let sv: Vec<bool> = selection_vector.drain(0..batch.num_rows()).collect();
                Some(sv)
            } else {
                let remaining = batch.num_rows() - selection_vector.len();
                let sel_len = selection_vector.len();
                let mut sv: Vec<bool> = selection_vector.drain(0..sel_len).collect();
                sv.extend(vec![true; remaining]);
                Some(sv)
            }
        } else {
            None
        };

        let batch = if let Some(selection) = selection {
            filter_record_batch(&batch, &BooleanArray::from(selection))?
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
                self.scan_plan.scan.physical_schema().clone(),
                transform.clone(),
                self.kernel_type.clone(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let result = evaluator
            .evaluate_arrow(batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let result = if let Some(projection) = self.scan_plan.result_projection.as_ref() {
            result.project(projection)?
        } else {
            result
        };

        // TODO: all casting should be done in the expression evaluator
        Ok(cast_record_batch(
            &result,
            self.scan_plan.result_schema.clone(),
            true,
            true,
        )?)
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
        Arc::clone(&self.scan_plan.result_schema)
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
            DataFusionError::Internal(
                "Expected file id column to be a dictionary of strings".to_string(),
            )
        })?
        .value(0)
        .to_string();

    Ok((file_id, file_id_idx))
}
