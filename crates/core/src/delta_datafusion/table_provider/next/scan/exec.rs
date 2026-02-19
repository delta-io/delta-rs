//! Physical execution for Delta table scans.
//!
//! This module implements [`DeltaScanExec`], the core execution plan that reads Parquet files
//! and applies Delta Lake protocol transformations to produce logical table data.

use std::any::Any;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{SchemaRef, UInt16Type};
use arrow_array::StringViewArray;
use arrow_array::{Array, BooleanArray};
use dashmap::DashMap;
use datafusion::common::config::ConfigOptions;
use datafusion::common::error::{DataFusionError, Result};
use datafusion::common::{
    ColumnStatistics, HashMap, internal_datafusion_err, internal_err, plan_err,
};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, PlanProperties};
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, Statistics,
};
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{EvaluationHandler, ExpressionRef};
use futures::stream::{Stream, StreamExt};

use super::plan::KernelScanPlan;
use crate::kernel::ARROW_HANDLER;
use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt;

#[derive(Debug, PartialEq)]
pub(crate) struct DvMaskResult {
    pub selection: Option<Vec<bool>>,
    pub should_remove: bool,
}

/// Consume the per-file deletion-vector keep-mask for the current batch.
///
/// The keep-mask is stored once per file and consumed incrementally as parquet
/// batches are produced:
/// - If the mask is shorter than the batch, missing trailing entries are
///   treated as `true` (keep row).
/// - If the mask is longer than the batch, the remainder is preserved for the
///   next batch from the same file.
///
/// This function intentionally does not error when `selection_vector.len()` is
/// greater than `batch_num_rows`; that is expected when one file spans multiple
/// input batches.
pub(crate) fn consume_dv_mask(
    selection_vector: &mut Vec<bool>,
    batch_num_rows: usize,
) -> DvMaskResult {
    if selection_vector.is_empty() {
        return DvMaskResult {
            selection: None,
            should_remove: true,
        };
    }

    if selection_vector.len() >= batch_num_rows {
        let sv: Vec<bool> = selection_vector.drain(0..batch_num_rows).collect();
        let is_empty = selection_vector.is_empty();
        DvMaskResult {
            selection: Some(sv),
            should_remove: is_empty,
        }
    } else {
        let mut sv: Vec<bool> = selection_vector.drain(..).collect();
        sv.resize(batch_num_rows, true);
        DvMaskResult {
            selection: Some(sv),
            should_remove: true,
        }
    }
}

/// Physical execution plan for scanning Delta tables.
///
/// Wraps a Parquet reader execution plan and applies Delta Lake protocol transformations
/// to produce the logical table data. This includes:
///
/// - **Column mapping**: Translates physical column names to logical names
/// - **Partition values**: Materializes partition column values from file paths
/// - **Deletion vectors**: Filters out deleted rows using per-file selection vectors
/// - **Schema evolution**: Handles missing columns and type coercion
///
/// # Data Flow
///
/// 1. Inner [`input`](Self::input) plan reads raw Parquet data
/// 2. Per-file [`transforms`](Self::transforms) convert physical to logical schema
/// 3. [`selection_vectors`](Self::selection_vectors) filter deleted rows
/// 4. Result is cast to [`result_schema`](KernelScanPlan::result_schema)
#[derive(Clone, Debug)]
pub struct DeltaScanExec {
    scan_plan: Arc<KernelScanPlan>,
    /// Execution plan yielding the raw data read from data files.
    input: Arc<dyn ExecutionPlan>,
    /// Transforms to be applied to data eminating from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Selection vectors to be applied to data read from individual files
    selection_vectors: Arc<DashMap<String, Vec<bool>>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Column name for the file id
    file_id_column: String,
    /// plan properties
    properties: PlanProperties,
    /// Denotes if file ids should be returned as part of the output
    retain_file_ids: bool,
    /// Aggregated partition column statistics
    partition_stats: HashMap<String, ColumnStatistics>,
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
    pub(crate) fn new(
        scan_plan: Arc<KernelScanPlan>,
        input: Arc<dyn ExecutionPlan>,
        transforms: Arc<HashMap<String, ExpressionRef>>,
        selection_vectors: Arc<DashMap<String, Vec<bool>>>,
        partition_stats: HashMap<String, ColumnStatistics>,
        file_id_column: String,
        retain_file_ids: bool,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(scan_plan.output_schema.clone()),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        );
        Self {
            scan_plan,
            input,
            transforms,
            selection_vectors,
            partition_stats,
            metrics,
            file_id_column,
            retain_file_ids,
            properties,
        }
    }

    /// Transform the statistics from the inner physical parquet read plan to the logical
    /// schema we expose via the table provider. We do not attempt to provide meaningful
    /// statistics for metadata columns as we do not expect these to be useful in planning.
    /// - predicates on metadata columns (like file id) are not really useful (random etc.)
    fn map_statistics(&self, mut stats: Statistics) -> Result<Statistics> {
        // Column statistics include stats for the added file id column, so we expect the
        // number of physical schema fields + 1 to match the number of column statistics.
        // We validate this to en sure we can safely remap the statistics below.
        if self.scan_plan.scan.physical_schema().fields().len() > stats.column_statistics.len() {
            return internal_err!(
                "mismatched number of column statistics: expected {}, got {}",
                self.scan_plan.scan.physical_schema().fields().len(),
                stats.column_statistics.len()
            );
        }

        let config = self.scan_plan.table_configuration();
        let mut new_stats = Vec::with_capacity(self.schema().fields().len());

        if config.is_feature_enabled(&TableFeature::ColumnMapping) {
            let get_index = |name| {
                if let Some(logical) = self.scan_plan.scan.logical_schema().field(name) {
                    let physical = logical.make_physical(config.column_mapping_mode());
                    self.input.schema().index_of(physical.name()).ok()
                } else {
                    None
                }
            };

            for field in self.schema().fields() {
                if let Some(index) = get_index(field.name()) {
                    new_stats.push(stats.column_statistics[index].clone());
                } else if let Some(part_stat) = self.partition_stats.get(field.name()) {
                    new_stats.push(part_stat.clone());
                } else {
                    new_stats.push(Default::default());
                }
            }
        } else {
            for field in self.schema().fields() {
                if let Some((index, _)) = self
                    .scan_plan
                    .scan
                    .physical_schema()
                    .field_with_index(field.name())
                {
                    new_stats.push(stats.column_statistics[index].clone());
                } else if let Some(part_stat) = self.partition_stats.get(field.name()) {
                    new_stats.push(part_stat.clone());
                } else {
                    new_stats.push(Default::default());
                }
            }
        }

        stats.column_statistics = new_stats;
        Ok(stats)
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

    // TODO: setting this will fail certain tests, but why
    // fn maintains_input_order(&self) -> Vec<bool> {
    //     vec![true]
    // }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("DeltaScan: wrong number of children {}", children.len());
        }
        Ok(Arc::new(Self::new(
            self.scan_plan.clone(),
            children[0].clone(),
            self.transforms.clone(),
            self.selection_vectors.clone(),
            self.partition_stats.clone(),
            self.file_id_column.clone(),
            self.retain_file_ids,
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
            return_file_ids: self.retain_file_ids,
            pending: VecDeque::new(),
            schema_adapter: super::SchemaAdapter::new(Arc::clone(&self.scan_plan.result_schema)),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let new_input = self.input.with_fetch(limit)?;
        let mut new_plan = self.clone();
        new_plan.input = new_input;
        Some(Arc::new(new_plan))
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input
            .partition_statistics(partition)
            .and_then(|stats| self.map_statistics(stats))
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

/// Stream that produces logical RecordBatches from a Delta table scan.
///
/// Consumes raw Parquet data from the input stream and applies Delta Lake transformations
/// per-file to yield logical table data. Handles:
///
/// - Deletion vectors: Filters rows marked as deleted
/// - Column transforms: Applies partition value injection and column mapping
/// - Schema projection: Projects to requested columns only
/// - Type casting: Ensures output matches expected logical schema
///
/// Input batches may contain rows from multiple file IDs (e.g., due to upstream coalescing).
/// The stream splits such batches by contiguous file-id runs and applies per-file transforms.
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
    /// Denotes if file ids should be returned as part of the output
    return_file_ids: bool,
    pending: VecDeque<RecordBatch>,
    /// Cached schema adapter for efficient batch adaptation across batches
    schema_adapter: super::SchemaAdapter,
}

impl DeltaScanStream {
    fn batch_project(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        if batch.num_rows() == 0 {
            return Ok(vec![RecordBatch::new_empty(Arc::clone(
                &self.scan_plan.output_schema,
            ))]);
        }

        let file_id_idx = file_id_column_idx(&batch, &self.file_id_column)?;
        let file_runs = split_by_file_id_runs(&batch, file_id_idx)?;

        file_runs
            .into_iter()
            .map(|(file_id, slice)| {
                Self::batch_project_single_file_inner(
                    &self.scan_plan,
                    &self.kernel_type,
                    &self.transforms,
                    &self.selection_vectors,
                    self.return_file_ids,
                    &mut self.schema_adapter,
                    slice,
                    file_id,
                    file_id_idx,
                )
            })
            .collect::<Result<Vec<_>>>()
    }

    fn batch_project_single_file_inner(
        scan_plan: &KernelScanPlan,
        kernel_type: &KernelDataType,
        transforms: &HashMap<String, ExpressionRef>,
        selection_vectors: &DashMap<String, Vec<bool>>,
        return_file_ids: bool,
        schema_adapter: &mut super::SchemaAdapter,
        batch: RecordBatch,
        file_id: String,
        file_id_idx: usize,
    ) -> Result<RecordBatch> {
        let dv_result = if let Some(mut selection_vector) = selection_vectors.get_mut(&file_id) {
            consume_dv_mask(&mut selection_vector, batch.num_rows())
        } else {
            DvMaskResult {
                selection: None,
                should_remove: false,
            }
        };

        if dv_result.should_remove {
            selection_vectors.remove(&file_id);
        }

        let mut batch = if let Some(selection) = dv_result.selection {
            filter_record_batch(&batch, &BooleanArray::from(selection))?
        } else {
            batch
        };

        let file_id_field = batch.schema_ref().field(file_id_idx).clone();
        let file_id_col = batch.remove_column(file_id_idx);

        let result = if let Some(transform) = transforms.get(&file_id) {
            let evaluator = ARROW_HANDLER
                .new_expression_evaluator(
                    scan_plan.scan.physical_schema().clone(),
                    transform.clone(),
                    kernel_type.clone(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            evaluator
                .evaluate_arrow(batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            batch
        };

        if return_file_ids {
            super::finalize_transformed_batch(
                result,
                scan_plan,
                Some((file_id_col, Arc::new(file_id_field))),
                schema_adapter,
            )
        } else {
            super::finalize_transformed_batch(result, scan_plan, None, schema_adapter)
        }
    }
}

impl Stream for DeltaScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.pending.pop_front() {
            return self
                .baseline_metrics
                .record_poll(Poll::Ready(Some(Ok(batch))));
        }

        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let projected = match self.batch_project(batch) {
                    Ok(outputs) => {
                        let mut outputs = outputs.into_iter();
                        match outputs.next() {
                            Some(first) => {
                                self.pending.extend(outputs);
                                Ok(first)
                            }
                            None => {
                                Err(internal_datafusion_err!("batch_project returned no output"))
                            }
                        }
                    }
                    Err(err) => Err(err),
                };
                Some(projected)
            }
            other => other,
        });

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (low, _high) = self.input.size_hint();
        (self.pending.len() + low, None)
    }
}

impl RecordBatchStream for DeltaScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.scan_plan.output_schema)
    }
}

#[inline]
fn file_id_column_idx(batch: &RecordBatch, file_id_column: &str) -> Result<usize> {
    batch
        .schema_ref()
        .fields()
        .iter()
        .position(|f| f.name() == file_id_column)
        .ok_or_else(|| {
            internal_datafusion_err!(
                "Expected column '{}' to be present in the input",
                file_id_column
            )
        })
}

/// Split batch into contiguous runs by file ID. Compares dictionary keys for efficiency.
/// Returns zero-copy slices. Errors on null file IDs or unexpected column type.
fn split_by_file_id_runs(
    batch: &RecordBatch,
    file_id_idx: usize,
) -> Result<Vec<(String, RecordBatch)>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let dict = batch
        .column(file_id_idx)
        .as_any()
        .downcast_ref::<arrow_array::DictionaryArray<UInt16Type>>()
        .ok_or_else(|| {
            internal_datafusion_err!(
                "Expected file id column '{}' to be Dictionary<UInt16, Utf8|Utf8View>, got {:?}",
                batch.schema_ref().field(file_id_idx).name(),
                batch.column(file_id_idx).data_type()
            )
        })?;

    // Parquet reads may yield Utf8 or Utf8View depending on DataFusion settings.
    // Accept either for the synthetic file id column.
    let keys = dict.keys();

    enum FileIdValues<'a> {
        Utf8(&'a StringArray),
        Utf8View(&'a StringViewArray),
    }

    let values = if let Some(values) = dict
        .values()
        .as_ref()
        .as_any()
        .downcast_ref::<StringArray>()
    {
        FileIdValues::Utf8(values)
    } else if let Some(values) = dict
        .values()
        .as_ref()
        .as_any()
        .downcast_ref::<StringViewArray>()
    {
        FileIdValues::Utf8View(values)
    } else {
        return Err(internal_datafusion_err!(
            "Expected file id column '{}' to be Dictionary<UInt16, Utf8|Utf8View>, got {:?}",
            batch.schema_ref().field(file_id_idx).name(),
            batch.column(file_id_idx).data_type()
        ));
    };

    let file_id_for_row = |row: usize| -> String {
        let key = keys.value(row) as usize;
        match values {
            FileIdValues::Utf8(arr) => arr.value(key).to_string(),
            FileIdValues::Utf8View(arr) => arr.value(key).to_string(),
        }
    };

    if dict.is_null(0) {
        return Err(internal_datafusion_err!("file id value must not be null"));
    }

    let mut prev_key = keys.value(0);
    let mut start = 0usize;
    let mut runs = Vec::new();

    for i in 1..batch.num_rows() {
        if dict.is_null(i) {
            return Err(internal_datafusion_err!("file id value must not be null"));
        }
        let key = keys.value(i);
        if key != prev_key {
            let file_id = file_id_for_row(start);
            runs.push((file_id, batch.slice(start, i - start)));
            start = i;
            prev_key = key;
        }
    }

    let file_id = file_id_for_row(start);
    runs.push((file_id, batch.slice(start, batch.num_rows() - start)));

    Ok(runs)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::AsArray;
    use arrow::datatypes::DataType;
    use arrow_array::Array;
    use arrow_array::ArrayAccessor;
    use datafusion::{
        common::stats::Precision,
        physical_plan::{collect, collect_partitioned},
        prelude::{col, lit},
    };

    use super::*;
    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::{session::create_session, table_provider::next::FILE_ID_COLUMN_DEFAULT},
        test_utils::{TestResult, open_fs_path},
    };

    #[tokio::test]
    async fn test_scan_nested() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/nested_types/delta");
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        let scan = provider.scan(&session.state(), None, &[], None).await?;

        let batches = collect(scan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-----------------------------+-----------------+--------------------------+",
            "| pk | struct                      | array           | map                      |",
            "+----+-----------------------------+-----------------+--------------------------+",
            "| 0  | {float64: 0.0, bool: true}  | [0]             | {}                       |",
            "| 1  | {float64: 1.0, bool: false} | [0, 1]          | {0: 0}                   |",
            "| 2  | {float64: 2.0, bool: true}  | [0, 1, 2]       | {0: 0, 1: 1}             |",
            "| 3  | {float64: 3.0, bool: false} | [0, 1, 2, 3]    | {0: 0, 1: 1, 2: 2}       |",
            "| 4  | {float64: 4.0, bool: true}  | [0, 1, 2, 3, 4] | {0: 0, 1: 1, 2: 2, 3: 3} |",
            "+----+-----------------------------+-----------------+--------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_id() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        let scan = provider
            .scan(&session.state(), None, &[col("letter").eq(lit("b"))], None)
            .await?;

        let downcast = scan.as_any().downcast_ref::<DeltaScanExec>();
        assert!(downcast.is_some());
        assert!(downcast.unwrap().retain_file_ids);

        let data = collect_partitioned(scan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // Verify that file_id column is present in the result
        assert!(data[0].schema().column_with_name("file_id").is_some());
        assert_eq!(data[0].num_rows(), 1);

        // Verify file_id column has the correct type
        let schema = data[0].schema();
        let file_id_field = schema.column_with_name("file_id").unwrap().1;
        match file_id_field.data_type() {
            DataType::Dictionary(_, value_type)
                if value_type.as_ref() == &DataType::Utf8
                    || value_type.as_ref() == &DataType::Utf8View =>
            {
                // ok
            }
            other => panic!("unexpected file_id dtype: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_id_projection() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        // Select only data and file_id columns (both can be satisfied from metadata)
        let data_idx = provider.schema().index_of("data").unwrap();
        let file_id_idx = provider.schema().index_of("file_id").unwrap();

        let scan = provider
            .scan(
                &session.state(),
                Some(&vec![data_idx, file_id_idx]),
                &[col("letter").eq(lit("b"))],
                None,
            )
            .await?;

        // Scan could be either DeltaScanExec or DeltaScanMetaExec depending on whether
        // data column requires physical file access
        let data = collect_partitioned(scan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // Should have 2 columns: data and file_id
        assert_eq!(data[0].num_columns(), 2);
        assert!(data[0].schema().column_with_name("data").is_some());
        assert!(data[0].schema().column_with_name("file_id").is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_id_groupby() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        session.register_table("delta_table", provider).unwrap();

        // Query that groups by file_id to verify each file's contribution
        let df = session
            .sql("SELECT file_id, COUNT(*) as count FROM delta_table GROUP BY file_id ORDER BY file_id")
            .await
            .unwrap();
        let batches = df.collect().await?;

        // Should have 2 or more groups (one for each file)
        assert!(batches[0].num_rows() >= 2);
        assert_eq!(batches[0].num_columns(), 2);

        // Verify file_id column is present
        assert!(batches[0].schema().column_with_name("file_id").is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_without_file_id() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().await?;
        let session = create_session().into_inner();

        let scan = provider
            .scan(&session.state(), None, &[col("letter").eq(lit("b"))], None)
            .await?;

        let downcast = scan.as_any().downcast_ref::<DeltaScanExec>();
        assert!(downcast.is_some());
        assert!(!downcast.unwrap().retain_file_ids);

        let data = collect_partitioned(scan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // Verify that file_id column is NOT present when not requested
        assert!(data[0].schema().column_with_name("file_id").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_id_all_data() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = create_session().into_inner();

        session.register_table("delta_table", provider).unwrap();

        // Query to verify file_id is present for all rows
        let df = session
            .sql("SELECT data, letter, file_id FROM delta_table WHERE letter = 'b'")
            .await
            .unwrap();
        let batches = df.collect().await?;

        // Verify the result has the expected structure
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 3);

        // Verify all expected columns are present
        assert!(batches[0].schema().column_with_name("data").is_some());
        assert!(batches[0].schema().column_with_name("letter").is_some());
        assert!(batches[0].schema().column_with_name("file_id").is_some());

        // Verify file_id column has a value (full file path)
        let file_id_col = batches[0].column_by_name("file_id").unwrap();
        assert_eq!(file_id_col.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_id_extract_filename() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        session.register_table("delta_table", provider).unwrap();

        // Extract just the filename from the full path using SQL
        // Use REVERSE and STRPOS to find the last '/' and extract everything after it
        let df = session
            .sql(
                "SELECT
                    data,
                    letter,
                    REVERSE(SUBSTRING(REVERSE(file_id), 1, STRPOS(REVERSE(file_id), '/') - 1)) as filename
                 FROM delta_table
                 WHERE letter = 'b'"
            )
            .await
            .unwrap();
        let batches = df.collect().await?;

        // Verify the filename contains expected patterns (UUID and .parquet extension)
        let expected = vec![
            "+----------+--------+---------------------------------------------------------------------+",
            "| data     | letter | filename                                                            |",
            "+----------+--------+---------------------------------------------------------------------+",
            "| f09f9888 | b      | part-00000-b300ccc0-7096-4f4f-acf9-3811211dca3e.c000.snappy.parquet |",
            "+----------+--------+---------------------------------------------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_id_multiple_files() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        session.register_table("delta_table", provider).unwrap();

        // Query all data and extract filenames
        let df = session
            .sql(
                "SELECT
                    letter,
                    COUNT(*) as count,
                    REVERSE(SUBSTRING(REVERSE(file_id), 1, STRPOS(REVERSE(file_id), '/') - 1)) as filename
                 FROM delta_table
                 GROUP BY letter, file_id
                 ORDER BY letter, filename"
            )
            .await
            .unwrap();
        let batches = df.collect().await?;

        // Should have multiple groups (one for each unique file)
        assert!(batches[0].num_rows() >= 2);

        // Verify columns are present
        assert!(batches[0].schema().column_with_name("letter").is_some());
        assert!(batches[0].schema().column_with_name("count").is_some());
        assert!(batches[0].schema().column_with_name("filename").is_some());

        // Verify each group has a valid parquet filename
        let filename_col = batches[0]
            .column_by_name("filename")
            .unwrap()
            .as_string::<i32>();

        for i in 0..filename_col.len() {
            let filename = filename_col.value(i);
            assert!(
                filename.ends_with(".parquet"),
                "Filename should end with .parquet: {}",
                filename
            );
            assert!(
                filename.contains("part-"),
                "Filename should contain 'part-': {}",
                filename
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_id_data_validation() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        session.register_table("delta_table", provider).unwrap();

        // Query to validate that file_id is present for each partition
        let df = session
            .sql(
                "SELECT
                    letter,
                    data,
                    REVERSE(SUBSTRING(REVERSE(file_id), 1, STRPOS(REVERSE(file_id), '/') - 1)) as filename
                 FROM delta_table
                 WHERE letter = 'b'
                 ORDER BY letter"
            )
            .await
            .unwrap();
        let batches = df.collect().await?;

        let expected = vec![
            "+--------+----------+---------------------------------------------------------------------+",
            "| letter | data     | filename                                                            |",
            "+--------+----------+---------------------------------------------------------------------+",
            "| b      | f09f9888 | part-00000-b300ccc0-7096-4f4f-acf9-3811211dca3e.c000.snappy.parquet |",
            "+--------+----------+---------------------------------------------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics() -> TestResult {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/all_primitive_types/delta");
        table.load().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        // for scans without prodicates, we gather only top level statistic
        // and omit collecting column level statistics
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let statistics = scan.partition_statistics(None)?;
        assert_eq!(statistics.num_rows, Precision::Exact(5));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(3240));
        for col_stat in statistics.column_statistics.iter() {
            assert_eq!(col_stat.null_count, Precision::Absent);
            assert_eq!(col_stat.min_value, Precision::Absent);
            assert_eq!(col_stat.max_value, Precision::Absent);
        }

        // for scans with predicates, we gather full statistics
        let predicates = table
            .snapshot()?
            .schema()
            .field_names()
            .map(|c| col(c).is_not_null())
            .collect::<Vec<_>>();
        let scan = provider
            .scan(&session.state(), None, &predicates, None)
            .await?;
        let statistics = scan.partition_statistics(None)?;
        for (col_stat, field) in statistics
            .column_statistics
            .iter()
            .zip(provider.schema().fields())
        {
            // skip boolean and binary columns as they do not have min/max stats
            if matches!(
                field.data_type(),
                &DataType::Boolean | &DataType::Binary | &DataType::BinaryView
            ) {
                assert!(matches!(col_stat.null_count, Precision::Inexact(_)));
                continue;
            }
            assert!(matches!(col_stat.null_count, Precision::Inexact(_)));
            assert!(matches!(col_stat.min_value, Precision::Inexact(_)));
            assert!(matches!(col_stat.max_value, Precision::Inexact(_)));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_column_mapping() -> TestResult {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/column_mapping/delta");
        table.load().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        // for scans with predicates, we gather full statistics
        let predicates = table
            .snapshot()?
            .schema()
            .field_names()
            .map(|c| col(c).is_not_null())
            .collect::<Vec<_>>();
        let scan = provider
            .scan(&session.state(), None, &predicates, None)
            .await?;
        let statistics = scan.partition_statistics(None)?;
        assert_eq!(
            statistics.column_statistics.len(),
            provider.schema().fields().len()
        );
        for col_stat in statistics.column_statistics.iter() {
            assert!(matches!(col_stat.null_count, Precision::Inexact(_)));
            assert!(matches!(col_stat.min_value, Precision::Inexact(_)));
            assert!(matches!(col_stat.max_value, Precision::Inexact(_)));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_partitioned() -> TestResult {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        table.load().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        // for scans with predicates, we gather full statistics
        let predicates = table
            .snapshot()?
            .schema()
            .field_names()
            .map(|c| col(c).is_not_null())
            .collect::<Vec<_>>();
        let scan = provider
            .scan(&session.state(), None, &predicates, None)
            .await?;
        let statistics = scan.partition_statistics(None)?;
        for (col_stat, _field) in statistics
            .column_statistics
            .iter()
            .zip(provider.schema().fields())
        {
            assert!(matches!(
                col_stat.null_count,
                Precision::Exact(_) | Precision::Inexact(_)
            ));
            assert!(matches!(
                col_stat.min_value,
                Precision::Exact(_) | Precision::Inexact(_)
            ));
            assert!(matches!(
                col_stat.max_value,
                Precision::Exact(_) | Precision::Inexact(_)
            ));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_deletion_vectors() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/deletion_vectors/delta");
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        let scan = provider.scan(&session.state(), None, &[], None).await?;

        let downcast = scan.as_any().downcast_ref::<DeltaScanExec>();
        assert!(downcast.is_some(), "Expected DeltaScanExec for DV test");

        let batches = collect(scan, session.task_ctx()).await?;

        let expected = vec![
            "+--------+-----+------------+",
            "| letter | int | date       |",
            "+--------+-----+------------+",
            "| b      | 228 | 1978-12-01 |",
            "+--------+-----+------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    // DV test helpers
    const DV_TABLE_PATH: &str = "../../dat/v0.0.3/reader_tests/generated/deletion_vectors/delta";

    async fn dv_kernel_type_and_int32_scan_plan()
    -> TestResult<(KernelDataType, Arc<KernelScanPlan>)> {
        use arrow::datatypes::{Field, Schema};

        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .as_any()
            .downcast_ref::<DeltaScanExec>()
            .expect("Expected DeltaScanExec");

        let kernel_type = Arc::clone(exec.scan_plan.scan.logical_schema()).into();

        let mut scan_plan = exec.scan_plan.as_ref().clone();
        scan_plan.result_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        scan_plan.output_schema = Arc::clone(&scan_plan.result_schema);
        scan_plan.result_projection = None;
        scan_plan.parquet_read_schema = Arc::clone(&scan_plan.result_schema);

        Ok((kernel_type, Arc::new(scan_plan)))
    }

    fn selection_vectors_f1_f2() -> Arc<DashMap<String, Vec<bool>>> {
        let selection_vectors: Arc<DashMap<String, Vec<bool>>> = Arc::new(DashMap::new());
        selection_vectors.insert("f1".to_string(), vec![true, false]);
        selection_vectors.insert("f2".to_string(), vec![false, true]);
        selection_vectors
    }

    fn value_and_file_id_batch(
        values: &[i32],
        file_ids: &[Option<&str>],
        file_id_nullable: bool,
    ) -> TestResult<RecordBatch> {
        use arrow::datatypes::{Field, Schema};
        use arrow_array::{DictionaryArray, Int32Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new(
                FILE_ID_COLUMN_DEFAULT,
                DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
                file_id_nullable,
            ),
        ]));

        let mut file_id_builder =
            arrow_array::builder::StringDictionaryBuilder::<UInt16Type>::new();
        for file_id in file_ids {
            match file_id {
                Some(file_id) => file_id_builder.append_value(file_id),
                None => file_id_builder.append_null(),
            }
        }
        let file_id: DictionaryArray<UInt16Type> = file_id_builder.finish();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(values.to_vec())),
                Arc::new(file_id),
            ],
        )?;

        Ok(batch)
    }

    fn test_scan_stream(
        scan_plan: Arc<KernelScanPlan>,
        kernel_type: KernelDataType,
        selection_vectors: Arc<DashMap<String, Vec<bool>>>,
        input_batches: Vec<RecordBatch>,
        return_file_ids: bool,
    ) -> DeltaScanStream {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let input_schema = input_batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::clone(&scan_plan.output_schema));

        let input = Box::pin(RecordBatchStreamAdapter::new(
            input_schema,
            futures::stream::iter(input_batches.into_iter().map(Ok)),
        ));

        let schema_adapter = super::super::SchemaAdapter::new(Arc::clone(&scan_plan.result_schema));
        DeltaScanStream {
            scan_plan,
            kernel_type,
            input,
            baseline_metrics: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            transforms: Arc::new(HashMap::new()),
            selection_vectors,
            file_id_column: FILE_ID_COLUMN_DEFAULT.to_string(),
            return_file_ids,
            pending: VecDeque::new(),
            schema_adapter,
        }
    }

    #[tokio::test]
    async fn test_batch_project_splits_mixed_file_batches_for_dv_masks() -> TestResult {
        let (kernel_type, scan_plan) = dv_kernel_type_and_int32_scan_plan().await?;
        let selection_vectors = selection_vectors_f1_f2();

        let batch = value_and_file_id_batch(
            &[10, 11, 20, 21],
            &[Some("f1"), Some("f1"), Some("f2"), Some("f2")],
            false,
        )?;

        let file_id_idx = file_id_column_idx(&batch, FILE_ID_COLUMN_DEFAULT)?;
        let runs = split_by_file_id_runs(&batch, file_id_idx)?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].0, "f1");
        assert_eq!(runs[1].0, "f2");
        assert!(selection_vectors.contains_key("f1"));
        assert!(selection_vectors.contains_key("f2"));

        let mut stream = test_scan_stream(
            Arc::clone(&scan_plan),
            kernel_type,
            selection_vectors,
            Vec::new(),
            false,
        );

        let outputs = stream.batch_project(batch)?;
        assert_eq!(outputs.len(), 2);

        let out1 = outputs[0]
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let out2 = outputs[1]
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(out1.values(), &[10]);
        assert_eq!(out2.values(), &[21]);

        Ok(())
    }

    #[tokio::test]
    async fn test_poll_next_buffers_fanout_batches() -> TestResult {
        use futures::StreamExt;

        let (kernel_type, scan_plan) = dv_kernel_type_and_int32_scan_plan().await?;
        let selection_vectors = selection_vectors_f1_f2();

        let batch = value_and_file_id_batch(
            &[10, 11, 20, 21],
            &[Some("f1"), Some("f1"), Some("f2"), Some("f2")],
            false,
        )?;

        let mut stream = test_scan_stream(
            scan_plan,
            kernel_type,
            selection_vectors,
            vec![batch],
            false,
        );

        let batch1 = stream.next().await.transpose()?.expect("first batch");
        let batch2 = stream.next().await.transpose()?.expect("second batch");
        assert!(stream.next().await.is_none());

        let out1 = batch1
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let out2 = batch2
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(out1.values(), &[10]);
        assert_eq!(out2.values(), &[21]);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_project_handles_interleaved_file_ids() -> TestResult {
        let (kernel_type, scan_plan) = dv_kernel_type_and_int32_scan_plan().await?;
        let selection_vectors = selection_vectors_f1_f2();

        let batch = value_and_file_id_batch(
            &[10, 20, 11, 21],
            &[Some("f1"), Some("f2"), Some("f1"), Some("f2")],
            false,
        )?;

        let mut stream = test_scan_stream(
            Arc::clone(&scan_plan),
            kernel_type,
            selection_vectors,
            Vec::new(),
            false,
        );

        let outputs = stream.batch_project(batch)?;
        let kept: Vec<i32> = outputs
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(kept, vec![10, 21]);

        Ok(())
    }

    #[test]
    fn test_split_by_file_id_runs_invalid_type_returns_error() -> TestResult {
        use arrow::datatypes::{Field, Schema};
        use arrow_array::Int32Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new(FILE_ID_COLUMN_DEFAULT, DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )?;

        let file_id_idx = file_id_column_idx(&batch, FILE_ID_COLUMN_DEFAULT)?;
        let err = split_by_file_id_runs(&batch, file_id_idx).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("Dictionary<UInt16"));
        assert!(message.contains("Int32"));

        Ok(())
    }

    #[test]
    fn test_split_by_file_id_runs_null_file_id_returns_error() -> TestResult {
        let batch = value_and_file_id_batch(&[1, 2], &[Some("f1"), None], true)?;

        let file_id_idx = file_id_column_idx(&batch, FILE_ID_COLUMN_DEFAULT)?;
        let err = split_by_file_id_runs(&batch, file_id_idx).unwrap_err();
        assert!(err.to_string().contains("file id value must not be null"));

        Ok(())
    }

    #[test]
    fn test_split_by_file_id_runs_preserves_dictionary_key_mapping() -> TestResult {
        use arrow::datatypes::{Field, Schema};
        use arrow_array::{DictionaryArray, Int32Array, StringArray, UInt16Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new(
                FILE_ID_COLUMN_DEFAULT,
                DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
                false,
            ),
        ]));

        // Dictionary keys intentionally start with key=1 to ensure run labels are taken from keys,
        // not from row indexes.
        let keys = UInt16Array::from(vec![Some(1), Some(1), Some(0), Some(0), Some(1)]);
        let values = StringArray::from(vec!["f0", "f1"]);
        let file_ids = DictionaryArray::new(keys, Arc::new(values));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 11, 20, 21, 12])),
                Arc::new(file_ids),
            ],
        )?;

        let file_id_idx = file_id_column_idx(&batch, FILE_ID_COLUMN_DEFAULT)?;
        let runs = split_by_file_id_runs(&batch, file_id_idx)?;
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[0].0, "f1");
        assert_eq!(runs[1].0, "f0");
        assert_eq!(runs[2].0, "f1");

        let run0 = runs[0]
            .1
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let run1 = runs[1]
            .1
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let run2 = runs[2]
            .1
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(run0.values(), &[10, 11]);
        assert_eq!(run1.values(), &[20, 21]);
        assert_eq!(run2.values(), &[12]);

        Ok(())
    }

    #[tokio::test]
    async fn test_poll_next_fanout_preserves_file_ids() -> TestResult {
        use futures::StreamExt;

        let (kernel_type, scan_plan) = dv_kernel_type_and_int32_scan_plan().await?;
        let selection_vectors = selection_vectors_f1_f2();

        let batch = value_and_file_id_batch(
            &[10, 11, 20, 21],
            &[Some("f1"), Some("f1"), Some("f2"), Some("f2")],
            false,
        )?;

        let mut stream =
            test_scan_stream(scan_plan, kernel_type, selection_vectors, vec![batch], true);

        let batch1 = stream.next().await.transpose()?.expect("first batch");
        let batch2 = stream.next().await.transpose()?.expect("second batch");
        assert!(stream.next().await.is_none());

        assert_eq!(batch1.num_columns(), 2);
        assert_eq!(batch2.num_columns(), 2);

        let file_id1 = batch1
            .column(1)
            .as_dictionary::<UInt16Type>()
            .downcast_dict::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        let file_id2 = batch2
            .column(1)
            .as_dictionary::<UInt16Type>()
            .downcast_dict::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();

        assert_eq!(file_id1, "f1");
        assert_eq!(file_id2, "f2");

        Ok(())
    }

    #[test]
    fn test_dv_short_mask_drain_and_pad() {
        use super::{DvMaskResult, consume_dv_mask};

        let mut sv = vec![true, false, true];
        let result = consume_dv_mask(&mut sv, 5);

        assert_eq!(
            result,
            DvMaskResult {
                selection: Some(vec![true, false, true, true, true]),
                should_remove: true,
            }
        );
        assert!(sv.is_empty());
    }

    #[test]
    fn test_dv_mask_exhaustion_across_batches() {
        use super::{DvMaskResult, consume_dv_mask};
        use dashmap::DashMap;

        let selection_vectors: DashMap<String, Vec<bool>> = DashMap::new();
        let file_id = "test_file.parquet".to_string();
        selection_vectors.insert(file_id.clone(), vec![false, true]);

        let result1 = {
            let mut sv = selection_vectors.get_mut(&file_id).unwrap();
            consume_dv_mask(&mut sv, 5)
        };
        assert_eq!(
            result1,
            DvMaskResult {
                selection: Some(vec![false, true, true, true, true]),
                should_remove: true,
            }
        );
        if result1.should_remove {
            selection_vectors.remove(&file_id);
        }

        let result2 = if let Some(mut sv) = selection_vectors.get_mut(&file_id) {
            consume_dv_mask(&mut sv, 5)
        } else {
            DvMaskResult {
                selection: None,
                should_remove: false,
            }
        };
        assert_eq!(
            result2,
            DvMaskResult {
                selection: None,
                should_remove: false,
            }
        );
    }

    #[test]
    fn test_dv_normal_mask_drains_exactly() {
        use super::{DvMaskResult, consume_dv_mask};

        let mut sv = vec![
            true, false, true, false, true, true, false, true, false, true,
        ];

        let result1 = consume_dv_mask(&mut sv, 3);
        assert_eq!(
            result1,
            DvMaskResult {
                selection: Some(vec![true, false, true]),
                should_remove: false,
            }
        );
        assert_eq!(sv.len(), 7);

        let result2 = consume_dv_mask(&mut sv, 3);
        assert_eq!(
            result2,
            DvMaskResult {
                selection: Some(vec![false, true, true]),
                should_remove: false,
            }
        );
        assert_eq!(sv, vec![false, true, false, true]);

        let result3 = consume_dv_mask(&mut sv, 5);
        assert_eq!(
            result3,
            DvMaskResult {
                selection: Some(vec![false, true, false, true, true]),
                should_remove: true,
            }
        );
        assert!(sv.is_empty());

        let result4 = consume_dv_mask(&mut sv, 5);
        assert_eq!(
            result4,
            DvMaskResult {
                selection: None,
                should_remove: true,
            }
        );
    }

    #[test]
    fn test_dv_long_mask_retains_remainder_for_next_batch() {
        use super::{DvMaskResult, consume_dv_mask};

        let mut sv = vec![true, false, false, true];
        let result = consume_dv_mask(&mut sv, 2);

        assert_eq!(
            result,
            DvMaskResult {
                selection: Some(vec![true, false]),
                should_remove: false,
            }
        );
        assert_eq!(sv, vec![false, true]);
    }
}
