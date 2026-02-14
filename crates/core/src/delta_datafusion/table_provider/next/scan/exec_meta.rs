//! Metadata-only table scans for optimization.
//!
//! This module implements [`DeltaScanMetaExec`], which answers queries using only file
//! metadata and statistics, avoiding the cost of reading actual Parquet data files.

use std::any::Any;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{SchemaRef, UInt16Type};
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, RecordBatchOptions, StringArray, StringViewArray,
    UInt16Array,
};
use arrow_schema::{DataType, FieldRef, Fields, Schema};
use dashmap::DashMap;
use datafusion::common::config::ConfigOptions;
use datafusion::common::error::{DataFusionError, Result};
use datafusion::common::{HashMap, internal_datafusion_err};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, PlanProperties,
};
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, Statistics,
};
use delta_kernel::schema::{Schema as KernelSchema, SchemaRef as KernelSchemaRef};
use delta_kernel::{EvaluationHandler, ExpressionRef};
use futures::stream::Stream;
use itertools::Itertools as _;
use tracing::debug;

use crate::delta_datafusion::table_provider::next::KernelScanPlan;
use crate::kernel::ARROW_HANDLER;
use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt;

/// Optimized execution plan that uses only file metadata to answer queries.
///
/// This plan avoids reading Parquet data files by synthesizing results from file-level
/// metadata and statistics. It's automatically selected when possible for queries like:
///
/// - `SELECT COUNT(*) FROM table` - Uses row counts from file statistics
/// - `SELECT COUNT(*) FROM table WHERE partition_col = 'value'` - Counts matching files
/// - Queries projecting only partition columns with row count aggregates
///
/// # Benefits
///
/// - **Performance**: Orders of magnitude faster than reading data files
/// - **Cost**: Reduces I/O and compute for cloud storage scenarios
/// - **Scalability**: Handles large tables efficiently
///
/// # Limitations
///
/// Only works when:
/// - Query requires no actual column data (COUNT(*) or partition columns only)
/// - File statistics contain accurate row counts
/// - No deletion vectors affect the results (or they are accounted for)
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
    file_id_field: Option<FieldRef>,
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
                    self.file_id_field
                        .as_ref()
                        .map(|f| f.name().to_owned())
                        .unwrap_or("<none>".to_string())
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
        file_id_field: Option<FieldRef>,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(scan_plan.output_schema.clone()),
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
            file_id_field,
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
            dv_short_mask_padded_files_total: MetricBuilder::new(&self.metrics)
                .counter("dv_short_mask_padded_files_total", partition),
            transforms: Arc::clone(&self.transforms),
            selection_vectors: Arc::clone(&self.selection_vectors),
            file_id_field: self.file_id_field.clone(),
            schema_adapter: super::SchemaAdapter::new(Arc::clone(&self.scan_plan.result_schema)),
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

/// Stream that generates RecordBatches from file metadata without reading data files.
///
/// Synthesizes logical table rows by:
///
/// 1. Creating empty batches with row counts from file statistics
/// 2. Applying deletion vectors to adjust row counts
/// 3. Evaluating partition value expressions to materialize columns
/// 4. Casting results to the expected logical schema
///
/// Each file is processed independently, producing a batch with the appropriate
/// number of rows but no actual column data (unless partition columns are requested).
struct DeltaScanMetaStream {
    scan_plan: Arc<KernelScanPlan>,
    /// Input stream yielding raw data read from data files.
    input: VecDeque<(String, usize)>,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// Count of file batches where short deletion-vector masks required padding.
    dv_short_mask_padded_files_total: Count,
    /// Transforms to be applied to data read from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Selection vectors to be applied to data read from individual files
    selection_vectors: Arc<DashMap<String, Vec<bool>>>,
    /// Column name for the file id
    file_id_field: Option<FieldRef>,
    /// Cached schema adapter for efficient batch adaptation across batches
    schema_adapter: super::SchemaAdapter,
}

impl DeltaScanMetaStream {
    /// Apply the per-file transformation to a RecordBatch.
    fn batch_project(&mut self, file_id: String, row_count: usize) -> Result<RecordBatch> {
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
            let mask_len = selection.len();
            let (batch, padded_rows) = apply_selection_vector(batch, selection.value(), &file_id)?;
            if padded_rows > 0 {
                self.dv_short_mask_padded_files_total.add(1);
                debug!(
                    file_id = file_id.as_str(),
                    mask_len,
                    row_count,
                    padded_rows,
                    "Padded short deletion-vector keep-mask in metadata scan"
                );
            }
            batch
        } else {
            batch
        };

        let result = if let Some(transform) = self.transforms.get(&file_id) {
            let evaluator = ARROW_HANDLER
                .new_expression_evaluator(
                    EMPTY_KERNEL_SCHEMA.clone(),
                    transform.clone(),
                    self.scan_plan.scan.logical_schema().clone().into(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            evaluator
                .evaluate_arrow(batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            batch
        };

        if let Some(file_id_field) = &self.file_id_field {
            let row_count = result.num_rows();

            let keys = UInt16Array::from(vec![0u16; row_count]);
            let values: ArrayRef = match file_id_field.data_type() {
                DataType::Dictionary(_, value_type)
                    if value_type.as_ref() == &DataType::Utf8View =>
                {
                    if row_count == 0 {
                        Arc::new(StringViewArray::from_iter_values(std::iter::empty::<&str>()))
                    } else {
                        Arc::new(StringViewArray::from_iter_values([file_id.as_str()]))
                    }
                }
                _ => {
                    if row_count == 0 {
                        Arc::new(StringArray::from(Vec::<Option<&str>>::new()))
                    } else {
                        Arc::new(StringArray::from(vec![Some(file_id.as_str())]))
                    }
                }
            };

            let file_id_array: DictionaryArray<UInt16Type> =
                DictionaryArray::try_new(keys, values)?;
            super::finalize_transformed_batch(
                result,
                &self.scan_plan,
                Some((Arc::new(file_id_array), file_id_field.clone())),
                &mut self.schema_adapter,
            )
        } else {
            super::finalize_transformed_batch(
                result,
                &self.scan_plan,
                None,
                &mut self.schema_adapter,
            )
        }
    }
}

fn apply_selection_vector(
    batch: RecordBatch,
    selection: &[bool],
    file_id: &str,
) -> Result<(RecordBatch, usize)> {
    if selection.len() > batch.num_rows() {
        return Err(internal_datafusion_err!(
            "Selection vector length ({}) exceeds row count ({}) for file '{}'. \
             This indicates a bug in deletion vector processing.",
            selection.len(),
            batch.num_rows(),
            file_id
        ));
    }

    let n_rows_to_pad = batch.num_rows() - selection.len();
    // Delta Kernel may emit short keep-masks; missing trailing entries are
    // implicitly `true` (row is kept).
    let filter = BooleanArray::from_iter(
        selection
            .iter()
            .copied()
            .chain(std::iter::repeat_n(true, n_rows_to_pad)),
    );
    Ok((filter_record_batch(&batch, &filter)?, n_rows_to_pad))
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
        Arc::clone(&self.scan_plan.output_schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow_array::RecordBatchOptions;
    use arrow_schema::{Fields, Schema};
    use datafusion::{
        physical_plan::collect_partitioned,
        prelude::{col, lit},
    };

    use super::*;
    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::session::create_session,
        test_utils::{TestResult, open_fs_path},
    };

    #[test]
    fn test_apply_selection_vector_short_mask_pads_with_true() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(Fields::empty())),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(4)),
        )
        .unwrap();

        // Short masks are valid: missing trailing entries are implicitly true.
        let (filtered, padded_rows) =
            apply_selection_vector(batch, &[false, true], "file:///f.parquet").unwrap();
        assert_eq!(filtered.num_rows(), 3);
        assert_eq!(padded_rows, 2);
    }

    #[test]
    fn test_apply_selection_vector_no_padding_when_lengths_match() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(Fields::empty())),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();

        let (filtered, padded_rows) =
            apply_selection_vector(batch, &[true, false], "file:///f.parquet").unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(padded_rows, 0);
    }

    #[test]
    fn test_apply_selection_vector_longer_than_batch_errors() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(Fields::empty())),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();

        let err = apply_selection_vector(batch, &[true, true, true], "file:///f.parquet")
            .expect_err("selection vector longer than row count must error");
        assert!(
            err.to_string().contains("Selection vector length"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_meta_only_scan() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        let scan = provider
            .scan(
                &session.state(),
                Some(&vec![]),
                &[col("letter").eq(lit("b"))],
                None,
            )
            .await?;

        let downcast = scan.as_any().downcast_ref::<DeltaScanMetaExec>();
        assert!(downcast.is_some());

        session.register_table("delta_table", provider).unwrap();

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
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        let idx = provider.schema().index_of("data").unwrap();

        let scan = provider
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

        session.register_table("delta_table", provider).unwrap();
        let df = session
            .sql("SELECT data FROM delta_table WHERE letter = 'b'")
            .await
            .unwrap();
        let batches = df.collect().await?;
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_only_scan_with_file_id() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        session
            .register_table("delta_table", provider.clone())
            .unwrap();

        // Use COUNT(*) query which can be satisfied with metadata only
        let df = session
            .sql("SELECT COUNT(*) as count, file_id FROM delta_table WHERE letter = 'b' GROUP BY file_id")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await?;

        // The plan should contain a DeltaScanMetaExec somewhere in the tree
        let plan_str = format!("{:?}", plan);
        assert!(plan_str.contains("DeltaScanMetaExec"));

        let batches = df.collect().await?;
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);

        // Verify that file_id column is present
        assert!(batches[0].schema().column_with_name("file_id").is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_only_scan_with_file_id_count_all() -> TestResult {
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

        // Should have 2 groups (one for each file)
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_only_scan_with_file_id_projection() -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        // Select only data and file_id columns
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

        let downcast = scan.as_any().downcast_ref::<DeltaScanMetaExec>();
        assert!(downcast.is_some());

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
}
