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
use datafusion::common::{HashMap, internal_datafusion_err, stats::Precision};
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
    properties: Arc<PlanProperties>,
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
    fn make_properties(scan_plan: &KernelScanPlan, partition_count: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(scan_plan.output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    pub(super) fn new(
        scan_plan: Arc<KernelScanPlan>,
        input: Vec<VecDeque<(String, usize)>>,
        transforms: Arc<HashMap<String, ExpressionRef>>,
        selection_vectors: Arc<DashMap<String, Vec<bool>>>,
        file_id_field: Option<FieldRef>,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        let properties = Arc::new(Self::make_properties(scan_plan.as_ref(), input.len()));
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

    fn effective_row_count_for_file(&self, file_id: &str, row_count: usize) -> Result<usize> {
        if let Some(selection) = self.selection_vectors.get(file_id) {
            let (kept_rows, _) = effective_row_count(row_count, selection.value(), file_id)?;
            Ok(kept_rows)
        } else {
            Ok(row_count)
        }
    }

    fn exact_num_rows(&self, partition: Option<usize>) -> Result<usize> {
        match partition {
            Some(partition) => self.exact_num_rows_for_partition(partition),
            None => self
                .input
                .iter()
                .enumerate()
                .try_fold(0usize, |total, (partition, _)| {
                    let partition_rows = self.exact_num_rows_for_partition(partition)?;
                    total.checked_add(partition_rows).ok_or_else(|| {
                        DataFusionError::Execution(
                            "DeltaScanMetaExec exact row count overflow".to_string(),
                        )
                    })
                }),
        }
    }

    fn exact_num_rows_for_partition(&self, partition: usize) -> Result<usize> {
        let files = self.input.get(partition).ok_or_else(|| {
            DataFusionError::Plan(format!("DeltaScanMetaExec: invalid partition {partition}"))
        })?;

        files
            .iter()
            .try_fold(0usize, |total, (file_id, row_count)| {
                let file_rows = self.effective_row_count_for_file(file_id, *row_count)?;
                total.checked_add(file_rows).ok_or_else(|| {
                    DataFusionError::Execution(
                        "DeltaScanMetaExec exact row count overflow".to_string(),
                    )
                })
            })
    }
}

impl ExecutionPlan for DeltaScanMetaExec {
    fn name(&self) -> &'static str {
        "DeltaScanMetaExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        let properties = Arc::new(Self::make_properties(
            self.scan_plan.as_ref(),
            new_input.len(),
        ));

        Ok(Some(Arc::new(Self {
            properties,
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Exact(self.exact_num_rows(partition)?),
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(self.schema().as_ref()),
        })
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
    let (_, n_rows_to_pad) = effective_row_count(batch.num_rows(), selection, file_id)?;
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

fn effective_row_count(
    row_count: usize,
    selection: &[bool],
    file_id: &str,
) -> Result<(usize, usize)> {
    if selection.len() > row_count {
        return Err(internal_datafusion_err!(
            "Selection vector length ({}) exceeds row count ({}) for file '{}'. \
             This indicates a bug in deletion vector processing.",
            selection.len(),
            row_count,
            file_id
        ));
    }

    let n_rows_to_pad = row_count - selection.len();
    let kept_rows = selection.iter().filter(|keep| **keep).count() + n_rows_to_pad;
    Ok((kept_rows, n_rows_to_pad))
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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow_array::{Int64Array, RecordBatchOptions, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion::{
        catalog::TableProvider,
        common::stats::Precision,
        physical_plan::collect_partitioned,
        physical_plan::displayable,
        prelude::{col, lit},
    };
    use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
    use futures::TryStreamExt as _;

    use super::*;
    use crate::{
        DeltaTable, assert_batches_sorted_eq,
        delta_datafusion::session::create_session,
        ensure_table_uri,
        protocol::SaveMode,
        test_utils::{TestResult, TestTables, open_fs_path},
    };

    const REPRO_PARTITION_COUNT: usize = 11;
    const REPRO_ROWS_PER_PARTITION: usize = 100;
    const REPRO_TOTAL_ROWS: usize = REPRO_PARTITION_COUNT * REPRO_ROWS_PER_PARTITION;
    const TWO_COMMIT_INITIAL_ROWS_PER_PARTITION: usize = 1;
    const TWO_COMMIT_APPEND_ROWS_PER_PARTITION: usize =
        REPRO_ROWS_PER_PARTITION - TWO_COMMIT_INITIAL_ROWS_PER_PARTITION;

    fn repro_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("part", DataType::Utf8, false),
        ]))
    }

    fn make_partition_batch(partition: usize, row_count: usize) -> RecordBatch {
        let start = (partition * REPRO_ROWS_PER_PARTITION) as i64;
        let ids = Int64Array::from_iter_values(start..start + row_count as i64);
        let part = format!("p{partition:02}");
        let parts = StringArray::from(vec![part; row_count]);
        RecordBatch::try_new(repro_schema(), vec![Arc::new(ids), Arc::new(parts)]).unwrap()
    }

    fn make_all_partitions_batch(start_round: usize, round_count: usize) -> RecordBatch {
        let rows = (start_round..start_round + round_count).flat_map(|round| {
            (0..REPRO_PARTITION_COUNT).map(move |partition| {
                let row_id = round * REPRO_PARTITION_COUNT + partition;
                (row_id as i64, format!("p{partition:02}"))
            })
        });
        let (ids, parts): (Vec<_>, Vec<_>) = rows.unzip();
        RecordBatch::try_new(
            repro_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(parts)),
            ],
        )
        .unwrap()
    }

    async fn create_partitioned_repro_table() -> TestResult<(tempfile::TempDir, DeltaTable)> {
        let table_dir = tempfile::tempdir()?;
        let table_uri = ensure_table_uri(table_dir.path().to_str().unwrap())?;
        let table_schema: crate::kernel::StructType = repro_schema().try_into_kernel()?;

        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .create()
            .with_save_mode(SaveMode::Ignore)
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(vec!["part".to_string()])
            .await?;

        Ok((table_dir, table))
    }

    async fn write_single_file_per_partition_repro_table()
    -> TestResult<(tempfile::TempDir, DeltaTable)> {
        let (table_dir, mut table) = create_partitioned_repro_table().await?;
        for partition in 0..REPRO_PARTITION_COUNT {
            table = table
                .write(vec![make_partition_batch(
                    partition,
                    REPRO_ROWS_PER_PARTITION,
                )])
                .with_save_mode(SaveMode::Append)
                .await?;
        }
        Ok((table_dir, table))
    }

    async fn write_two_commit_two_files_per_partition_repro_table()
    -> TestResult<(tempfile::TempDir, DeltaTable)> {
        let (table_dir, table) = create_partitioned_repro_table().await?;
        let table = table
            .write(vec![make_all_partitions_batch(
                0,
                TWO_COMMIT_INITIAL_ROWS_PER_PARTITION,
            )])
            .with_save_mode(SaveMode::Append)
            .await?;
        let table = table
            .write(vec![make_all_partitions_batch(
                TWO_COMMIT_INITIAL_ROWS_PER_PARTITION,
                TWO_COMMIT_APPEND_ROWS_PER_PARTITION,
            )])
            .with_save_mode(SaveMode::Append)
            .await?;
        Ok((table_dir, table))
    }

    async fn add_file_diagnostics(table: &DeltaTable) -> TestResult<Vec<(String, Option<usize>)>> {
        let state = table.snapshot()?;
        let log_store = table.log_store();
        Ok(state
            .snapshot()
            .file_views(log_store.as_ref(), None)
            .map_ok(|view| (view.path_raw().to_string(), view.num_records()))
            .try_collect()
            .await?)
    }

    async fn add_file_dv_diagnostics(
        table: &DeltaTable,
    ) -> TestResult<Vec<(String, Option<usize>, usize)>> {
        let state = table.snapshot()?;
        let log_store = table.log_store();
        Ok(state
            .snapshot()
            .file_views(log_store.as_ref(), None)
            .map_ok(|view| {
                (
                    view.path_raw().to_string(),
                    view.num_records(),
                    view.deletion_vector_descriptor()
                        .map(|dv| dv.cardinality as usize)
                        .unwrap_or_default(),
                )
            })
            .try_collect()
            .await?)
    }

    fn format_add_file_diagnostics(files: &[(String, Option<usize>)]) -> String {
        files
            .iter()
            .map(|(path, num_records)| format!("{path} => numRecords={num_records:?}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn format_add_file_dv_diagnostics(files: &[(String, Option<usize>, usize)]) -> String {
        files
            .iter()
            .map(|(path, num_records, deleted_rows)| {
                format!("{path} => numRecords={num_records:?}, deletedRows={deleted_rows}")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn active_files_per_partition(files: &[(String, Option<usize>)]) -> BTreeMap<String, usize> {
        files.iter().fold(BTreeMap::new(), |mut counts, (path, _)| {
            let partition = path
                .split('/')
                .find(|segment| segment.starts_with("part="))
                .unwrap_or(path)
                .to_string();
            *counts.entry(partition).or_default() += 1;
            counts
        })
    }

    fn read_single_i64(batch: &RecordBatch, column_name: &str) -> i64 {
        batch
            .column_by_name(column_name)
            .unwrap_or_else(|| panic!("missing column '{column_name}'"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column '{column_name}' was not Int64"))
            .value(0)
    }

    async fn assert_meta_only_count_results(
        provider: Arc<dyn TableProvider>,
        expected_row_count: usize,
        diagnostics: &str,
    ) -> TestResult {
        let session = Arc::new(create_session().into_inner());
        let empty_projection = vec![];
        let scan = provider
            .scan(&session.state(), Some(&empty_projection), &[], None)
            .await?;
        assert!(
            scan.as_any().downcast_ref::<DeltaScanMetaExec>().is_some(),
            "expected metadata-only scan\n{diagnostics}"
        );
        assert_eq!(
            scan.partition_statistics(None)?.num_rows,
            Precision::Exact(expected_row_count),
            "metadata-only scan reported the wrong row count\n{diagnostics}"
        );

        session
            .register_table("delta_table", Arc::clone(&provider))
            .unwrap();

        let sql_batches = session
            .sql("SELECT COUNT(*) AS row_count FROM delta_table")
            .await?
            .collect()
            .await?;
        let sql_count = read_single_i64(&sql_batches[0], "row_count");
        assert_eq!(
            sql_count, expected_row_count as i64,
            "SELECT COUNT(*) returned the wrong row count\n{diagnostics}"
        );

        let dataframe_count = session
            .sql("SELECT * FROM delta_table")
            .await?
            .count()
            .await?;
        assert_eq!(
            dataframe_count, expected_row_count,
            "DataFrame.count() returned the wrong row count\n{diagnostics}"
        );

        Ok(())
    }

    async fn assert_count_query_uses_placeholder_row_exec(
        provider: Arc<dyn TableProvider>,
        diagnostics: &str,
    ) -> TestResult {
        let session = Arc::new(create_session().into_inner());
        session.register_table("delta_table", provider).unwrap();

        let sql_plan = session
            .sql("SELECT COUNT(*) AS row_count FROM delta_table")
            .await?
            .create_physical_plan()
            .await?;
        let sql_plan_str = displayable(sql_plan.as_ref()).indent(true).to_string();
        assert!(
            sql_plan_str.contains("PlaceholderRowExec"),
            "expected COUNT(*) to optimize to PlaceholderRowExec\nPlan:\n{sql_plan_str}\n{diagnostics}"
        );

        Ok(())
    }

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

    #[tokio::test]
    async fn test_meta_only_count_on_partitioned_written_table_counts_rows_not_files() -> TestResult
    {
        let (_table_dir, table) = write_single_file_per_partition_repro_table().await?;
        let add_files = add_file_diagnostics(&table).await?;
        let add_file_diag = format_add_file_diagnostics(&add_files);
        let total_num_records: usize = add_files
            .iter()
            .map(|(_, rows)| rows.unwrap_or_default())
            .sum();

        assert_eq!(
            add_files.len(),
            REPRO_PARTITION_COUNT,
            "expected one active Add file per partition\n{add_file_diag}"
        );
        assert!(
            add_files.iter().all(|(_, rows)| rows.is_some()),
            "expected numRecords stats on every Add file\n{add_file_diag}"
        );
        assert_eq!(
            total_num_records, REPRO_TOTAL_ROWS,
            "expected Add file numRecords to sum to the written row count\n{add_file_diag}"
        );

        let provider = table.table_provider().await?;
        assert_meta_only_count_results(provider, REPRO_TOTAL_ROWS, &add_file_diag).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_only_count_on_two_commit_partitioned_table_uses_placeholder_row_exec()
    -> TestResult {
        let (_table_dir, table) = write_two_commit_two_files_per_partition_repro_table().await?;
        let add_files = add_file_diagnostics(&table).await?;
        let add_file_diag = format_add_file_diagnostics(&add_files);
        let total_num_records: usize = add_files
            .iter()
            .map(|(_, rows)| rows.unwrap_or_default())
            .sum();
        let files_per_partition = active_files_per_partition(&add_files);

        assert_eq!(
            add_files.len(),
            REPRO_PARTITION_COUNT * 2,
            "expected two active Add files per partition across two commits\n{add_file_diag}"
        );
        assert!(
            files_per_partition.values().all(|count| *count == 2),
            "expected exactly two active files per partition\n{files_per_partition:#?}\n{add_file_diag}"
        );
        assert!(
            add_files.iter().all(|(_, rows)| rows.is_some()),
            "expected numRecords stats on every Add file\n{add_file_diag}"
        );
        assert_eq!(
            total_num_records, REPRO_TOTAL_ROWS,
            "expected Add file numRecords to sum to 1100\n{add_file_diag}"
        );

        let provider = table.table_provider().await?;
        assert_count_query_uses_placeholder_row_exec(Arc::clone(&provider), &add_file_diag).await?;
        assert_meta_only_count_results(provider, REPRO_TOTAL_ROWS, &add_file_diag).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_only_count_on_table_with_deletion_vectors_uses_exact_stats() -> TestResult {
        let table = TestTables::WithDvSmall.table_builder()?.load().await?;
        let add_files = add_file_dv_diagnostics(&table).await?;
        let add_file_diag = format_add_file_dv_diagnostics(&add_files);

        assert!(
            add_files.iter().all(|(_, rows, _)| rows.is_some()),
            "expected numRecords stats on every Add file\n{add_file_diag}"
        );
        assert!(
            add_files
                .iter()
                .any(|(_, _, deleted_rows)| *deleted_rows > 0),
            "expected at least one file with deleted rows\n{add_file_diag}"
        );

        let expected_effective_rows: usize = add_files
            .iter()
            .map(|(_, rows, deleted_rows)| {
                rows.unwrap_or_default()
                    .checked_sub(*deleted_rows)
                    .expect("deletion vector cardinality must not exceed numRecords")
            })
            .sum();

        let provider = table.table_provider().await?;
        assert_meta_only_count_results(provider, expected_effective_rows, &add_file_diag).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_partition_statistics_reports_exact_rows_per_exec_partition_after_repartition()
    -> TestResult {
        let (_table_dir, table) = write_single_file_per_partition_repro_table().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let empty_projection = vec![];
        let scan = provider
            .scan(&session.state(), Some(&empty_projection), &[], None)
            .await?;
        let template = scan
            .as_any()
            .downcast_ref::<DeltaScanMetaExec>()
            .expect("expected metadata-only scan");

        let selection_vectors: Arc<DashMap<String, Vec<bool>>> = Arc::new(DashMap::new());
        selection_vectors.insert("f2".to_string(), vec![true, false, true, false]);

        let exec = DeltaScanMetaExec::new(
            Arc::clone(&template.scan_plan),
            vec![VecDeque::from([
                ("f1".to_string(), 10usize),
                ("f2".to_string(), 4usize),
                ("f3".to_string(), 6usize),
                ("f4".to_string(), 2usize),
            ])],
            Arc::clone(&template.transforms),
            selection_vectors,
            template.file_id_field.clone(),
            ExecutionPlanMetricsSet::new(),
        );

        let repartitioned = exec
            .repartitioned(2, &ConfigOptions::default())?
            .expect("expected repartitioned metadata scan");
        assert_eq!(
            repartitioned
                .properties()
                .output_partitioning()
                .partition_count(),
            2
        );

        let repartitioned = repartitioned
            .as_any()
            .downcast_ref::<DeltaScanMetaExec>()
            .expect("expected repartitioned DeltaScanMetaExec");
        assert_eq!(repartitioned.input.len(), 2);
        assert_eq!(
            repartitioned.partition_statistics(Some(0))?.num_rows,
            Precision::Exact(12)
        );
        assert_eq!(
            repartitioned.partition_statistics(Some(1))?.num_rows,
            Precision::Exact(8)
        );
        assert_eq!(
            repartitioned.partition_statistics(None)?.num_rows,
            Precision::Exact(20)
        );

        Ok(())
    }
}
