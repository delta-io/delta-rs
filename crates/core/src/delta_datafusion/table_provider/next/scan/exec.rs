//! Physical execution for Delta table scans.
//!
//! This module implements [`DeltaScanExec`], the core execution plan that reads Parquet files
//! and applies Delta Lake protocol transformations to produce logical table data.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayAccessor, AsArray, RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{SchemaRef, UInt16Type};
use arrow_array::BooleanArray;
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

    pub(crate) fn delta_plan(&self) -> &KernelScanPlan {
        &self.scan_plan
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
/// Each batch is processed independently, with transformations looked up by file ID
/// from the batch's partition values.
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
}

impl DeltaScanStream {
    /// Apply the per-file transformation to a RecordBatch.
    fn batch_project(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(
                &self.scan_plan.output_schema,
            )));
        }

        let (file_id, file_id_idx) = extract_file_id(&batch, &self.file_id_column)?;

        let dv_result = if let Some(mut selection_vector) = self.selection_vectors.get_mut(&file_id)
        {
            consume_dv_mask(&mut selection_vector, batch.num_rows())
        } else {
            DvMaskResult {
                selection: None,
                should_remove: false,
            }
        };

        if dv_result.should_remove {
            self.selection_vectors.remove(&file_id);
        }

        let mut batch = if let Some(selection) = dv_result.selection {
            filter_record_batch(&batch, &BooleanArray::from(selection))?
        } else {
            batch
        };

        // NOTE: we remove the file id column after applying the selection vector
        // to get the correct number of rows in case we need to return source file ids later.
        let file_id_field = batch.schema_ref().field(file_id_idx).clone();
        let file_id_col = batch.remove_column(file_id_idx);

        let result = if let Some(transform) = self.transforms.get(&file_id) {
            let evaluator = ARROW_HANDLER
                .new_expression_evaluator(
                    self.scan_plan.scan.physical_schema().clone(),
                    transform.clone(),
                    self.kernel_type.clone(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            evaluator
                .evaluate_arrow(batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            batch
        };

        if self.return_file_ids {
            super::finalize_transformed_batch(
                result,
                &self.scan_plan,
                Some((file_id_col, Arc::new(file_id_field))),
            )
        } else {
            super::finalize_transformed_batch(result, &self.scan_plan, None)
        }
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
        Arc::clone(&self.scan_plan.output_schema)
    }
}

fn extract_file_id(batch: &RecordBatch, file_id_column: &str) -> Result<(String, usize)> {
    if batch.num_rows() == 0 {
        return Err(internal_datafusion_err!(
            "Cannot extract file_id from empty batch"
        ));
    }

    let file_id_idx = batch
        .schema_ref()
        .fields()
        .iter()
        .position(|f| f.name() == file_id_column)
        .ok_or_else(|| {
            internal_datafusion_err!(
                "Expected column '{}' to be present in the input",
                file_id_column
            )
        })?;

    let file_id = batch
        .column(file_id_idx)
        .as_dictionary::<UInt16Type>()
        .downcast_dict::<StringArray>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected file id column to be a dictionary of strings")
        })?
        .value(0)
        .to_string();

    Ok((file_id, file_id_idx))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use arrow_array::Array;
    use datafusion::{
        common::stats::Precision,
        physical_plan::{collect, collect_partitioned},
        prelude::{col, lit},
    };

    use super::*;
    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::session::create_session,
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
        assert_eq!(
            file_id_field.data_type(),
            &DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into())
        );

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
        assert_eq!(statistics.total_byte_size, Precision::Exact(3240));
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

    #[test]
    fn test_extract_file_id_empty_batch_returns_error() {
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "file_id",
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        )]));
        let empty_batch = RecordBatch::new_empty(schema);

        let result = super::extract_file_id(&empty_batch, "file_id");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot extract file_id from empty batch")
        );
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
}
