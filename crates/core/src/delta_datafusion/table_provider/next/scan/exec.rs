//! Physical execution for Delta table scans.
//!
//! This module implements [`DeltaScanExec`], the core execution plan that reads Parquet files
//! and applies Delta Lake protocol transformations to produce logical table data.

use std::collections::{HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{FieldRef, Schema, SchemaRef, UInt16Type};
use arrow_array::StringViewArray;
use arrow_array::{Array, ArrayRef, UInt64Array};
use datafusion::common::config::ConfigOptions;
use datafusion::common::error::{DataFusionError, Result};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    ColumnStatistics, HashMap, internal_datafusion_err, internal_err, plan_err,
};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, PlanProperties};
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, PhysicalExpr, ReplaceChildrenOptions, Statistics,
    coalesce_partitions::CoalescePartitionsExec, union::UnionExec,
};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::{file_scan_config::FileScanConfig, source::DataSourceExec};
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{EvaluationHandler, ExpressionRef};
use futures::stream::{Stream, StreamExt};

use super::deletion_vector::DeletionVectorIndex;
use super::expr_adapter::DeltaPhysicalExprAdapterFactory;
use super::plan::KernelScanPlan;
use crate::delta_datafusion::file_id::file_id_field;
use crate::kernel::ARROW_HANDLER;
use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt;

const DELTA_MATERIALIZED_PUSHDOWN_SENTINEL: &str =
    "__delta_rs_unpushable_delta_materialized_filter";

/// Physical execution plan for scanning Delta tables.
///
/// Wraps a Parquet reader execution plan and applies Delta Lake protocol transformations
/// to produce the logical table data. This includes:
///
/// - **Column mapping**: Translates physical column names to logical names
/// - **Partition values**: Materializes partition column values from file paths
/// - **Deletion vectors**: Filters deleted rows by immutable physical Parquet row position
/// - **Schema evolution**: Handles missing columns and type coercion
///
/// # Data Flow
///
/// 1. Inner [`input`](Self::input) plan reads raw Parquet data
/// 2. Per-file [`transforms`](Self::transforms) convert physical to logical schema
/// 3. [`deletion_vectors`](Self::deletion_vectors) filter deleted rows
/// 4. Result is cast to the projected scan contract's result schema
#[derive(Clone, Debug)]
pub struct DeltaScanExec {
    scan_plan: Arc<KernelScanPlan>,
    /// Execution plan yielding the raw data read from data files.
    input: Arc<dyn ExecutionPlan>,
    /// Transforms to be applied to data eminating from individual files
    transforms: Arc<HashMap<String, ExpressionRef>>,
    /// Immutable deletion vectors keyed by compact scan file id.
    deletion_vectors: Arc<DeletionVectorIndex>,
    /// Public file paths keyed by compact scan file id.
    public_file_ids: Arc<super::PublicFileIdMap>,
    /// Expected physical file ownership keyed by compact scan file id.
    physical_file_identities: Arc<super::PhysicalFileIdentityMap>,
    /// Authoritative hidden physical input schema and resolved support-column indices.
    physical_input_contract: Arc<super::PhysicalInputContract>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// User-visible file-id column name when projected in the output.
    file_id_column: Option<String>,
    /// plan properties
    properties: Arc<PlanProperties>,
    /// Aggregated partition column statistics
    partition_stats: HashMap<String, ColumnStatistics>,
}

pub(super) struct PhysicalScanContext {
    pub(super) deletion_vectors: Arc<DeletionVectorIndex>,
    pub(super) public_file_ids: Arc<super::PublicFileIdMap>,
    pub(super) physical_file_identities: Arc<super::PhysicalFileIdentityMap>,
    pub(super) physical_input_contract: Arc<super::PhysicalInputContract>,
}

impl DisplayAs for DeltaScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO: actually implement formatting according to the type
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DeltaScanExec")?;
                if let Some(file_id_column) = &self.file_id_column {
                    write!(f, ": file_id_column={file_id_column}")?;
                }
                if let Some(row_index_field) = self.scan_plan.contract.retained_row_index_field() {
                    write!(f, ": row_index_column={}", row_index_field.name())?;
                }
                write!(
                    f,
                    ": dv_mode={}, physical_row_index={}, file_group_layout_locked={}",
                    if self.has_deletion_vectors() {
                        "physical_position"
                    } else {
                        "none"
                    },
                    self.physical_input_contract
                        .physical_row_position_field
                        .is_some(),
                    self.has_deletion_vectors()
                )?;
                Ok(())
            }
        }
    }
}

impl DeltaScanExec {
    fn register_dv_file_metric(
        metrics: &ExecutionPlanMetricsSet,
        deletion_vectors: &DeletionVectorIndex,
    ) {
        if deletion_vectors.has_vectors() {
            MetricBuilder::new(metrics)
                .global_counter("dv_files")
                .add(deletion_vectors.file_count());
        }
    }

    pub(super) fn try_new(
        scan_plan: Arc<KernelScanPlan>,
        input: Arc<dyn ExecutionPlan>,
        transforms: Arc<HashMap<String, ExpressionRef>>,
        context: PhysicalScanContext,
        partition_stats: HashMap<String, ColumnStatistics>,
        metrics: ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let PhysicalScanContext {
            deletion_vectors,
            public_file_ids,
            physical_file_identities,
            physical_input_contract,
        } = context;
        Self::register_dv_file_metric(&metrics, &deletion_vectors);
        let file_id_column = scan_plan
            .contract
            .retain_file_id
            .then(|| scan_plan.contract.file_id_field.name().to_owned());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&scan_plan.contract.output_schema)),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(
                input.properties().partitioning.partition_count(),
            ),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        let exec = Self {
            scan_plan,
            input,
            transforms,
            deletion_vectors,
            public_file_ids,
            physical_file_identities,
            physical_input_contract,
            partition_stats,
            metrics,
            file_id_column,
            properties,
        };
        if exec.has_deletion_vectors() {
            exec.validate_dv_child_topology(&exec.input)?;
        }
        Ok(exec)
    }

    #[cfg(test)]
    fn new(
        scan_plan: Arc<KernelScanPlan>,
        input: Arc<dyn ExecutionPlan>,
        transforms: Arc<HashMap<String, ExpressionRef>>,
        context: PhysicalScanContext,
        stats: HashMap<String, ColumnStatistics>,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        Self::try_new(scan_plan, input, transforms, context, stats, metrics).unwrap()
    }

    fn with_new_input_same_properties(&self, input: Arc<dyn ExecutionPlan>) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        Self::register_dv_file_metric(&metrics, &self.deletion_vectors);
        let properties = self.properties.as_ref().clone().with_partitioning(
            datafusion::physical_expr::Partitioning::UnknownPartitioning(
                input.properties().partitioning.partition_count(),
            ),
        );
        Self {
            input,
            metrics,
            properties: Arc::new(properties),
            ..Self::clone(self)
        }
    }

    fn with_new_input(&self, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        Self::try_new(
            Arc::clone(&self.scan_plan),
            input,
            Arc::clone(&self.transforms),
            PhysicalScanContext {
                deletion_vectors: Arc::clone(&self.deletion_vectors),
                public_file_ids: Arc::clone(&self.public_file_ids),
                physical_file_identities: Arc::clone(&self.physical_file_identities),
                physical_input_contract: Arc::clone(&self.physical_input_contract),
            },
            self.partition_stats.clone(),
            ExecutionPlanMetricsSet::new(),
        )
    }

    fn has_deletion_vectors(&self) -> bool {
        self.deletion_vectors.has_vectors()
    }

    pub(super) fn validate_dv_child_topology(&self, input: &Arc<dyn ExecutionPlan>) -> Result<()> {
        fn scalar_file_id(value: &ScalarValue) -> Option<&str> {
            match value {
                ScalarValue::Dictionary(_, value) => scalar_file_id(value),
                ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                    Some(value.as_str())
                }
                _ => None,
            }
        }

        fn visit(
            plan: &Arc<dyn ExecutionPlan>,
            expected: &super::PhysicalFileIdentityMap,
            observed: &mut HashSet<String>,
            contract: &super::PhysicalInputContract,
        ) -> Result<()> {
            if let Some(fetch) = plan.fetch() {
                return plan_err!(
                    "DeltaScanExec rejects child fetch limit {fetch} during sequential deletion vector scans"
                );
            }
            if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
                return visit(coalesce.input(), expected, observed, contract);
            }
            if let Some(union) = plan.downcast_ref::<UnionExec>() {
                for input in union.inputs() {
                    visit(input, expected, observed, contract)?;
                }
                return Ok(());
            }
            let Some(source_exec) = plan.downcast_ref::<DataSourceExec>() else {
                return plan_err!(
                    "DeltaScanExec deletion-vector topology contains unsupported node {}",
                    plan.name()
                );
            };
            let Some(config) = source_exec.data_source().downcast_ref::<FileScanConfig>() else {
                return plan_err!(
                    "DeltaScanExec deletion-vector topology requires FileScanConfig leaves"
                );
            };
            let source = config
                .file_source
                .downcast_ref::<datafusion::datasource::physical_plan::ParquetSource>()
                .ok_or_else(|| internal_datafusion_err!("expected native Parquet source"))?;
            let expected_source = contract
                .sources
                .iter()
                .find(|planned| planned.object_store_url == config.object_store_url)
                .ok_or_else(|| internal_datafusion_err!("unplanned store binding"))?;
            let original = expected_source
                .file_source
                .downcast_ref::<datafusion::datasource::physical_plan::ParquetSource>()
                .ok_or_else(|| internal_datafusion_err!("lost planned Parquet source"))?;
            let same_reader = source
                .parquet_file_reader_factory()
                .zip(original.parquet_file_reader_factory())
                .is_some_and(|(a, b)| Arc::ptr_eq(a, b));
            let same_adapter = config
                .expr_adapter_factory
                .as_ref()
                .zip(expected_source.expr_adapter_factory.as_ref())
                .is_some_and(|(a, b)| Arc::ptr_eq(a, b));
            if !Arc::ptr_eq(&config.file_source, &expected_source.file_source)
                || !same_reader
                || !same_adapter
                || plan.schema() != *contract.table_schema.table_schema()
            {
                return plan_err!(
                    "physical input reader, adapter, or support-field lineage changed"
                );
            }
            if !matches!(
                config.output_partitioning,
                Some(datafusion::physical_expr::Partitioning::UnknownPartitioning(partitions))
                    if partitions == config.file_groups.len()
            ) {
                return plan_err!(
                    "DeltaScanExec requires a locked file group layout during sequential deletion vector scans"
                );
            }
            if config.file_source.filter().is_some() {
                return plan_err!(
                    "DeltaScanExec rejects file source filters during sequential deletion vector scans"
                );
            }

            for group in &config.file_groups {
                for file in group.iter() {
                    if !file.extensions.is_empty() {
                        return plan_err!("unplanned reader-affecting file extensions");
                    }
                    let Some(file_id) = file.partition_values.first().and_then(scalar_file_id)
                    else {
                        return plan_err!(
                            "DeltaScanExec deletion-vector file is missing a compact file id"
                        );
                    };
                    let planned_file = contract
                        .planned_files
                        .get(file_id)
                        .ok_or_else(|| internal_datafusion_err!("unplanned partition constants"))?;
                    if planned_file.partition_values != file.partition_values {
                        return plan_err!("unplanned partition constants");
                    }
                    if planned_file.object_meta != file.object_meta
                        || planned_file.arrow_schema != file.arrow_schema
                        || planned_file.statistics != file.statistics
                        || planned_file.ordering != file.ordering
                        || planned_file.metadata_size_hint != file.metadata_size_hint
                        || planned_file.table_reference != file.table_reference
                    {
                        return plan_err!("immutable planned file metadata changed");
                    }
                    if file.range.is_some() {
                        return plan_err!(
                            "DeltaScanExec deletion-vector file id '{file_id}' has a byte range"
                        );
                    }
                    let Some(expected_file) = expected.get(file_id) else {
                        return plan_err!(
                            "DeltaScanExec deletion-vector topology contains unknown file id '{file_id}'"
                        );
                    };
                    if expected_file.object_store_url != config.object_store_url
                        || expected_file.location != file.object_meta.location
                    {
                        return plan_err!(
                            "DeltaScanExec deletion-vector topology has an inconsistent mapping for file id '{file_id}'"
                        );
                    }
                    if !observed.insert(file_id.to_owned()) {
                        return plan_err!(
                            "DeltaScanExec deletion-vector topology has duplicate ownership for file id '{file_id}'"
                        );
                    }
                }
            }
            Ok(())
        }

        let mut observed = HashSet::new();
        visit(
            input,
            &self.physical_file_identities,
            &mut observed,
            &self.physical_input_contract,
        )?;
        if observed.len() != self.physical_file_identities.len() {
            return plan_err!("DeltaScanExec deletion-vector topology is missing selected files");
        }
        Ok(())
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
                    let physical = logical.make_physical(config.column_mapping_mode()).ok()?;
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
        if self.has_deletion_vectors() {
            // Child statistics describe physical Parquet rows. This node reports conservative
            // bounds unless the DV index contains numRecords.
            stats = stats.to_inexact();
        }
        Ok(stats)
    }

    /// The default physical expr adapter rewrites missing nullable columns to null literals.
    /// That rewrite supports schema evolution. Delta materialized columns live above the
    /// Parquet child, including partition values, file id, and row index. Dynamic filters
    /// on those columns must stay bound to this exec's output schema; rewriting them
    /// against the Parquet child can turn them into null literals and drop rows.
    fn references_delta_materialized_column(&self, filter: &Arc<dyn PhysicalExpr>) -> bool {
        let input_schema = self.input.schema();
        collect_columns(filter).iter().any(|column| {
            if self.file_id_column.as_deref() == Some(column.name()) {
                return true;
            }
            if self
                .scan_plan
                .contract
                .retained_row_index_field()
                .as_ref()
                .is_some_and(|field| field.name() == column.name())
            {
                return true;
            }
            self.scan_plan
                .contract
                .result_schema
                .field_with_name(column.name())
                .is_ok()
                && input_schema.field_with_name(column.name()).is_err()
        })
    }
}

impl ExecutionPlan for DeltaScanExec {
    fn name(&self) -> &'static str {
        "DeltaScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        if self.scan_plan.contract.retained_row_index_field().is_some()
            || self.has_deletion_vectors()
        {
            // Retained row indexes require ordered, stream-local state. Physical-position DVs
            // retain the PR 0 single-partition containment until the PR 2 optimizer unlock.
            InputDistributionRequirements::new(vec![Distribution::SinglePartition])
        } else {
            InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
        }
    }

    // TODO: setting this will fail certain tests, but why
    // fn maintains_input_order(&self) -> Vec<bool> {
    //     vec![true]
    // }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("DeltaScan: wrong number of children {}", children.len());
        }
        let input = children.remove(0);
        if self.has_deletion_vectors() {
            self.validate_dv_child_topology(&input)?;
        }
        match options.children_properties {
            ChildrenPropertiesMode::Keep => {
                Ok(Arc::new(self.with_new_input_same_properties(input)))
            }
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(self.with_new_input(input)?)),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if self.scan_plan.contract.retained_row_index_field().is_some()
            || self.has_deletion_vectors()
        {
            // Row ordinals retain stream-local counters. Physical-position DVs retain the PR 0
            // repartition barrier until the PR 2 optimizer unlock is qualified.
            return Ok(None);
        }

        if let Some(input) = self.input.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(self.with_new_input(input)?)))
        } else {
            Ok(None)
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Normal planning enforces this through EnforceDistribution. Keep this check for
        // callers that build DeltaScanExec directly or replace its child plan.
        let retains_row_index = self.scan_plan.contract.retained_row_index_field().is_some();
        let has_deletion_vectors = self.has_deletion_vectors();
        if has_deletion_vectors {
            self.validate_dv_child_topology(&self.input)?;
        }
        if retains_row_index || has_deletion_vectors {
            let input_partition_count = self.input.properties().partitioning.partition_count();
            if input_partition_count > 1 {
                if retains_row_index {
                    return plan_err!(
                        "DeltaScanExec retained row indexes require a single input partition, got {input_partition_count}"
                    );
                }
                return plan_err!(
                    "DeltaScanExec contained deletion-vector scans require a single input partition, got {input_partition_count}"
                );
            }
        }

        let dv_rows_checked =
            MetricBuilder::new(&self.metrics).counter("dv_rows_checked", partition);
        let dv_rows_removed =
            MetricBuilder::new(&self.metrics).counter("dv_rows_removed", partition);

        Ok(Box::pin(DeltaScanStream {
            scan_plan: Arc::clone(&self.scan_plan),
            kernel_type: Arc::clone(self.scan_plan.scan.logical_schema()).into(),
            input: self.input.execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            transforms: Arc::clone(&self.transforms),
            deletion_vectors: Arc::clone(&self.deletion_vectors),
            physical_input_contract: Arc::clone(&self.physical_input_contract),
            dv_rows_checked,
            dv_rows_removed,
            public_file_ids: Arc::clone(&self.public_file_ids),
            file_id_column: self.file_id_column.clone(),
            row_index_field: self.scan_plan.contract.retained_row_index_field(),
            row_index_by_file: HashMap::new(),
            pending: VecDeque::new(),
            schema_adapter: super::SchemaAdapter::new(Arc::clone(
                &self.scan_plan.contract.result_schema,
            )),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn supports_limit_pushdown(&self) -> bool {
        !self.has_deletion_vectors() && self.input.supports_limit_pushdown()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        if self.has_deletion_vectors() {
            CardinalityEffect::LowerEqual
        } else {
            CardinalityEffect::Equal
        }
    }

    fn fetch(&self) -> Option<usize> {
        if self.has_deletion_vectors() {
            None
        } else {
            self.input.fetch()
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if self.has_deletion_vectors() {
            return None;
        }
        let new_input = self.input.with_fetch(limit)?;
        Some(Arc::new(self.with_new_input_same_properties(new_input)))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        // We remap the child's column statistics onto the logical output schema in
        // `map_statistics`, so we need the child's statistics resolved.
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let stats = input_stats.first().ok_or_else(|| {
            internal_datafusion_err!("DeltaScanExec expects statistics for exactly one child")
        })?;
        self.map_statistics(Statistics::clone(stats)).map(Arc::new)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        if self.has_deletion_vectors() {
            return Ok(FilterDescription::all_unsupported(
                &parent_filters,
                &self.children(),
            ));
        }

        // Parent filters are bound against the logical output schema. For column mapped tables
        // the child parquet schema uses physical column names, so pushing the parent filter
        // through this exec again can rewrite it against the wrong child field. Provider level
        // predicate planning already handles the safe parquet pushdown path for these tables.
        if self
            .scan_plan
            .table_configuration()
            .is_feature_enabled(&TableFeature::ColumnMapping)
        {
            return Ok(FilterDescription::all_unsupported(
                &parent_filters,
                &self.children(),
            ));
        }

        let adapter_factory = DeltaPhysicalExprAdapterFactory;
        let adapted_filters = adapter_factory
            .create(
                Arc::clone(&self.scan_plan.contract.result_schema),
                self.input.schema(),
            )
            .and_then(|adapter| {
                parent_filters
                    .iter()
                    .map(|filter| {
                        if self.references_delta_materialized_column(filter) {
                            // DataFusion has no public API for mixed parent filter support.
                            // Pass an impossible child column to `from_children`; DataFusion
                            // reports this parent filter as unsupported.
                            Ok(Arc::new(Column::new(
                                DELTA_MATERIALIZED_PUSHDOWN_SENTINEL,
                                usize::MAX,
                            )) as Arc<dyn PhysicalExpr>)
                        } else {
                            adapter.rewrite(Arc::clone(filter))
                        }
                    })
                    .collect::<Result<Vec<_>>>()
            });

        match adapted_filters {
            Ok(filters) => FilterDescription::from_children(filters, &self.children()),
            Err(_) => Ok(FilterDescription::all_unsupported(
                &parent_filters,
                &self.children(),
            )),
        }
    }

    fn apply_expressions(
        &self,
        _expr_rewriter: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
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
    /// Immutable deletion-vector lookup by physical row position.
    deletion_vectors: Arc<DeletionVectorIndex>,
    physical_input_contract: Arc<super::PhysicalInputContract>,
    dv_rows_checked: Count,
    dv_rows_removed: Count,
    /// Public file paths keyed by compact scan file id.
    public_file_ids: Arc<super::PublicFileIdMap>,
    /// File id column name carried by the input batches for per file correlation.
    /// User-visible file-id column name when projected in the output.
    file_id_column: Option<String>,
    /// Row index field included in projected output.
    row_index_field: Option<FieldRef>,
    /// Per file ordinal state for this execution partition.
    ///
    /// `DataSourceExec` assigns whole `PartitionedFile`s to file groups. Each physical file has
    /// one scan stream partition owner.
    row_index_by_file: HashMap<String, u64>,
    pending: VecDeque<RecordBatch>,
    /// Cached schema adapter for efficient batch adaptation across batches
    schema_adapter: super::SchemaAdapter,
}

impl DeltaScanStream {
    fn batch_project(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        // Clone the metric so the timer guard does not immutably borrow `self`,
        // which would conflict with the `&mut self` calls below.
        let elapsed = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed.timer();

        let file_id_idx = self.physical_input_contract.file_id_index;
        if batch.schema() != *self.physical_input_contract.table_schema.table_schema() {
            return internal_err!(
                "physical input schema does not preserve compact file-id field '{}' and its support fields",
                self.physical_input_contract.file_id_field.name()
            );
        }
        if batch.num_rows() == 0 {
            return Ok(vec![RecordBatch::new_empty(self.schema())]);
        }
        let file_runs = split_by_file_id_runs(&batch, file_id_idx)?;

        let mut results = Vec::with_capacity(file_runs.len());
        for (file_id, slice) in file_runs {
            results.push(self.batch_project_single_file(slice, file_id, file_id_idx)?);
        }
        Ok(results)
    }

    fn batch_project_single_file(
        &mut self,
        batch: RecordBatch,
        file_id: String,
        file_id_idx: usize,
    ) -> Result<RecordBatch> {
        let input_row_count = batch.num_rows();
        let has_deletion_vector = self.deletion_vectors.has_vector(&file_id);
        let mut batch = if self
            .physical_input_contract
            .physical_row_position_index
            .is_some()
        {
            let position_idx = self
                .physical_input_contract
                .physical_row_position_index
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "physical-position DV mode is missing its row-position field"
                    )
                })?;
            let positions = batch
                .column(position_idx)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "physical row-position column is not a nullable Int64 array"
                    )
                })?;
            if !has_deletion_vector {
                let count = self
                    .physical_input_contract
                    .footer_bounds
                    .get(&file_id)
                    .and_then(|bound| bound.get())
                    .ok_or_else(|| {
                        internal_datafusion_err!("missing reader-verified physical bounds")
                    })?;
                for position in positions.iter() {
                    if !position.is_some_and(|position| position >= 0 && (position as u64) < *count)
                    {
                        return internal_err!(
                            "physical position outside reader-verified file population"
                        );
                    }
                }
            }
            let selection = self
                .deletion_vectors
                .selection_for_positions(&file_id, positions)?;
            match selection.true_count() {
                count if count == batch.num_rows() => batch,
                0 => batch.slice(0, 0),
                _ => filter_record_batch(&batch, &selection)?,
            }
        } else {
            batch
        };
        if has_deletion_vector {
            self.dv_rows_checked.add(input_row_count);
            self.dv_rows_removed
                .add(input_row_count.saturating_sub(batch.num_rows()));
        }

        let mut hidden_indices = vec![file_id_idx];
        if let Some(position_idx) = self.physical_input_contract.physical_row_position_index {
            hidden_indices.push(position_idx);
        }
        hidden_indices.sort_unstable_by(|left, right| right.cmp(left));
        for index in hidden_indices {
            batch.remove_column(index);
        }

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

        let result = if let Some(file_id_column) = &self.file_id_column {
            let public_file_id = super::public_file_id(&self.public_file_ids, &file_id)?;
            let file_id_field = file_id_field(Some(file_id_column));
            let file_id_col =
                super::file_id_array_for_value(&file_id_field, public_file_id, result.num_rows())?;
            super::finalize_transformed_batch(
                result,
                &self.scan_plan,
                Some((file_id_col, file_id_field)),
                &mut self.schema_adapter,
            )
        } else {
            super::finalize_transformed_batch(
                result,
                &self.scan_plan,
                None,
                &mut self.schema_adapter,
            )
        }?;

        self.append_row_index(result, &file_id)
    }

    fn append_row_index(&mut self, batch: RecordBatch, file_id: &str) -> Result<RecordBatch> {
        let Some(row_index_field) = self.row_index_field.clone() else {
            return Ok(batch);
        };

        let row_count = u64::try_from(batch.num_rows()).map_err(|_| {
            internal_datafusion_err!("batch row count does not fit u64 while assigning row indexes")
        })?;
        let next_row_index = self
            .row_index_by_file
            .entry(file_id.to_string())
            .or_default();
        let end = next_row_index.checked_add(row_count).ok_or_else(|| {
            internal_datafusion_err!(
                "row index overflow while assigning row indexes for file '{file_id}'"
            )
        })?;

        let values = if row_count == 0 {
            Vec::new()
        } else {
            ((*next_row_index + 1)..=end).collect()
        };
        *next_row_index = end;

        let row_index: ArrayRef = Arc::new(UInt64Array::from(values));
        let mut columns = batch.columns().to_vec();
        columns.push(row_index);
        let mut fields = batch.schema().fields().to_vec();
        fields.push(row_index_field);

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
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
        Arc::clone(&self.scan_plan.contract.output_schema)
    }
}

#[inline]
#[cfg(test)]
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
    use arrow::datatypes::{DataType, Field, Schema, UInt64Type};
    use arrow_array::Array;
    use arrow_array::ArrayAccessor;
    use datafusion::{
        catalog::MemTable,
        common::{ToDFSchema, stats::Precision},
        datasource::{TableProvider, physical_plan::ParquetSource},
        logical_expr::Operator,
        physical_expr::expressions::{
            BinaryExpr, Column, DynamicFilterPhysicalExpr, lit as physical_lit,
        },
        physical_expr::{Distribution, Partitioning},
        physical_plan::{
            PhysicalExpr,
            coalesce_partitions::CoalescePartitionsExec,
            collect, collect_partitioned,
            filter_pushdown::{FilterPushdownPhase, PushedDown},
            repartition::RepartitionExec,
            statistics::StatisticsContext,
        },
        physical_planner::DefaultPhysicalPlanner,
        prelude::{SessionConfig, col, lit},
        scalar::ScalarValue,
    };
    use datafusion_datasource::{
        FileRange,
        file_groups::FileGroup,
        file_scan_config::{FileScanConfig, FileScanConfigBuilder},
        source::DataSourceExec,
    };

    use super::*;
    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::{
            DeltaScanConfig, session::create_session, table_provider::next::FILE_ID_COLUMN_DEFAULT,
        },
        test_utils::{TestResult, TestTables, open_fs_path},
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

        let downcast = scan.downcast_ref::<DeltaScanExec>();
        assert!(downcast.is_some());
        assert_eq!(downcast.unwrap().file_id_column.as_deref(), Some("file_id"));

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
    async fn test_scan_with_file_id_provider_does_not_force_output_when_unprojected() -> TestResult
    {
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());
        let id_idx = provider.schema().index_of("id").unwrap();

        let scan = provider
            .scan(&session.state(), Some(&vec![id_idx]), &[], None)
            .await?;

        let downcast = scan.downcast_ref::<DeltaScanExec>();
        assert!(downcast.is_some());
        assert!(downcast.unwrap().file_id_column.is_none());

        let data = collect_partitioned(scan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert!(!data.is_empty());
        assert_eq!(data[0].num_columns(), 1);
        assert!(data[0].schema().column_with_name("id").is_some());
        assert!(data[0].schema().column_with_name("file_id").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_adapts_override_schema_predicates() -> TestResult {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        table.load().await?;

        let provider = crate::delta_datafusion::table_provider::next::DeltaScan::new(
            table.snapshot()?.snapshot().clone(),
            DeltaScanConfig::default().with_schema(
                crate::delta_datafusion::table_provider::next::test_multi_partitioned_override_schema(),
            ),
        )?
        .with_log_store(table.log_store());

        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("planner must return DeltaScanExec");

        let filter = session.state().create_physical_expr(
            col("number").lt(lit(ScalarValue::TimestampMillisecond(Some(7), None))),
            &exec.schema().clone().to_dfschema()?,
        )?;

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Pre,
            vec![filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 1);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::Yes));

        let input_batches = collect(Arc::clone(&exec.input), session.task_ctx()).await?;
        assert!(!input_batches.is_empty());
        child_filters[0][0].predicate.evaluate(&input_batches[0])?;

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_rejects_delta_materialized_dynamic_filters()
    -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("planner must return DeltaScanExec");

        let letter_idx = exec.schema().index_of("letter")?;
        assert!(exec.schema().field_with_name("letter").is_ok());
        assert!(exec.input.schema().field_with_name("letter").is_err());

        let letter: Arc<dyn PhysicalExpr> = Arc::new(Column::new("letter", letter_idx));
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&letter),
            Operator::Eq,
            physical_lit("a"),
        ));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![letter], predicate));

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 1);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::No));

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_rejects_mixed_delta_materialized_dynamic_filters()
    -> TestResult {
        let table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let letter_idx = exec.schema().index_of("letter")?;
        let number_idx = exec.schema().index_of("number")?;
        assert!(exec.schema().field_with_name("letter").is_ok());
        assert!(exec.schema().field_with_name("number").is_ok());
        assert!(exec.input.schema().field_with_name("letter").is_err());
        assert!(exec.input.schema().field_with_name("number").is_ok());

        let letter: Arc<dyn PhysicalExpr> = Arc::new(Column::new("letter", letter_idx));
        let number: Arc<dyn PhysicalExpr> = Arc::new(Column::new("number", number_idx));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![letter, number],
            physical_lit(true),
        ));

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 1);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::No));

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_rejects_file_id_filters() -> TestResult {
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());
        let id_idx = provider.schema().index_of("id")?;

        let scan = provider
            .scan(
                &session.state(),
                Some(&vec![id_idx]),
                &[col("file_id").eq(lit("file:///tmp/part-00000.parquet"))],
                None,
            )
            .await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let file_id_idx = exec.schema().index_of("file_id")?;
        let file_id: Arc<dyn PhysicalExpr> = Arc::new(Column::new("file_id", file_id_idx));
        let filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&file_id),
            Operator::Eq,
            physical_lit("file:///tmp/part-00000.parquet"),
        ));

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 1);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::No));

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_keeps_data_filter_when_file_id_filter_is_present()
    -> TestResult {
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let file_id_idx = exec.schema().index_of("file_id")?;
        let id_idx = exec.schema().index_of("id")?;
        assert!(exec.input.schema().field_with_name("id").is_ok());
        let file_id: Arc<dyn PhysicalExpr> = Arc::new(Column::new("file_id", file_id_idx));
        let id: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", id_idx));
        let file_id_filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            file_id,
            Operator::Eq,
            physical_lit("file:///tmp/part-00000.parquet"),
        ));
        let id_filter: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![id], physical_lit(true)));

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![file_id_filter, id_filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 2);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::No));
        assert!(matches!(child_filters[0][1].discriminant, PushedDown::Yes));

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_keeps_parquet_dynamic_filters() -> TestResult {
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let id_idx = exec.schema().index_of("id")?;
        assert!(exec.schema().field_with_name("id").is_ok());
        assert!(exec.input.schema().field_with_name("id").is_ok());

        let data: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", id_idx));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![data],
            physical_lit(true),
        ));

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 1);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::Yes));

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_rejects_dv_data_filters() -> TestResult {
        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        assert!(
            exec.deletion_vectors.has_vectors(),
            "fixture must select at least one deletion vector"
        );
        let int_idx = exec.schema().index_of("int")?;
        let int: Arc<dyn PhysicalExpr> = Arc::new(Column::new("int", int_idx));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![int],
            physical_lit(true),
        ));

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 1);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::No));

        Ok(())
    }

    #[tokio::test]
    async fn test_gather_filters_for_pushdown_skips_column_mapping_parent_filters() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let filter = session.state().create_physical_expr(
            col(r#""Super Name""#).eq(lit(ScalarValue::Utf8View(Some("Timothy Lamb".to_string())))),
            &exec.schema().clone().to_dfschema()?,
        )?;

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Pre,
            vec![filter],
            session.state().config().options(),
        )?;

        let child_filters = description.parent_filters();
        assert_eq!(child_filters.len(), 1);
        assert_eq!(child_filters[0].len(), 1);
        assert!(matches!(child_filters[0][0].discriminant, PushedDown::No));

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

        let downcast = scan.downcast_ref::<DeltaScanExec>();
        assert!(downcast.is_some());
        assert!(downcast.unwrap().file_id_column.is_none());

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
    async fn test_scan_with_file_id_filter_omits_unprojected_file_id_from_final_output()
    -> TestResult {
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = Arc::new(create_session().into_inner());

        session
            .register_table("delta_table", provider.clone())
            .unwrap();

        let file_id_batches = session
            .sql("SELECT CAST(file_id AS STRING) AS file_id FROM delta_table LIMIT 1")
            .await
            .unwrap()
            .collect()
            .await?;
        let file_id = file_id_batches[0].column(0).as_string_view().value(0);
        let file_id = file_id.replace('\'', "''");

        let df = session
            .sql(&format!(
                "SELECT id FROM delta_table WHERE file_id = '{file_id}'"
            ))
            .await
            .unwrap();
        let batches = df.collect().await?;

        assert_eq!(batches[0].num_columns(), 1);
        assert!(batches[0].schema().column_with_name("id").is_some());
        assert!(batches[0].schema().column_with_name("file_id").is_none());

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
        //
        // DataFusion 55 changed `reverse()`'s signature from
        // `Signature::uniform(1, vec![Utf8View, Utf8, LargeUtf8])` to
        // `Signature::coercible(Native(logical_string()))` (apache/datafusion#23930).
        // The old uniform signature widened our dictionary-encoded `file_id` column to
        // `Utf8View` (first entry in the list); the new coercible signature preserves the
        // origin string type instead, so this expression now yields `Utf8` rather than
        // `Utf8View`. Normalize before asserting so the test is encoding-agnostic.
        let filename_raw = batches[0].column_by_name("filename").unwrap();
        let filename_flat = arrow::compute::cast(filename_raw, &DataType::Utf8View)?;
        let filename_col = filename_flat.as_string_view();

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
        let statistics = StatisticsContext::new().compute(scan.as_ref(), &StatisticsArgs::new())?;
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
        let statistics = StatisticsContext::new().compute(scan.as_ref(), &StatisticsArgs::new())?;
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
        let statistics = StatisticsContext::new().compute(scan.as_ref(), &StatisticsArgs::new())?;
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
        let statistics = StatisticsContext::new().compute(scan.as_ref(), &StatisticsArgs::new())?;
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

        let downcast = scan.downcast_ref::<DeltaScanExec>();
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

    #[tokio::test]
    async fn test_dv_scan_rejects_unsafe_pushdowns() -> TestResult {
        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], Some(1)).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("planner must return DeltaScanExec");

        assert!(exec.deletion_vectors.has_vectors());
        assert!(!exec.supports_limit_pushdown());
        assert!(matches!(
            exec.cardinality_effect(),
            CardinalityEffect::LowerEqual
        ));
        assert_eq!(exec.fetch(), None);
        assert!(exec.with_fetch(Some(1)).is_none());

        let int_idx = exec.schema().index_of("int")?;
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("int", int_idx))],
            physical_lit(true),
        ));
        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Post,
            vec![filter],
            session.state().config().options(),
        )?;
        assert!(matches!(
            description.parent_filters()[0][0].discriminant,
            PushedDown::No
        ));

        let batches = collect(scan, session.task_ctx()).await?;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "the limit must run after DV filtering removes four rows"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dv_scan_rejects_row_dropping_child_rewrites() -> TestResult {
        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("planner must return DeltaScanExec");
        let source_exec = exec
            .input
            .downcast_ref::<DataSourceExec>()
            .expect("fixture must produce one DataSourceExec");
        let config = source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("DataSourceExec must hold a parquet FileScanConfig");

        for change in ["schema", "statistics", "ordering", "hint"] {
            let mut groups = config
                .file_groups
                .iter()
                .map(|group| group.iter().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>();
            let file = &mut groups[0][0];
            match change {
                "schema" => file.arrow_schema = Some(Arc::new(arrow_schema::Schema::empty())),
                "statistics" => file.statistics = None,
                "ordering" => {
                    file.ordering = Some(
                        datafusion::physical_expr::LexOrdering::new(vec![
                            datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(
                                Column::new("value", 0),
                            )),
                        ])
                        .unwrap(),
                    )
                }
                _ => file.metadata_size_hint = Some(1),
            }
            let replacement = DataSourceExec::from_data_source(
                FileScanConfigBuilder::from(config.clone())
                    .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
                    .build(),
            );
            assert!(
                Arc::new(exec.clone())
                    .with_new_children(vec![replacement])
                    .is_err(),
                "must reject changed per-file {change}"
            );
        }

        let mut fragment = config.file_groups[0][0].clone();
        fragment.range = Some(FileRange {
            start: 0,
            end: i64::try_from(fragment.object_meta.size / 2)?,
        });
        let fragmented_config = FileScanConfigBuilder::from(config.clone())
            .with_file_groups(vec![FileGroup::new(vec![fragment])])
            .with_output_partitioning(Some(Partitioning::UnknownPartitioning(1)))
            .build();
        let fragmented = DataSourceExec::from_data_source(fragmented_config);
        let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(fragmented));

        let result = Arc::new(exec.clone()).replace_children(
            vec![coalesced],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        );
        assert!(
            result.is_err(),
            "coalescing to one output must not hide ranged DV file fragments"
        );

        let limited_config = FileScanConfigBuilder::from(config.clone())
            .with_limit(Some(1))
            .build();
        let limited: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(limited_config);
        let limited_result = Arc::new(exec.clone()).replace_children(
            vec![limited],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        );

        let parquet_source = config
            .file_source
            .downcast_ref::<ParquetSource>()
            .expect("FileScanConfig must hold ParquetSource");
        let filtered_config = FileScanConfigBuilder::from(config.clone())
            .with_source(Arc::new(parquet_source.with_predicate(physical_lit(false))))
            .build();
        let filtered: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(filtered_config);
        let filtered_result = Arc::new(exec.clone()).replace_children(
            vec![filtered],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        );
        assert!(
            limited_result.is_err() && filtered_result.is_err(),
            "DeltaScanExec must reject child rewrites that remove rows: limit={}, predicate={}",
            limited_result.is_err(),
            filtered_result.is_err()
        );

        Ok(())
    }

    fn fixture_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
        let mut rows = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(batch.schema().index_of("id").unwrap())
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                let values = batch
                    .column(batch.schema().index_of("value").unwrap())
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                ids.values()
                    .iter()
                    .copied()
                    .zip(values.values().iter().copied())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }

    #[tokio::test]
    async fn test_optimized_plan_reset_preserves_shared_visibility() -> TestResult {
        use super::super::test_support::{FileSpec, Fixture};
        use datafusion::physical_plan::execution_plan::reset_plan_states;
        let fixture = Fixture::new(
            vec![FileSpec {
                rows: 161,
                groups: vec![37, 53, 71],
                deleted: Some([0, 1, 37, 90].into_iter().collect()),
                log_stats: true,
            }],
            8,
        )?;
        let table = crate::open_table(fixture.url()).await?;
        let context = datafusion::prelude::SessionContext::new();
        context.register_table("t", table.table_provider().await?)?;
        let plan = context
            .sql("select * from t where id > 0")
            .await?
            .create_physical_plan()
            .await?;
        fn visibility(plan: &Arc<dyn ExecutionPlan>) -> Arc<DeletionVectorIndex> {
            if let Some(scan) = plan.downcast_ref::<DeltaScanExec>() {
                return scan.deletion_vectors.clone();
            }
            for child in plan.children() {
                if child.name().contains("DeltaScan") || !child.children().is_empty() {
                    return visibility(child);
                }
            }
            panic!("missing Delta scan");
        }
        let index = visibility(&plan);
        let expected = fixture
            .live_coordinates()
            .into_iter()
            .map(|r| (r.2, r.3))
            .collect::<Vec<_>>();
        for _ in 0..3 {
            let reset = reset_plan_states(plan.clone())?;
            assert!(Arc::ptr_eq(&index, &visibility(&reset)));
            assert_eq!(
                fixture_rows(&collect(reset, context.task_ctx()).await?),
                expected
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_coordinate_matrix_against_generator_oracle() -> TestResult {
        use super::super::test_support::{FileSpec, Fixture};
        let fixture = Fixture::new(
            vec![FileSpec {
                rows: 161,
                groups: vec![37, 53, 71],
                deleted: Some([0, 36, 37, 42, 89, 90, 144, 160].into_iter().collect()),
                log_stats: true,
            }],
            8,
        )?;
        let table = crate::open_table(fixture.url()).await?;
        let provider = table.table_provider().await?;
        let session = create_session().into_inner();
        let base_plan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = base_plan.downcast_ref::<DeltaScanExec>().unwrap();
        let source_exec = exec.input.downcast_ref::<DataSourceExec>().unwrap();
        let original = source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .unwrap();
        let source = original
            .file_source
            .downcast_ref::<ParquetSource>()
            .unwrap();
        let expected = fixture
            .live_coordinates()
            .into_iter()
            .filter(|row| row.2 >= 40 && row.2 < 145)
            .collect::<Vec<_>>();
        let footer =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new_with_options(
                std::fs::File::open(fixture.directory.path().join("part-0.parquet"))?,
                parquet::arrow::arrow_reader::ArrowReaderOptions::new()
                    .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required),
            )?;
        assert_eq!(
            footer
                .metadata()
                .row_groups()
                .iter()
                .map(|g| g.num_rows())
                .collect::<Vec<_>>(),
            [37, 53, 71]
        );
        assert!(
            footer
                .metadata()
                .offset_index()
                .unwrap()
                .iter()
                .all(|group| group.iter().all(|column| column.page_locations().len() > 1))
        );
        let mut cases = 0;
        let mut observed_ranges = false;
        let mut observed_group_pruning = false;
        let mut observed_page_pruning = false;
        for pages in [false, true] {
            for row_filter in [false, true] {
                for batch_size in [1, 7, 8192] {
                    for partitions in [1, 2, 4, 8] {
                        for repartition in [false, true] {
                            let mut options = SessionConfig::new()
                                .with_batch_size(batch_size)
                                .with_target_partitions(partitions);
                            options.options_mut().optimizer.repartition_file_scans = repartition;
                            options.options_mut().optimizer.repartition_file_min_size = 0;
                            options.options_mut().execution.parquet.enable_page_index = pages;
                            options.options_mut().execution.parquet.pushdown_filters = row_filter;
                            let context = datafusion::prelude::SessionContext::new_with_config_rt(
                                options,
                                session.runtime_env(),
                            );
                            let predicate = context.state().create_physical_expr(
                                col("id").gt_eq(lit(40_i64)).and(col("id").lt(lit(145_i64))),
                                &exec.input.schema().to_dfschema()?,
                            )?;
                            let native = source
                                .with_predicate(predicate)
                                .with_pushdown_filters(row_filter)
                                .with_enable_page_index(pages);
                            let config = FileScanConfigBuilder::from(original.clone())
                                .with_source(Arc::new(native))
                                .with_output_partitioning(None)
                                .build();
                            let input: Arc<dyn ExecutionPlan> =
                                DataSourceExec::from_data_source(config);
                            let optimized = DefaultPhysicalPlanner::default()
                                .optimize_physical_plan(input, &context.state(), |_, _| {})?;
                            if let Some(source) = optimized.downcast_ref::<DataSourceExec>() {
                                let config = source
                                    .data_source()
                                    .downcast_ref::<FileScanConfig>()
                                    .unwrap();
                                observed_ranges |= config
                                    .file_groups
                                    .iter()
                                    .flat_map(|g| g.iter())
                                    .any(|file| file.range.is_some());
                            }
                            let batches = collect(optimized.clone(), context.task_ctx()).await?;
                            if let Some(metrics) = optimized.metrics() {
                                for metric in metrics.iter() {
                                    let name = metric.value().name();
                                    if let datafusion::physical_plan::metrics::MetricValue::PruningMetrics { pruning_metrics, .. } = metric.value() {
                                        observed_group_pruning |= name == "row_groups_pruned_statistics" && pruning_metrics.pruned() > 0;
                                        observed_page_pruning |= name == "page_index_rows_pruned" && pruning_metrics.pruned() > 0;
                                    }
                                }
                            }
                            let mut visible = Vec::new();
                            for batch in batches {
                                let positions = batch
                                    .column(
                                        exec.physical_input_contract
                                            .physical_row_position_index
                                            .unwrap(),
                                    )
                                    .as_any()
                                    .downcast_ref::<arrow_array::Int64Array>()
                                    .unwrap();
                                let ids = batch
                                    .column(batch.schema().index_of("id")?)
                                    .as_any()
                                    .downcast_ref::<arrow_array::Int64Array>()
                                    .unwrap();
                                let keep = exec
                                    .deletion_vectors
                                    .selection_for_positions("0", positions)?;
                                for ((id, position), keep) in ids
                                    .values()
                                    .iter()
                                    .zip(positions.values())
                                    .zip(keep.values())
                                {
                                    assert_eq!(
                                        id, position,
                                        "reader coordinate differs from generated coordinate: pages={pages}, row_filter={row_filter}, batch={batch_size}, partitions={partitions}, repartition={repartition}"
                                    );
                                    if keep && *id >= 40 && *id < 145 {
                                        visible.push((0, *position as u64, *id, -*id));
                                    }
                                }
                            }
                            visible.sort_unstable();
                            assert_eq!(
                                visible, expected,
                                "pages={pages}, row_filter={row_filter}, batch={batch_size}, partitions={partitions}, repartition={repartition}"
                            );
                            cases += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(cases, 96);
        assert!(
            observed_group_pruning,
            "fixture must execute row-group pruning"
        );
        assert!(
            observed_page_pruning,
            "fixture must execute page-index pruning"
        );
        assert!(
            observed_ranges,
            "private coordinate qualification must execute native ranged reads"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_visibility_is_frozen_across_concurrent_plans_and_advancement()
    -> TestResult {
        use super::super::test_support::{FileSpec, Fixture};
        let mut fixture = Fixture::new(
            vec![FileSpec {
                rows: 161,
                groups: vec![37, 53, 71],
                deleted: Some([1].into_iter().collect()),
                log_stats: true,
            }],
            8,
        )?;
        let session = create_session().into_inner();
        let first_table = crate::open_table(fixture.url()).await?;
        let first_provider = first_table.table_provider().await?;
        let first_plan = first_provider
            .scan(&session.state(), None, &[], None)
            .await?;
        use datafusion_proto::logical_plan::LogicalExtensionCodec;
        let codec = crate::delta_datafusion::DeltaLogicalCodec {};
        let table_ref = datafusion::common::TableReference::bare("snapshot");
        let mut encoded = Vec::new();
        codec.try_encode_table_provider(&table_ref, first_provider.clone(), &mut encoded)?;
        let decoded = codec.try_decode_table_provider(
            &encoded,
            &table_ref,
            first_provider.schema(),
            &session.task_ctx(),
        )?;
        let expected_first = fixture
            .live_coordinates()
            .into_iter()
            .map(|(_, _, id, value)| (id, value))
            .collect::<Vec<_>>();
        fixture.replace_visibility(0, [2].into_iter().collect())?;
        let second_table = crate::open_table(fixture.url()).await?;
        let second_provider = second_table.table_provider().await?;
        let second_plan = second_provider
            .scan(&session.state(), None, &[], None)
            .await?;
        let expected_second = fixture
            .live_coordinates()
            .into_iter()
            .map(|(_, _, id, value)| (id, value))
            .collect::<Vec<_>>();
        let (a, b) = tokio::join!(
            collect(first_plan.clone(), session.task_ctx()),
            collect(second_plan, session.task_ctx())
        );
        assert_eq!(fixture_rows(&a?), expected_first);
        assert_eq!(fixture_rows(&b?), expected_second);
        fixture.replace_visibility(0, [3, 4].into_iter().collect())?;
        let mut advanced = first_table;
        advanced.update_state().await?;
        assert_eq!(
            fixture_rows(&collect(first_plan.clone(), session.task_ctx()).await?),
            expected_first
        );
        let decoded_plan = decoded.scan(&session.state(), None, &[], None).await?;
        assert_eq!(
            fixture_rows(&collect(decoded_plan, session.task_ctx()).await?),
            expected_first
        );
        // Cancellation must not consume visibility shared by subsequent streams.
        drop(first_plan.execute(0, session.task_ctx())?);
        assert_eq!(
            fixture_rows(&collect(first_plan, session.task_ctx()).await?),
            expected_first
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_generated_mixed_scan_checks_coordinates_and_logical_rows_independently()
    -> TestResult {
        use super::super::test_support::{FileSpec, Fixture};
        let fixture = Fixture::new(
            vec![
                FileSpec {
                    rows: 161,
                    groups: vec![37, 53, 71],
                    deleted: Some([0, 36, 37, 89, 90, 160].into_iter().collect()),
                    log_stats: true,
                },
                FileSpec {
                    rows: 107,
                    groups: vec![41, 66],
                    deleted: None,
                    log_stats: false,
                },
            ],
            8,
        )?;
        let table = crate::open_table(fixture.url()).await?;
        let provider = table.table_provider().await?;
        let session = create_session().into_inner();
        let plan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = plan.downcast_ref::<DeltaScanExec>().unwrap();
        let physical = collect(exec.input.clone(), session.task_ctx()).await?;
        let expected_by_id = fixture
            .coordinates
            .iter()
            .map(|row| (row.2, *row))
            .collect::<HashMap<_, _>>();
        let mut actual_visible = Vec::new();
        for batch in physical {
            let positions = batch
                .column(
                    exec.physical_input_contract
                        .physical_row_position_index
                        .unwrap(),
                )
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            let ids = batch
                .column(batch.schema().index_of("id")?)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            for (id, position) in ids.values().iter().zip(positions.values()) {
                assert_eq!(*position as u64, expected_by_id[id].1);
            }
            for (file_id, run) in
                split_by_file_id_runs(&batch, exec.physical_input_contract.file_id_index)?
            {
                let positions = run
                    .column(
                        exec.physical_input_contract
                            .physical_row_position_index
                            .unwrap(),
                    )
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                let selection = exec
                    .deletion_vectors
                    .selection_for_positions(&file_id, positions)?;
                let ids = run
                    .column(run.schema().index_of("id")?)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                for (id, keep) in ids.values().iter().zip(selection.values()) {
                    if keep {
                        actual_visible.push(expected_by_id[id]);
                    }
                }
            }
        }
        actual_visible.sort_unstable();
        assert_eq!(actual_visible, fixture.live_coordinates());
        let expected = fixture
            .live_coordinates()
            .iter()
            .map(|row| (row.2, row.3))
            .collect::<Vec<_>>();
        assert_eq!(
            fixture_rows(&collect(plan.clone(), session.task_ctx()).await?),
            expected
        );
        assert_eq!(
            fixture_rows(&collect(plan, session.task_ctx()).await?),
            expected
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(deprecated, reason = "qualify both supported child replacement APIs")]
    async fn test_dv_provenance_rejects_extensions_and_fabricated_positions() -> TestResult {
        use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
        use datafusion::physical_plan::projection::ProjectionExec;
        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan.downcast_ref::<DeltaScanExec>().unwrap();
        let source = exec.input.downcast_ref::<DataSourceExec>().unwrap();
        let config = source
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .unwrap();
        let mut files = config.file_groups[0].iter().cloned().collect::<Vec<_>>();
        files[0].extensions.insert(ParquetAccessPlan::new_all(1));
        let injected = DataSourceExec::from_data_source(
            FileScanConfigBuilder::from(config.clone())
                .with_file_groups(vec![FileGroup::new(files)])
                .build(),
        );
        assert!(
            Arc::new(exec.clone())
                .with_new_children(vec![injected.clone()])
                .is_err()
        );
        assert!(
            Arc::new(exec.clone())
                .replace_children(
                    vec![injected],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep)
                )
                .is_err()
        );
        let position = exec
            .physical_input_contract
            .physical_row_position_index
            .unwrap();
        for computed in [false, true] {
            let expressions = exec
                .input
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
                    let expression = if index == position {
                        if computed {
                            Arc::new(BinaryExpr::new(column, Operator::Plus, physical_lit(0_i64)))
                                as Arc<dyn PhysicalExpr>
                        } else {
                            physical_lit(ScalarValue::Int64(None))
                        }
                    } else {
                        column
                    };
                    (expression, field.name().clone())
                })
                .collect::<Vec<_>>();
            let fabricated: Arc<dyn ExecutionPlan> =
                Arc::new(ProjectionExec::try_new(expressions, exec.input.clone())?);
            for (actual, expected) in fabricated
                .schema()
                .fields()
                .iter()
                .zip(exec.input.schema().fields())
            {
                assert_eq!(
                    (actual.name(), actual.data_type(), actual.is_nullable()),
                    (
                        expected.name(),
                        expected.data_type(),
                        expected.is_nullable()
                    )
                );
            }
            assert!(
                Arc::new(exec.clone())
                    .with_new_children(vec![fabricated])
                    .is_err()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_dv_scan_downgrades_all_exact_physical_statistics() -> TestResult {
        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("planner must return DeltaScanExec");

        let mut physical = Statistics::new_unknown(exec.input.schema().as_ref());
        physical.num_rows = Precision::Exact(5);
        physical.total_byte_size = Precision::Exact(1_006);
        let first_column = physical
            .column_statistics
            .first_mut()
            .expect("DV fixture must read at least one physical column");
        first_column.byte_size = Precision::Exact(128);
        first_column.sum_value = Precision::Exact(ScalarValue::Int64(Some(42)));

        let logical = exec.map_statistics(physical)?;
        assert_eq!(logical.num_rows, Precision::Inexact(5));
        assert_eq!(logical.total_byte_size, Precision::Inexact(1_006));
        assert_eq!(
            logical.column_statistics[0].byte_size,
            Precision::Inexact(128)
        );
        assert_eq!(
            logical.column_statistics[0].sum_value,
            Precision::Inexact(ScalarValue::Int64(Some(42)))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_optimized_multi_group_dv_scan_is_concurrently_repeatable() -> TestResult {
        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let config = SessionConfig::new().with_target_partitions(2);
        let session = Arc::new(datafusion::prelude::SessionContext::new_with_config(config));
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("planner must return DeltaScanExec");
        let source_exec = exec
            .input
            .downcast_ref::<DataSourceExec>()
            .expect("fixture must produce one DataSourceExec");
        let source_config = source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("DataSourceExec must hold a parquet FileScanConfig");
        let original_file = source_config.file_groups[0][0].clone();
        let original_file_id = exec.deletion_vectors.entries.keys().next().unwrap().clone();

        let duplicate_file_id = "duplicate".to_string();
        let duplicate_location = original_file.object_meta.location.clone();
        let mut duplicate_file = original_file.clone();
        duplicate_file.object_meta.location = duplicate_location.clone();
        duplicate_file.partition_values =
            vec![crate::delta_datafusion::file_id::wrap_file_id_value(
                duplicate_file_id.clone(),
            )];
        let grouped_config = FileScanConfigBuilder::from(source_config.clone())
            .with_file_groups(vec![
                FileGroup::new(vec![original_file]),
                FileGroup::new(vec![duplicate_file]),
            ])
            .with_output_partitioning(Some(Partitioning::UnknownPartitioning(2)))
            .build();
        let grouped_input: Arc<dyn ExecutionPlan> =
            DataSourceExec::from_data_source(grouped_config);
        let mut grouped_contract = exec.physical_input_contract.as_ref().clone();
        grouped_contract.sources.clear();
        grouped_contract.planned_files.clear();
        grouped_contract.bind_sources(&grouped_input)?;

        let mut entries = exec.deletion_vectors.entries.clone();
        entries.insert(
            duplicate_file_id.clone(),
            entries[&original_file_id].clone(),
        );
        let mut identities = exec.physical_file_identities.as_ref().clone();
        identities.insert(
            duplicate_file_id.clone(),
            super::super::PhysicalFileIdentity {
                object_store_url: source_config.object_store_url.clone(),
                location: duplicate_location,
            },
        );
        let grouped_scan: Arc<dyn ExecutionPlan> = Arc::new(DeltaScanExec::new(
            Arc::clone(&exec.scan_plan),
            grouped_input,
            Arc::clone(&exec.transforms),
            PhysicalScanContext {
                deletion_vectors: Arc::new(DeletionVectorIndex::try_new(entries)?),
                public_file_ids: Arc::clone(&exec.public_file_ids),
                physical_file_identities: Arc::new(identities),
                physical_input_contract: Arc::new(grouped_contract),
            },
            exec.partition_stats.clone(),
            ExecutionPlanMetricsSet::new(),
        ));

        let optimized = DefaultPhysicalPlanner::default().optimize_physical_plan(
            grouped_scan,
            &session.state(),
            |_, _| {},
        )?;
        let optimized_scan = optimized
            .downcast_ref::<DeltaScanExec>()
            .expect("optimizer must retain DeltaScanExec");
        assert!(
            optimized_scan
                .input
                .downcast_ref::<CoalescePartitionsExec>()
                .is_some(),
            "fixture must exercise multiple file groups under one coalescing node"
        );

        let (first, second) = tokio::join!(
            collect(Arc::clone(&optimized), session.task_ctx()),
            collect(optimized, session.task_ctx())
        );
        let expected = vec![
            "+--------+-----+------------+",
            "| letter | int | date       |",
            "+--------+-----+------------+",
            "| b      | 228 | 1978-12-01 |",
            "| b      | 228 | 1978-12-01 |",
            "+--------+-----+------------+",
        ];
        assert_batches_sorted_eq!(&expected, &first?);
        assert_batches_sorted_eq!(&expected, &second?);

        Ok(())
    }

    #[tokio::test]
    async fn test_dv_scan_uses_hidden_physical_row_positions() -> TestResult {
        let table = open_fs_path(DV_TABLE_PATH);
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let explain = datafusion::physical_plan::displayable(scan.as_ref())
            .indent(false)
            .to_string();
        assert!(explain.contains("dv_mode=physical_position"), "{explain}");
        assert!(explain.contains("physical_row_index=true"), "{explain}");
        let input_schema = exec.input.schema();
        let hidden_position = input_schema
            .fields()
            .iter()
            .find(|field| field.name().starts_with("__delta_rs_physical_row_position"))
            .expect("physical child must materialize a hidden row position");
        let hidden_position_name = hidden_position.name().clone();
        assert_eq!(hidden_position.data_type(), &DataType::Int64);
        assert!(
            exec.schema()
                .fields()
                .iter()
                .all(|field| field.name() != &hidden_position_name)
        );

        let batches = collect(scan, session.task_ctx()).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert!(batches.iter().all(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| field.name() != &hidden_position_name)
        }));

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
            .downcast_ref::<DeltaScanExec>()
            .expect("Expected DeltaScanExec");

        let kernel_type = Arc::clone(exec.scan_plan.scan.logical_schema()).into();

        let mut scan_plan = exec.scan_plan.as_ref().clone();
        scan_plan.contract.result_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        scan_plan.contract.output_schema = Arc::clone(&scan_plan.contract.result_schema);
        scan_plan.contract.result_projection = None;
        scan_plan.parquet_read_schema = Arc::clone(&scan_plan.contract.result_schema);

        Ok((kernel_type, Arc::new(scan_plan)))
    }

    async fn int32_scan_plan() -> TestResult<(KernelDataType, Arc<KernelScanPlan>)> {
        use arrow::datatypes::{Field, Schema};

        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());

        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("Expected DeltaScanExec");

        let kernel_type = Arc::clone(exec.scan_plan.scan.logical_schema()).into();

        let mut scan_plan = exec.scan_plan.as_ref().clone();
        scan_plan.contract.result_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        scan_plan.contract.output_schema = Arc::clone(&scan_plan.contract.result_schema);
        scan_plan.contract.result_projection = None;
        scan_plan.parquet_read_schema = Arc::clone(&scan_plan.contract.result_schema);

        Ok((kernel_type, Arc::new(scan_plan)))
    }

    fn selection_vectors_f1_f2() -> HashMap<String, Vec<bool>> {
        HashMap::from([
            ("f1".to_string(), vec![true, false]),
            ("f2".to_string(), vec![false, true]),
        ])
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
        selection_vectors: HashMap<String, Vec<bool>>,
        input_batches: Vec<RecordBatch>,
        file_id_column: Option<String>,
    ) -> DeltaScanStream {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let input_file_id_column = scan_plan.contract.file_id_field.name().clone();
        let mut public_file_ids = super::super::PublicFileIdMap::default();
        for file_id in selection_vectors.keys() {
            public_file_ids.insert(file_id.clone(), file_id.clone());
        }
        for batch in &input_batches {
            if let Ok(file_id_idx) = file_id_column_idx(batch, &input_file_id_column) {
                for (file_id, _) in
                    split_by_file_id_runs(batch, file_id_idx).expect("valid test file ids")
                {
                    public_file_ids.entry(file_id.clone()).or_insert(file_id);
                }
            }
        }

        let has_deletion_vectors = !selection_vectors.is_empty();
        let mut physical_input_contract = Arc::new(super::super::PhysicalInputContract::new(
            &scan_plan.parquet_read_schema,
            scan_plan.contract.file_id_field.clone(),
            has_deletion_vectors,
        ));
        let mut next_position_by_file = HashMap::<String, i64>::new();
        let input_batches = input_batches
            .into_iter()
            .map(|batch| {
                if !has_deletion_vectors {
                    return batch;
                }
                append_test_physical_positions(
                    batch,
                    &physical_input_contract,
                    &input_file_id_column,
                    &mut next_position_by_file,
                )
            })
            .collect::<Vec<_>>();
        let deletion_entries: HashMap<String, super::super::deletion_vector::DeletionVectorEntry> =
            selection_vectors
                .iter()
                .map(|selection| {
                    let observed = next_position_by_file
                        .get(selection.0)
                        .copied()
                        .unwrap_or_default();
                    let physical_record_count = u64::try_from(observed)
                        .unwrap()
                        .max(u64::try_from(selection.1.len()).unwrap());
                    let deleted_cardinality =
                        u64::try_from(selection.1.iter().filter(|keep| !**keep).count()).unwrap();
                    (
                        selection.0.clone(),
                        super::super::deletion_vector::DeletionVectorEntry {
                            keep_mask: selection.1.clone(),
                            physical_record_count,
                            deleted_cardinality,
                        },
                    )
                })
                .collect();
        let mut selected = next_position_by_file
            .iter()
            .map(|(id, count)| (id.clone(), Some(*count as u64)))
            .collect::<HashMap<_, _>>();
        for (id, entry) in &deletion_entries {
            selected.insert(id.clone(), Some(entry.physical_record_count));
        }
        Arc::make_mut(&mut physical_input_contract).footer_bounds = selected
            .iter()
            .map(|(id, count)| {
                (
                    id.clone(),
                    Arc::new(std::sync::OnceLock::from(count.unwrap())),
                )
            })
            .collect();
        let deletion_vectors = Arc::new(
            DeletionVectorIndex::try_new(deletion_entries)
                .unwrap()
                .with_selected_files(selected)
                .expect("valid test deletion vectors"),
        );
        let input_schema = input_batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| physical_input_contract.table_schema.table_schema().clone());

        let input = Box::pin(RecordBatchStreamAdapter::new(
            input_schema,
            futures::stream::iter(input_batches.into_iter().map(Ok)),
        ));

        let schema_adapter =
            super::super::SchemaAdapter::new(Arc::clone(&scan_plan.contract.result_schema));
        let row_index_field = scan_plan.contract.retained_row_index_field();
        DeltaScanStream {
            scan_plan,
            kernel_type,
            input,
            baseline_metrics: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            transforms: Arc::new(HashMap::new()),
            deletion_vectors,
            physical_input_contract,
            dv_rows_checked: Count::new(),
            dv_rows_removed: Count::new(),
            public_file_ids: Arc::new(public_file_ids),
            file_id_column,
            row_index_field,
            row_index_by_file: HashMap::new(),
            pending: VecDeque::new(),
            schema_adapter,
        }
    }

    fn append_test_physical_positions(
        batch: RecordBatch,
        physical_input_contract: &super::super::PhysicalInputContract,
        input_file_id_column: &str,
        next_position_by_file: &mut HashMap<String, i64>,
    ) -> RecordBatch {
        let file_id_idx =
            file_id_column_idx(&batch, input_file_id_column).expect("valid compact file-id column");
        let mut positions = Vec::with_capacity(batch.num_rows());
        for (file_id, run) in
            split_by_file_id_runs(&batch, file_id_idx).expect("valid compact file-id runs")
        {
            let next = next_position_by_file.entry(file_id).or_default();
            positions.extend(*next..*next + i64::try_from(run.num_rows()).unwrap());
            *next += i64::try_from(run.num_rows()).unwrap();
        }
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(arrow_array::Int64Array::from(positions)));
        RecordBatch::try_new(
            physical_input_contract.table_schema.table_schema().clone(),
            columns,
        )
        .expect("test physical input batch")
    }

    fn retain_row_index(scan_plan: Arc<KernelScanPlan>, column: &str) -> Arc<KernelScanPlan> {
        let mut scan_plan = scan_plan.as_ref().clone();
        let row_index_field = Arc::new(Field::new(column, DataType::UInt64, false));
        let mut fields = scan_plan.contract.result_schema.fields().to_vec();
        // Keep the same row index field order as ProjectedScanContract::try_new.
        fields.push(row_index_field.clone());
        scan_plan.contract.output_schema = Arc::new(Schema::new(fields));
        scan_plan.contract.row_index_field = Some(row_index_field);
        scan_plan.contract.retain_row_index = true;
        Arc::new(scan_plan)
    }

    fn row_ordinals(batch: &RecordBatch, column: &str) -> Vec<u64> {
        batch
            .column_by_name(column)
            .expect("row ordinal column")
            .as_primitive::<UInt64Type>()
            .values()
            .to_vec()
    }

    #[tokio::test]
    async fn test_required_input_distribution_tracks_retained_row_index_contract() -> TestResult {
        let (_kernel_type, scan_plan) = int32_scan_plan().await?;
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let distribution = exec.input_distribution_requirements();
        assert!(
            matches!(
                distribution.child_distribution(0),
                Some(Distribution::UnspecifiedDistribution)
            ),
            "unexpected distribution: {distribution:?}"
        );

        let retained_exec = DeltaScanExec::new(
            retain_row_index(scan_plan, "row_ordinal"),
            Arc::clone(&exec.input),
            Arc::clone(&exec.transforms),
            PhysicalScanContext {
                deletion_vectors: Arc::clone(&exec.deletion_vectors),
                public_file_ids: Arc::clone(&exec.public_file_ids),
                physical_file_identities: Arc::clone(&exec.physical_file_identities),
                physical_input_contract: Arc::clone(&exec.physical_input_contract),
            },
            exec.partition_stats.clone(),
            exec.metrics.clone(),
        );

        let distribution = retained_exec.input_distribution_requirements();
        assert!(
            matches!(
                distribution.child_distribution(0),
                Some(Distribution::SinglePartition)
            ),
            "unexpected distribution: {distribution:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_retained_row_index_execute_rejects_multi_partition_child() -> TestResult {
        let (_kernel_type, scan_plan) = int32_scan_plan().await?;
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().await?;
        let session = Arc::new(create_session().into_inner());
        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");

        let repartitioned_input = Arc::new(RepartitionExec::try_new(
            Arc::clone(&exec.input),
            Partitioning::RoundRobinBatch(2),
        )?);

        let retained_exec = DeltaScanExec::new(
            retain_row_index(scan_plan, "row_ordinal"),
            repartitioned_input,
            Arc::clone(&exec.transforms),
            PhysicalScanContext {
                deletion_vectors: Arc::clone(&exec.deletion_vectors),
                public_file_ids: Arc::clone(&exec.public_file_ids),
                physical_file_identities: Arc::clone(&exec.physical_file_identities),
                physical_input_contract: Arc::clone(&exec.physical_input_contract),
            },
            exec.partition_stats.clone(),
            exec.metrics.clone(),
        );

        let err = match retained_exec.execute(0, session.task_ctx()) {
            Ok(_) => panic!("retained row-index scans must reject multi-partition children"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("retained row indexes require a single input partition"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_repartitioned_recomputes_properties_and_executes_all_rows() -> TestResult {
        let (_kernel_type, scan_plan) = int32_scan_plan().await?;
        let first = value_and_file_id_batch(&[10], &[Some("f1")], false)?;
        let second = value_and_file_id_batch(&[20], &[Some("f2")], false)?;
        let input_schema = first.schema();
        let table = MemTable::try_new(input_schema, vec![vec![first, second]])?;
        let session = Arc::new(create_session().into_inner());
        let input = table.scan(&session.state(), None, &[], None).await?;

        let mut public_file_ids = super::super::PublicFileIdMap::default();
        public_file_ids.insert("f1".to_string(), "f1".to_string());
        public_file_ids.insert("f2".to_string(), "f2".to_string());
        let physical_input_contract = Arc::new(super::super::PhysicalInputContract::new(
            &scan_plan.parquet_read_schema,
            scan_plan.contract.file_id_field.clone(),
            false,
        ));
        let exec = DeltaScanExec::new(
            scan_plan,
            input,
            Arc::new(HashMap::new()),
            PhysicalScanContext {
                deletion_vectors: Arc::new(DeletionVectorIndex::default()),
                public_file_ids: Arc::new(public_file_ids),
                physical_file_identities: Arc::new(super::super::PhysicalFileIdentityMap::default()),
                physical_input_contract,
            },
            HashMap::new(),
            ExecutionPlanMetricsSet::new(),
        );

        let repartitioned = exec
            .repartitioned(2, &ConfigOptions::new())?
            .expect("MemoryExec::repartitioned returned None for two batches");
        let child_partition_count = repartitioned.children()[0]
            .properties()
            .partitioning
            .partition_count();

        assert_eq!(child_partition_count, 2);
        assert_eq!(
            repartitioned.properties().partitioning.partition_count(),
            child_partition_count,
            "wrapper partition count must match its child"
        );

        let row_count = collect(repartitioned, session.task_ctx())
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        assert_eq!(row_count, 2, "repartitioned plan must return two rows");
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_project_appends_row_ordinals_from_scan_contract() -> TestResult {
        let (kernel_type, scan_plan) = int32_scan_plan().await?;
        let scan_plan = retain_row_index(scan_plan, "row_ordinal");
        let batch = value_and_file_id_batch(&[10, 11], &[Some("f1"), Some("f1")], false)?;

        let mut stream = test_scan_stream(scan_plan, kernel_type, HashMap::new(), Vec::new(), None);

        assert!(stream.schema().column_with_name("row_ordinal").is_some());
        let outputs = stream.batch_project(batch)?;
        assert_eq!(outputs.len(), 1);
        assert_eq!(row_ordinals(&outputs[0], "row_ordinal"), vec![1, 2]);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_project_resets_row_ordinals_per_file() -> TestResult {
        let (kernel_type, scan_plan) = int32_scan_plan().await?;
        let scan_plan = retain_row_index(scan_plan, "row_ordinal");
        let batch = value_and_file_id_batch(
            &[10, 11, 20, 21],
            &[Some("f1"), Some("f1"), Some("f2"), Some("f2")],
            false,
        )?;

        let mut stream = test_scan_stream(scan_plan, kernel_type, HashMap::new(), Vec::new(), None);

        let outputs = stream.batch_project(batch)?;
        assert_eq!(outputs.len(), 2);
        assert_eq!(row_ordinals(&outputs[0], "row_ordinal"), vec![1, 2]);
        assert_eq!(row_ordinals(&outputs[1], "row_ordinal"), vec![1, 2]);

        Ok(())
    }

    #[tokio::test]
    async fn test_poll_next_continues_row_ordinals_across_batches_for_same_file() -> TestResult {
        use futures::StreamExt;

        let (kernel_type, scan_plan) = int32_scan_plan().await?;
        let scan_plan = retain_row_index(scan_plan, "row_ordinal");
        let first = value_and_file_id_batch(&[10, 11], &[Some("f1"), Some("f1")], false)?;
        let second = value_and_file_id_batch(&[12, 13], &[Some("f1"), Some("f1")], false)?;

        let mut stream = test_scan_stream(
            scan_plan,
            kernel_type,
            HashMap::new(),
            vec![first, second],
            None,
        );

        let batch1 = stream.next().await.transpose()?.expect("first batch");
        let batch2 = stream.next().await.transpose()?.expect("second batch");
        assert!(stream.next().await.is_none());
        assert_eq!(row_ordinals(&batch1, "row_ordinal"), vec![1, 2]);
        assert_eq!(row_ordinals(&batch2, "row_ordinal"), vec![3, 4]);

        Ok(())
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
            None,
        );

        let batch = append_test_physical_positions(
            batch,
            &stream.physical_input_contract,
            FILE_ID_COLUMN_DEFAULT,
            &mut HashMap::new(),
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

        let mut stream =
            test_scan_stream(scan_plan, kernel_type, selection_vectors, vec![batch], None);

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
            None,
        );

        let batch = append_test_physical_positions(
            batch,
            &stream.physical_input_contract,
            FILE_ID_COLUMN_DEFAULT,
            &mut HashMap::new(),
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

    #[tokio::test]
    async fn test_batch_project_empty_batch_uses_contract_output_schema_without_metadata()
    -> TestResult {
        let (kernel_type, scan_plan) = int32_scan_plan().await?;
        let mut stream = test_scan_stream(
            Arc::clone(&scan_plan),
            kernel_type,
            HashMap::new(),
            Vec::new(),
            None,
        );
        assert!(
            stream
                .batch_project(RecordBatch::new_empty(
                    scan_plan.contract.output_schema.clone()
                ))
                .is_err()
        );
        let batches = stream.batch_project(RecordBatch::new_empty(
            stream
                .physical_input_contract
                .table_schema
                .table_schema()
                .clone(),
        ))?;
        let columns = batches[0].columns();

        assert_eq!(batches[0].schema(), scan_plan.contract.output_schema);
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].data_type(), &DataType::Int32);
        assert!(columns[0].is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_project_empty_batch_uses_contract_output_schema_with_row_index()
    -> TestResult {
        let (kernel_type, scan_plan) = int32_scan_plan().await?;
        let scan_plan = retain_row_index(scan_plan, "row_ordinal");
        let mut stream = test_scan_stream(
            Arc::clone(&scan_plan),
            kernel_type,
            HashMap::new(),
            Vec::new(),
            None,
        );

        assert!(
            stream
                .batch_project(RecordBatch::new_empty(
                    scan_plan.contract.output_schema.clone()
                ))
                .is_err()
        );
        let batches = stream.batch_project(RecordBatch::new_empty(
            stream
                .physical_input_contract
                .table_schema
                .table_schema()
                .clone(),
        ))?;

        assert_eq!(batches[0].schema(), scan_plan.contract.output_schema);
        let schema = batches[0].schema();
        let row_ordinal = schema
            .column_with_name("row_ordinal")
            .expect("row ordinal column")
            .1;
        assert_eq!(row_ordinal.data_type(), &DataType::UInt64);
        assert_eq!(batches[0].num_rows(), 0);
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

        let mut stream = test_scan_stream(
            scan_plan,
            kernel_type,
            selection_vectors,
            vec![batch],
            Some(FILE_ID_COLUMN_DEFAULT.to_string()),
        );

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

    #[tokio::test]
    async fn test_poll_next_rematerializes_public_file_ids_across_batches() -> TestResult {
        use futures::StreamExt;

        let (kernel_type, scan_plan) = int32_scan_plan().await?;
        let first = value_and_file_id_batch(&[10, 11], &[Some("0"), Some("0")], false)?;
        let second = value_and_file_id_batch(&[12, 13], &[Some("0"), Some("0")], false)?;
        let public_file_id = "file:///table/very/long/path/part-00000.parquet";

        let mut stream = test_scan_stream(
            scan_plan,
            kernel_type,
            HashMap::new(),
            vec![first, second],
            Some("file_id".to_string()),
        );
        stream.public_file_ids = Arc::new(
            [("0".to_string(), public_file_id.to_string())]
                .into_iter()
                .collect(),
        );

        let batch1 = stream.next().await.transpose()?.expect("first batch");
        let batch2 = stream.next().await.transpose()?.expect("second batch");
        assert!(stream.next().await.is_none());

        for batch in [batch1, batch2] {
            let file_id = batch
                .column_by_name("file_id")
                .expect("file_id column")
                .as_dictionary::<UInt16Type>()
                .downcast_dict::<StringArray>()
                .unwrap();
            assert_eq!(file_id.len(), 2);
            assert_eq!(file_id.value(0), public_file_id);
            assert_eq!(file_id.value(1), public_file_id);
        }

        Ok(())
    }
}
