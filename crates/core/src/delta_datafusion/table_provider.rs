use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::TableProvider;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, Statistics, ToDFSchema};
use datafusion::config::{ConfigOptions, TableParquetOptions};
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileSource,
};
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{BinaryExpr, LogicalPlan, Operator};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr,
    PlanProperties,
};
use datafusion::{
    catalog::Session,
    common::{HashMap, HashSet},
    datasource::listing::PartitionedFile,
    logical_expr::{utils::conjunction, TableProviderFilterPushDown},
    prelude::Expr,
    scalar::ScalarValue,
};
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use futures::StreamExt as _;
use itertools::Itertools;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

use crate::delta_datafusion::schema_adapter::DeltaSchemaAdapterFactory;
use crate::delta_datafusion::{
    get_null_of_arrow_type, register_store, to_correct_scalar_value, DataFusionMixins as _,
    LogDataHandler,
};
use crate::kernel::schema::cast::cast_record_batch;
use crate::kernel::transaction::{CommitBuilder, PROTOCOL};
use crate::kernel::{Action, Add, EagerSnapshot, Remove};
use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::operations::write::WriterStatsConfig;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::{ensure_table_uri, DeltaTable};
use crate::{logstore::LogStoreRef, DeltaResult, DeltaTableError};

const PATH_COLUMN: &str = "__delta_rs_path";

/// DataSink implementation for delta lake
/// This uses DataSinkExec to handle the insert operation
/// Implements writing streams of RecordBatches to delta.
#[derive(Debug)]
pub struct DeltaDataSink {
    /// The log store
    log_store: LogStoreRef,
    /// The snapshot
    snapshot: EagerSnapshot,
    /// The save mode
    save_mode: SaveMode,
    /// The schema
    schema: SchemaRef,
    /// Metrics for monitoring throughput
    metrics: ExecutionPlanMetricsSet,
}

/// A [`DataSink`] implementation for writing to Delta Lake.
///
/// `DeltaDataSink` is used by [`DataSinkExec`] during query execution to
/// stream [`RecordBatch`]es into a Delta table. It encapsulates everything
/// needed to perform an insert/append/overwrite operation, including
/// transaction log access, snapshot state, and session configuration.
impl DeltaDataSink {
    /// Create a new `DeltaDataSink`
    pub fn new(log_store: LogStoreRef, snapshot: EagerSnapshot, save_mode: SaveMode) -> Self {
        Self {
            log_store,
            schema: snapshot.read_schema(),
            snapshot,
            save_mode,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a streaming transformed version of the input that converts dictionary columns
    /// This is used to convert dictionary columns to their native types
    fn create_converted_stream(
        &self,
        input: SendableRecordBatchStream,
        target_schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        use futures::StreamExt;

        let schema_for_closure = Arc::clone(&target_schema);
        let converted_stream = input.map(move |batch_result| {
            batch_result.and_then(|batch| {
                cast_record_batch(&batch, Arc::clone(&schema_for_closure), false, true)
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })
        });

        Box::pin(RecordBatchStreamAdapter::new(
            target_schema,
            converted_stream,
        ))
    }
}

/// Implementation of the `DataSink` trait for `DeltaDataSink`
/// This is used to write the data to the delta table
/// It implements the `DataSink` trait and is used by the `DataSinkExec` node
/// to write the data to the delta table
#[async_trait::async_trait]
impl DataSink for DeltaDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Write the data to the delta table
    /// This is used for insert into operation
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let target_schema = self.snapshot.input_schema();

        let mut stream = self.create_converted_stream(data, target_schema.clone());
        let partition_columns = self.snapshot.metadata().partition_columns();
        let object_store = self.log_store.object_store(None);
        let total_rows_metric = MetricBuilder::new(&self.metrics).counter("total_rows", 0);
        let stats_config = WriterStatsConfig::new(DataSkippingNumIndexedCols::AllColumns, None);
        let config = WriterConfig::new(
            self.snapshot.read_schema(),
            partition_columns.clone(),
            None,
            None,
            None,
            stats_config.num_indexed_cols,
            stats_config.stats_columns,
        );

        let mut writer = DeltaWriter::new(object_store, config);
        let mut total_rows = 0u64;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let batch_rows = batch.num_rows() as u64;
            total_rows += batch_rows;
            total_rows_metric.add(batch_rows as usize);
            writer
                .write(&batch)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        let add_actions = writer
            .close()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut actions = if self.save_mode == SaveMode::Overwrite {
            let current_files = self.snapshot.log_data().into_iter().map(|f| f.add_action());
            current_files
                .into_iter()
                .map(|add| {
                    Action::Remove(Remove {
                        path: add.path,
                        deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
                        data_change: true,
                        extended_file_metadata: Some(true),
                        partition_values: Some(add.partition_values),
                        size: Some(add.size),
                        tags: add.tags,
                        base_row_id: None,
                        default_row_commit_version: None,
                        deletion_vector: None,
                    })
                })
                .collect()
        } else {
            Vec::new()
        };

        actions.extend(add_actions.into_iter().map(Action::Add));

        let operation = DeltaOperation::Write {
            mode: self.save_mode,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns.clone())
            },
            predicate: None,
        };

        CommitBuilder::default()
            .with_actions(actions)
            .build(Some(&self.snapshot), self.log_store.clone(), operation)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(total_rows)
    }
}

/// Implementation of the `DisplayAs` trait for `DeltaDataSink`
impl DisplayAs for DeltaDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaDataSink")
    }
}

#[derive(Debug, Clone)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determined by `file_column_name`
    pub(super) include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    pub(super) file_column_name: Option<String>,
    /// Whether to wrap partition values in a dictionary encoding to potentially save space
    pub(super) wrap_partition_values: Option<bool>,
    /// Whether to push down filter in end result or just prune the files
    pub(super) enable_parquet_pushdown: bool,
    /// Schema to scan table with
    pub(super) schema: Option<SchemaRef>,
}

impl Default for DeltaScanConfigBuilder {
    fn default() -> Self {
        DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: None,
            enable_parquet_pushdown: true,
            schema: None,
        }
    }
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        self.file_column_name = Some(name.to_string());
        self.include_file_column = true;
        self
    }

    /// Whether to wrap partition values in a dictionary encoding
    pub fn wrap_partition_values(mut self, wrap: bool) -> Self {
        self.wrap_partition_values = Some(wrap);
        self
    }

    /// Allow pushdown of the scan filter
    /// When disabled the filter will only be used for pruning files
    pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
        self.enable_parquet_pushdown = pushdown;
        self
    }

    /// Use the provided [SchemaRef] for the [DeltaScan]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &EagerSnapshot) -> DeltaResult<DeltaScanConfig> {
        let file_column_name = if self.include_file_column {
            let input_schema = snapshot.input_schema();
            let mut column_names: HashSet<&String> = HashSet::new();
            for field in input_schema.fields.iter() {
                column_names.insert(field.name());
            }

            match &self.file_column_name {
                Some(name) => {
                    if column_names.contains(name) {
                        return Err(DeltaTableError::Generic(format!(
                            "Unable to add file path column since column with name {name} exits"
                        )));
                    }

                    Some(name.to_owned())
                }
                None => {
                    let prefix = PATH_COLUMN;
                    let mut idx = 0;
                    let mut name = prefix.to_owned();

                    while column_names.contains(&name) {
                        idx += 1;
                        name = format!("{prefix}_{idx}");
                    }

                    Some(name)
                }
            }
        } else {
            None
        };

        Ok(DeltaScanConfig {
            file_column_name,
            wrap_partition_values: self.wrap_partition_values.unwrap_or(true),
            enable_parquet_pushdown: self.enable_parquet_pushdown,
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additional metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
    /// Wrap partition values in a dictionary encoding
    pub wrap_partition_values: bool,
    /// Allow pushdown of the scan filter
    pub enable_parquet_pushdown: bool,
    /// Schema to read as
    pub schema: Option<SchemaRef>,
}

pub(crate) struct DeltaScanBuilder<'a> {
    snapshot: &'a EagerSnapshot,
    log_store: LogStoreRef,
    filter: Option<Expr>,
    session: &'a dyn Session,
    projection: Option<&'a Vec<usize>>,
    limit: Option<usize>,
    files: Option<&'a [Add]>,
    config: Option<DeltaScanConfig>,
}

impl<'a> DeltaScanBuilder<'a> {
    pub fn new(
        snapshot: &'a EagerSnapshot,
        log_store: LogStoreRef,
        session: &'a dyn Session,
    ) -> Self {
        DeltaScanBuilder {
            snapshot,
            log_store,
            filter: None,
            session,
            projection: None,
            limit: None,
            files: None,
            config: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_files(mut self, files: &'a [Add]) -> Self {
        self.files = Some(files);
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub async fn build(self) -> DeltaResult<DeltaScan> {
        PROTOCOL.can_read_from(self.snapshot)?;
        let config = match self.config {
            Some(config) => config,
            None => DeltaScanConfigBuilder::new().build(self.snapshot)?,
        };

        let schema = match config.schema.clone() {
            Some(value) => value,
            None => self.snapshot.read_schema(),
        };

        let logical_schema = df_logical_schema(
            self.snapshot,
            &config.file_column_name,
            Some(schema.clone()),
        )?;

        let logical_schema = if let Some(used_columns) = self.projection {
            let mut fields = Vec::with_capacity(used_columns.len());
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            // partition filters with Exact pushdown were removed from projection by DF optimizer,
            // we need to add them back for the predicate pruning to work
            if let Some(expr) = &self.filter {
                for c in expr.column_refs() {
                    let idx = logical_schema.index_of(c.name.as_str())?;
                    if !used_columns.contains(&idx) {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(Schema::new(fields))
        } else {
            logical_schema
        };

        let df_schema = logical_schema.clone().to_dfschema()?;

        let logical_filter = self
            .filter
            .clone()
            .map(|expr| simplify_expr(self.session, &df_schema, expr));
        // only inexact filters should be pushed down to the data source, doing otherwise
        // will make stats inexact and disable datafusion optimizations like AggregateStatistics
        let pushdown_filter = self
            .filter
            .and_then(|expr| {
                let predicates = split_conjunction(&expr);
                let pushdown_filters =
                    get_pushdown_filters(&predicates, self.snapshot.metadata().partition_columns());

                let filtered_predicates = predicates
                    .into_iter()
                    .zip(pushdown_filters.into_iter())
                    .filter_map(|(filter, pushdown)| {
                        if pushdown == TableProviderFilterPushDown::Inexact {
                            Some(filter.clone())
                        } else {
                            None
                        }
                    });
                conjunction(filtered_predicates)
            })
            .map(|expr| simplify_expr(self.session, &df_schema, expr));

        // Perform Pruning of files to scan
        let (files, files_scanned, files_pruned, pruning_mask) = match self.files {
            Some(files) => {
                let files = files.to_owned();
                let files_scanned = files.len();
                (files, files_scanned, 0, None)
            }
            None => {
                // early return in case we have no push down filters or limit
                if logical_filter.is_none() && self.limit.is_none() {
                    let files = self
                        .snapshot
                        .log_data()
                        .iter()
                        .map(|f| f.add_action())
                        .collect_vec();
                    let files_scanned = files.len();
                    (files, files_scanned, 0, None)
                } else {
                    let num_containers = self.snapshot.num_containers();

                    let files_to_prune = if let Some(predicate) = &logical_filter {
                        let pruning_predicate =
                            PruningPredicate::try_new(predicate.clone(), logical_schema.clone())?;
                        pruning_predicate.prune(self.snapshot)?
                    } else {
                        vec![true; num_containers]
                    };

                    // needed to enforce limit and deal with missing statistics
                    // rust port of https://github.com/delta-io/delta/pull/1495
                    let mut pruned_without_stats = Vec::new();
                    let mut rows_collected = 0;
                    let mut files = Vec::with_capacity(num_containers);

                    let file_actions: Vec<_> = self
                        .snapshot
                        .log_data()
                        .iter()
                        .map(|f| f.add_action())
                        .collect();

                    for (action, keep) in
                        file_actions.into_iter().zip(files_to_prune.iter().cloned())
                    {
                        // prune file based on predicate pushdown
                        if keep {
                            // prune file based on limit pushdown
                            if let Some(limit) = self.limit {
                                if let Some(stats) = action.get_stats()? {
                                    if rows_collected <= limit as i64 {
                                        rows_collected += stats.num_records;
                                        files.push(action.to_owned());
                                    } else {
                                        break;
                                    }
                                } else {
                                    // some files are missing stats; skipping but storing them
                                    // in a list in case we can't reach the target limit
                                    pruned_without_stats.push(action.to_owned());
                                }
                            } else {
                                files.push(action.to_owned());
                            }
                        }
                    }

                    if let Some(limit) = self.limit {
                        if rows_collected < limit as i64 {
                            files.extend(pruned_without_stats);
                        }
                    }

                    let files_scanned = files.len();
                    let files_pruned = num_containers - files_scanned;
                    (files, files_scanned, files_pruned, Some(files_to_prune))
                }
            }
        };

        // TODO we group files together by their partition values. If the table is partitioned
        // and partitions are somewhat evenly distributed, probably not the worst choice ...
        // However we may want to do some additional balancing in case we are far off from the above.
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        let table_partition_cols = &self.snapshot.metadata().partition_columns();

        for action in files.iter() {
            let mut part = partitioned_file_from_action(action, table_partition_cols, &schema);

            if config.file_column_name.is_some() {
                let partition_value = if config.wrap_partition_values {
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(action.path.clone())))
                } else {
                    ScalarValue::Utf8(Some(action.path.clone()))
                };
                part.partition_values.push(partition_value);
            }

            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        let file_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<arrow::datatypes::FieldRef>>(),
        ));

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|name| schema.field_with_name(name).map(|f| f.to_owned()))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        if let Some(file_column_name) = &config.file_column_name {
            let field_name_datatype = if config.wrap_partition_values {
                wrap_partition_type_in_dict(DataType::Utf8)
            } else {
                DataType::Utf8
            };
            table_partition_cols.push(Field::new(
                file_column_name.clone(),
                field_name_datatype,
                false,
            ));
        }

        // FIXME - where is the correct place to marry file pruning with statistics pruning?
        //  Temporarily re-generating the log handler, just so that we can compute the stats.
        //  Should we update datafusion_table_statistics to optionally take the mask?
        let stats = if let Some(mask) = pruning_mask {
            let es = self.snapshot.snapshot();
            let mut pruned_batches = Vec::new();
            let mut mask_offset = 0;

            for batch in self.snapshot.files()? {
                let batch_size = batch.num_rows();
                let batch_mask = &mask[mask_offset..mask_offset + batch_size];
                let batch_mask_array = BooleanArray::from(batch_mask.to_vec());
                let pruned_batch = filter_record_batch(batch, &batch_mask_array)?;
                if pruned_batch.num_rows() > 0 {
                    pruned_batches.push(pruned_batch);
                }
                mask_offset += batch_size;
            }

            LogDataHandler::new(&pruned_batches, es.table_configuration()).statistics()
        } else {
            self.snapshot.log_data().statistics()
        };

        let stats = stats.unwrap_or(Statistics::new_unknown(&schema));

        let parquet_options = TableParquetOptions {
            global: self.session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let mut file_source = ParquetSource::new(parquet_options);

        // Sometimes (i.e Merge) we want to prune files that don't make the
        // filter and read the entire contents for files that do match the
        // filter
        if let Some(predicate) = pushdown_filter {
            if config.enable_parquet_pushdown {
                file_source = file_source.with_predicate(predicate);
            }
        };
        let file_source =
            file_source.with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}))?;

        let file_scan_config =
            FileScanConfigBuilder::new(self.log_store.object_store_url(), file_schema, file_source)
                .with_file_groups(
                    // If all files were filtered out, we still need to emit at least one partition to
                    // pass datafusion sanity checks.
                    //
                    // See https://github.com/apache/datafusion/issues/11322
                    if file_groups.is_empty() {
                        vec![FileGroup::from(vec![])]
                    } else {
                        file_groups.into_values().map(FileGroup::from).collect()
                    },
                )
                .with_statistics(stats)
                .with_projection_indices(self.projection.cloned())
                .with_limit(self.limit)
                .with_table_partition_cols(table_partition_cols)
                .build();

        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics)
            .global_counter("files_scanned")
            .add(files_scanned);
        MetricBuilder::new(&metrics)
            .global_counter("files_pruned")
            .add(files_pruned);

        Ok(DeltaScan {
            table_uri: ensure_table_uri(self.log_store.root_uri())?.as_str().into(),
            parquet_scan: DataSourceExec::from_data_source(file_scan_config),
            config,
            logical_schema,
            metrics,
        })
    }
}

// TODO: implement this for Snapshot, not for DeltaTable since DeltaTable has unknown load state.
// the unwraps in the schema method are a dead giveaway ..
#[async_trait::async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.snapshot().unwrap().snapshot().read_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        register_store(self.log_store(), session.runtime_env().as_ref());
        let filter_expr = conjunction(filters.iter().cloned());

        let scan = DeltaScanBuilder::new(self.snapshot()?.snapshot(), self.log_store(), session)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .build()
            .await?;

        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot()?.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot().ok()?.datafusion_table_statistics()
    }
}

/// A Delta table provider that enables additional metadata columns to be included during the scan
#[derive(Debug)]
pub struct DeltaTableProvider {
    snapshot: EagerSnapshot,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: Arc<Schema>,
    files: Option<Vec<Add>>,
}

impl DeltaTableProvider {
    /// Build a DeltaTableProvider
    pub fn try_new(
        snapshot: EagerSnapshot,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: df_logical_schema(&snapshot, &config.file_column_name, config.schema.clone())?,
            snapshot,
            log_store,
            config,
            files: None,
        })
    }

    /// Define which files to consider while building a scan, for advanced usecases
    pub fn with_files(mut self, files: Vec<Add>) -> DeltaTableProvider {
        self.files = Some(files);
        self
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        register_store(self.log_store.clone(), session.runtime_env().as_ref());
        let filter_expr = conjunction(filters.iter().cloned());

        let mut scan = DeltaScanBuilder::new(&self.snapshot, self.log_store.clone(), session)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .with_scan_config(self.config.clone());

        if let Some(files) = &self.files {
            scan = scan.with_files(files);
        }
        Ok(Arc::new(scan.build().await?))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(get_pushdown_filters(
            filter,
            self.snapshot.metadata().partition_columns(),
        ))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot.log_data().statistics()
    }

    /// Insert the data into the delta table
    /// Insert operation is only supported for Append and Overwrite
    /// Return the execution plan
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        register_store(self.log_store.clone(), state.runtime_env().as_ref());

        let save_mode = match insert_op {
            InsertOp::Append => SaveMode::Append,
            InsertOp::Overwrite => SaveMode::Overwrite,
            InsertOp::Replace => {
                return Err(DataFusionError::Plan(
                    "Replace operation is not supported for DeltaTableProvider".to_string(),
                ))
            }
        };

        let data_sink =
            DeltaDataSink::new(self.log_store.clone(), self.snapshot.clone(), save_mode);

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(data_sink),
            None,
        )))
    }
}

// TODO: this will likely also need to perform column mapping later when we support reader protocol v2
/// A wrapper for parquet scans
#[derive(Debug)]
pub struct DeltaScan {
    /// The URL of the ObjectStore root
    pub table_uri: String,
    /// Column that contains an index that maps to the original metadata Add
    pub config: DeltaScanConfig,
    /// The parquet scan to wrap
    pub parquet_scan: Arc<dyn ExecutionPlan>,
    /// The schema of the table to be used when evaluating expressions
    pub logical_schema: Arc<Schema>,
    /// Metrics for scan reported via DataFusion
    pub(super) metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct DeltaScanWire {
    pub table_uri: String,
    pub config: DeltaScanConfig,
    pub logical_schema: Arc<Schema>,
}

impl DisplayAs for DeltaScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaScan")
    }
}

impl ExecutionPlan for DeltaScan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parquet_scan.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.parquet_scan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parquet_scan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "DeltaScan wrong number of children {}",
                children.len()
            )));
        }
        Ok(Arc::new(DeltaScan {
            table_uri: self.table_uri.clone(),
            config: self.config.clone(),
            parquet_scan: children[0].clone(),
            logical_schema: self.logical_schema.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(parquet_scan) = self.parquet_scan.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(DeltaScan {
                table_uri: self.table_uri.clone(),
                config: self.config.clone(),
                parquet_scan,
                logical_schema: self.logical_schema.clone(),
                metrics: self.metrics.clone(),
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
        self.parquet_scan.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.parquet_scan.partition_statistics(partition)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
fn df_logical_schema(
    snapshot: &EagerSnapshot,
    file_column_name: &Option<String>,
    schema: Option<SchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema(),
    };
    let table_partition_cols = snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|f| !table_partition_cols.contains(f.name()))
        .cloned()
        .collect();

    for partition_col in table_partition_cols.iter() {
        fields.push(Arc::new(
            input_schema
                .field_with_name(partition_col)
                .unwrap()
                .to_owned(),
        ));
    }

    if let Some(file_column_name) = file_column_name {
        fields.push(Arc::new(Field::new(file_column_name, DataType::Utf8, true)));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn simplify_expr(context: &dyn Session, df_schema: &DFSchema, expr: Expr) -> Arc<dyn PhysicalExpr> {
    // Simplify the expression first
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    let simplified = simplifier.simplify(expr).unwrap();

    context.create_physical_expr(simplified, df_schema).unwrap()
}

fn get_pushdown_filters(
    filter: &[&Expr],
    partition_cols: &[String],
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .cloned()
        .map(|expr| {
            let applicable = expr_is_exact_predicate_for_cols(partition_cols, expr);
            if !expr.column_refs().is_empty() && applicable {
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}

// inspired from datafusion::listing::helpers, but adapted to only stats based pruning
fn expr_is_exact_predicate_for_cols(partition_cols: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.apply(|expr| match expr {
        Expr::Column(Column { ref name, .. }) => {
            is_applicable &= partition_cols.contains(name);

            // TODO: decide if we should constrain this to Utf8 columns (including views, dicts etc)

            if is_applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::BinaryExpr(BinaryExpr { ref op, .. }) => {
            is_applicable &= matches!(
                op,
                Operator::And
                    | Operator::Or
                    | Operator::NotEq
                    | Operator::Eq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
            );
            if is_applicable {
                Ok(TreeNodeRecursion::Continue)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(_, _)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::Between(_)
        | Expr::InList(_) => Ok(TreeNodeRecursion::Continue),
        _ => {
            is_applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    })
    .unwrap();
    is_applicable
}

fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &Schema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            action
                .partition_values
                .get(part)
                .map(|val| {
                    schema
                        .field_with_name(part)
                        .map(|field| match val {
                            Some(value) => to_correct_scalar_value(
                                &serde_json::Value::String(value.to_string()),
                                field.data_type(),
                            )
                            .unwrap_or(Some(ScalarValue::Null))
                            .unwrap_or(ScalarValue::Null),
                            None => get_null_of_arrow_type(field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        })
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or(ScalarValue::Null)
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified = Utc.from_utc_datetime(
        &DateTime::from_timestamp(ts_secs, ts_ns as u32)
            .unwrap()
            .naive_utc(),
    );
    PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            ..action.try_into().unwrap()
        },
        partition_values,
        range: None,
        extensions: None,
        statistics: None,
        metadata_size_hint: None,
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::{DataType, PrimitiveType, StructField, StructType};
    use crate::operations::create::CreateBuilder;
    use crate::{DeltaTable, DeltaTableError};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use chrono::{TimeZone, Utc};
    use datafusion::common::ScalarValue;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionState;
    use datafusion::logical_expr::dml::InsertOp;
    use object_store::path::Path;
    use std::sync::Arc;

    use super::*;

    async fn create_test_table() -> Result<DeltaTable, DeltaTableError> {
        use tempfile::TempDir;

        let tmp_dir = TempDir::new().unwrap();
        let table_path = tmp_dir.path().to_str().unwrap();

        let schema = StructType::try_new(vec![
            StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Long),
                false,
            ),
            StructField::new(
                "value".to_string(),
                DataType::Primitive(PrimitiveType::String),
                false,
            ),
        ])?;

        CreateBuilder::new()
            .with_location(table_path)
            .with_columns(schema.fields().cloned())
            .await
    }

    fn create_test_session_state() -> Arc<SessionState> {
        use datafusion::execution::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;
        let config = SessionConfig::new();
        Arc::new(SessionStateBuilder::new().with_config(config).build())
    }

    #[tokio::test]
    async fn test_insert_into_simple() {
        let table = create_test_table().await.unwrap();
        let session_state = create_test_session_state();

        let scan_config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();
        let table_provider = DeltaTableProvider::try_new(
            table.snapshot().unwrap().snapshot().clone(),
            table.log_store(),
            scan_config,
        )
        .unwrap();

        let schema = table_provider.schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["test1", "test2"])),
            ],
        )
        .unwrap();

        let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let logical_plan = mem_table
            .scan(&*session_state, None, &[], None)
            .await
            .unwrap();

        let result_plan = table_provider
            .insert_into(&*session_state, logical_plan, InsertOp::Append)
            .await
            .unwrap();

        assert!(!result_plan.schema().fields().is_empty());

        let result_schema = result_plan.schema();
        assert_eq!(result_schema.fields().len(), 1);
        assert_eq!(result_schema.field(0).name(), "count");
        assert_eq!(
            result_schema.field(0).data_type(),
            &arrow::datatypes::DataType::UInt64
        );
        assert_eq!(result_plan.children().len(), 1);
        assert!(result_plan.metrics().is_some());
    }

    #[test]
    fn test_partitioned_file_from_action() {
        let mut partition_values = std::collections::HashMap::new();
        partition_values.insert("month".to_string(), Some("1".to_string()));
        partition_values.insert("year".to_string(), Some("2015".to_string()));
        let action = Add {
            path: "year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string(),
            size: 10644,
            partition_values,
            modification_time: 1660497727833,
            data_change: true,
            stats: None,
            deletion_vector: None,
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };
        let schema = Schema::new(vec![
            Field::new("year", ArrowDataType::Int64, true),
            Field::new("month", ArrowDataType::Int64, true),
        ]);

        let part_columns = vec!["year".to_string(), "month".to_string()];
        let file = partitioned_file_from_action(&action, &part_columns, &schema);
        let ref_file = PartitionedFile {
            object_meta: object_store::ObjectMeta {
                location: Path::from("year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string()),
                last_modified: Utc.timestamp_millis_opt(1660497727833).unwrap(),
                size: 10644,
                e_tag: None,
                version: None,
            },
            partition_values: [ScalarValue::Int64(Some(2015)), ScalarValue::Int64(Some(1))].to_vec(),
            range: None,
            extensions: None,
            statistics: None,
            metadata_size_hint: None,
        };
        assert_eq!(file.partition_values, ref_file.partition_values)
    }
}
