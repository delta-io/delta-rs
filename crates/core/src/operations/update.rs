//! Update records from a Delta Table for records satisfy a predicate
//!
//! When a predicate is not provided then all records are updated from the Delta
//! Table. Otherwise a scan of the Delta table is performed to mark any files
//! that contain records that satisfy the predicate. Once they are determined
//! then column values are updated with new values provided by the user
//!
//!
//! Predicates MUST be deterministic otherwise undefined behaviour may occur during the
//! scanning and rewriting phase.
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = UpdateBuilder::new(table.object_store(), table.state)
//!     .with_predicate(col("col1").eq(lit(1)))
//!     .with_update("value", col("value") + lit(20))
//!     .await?;
//! ````

use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow::{array::AsArray, datatypes::UInt16Type};
use arrow_array::StringArray;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Column, HashSet, ScalarValue, exec_datafusion_err},
    functions_aggregate::expr_fn::first_value,
    logical_expr::utils::{conjunction, split_conjunction_owned},
    optimizer::simplify_expressions::simplify_predicates,
    physical_plan::{execute_stream, visit_execution_plan},
    prelude::Expr,
};
use datafusion::{common::DFSchema, error::Result as DataFusionResult};
use datafusion::{
    datasource::provider_as_source,
    execution::context::SessionState,
    physical_plan::{ExecutionPlan, metrics::MetricBuilder},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode, case, col, lit, when,
    },
};
use datafusion_datasource::file_scan_config::wrap_partition_value_in_dict;
use futures::{StreamExt as _, TryStreamExt as _, future::BoxFuture, stream};
use itertools::Itertools as _;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use tracing::log::*;
use uuid::Uuid;

use super::write::WriterStatsConfig;
use super::{
    CustomExecuteHandler, Operation,
    write::execution::{write_execution_plan, write_execution_plan_cdc},
};
use crate::delta_datafusion::{
    DeltaScanNext, DeltaScanVisitor, Expression, FILE_ID_COLUMN_DEFAULT, update_datafusion_session,
};
use crate::kernel::resolve_snapshot;
use crate::operations::cdc::*;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use crate::{delta_datafusion::expr::parse_predicate_expression, logstore::LogStoreRef};
use crate::{
    delta_datafusion::{
        DeltaColumn, create_session,
        expr::fmt_expr_to_sql,
        logical::{LogicalPlanBuilderExt as _, LogicalPlanExt as _, MetricObserver},
        physical::{MetricObserverExec, find_metric_node, get_metric},
    },
    kernel::{
        Action, EagerSnapshot,
        transaction::{CommitBuilder, CommitProperties, PROTOCOL},
    },
    table::config::TablePropertiesExt,
};

/// Custom column name used for marking internal [RecordBatch] rows as updated
pub(crate) const UPDATE_PREDICATE_COLNAME: &str = "__delta_rs_update_predicate";

#[cfg(test)]
mod tests;

const UPDATE_COUNT_ID: &str = "update_source_count";
const UPDATE_ROW_COUNT: &str = "num_updated_rows";
const COPIED_ROW_COUNT: &str = "num_copied_rows";

/// Updates records in the Delta Table.
/// See this module's documentation for more information
pub struct UpdateBuilder {
    /// Which records to update
    predicate: Option<Expression>,
    /// How to update columns in a record that match the predicate
    updates: HashMap<Column, Expression>,
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    session: Option<Arc<dyn Session>>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// safe_cast determines how data types that do not match the underlying table are handled
    /// By default an error is returned
    safe_cast: bool,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

#[derive(Default, Serialize, Debug)]
/// Metrics collected during the Update operation
pub struct UpdateMetrics {
    /// Number of files added.
    pub num_added_files: usize,
    /// Number of files removed.
    pub num_removed_files: usize,
    /// Number of rows updated.
    pub num_updated_rows: usize,
    /// Number of rows just copied over in the process of updating files.
    pub num_copied_rows: usize,
    /// Time taken to execute the entire operation.
    pub execution_time_ms: u64,
    /// Time taken to scan the files for matches.
    pub scan_time_ms: u64,
}

impl super::Operation for UpdateBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl UpdateBuilder {
    /// Create a new ['UpdateBuilder']
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            predicate: None,
            updates: HashMap::new(),
            snapshot,
            log_store,
            session: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            safe_cast: false,
            custom_execute_handler: None,
        }
    }

    /// Which records to update
    pub fn with_predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// Perform an additional update expression during the operation
    pub fn with_update<S: Into<DeltaColumn>, E: Into<Expression>>(
        mut self,
        column: S,
        expression: E,
    ) -> Self {
        self.updates.insert(column.into().into(), expression.into());
        self
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Writer properties passed to parquet writer for when fiiles are rewritten
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Specify the cast options to use when casting columns that do not match
    /// the table's schema.  When `cast_options.safe` is set true then any
    /// failures to cast a datatype will use null instead of returning an error
    /// to the user.
    ///
    /// Example (column's type is int):
    /// Input               Output
    /// 123         ->      123
    /// Test123     ->      null
    pub fn with_safe_cast(mut self, safe_cast: bool) -> Self {
        self.safe_cast = safe_cast;
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct UpdateMetricExtensionPlanner {}

impl UpdateMetricExtensionPlanner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ExtensionPlanner for UpdateMetricExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(metric_observer) = node.as_any().downcast_ref::<MetricObserver>()
            && metric_observer.id.eq(UPDATE_COUNT_ID)
        {
            return Ok(Some(MetricObserverExec::try_new(
                UPDATE_COUNT_ID.into(),
                physical_inputs,
                |batch, metrics| {
                    let array = batch.column_by_name(UPDATE_PREDICATE_COLNAME).unwrap();
                    let copied_rows = array.null_count();
                    let num_updated = array.len() - copied_rows;

                    MetricBuilder::new(metrics)
                        .global_counter(UPDATE_ROW_COUNT)
                        .add(num_updated);

                    MetricBuilder::new(metrics)
                        .global_counter(COPIED_ROW_COUNT)
                        .add(copied_rows);
                },
            )?));
        }
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        operation = "update",
        version = snapshot.version(),
        table_uri = %log_store.root_url(),
    )
)]
async fn execute(
    predicate: Expr,
    updates: HashMap<Column, Expression>,
    log_store: LogStoreRef,
    snapshot: &EagerSnapshot,
    session: &dyn Session,
    writer_properties: Option<WriterProperties>,
    operation_id: Uuid,
) -> DeltaResult<(Vec<Action>, UpdateMetrics)> {
    // Validate the predicate and update expressions.
    //
    // If the predicate is not set, then all files need to be updated.
    // If it only contains partition columns then perform in memory-scan.
    // Otherwise, scan files for records that satisfy the predicate.
    //
    // For files that were identified, scan for records that match the predicate,
    // perform update operations, and then commit add and remove actions to
    // the log.

    let exec_start = Instant::now();
    let mut metrics = UpdateMetrics::default();

    let schema = DFSchema::try_from(snapshot.arrow_schema())?;
    let updates: HashMap<_, _> = updates
        .into_iter()
        .map(|(key, expr)| match expr {
            Expression::DataFusion(e) => Ok((key.name, e)),
            Expression::String(s) => {
                parse_predicate_expression(&schema, s, session).map(|e| (key.name, e))
            }
        })
        .try_collect()?;

    let current_metadata = snapshot.metadata();
    let table_partition_cols = current_metadata.partition_columns().clone();

    let scan_start = Instant::now();

    let skipping_pred = simplify_predicates(split_conjunction_owned(predicate))?;
    let predicate = conjunction(skipping_pred.clone()).unwrap_or(lit(true));

    // Scan the delta table with a dedicated predicate applied for file skipping
    // and with the source file path exosed as column.
    let table_source = provider_as_source(
        DeltaScanNext::builder()
            .with_eager_snapshot(snapshot.clone())
            .with_file_skipping_predicates(skipping_pred.clone())
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .await?,
    );

    // the kernel scan only provides a best effort file skipping, in this case
    // we want to determine the file we certainly need to rewrite. For this
    // we perform an initial aggreagte scan to see if we can quickly find
    // at least one matching record in the files.
    let files_plan = LogicalPlanBuilder::scan("scan", table_source.clone(), None)?
        .filter(predicate.clone())?
        .aggregate(
            [col(FILE_ID_COLUMN_DEFAULT)],
            [first_value(lit(1_i32), vec![])],
        )?
        .build()?;
    let files_exec = session.create_physical_plan(&files_plan).await?;
    let valid_files: HashSet<_> = execute_stream(files_exec, session.task_ctx())?
        .map_ok(|f| {
            let dict_arr = f.column(0).as_dictionary::<UInt16Type>();
            let typed_dict = dict_arr.downcast_dict::<StringArray>().unwrap();
            typed_dict
                .values()
                .iter()
                .flatten()
                .map(|s| s.to_string())
                .collect_vec()
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect();

    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis() as u64;

    if valid_files.is_empty() {
        return Ok((vec![], metrics));
    }

    // Run a table scan limiting the data to that originating from valid files.
    let file_list = valid_files
        .iter()
        .cloned()
        .map(|v| lit(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(v)))))
        .collect_vec();
    let plan = LogicalPlanBuilder::scan("target", table_source, None)?
        .filter(col(FILE_ID_COLUMN_DEFAULT).in_list(file_list, false))?
        .project_away([FILE_ID_COLUMN_DEFAULT])?
        .build()?;

    // Take advantage of how null counts are tracked in arrow arrays use the
    // null count to track how many records do NOT satisfy the predicate.  The
    // count is then exposed through the metrics through the `UpdateCountExec`
    // execution plan
    let predicate_null = when(predicate, lit(true)).otherwise(lit(ScalarValue::Boolean(None)))?;
    let input = plan
        .clone()
        .into_builder()
        .with_column(UPDATE_PREDICATE_COLNAME, predicate_null)?
        .build()?;

    let plan_with_metrics = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: UPDATE_COUNT_ID.into(),
            input,
            enable_pushdown: false,
        }),
    });

    let expressions: Vec<_> = plan_with_metrics
        .schema()
        .fields()
        .into_iter()
        .map(|field| {
            let expr = match updates.get(field.name()) {
                Some(expr) => case(col(UPDATE_PREDICATE_COLNAME))
                    .when(lit(true), expr.to_owned())
                    .otherwise(col(Column::from_name(field.name())))?
                    .alias(field.name()),
                None => col(Column::from_name(field.name())),
            };
            Ok::<_, DataFusionError>(expr)
        })
        .try_collect()?;

    let plan_updated = LogicalPlanBuilder::new(plan_with_metrics)
        .project(expressions.clone())?
        .project_away([UPDATE_PREDICATE_COLNAME])?
        .build()?;

    let physical_plan = session.create_physical_plan(&plan_updated).await?;
    let tracker = CDCTracker::new(plan, plan_updated);

    let writer_stats_config = WriterStatsConfig::from_config(snapshot.table_configuration());
    let mut actions = write_execution_plan(
        Some(snapshot),
        session,
        physical_plan.clone(),
        table_partition_cols.clone(),
        log_store.object_store(Some(operation_id)).clone(),
        Some(snapshot.table_properties().target_file_size().get() as usize),
        None,
        writer_properties.clone(),
        writer_stats_config.clone(),
    )
    .await?;

    let err = || DeltaTableError::Generic("Unable to locate expected metric node".into());
    let update_count = find_metric_node(UPDATE_COUNT_ID, &physical_plan).ok_or_else(err)?;
    let update_count_metrics = update_count.metrics().unwrap();

    metrics.num_updated_rows = get_metric(&update_count_metrics, UPDATE_ROW_COUNT);
    metrics.num_copied_rows = get_metric(&update_count_metrics, COPIED_ROW_COUNT);

    // extract the predicate from the execution plan as it may have been optimized
    // via the passed seeion or altered otherwise. So we get a consistent result.
    let mut visitor = DeltaScanVisitor::default();
    visit_execution_plan(physical_plan.as_ref(), &mut visitor)?;
    let delta_plan = visitor
        .delta_plan
        .ok_or_else(|| exec_datafusion_err!("Expected DeltaScan node to be in plan."))?;

    let root_url = Arc::new(snapshot.table_configuration().table_root().clone());
    let removes: Vec<_> = snapshot
        .file_views(log_store.as_ref(), delta_plan.skipping_predicate)
        .zip(stream::iter(std::iter::repeat((
            root_url,
            Arc::new(valid_files),
        ))))
        .map(|(f, u)| f.map(|f| (f, u)))
        .try_filter_map(|(f, (root, valid))| async move {
            let url = root
                .clone()
                .join(f.path_raw())
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            let is_valid = valid.contains(url.as_ref());
            Ok(is_valid.then(|| Action::Remove(f.remove_action(true))))
        })
        .try_collect()
        .await?;

    metrics.num_added_files = actions.len();
    metrics.num_removed_files = removes.len();

    actions.extend(removes);

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;

    if let Ok(true) = should_write_cdc(&snapshot) {
        match tracker.collect() {
            Ok(cdc_plan) => {
                let cdc_exec = session.create_physical_plan(&cdc_plan).await?;
                let cdc_actions = write_execution_plan_cdc(
                    Some(snapshot),
                    session,
                    cdc_exec,
                    table_partition_cols,
                    log_store.object_store(Some(operation_id)),
                    Some(snapshot.table_properties().target_file_size().get() as usize),
                    None,
                    writer_properties,
                    writer_stats_config,
                )
                .await?;
                actions.extend(cdc_actions);
            }
            Err(err) => {
                error!("Failed to collect CDC batches: {err:#?}");
            }
        };
    }

    Ok((actions, metrics))
}

impl std::future::IntoFuture for UpdateBuilder {
    type Output = DeltaResult<(DeltaTable, UpdateMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), true, None).await?;
            PROTOCOL.check_append_only(&snapshot)?;
            PROTOCOL.can_write_to(&snapshot)?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let session = if let Some(session) = this.session {
                session
            } else {
                Arc::new(create_session().into_inner().state())
            };
            update_datafusion_session(&this.log_store, session.as_ref(), Some(operation_id))?;

            if this.updates.is_empty() {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot)),
                    UpdateMetrics::default(),
                ));
            }

            let schema = DFSchema::try_from(snapshot.arrow_schema())?;
            let predicate = match this.predicate {
                Some(predicate) => match predicate {
                    Expression::DataFusion(expr) => Some(expr),
                    Expression::String(s) => {
                        Some(parse_predicate_expression(&schema, s, session.as_ref())?)
                    }
                },
                None => None,
            };

            let predicate = predicate.unwrap_or(lit(true));
            let operation = DeltaOperation::Update {
                predicate: Some(fmt_expr_to_sql(&predicate)?),
            };

            let (actions, metrics) = execute(
                predicate,
                this.updates,
                this.log_store.clone(),
                &snapshot,
                session.as_ref(),
                this.writer_properties,
                operation_id,
            )
            .await?;

            // if no files were re-written, we can skip the commit.
            if actions.is_empty() {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot)),
                    metrics,
                ));
            }

            let mut props = this.commit_properties;
            props
                .app_metadata
                .insert("readVersion".to_owned(), snapshot.version().into());
            props.app_metadata.insert(
                "operationMetrics".to_owned(),
                serde_json::to_value(&metrics)?,
            );

            let handle = this.custom_execute_handler.take();
            let snapshot = CommitBuilder::from(props)
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(handle)
                .build(Some(&snapshot), this.log_store.clone(), operation)
                .await?
                .snapshot()
                .snapshot;

            Ok((
                DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot)),
                metrics,
            ))
        })
    }
}
