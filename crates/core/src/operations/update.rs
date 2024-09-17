//! Update records from a Delta Table for records statisfy a predicate
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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use datafusion::error::Result as DataFusionResult;
use datafusion::{
    dataframe::DataFrame,
    datasource::provider_as_source,
    execution::context::SessionState,
    execution::session_state::SessionStateBuilder,
    physical_plan::{metrics::MetricBuilder, ExecutionPlan},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::SessionContext,
};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{
    case, col, lit, when, Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
};
use futures::future::BoxFuture;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use tracing::log::*;

use super::write::{write_execution_plan, write_execution_plan_cdc};
use super::{
    datafusion_utils::Expression,
    transaction::{CommitBuilder, CommitProperties},
};
use super::{transaction::PROTOCOL, write::WriterStatsConfig};
use crate::delta_datafusion::{find_files, planner::DeltaPlanner, register_store};
use crate::kernel::{Action, Remove};
use crate::logstore::LogStoreRef;
use crate::operations::cdc::*;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{
    delta_datafusion::{
        expr::fmt_expr_to_sql,
        logical::MetricObserver,
        physical::{find_metric_node, get_metric, MetricObserverExec},
        DataFusionMixins, DeltaColumn, DeltaScanConfigBuilder, DeltaSessionContext,
        DeltaTableProvider,
    },
    DeltaTableError,
};
use crate::{DeltaResult, DeltaTable};

/// Custom column name used for marking internal [RecordBatch] rows as updated
pub(crate) const UPDATE_PREDICATE_COLNAME: &str = "__delta_rs_update_predicate";

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
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// safe_cast determines how data types that do not match the underlying table are handled
    /// By default an error is returned
    safe_cast: bool,
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

impl super::Operation<()> for UpdateBuilder {}

impl UpdateBuilder {
    /// Create a new ['UpdateBuilder']
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            predicate: None,
            updates: HashMap::new(),
            snapshot,
            log_store,
            state: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            safe_cast: false,
        }
    }

    /// Which records to update
    pub fn with_predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// Perform an additional update expression during the operaton
    pub fn with_update<S: Into<DeltaColumn>, E: Into<Expression>>(
        mut self,
        column: S,
        expression: E,
    ) -> Self {
        self.updates.insert(column.into().into(), expression.into());
        self
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
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
}

#[derive(Clone)]
struct UpdateMetricExtensionPlanner {}

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
        if let Some(metric_observer) = node.as_any().downcast_ref::<MetricObserver>() {
            if metric_observer.id.eq(UPDATE_COUNT_ID) {
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
        }
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    predicate: Option<Expression>,
    updates: HashMap<Column, Expression>,
    log_store: LogStoreRef,
    snapshot: DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    mut commit_properties: CommitProperties,
    _safe_cast: bool,
) -> DeltaResult<(DeltaTableState, UpdateMetrics)> {
    // Validate the predicate and update expressions.
    //
    // If the predicate is not set, then all files need to be updated.
    // If it only contains partition columns then perform in memory-scan.
    // Otherwise, scan files for records that satisfy the predicate.
    //
    // For files that were identified, scan for records that match the predicate,
    // perform update operations, and then commit add and remove actions to
    // the log.
    if !&snapshot.load_config().require_files {
        return Err(DeltaTableError::NotInitializedWithFiles("UPDATE".into()));
    }

    let update_planner = DeltaPlanner::<UpdateMetricExtensionPlanner> {
        extension_planner: UpdateMetricExtensionPlanner {},
    };

    let state = SessionStateBuilder::new_from_existing(state)
        .with_query_planner(Arc::new(update_planner))
        .build();

    let exec_start = Instant::now();
    let mut metrics = UpdateMetrics::default();

    if updates.is_empty() {
        return Ok((snapshot, metrics));
    }

    let predicate = match predicate {
        Some(predicate) => match predicate {
            Expression::DataFusion(expr) => Some(expr),
            Expression::String(s) => Some(snapshot.parse_predicate_expression(s, &state)?),
        },
        None => None,
    };

    let updates = updates
        .into_iter()
        .map(|(key, expr)| match expr {
            Expression::DataFusion(e) => Ok((key.name, e)),
            Expression::String(s) => snapshot
                .parse_predicate_expression(s, &state)
                .map(|e| (key.name, e)),
        })
        .collect::<Result<HashMap<String, Expr>, _>>()?;

    let current_metadata = snapshot.metadata();
    let table_partition_cols = current_metadata.partition_columns.clone();

    let scan_start = Instant::now();
    let candidates = find_files(&snapshot, log_store.clone(), &state, predicate.clone()).await?;
    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis() as u64;

    if candidates.candidates.is_empty() {
        return Ok((snapshot, metrics));
    }

    let predicate = predicate.unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));

    let scan_config = DeltaScanConfigBuilder::default()
        .with_file_column(false)
        .with_schema(snapshot.input_schema()?)
        .build(&snapshot)?;

    // For each rewrite evaluate the predicate and then modify each expression
    // to either compute the new value or obtain the old one then write these batches
    let target_provider = Arc::new(
        DeltaTableProvider::try_new(snapshot.clone(), log_store.clone(), scan_config.clone())?
            .with_files(candidates.candidates.clone()),
    );

    let target_provider = provider_as_source(target_provider);
    let plan = LogicalPlanBuilder::scan("target", target_provider.clone(), None)?.build()?;

    let df = DataFrame::new(state.clone(), plan);

    // Take advantage of how null counts are tracked in arrow arrays use the
    // null count to track how many records do NOT statisfy the predicate.  The
    // count is then exposed through the metrics through the `UpdateCountExec`
    // execution plan
    let predicate_null =
        when(predicate.clone(), lit(true)).otherwise(lit(ScalarValue::Boolean(None)))?;

    let df_with_update_col = df
        .clone()
        .with_column(UPDATE_PREDICATE_COLNAME, predicate_null)?;

    let plan_with_metrics = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: UPDATE_COUNT_ID.into(),
            input: df_with_update_col.into_unoptimized_plan(),
            enable_pushdown: false,
        }),
    });

    let df_with_predicate_and_metrics = DataFrame::new(state.clone(), plan_with_metrics);

    let expressions: Vec<Expr> = df_with_predicate_and_metrics
        .schema()
        .fields()
        .into_iter()
        .map(|field| {
            let field_name = field.name();
            let expr = match updates.get(field_name) {
                Some(expr) => case(col(UPDATE_PREDICATE_COLNAME))
                    .when(lit(true), expr.to_owned())
                    .otherwise(col(Column::from_name(field_name)))?
                    .alias(field_name),
                None => col(Column::from_name(field_name)),
            };
            Ok(expr)
        })
        .collect::<DeltaResult<Vec<Expr>>>()?;

    let updated_df = df_with_predicate_and_metrics.select(expressions.clone())?;
    let physical_plan = updated_df.clone().create_physical_plan().await?;
    let writer_stats_config = WriterStatsConfig::new(
        snapshot.table_config().num_indexed_cols(),
        snapshot
            .table_config()
            .stats_columns()
            .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
    );

    let tracker = CDCTracker::new(df, updated_df.drop_columns(&[UPDATE_PREDICATE_COLNAME])?);

    let add_actions = write_execution_plan(
        Some(&snapshot),
        state.clone(),
        physical_plan.clone(),
        table_partition_cols.clone(),
        log_store.object_store().clone(),
        Some(snapshot.table_config().target_file_size() as usize),
        None,
        writer_properties.clone(),
        writer_stats_config.clone(),
        None,
    )
    .await?;

    let err = || DeltaTableError::Generic("Unable to locate expected metric node".into());
    let update_count = find_metric_node(UPDATE_COUNT_ID, &physical_plan).ok_or_else(err)?;
    let update_count_metrics = update_count.metrics().unwrap();

    metrics.num_updated_rows = get_metric(&update_count_metrics, UPDATE_ROW_COUNT);
    metrics.num_copied_rows = get_metric(&update_count_metrics, COPIED_ROW_COUNT);

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut actions: Vec<Action> = add_actions.clone();

    metrics.num_added_files = actions.len();
    metrics.num_removed_files = candidates.candidates.len();

    for action in candidates.candidates {
        actions.push(Action::Remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            deletion_vector: action.deletion_vector,
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
        }))
    }

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;

    let operation = DeltaOperation::Update {
        predicate: Some(fmt_expr_to_sql(&predicate)?),
    };

    commit_properties
        .app_metadata
        .insert("readVersion".to_owned(), snapshot.version().into());

    commit_properties.app_metadata.insert(
        "operationMetrics".to_owned(),
        serde_json::to_value(&metrics)?,
    );

    if let Ok(true) = should_write_cdc(&snapshot) {
        match tracker.collect() {
            Ok(df) => {
                let cdc_actions = write_execution_plan_cdc(
                    Some(&snapshot),
                    state,
                    df.create_physical_plan().await?,
                    table_partition_cols,
                    log_store.object_store(),
                    Some(snapshot.table_config().target_file_size() as usize),
                    None,
                    writer_properties,
                    writer_stats_config,
                    None,
                )
                .await?;
                actions.extend(cdc_actions);
            }
            Err(err) => {
                error!("Failed to collect CDC batches: {err:#?}");
            }
        };
    }

    let commit = CommitBuilder::from(commit_properties)
        .with_actions(actions)
        .build(Some(&snapshot), log_store, operation)
        .await?;

    Ok((commit.snapshot(), metrics))
}

impl std::future::IntoFuture for UpdateBuilder {
    type Output = DeltaResult<(DeltaTable, UpdateMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            PROTOCOL.check_append_only(&this.snapshot.snapshot)?;
            PROTOCOL.can_write_to(&this.snapshot.snapshot)?;

            let state = this.state.unwrap_or_else(|| {
                let session: SessionContext = DeltaSessionContext::default().into();

                // If a user provides their own their DF state then they must register the store themselves
                register_store(this.log_store.clone(), session.runtime_env());

                session.state()
            });

            let (snapshot, metrics) = execute(
                this.predicate,
                this.updates,
                this.log_store.clone(),
                this.snapshot,
                state,
                this.writer_properties,
                this.commit_properties,
                this.safe_cast,
            )
            .await?;

            Ok((
                DeltaTable::new_with_state(this.log_store, snapshot),
                metrics,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::kernel::DataType as DeltaDataType;
    use crate::kernel::{Action, PrimitiveType, Protocol, StructField, StructType};
    use crate::operations::load_cdf::*;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::datafusion::write_batch;
    use crate::writer::test_utils::{
        get_arrow_schema, get_delta_schema, get_record_batch, setup_table_with_configuration,
    };
    use crate::{DeltaTable, TableProperty};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::DataType;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::*;
    use serde_json::json;
    use std::sync::Arc;

    async fn setup_table(partitions: Option<Vec<&str>>) -> DeltaTable {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(partitions.unwrap_or_default())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        table
    }

    async fn prepare_values_table() -> DeltaTable {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![
                Some(0),
                None,
                Some(2),
                None,
                Some(4),
            ]))],
        )
        .unwrap();

        DeltaOps::new_in_memory().write(vec![batch]).await.unwrap()
    }

    #[tokio::test]
    async fn test_update_when_delta_table_is_append_only() {
        let table = setup_table_with_configuration(TableProperty::AppendOnly, Some("true")).await;
        let batch = get_record_batch(None, false);
        // Append
        let table = write_batch(table, batch).await;
        let _err = DeltaOps(table)
            .update()
            .with_update("modified", lit("2023-05-14"))
            .await
            .expect_err("Remove action is included when Delta table is append-only. Should error");
    }

    #[tokio::test]
    async fn test_update_no_predicate() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        let table = write_batch(table, batch).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_update("modified", lit("2023-05-14"))
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 1);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 4);
        assert_eq!(metrics.num_copied_rows, 0);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2023-05-14 |",
            "| A  | 10    | 2023-05-14 |",
            "| A  | 100   | 2023-05-14 |",
            "| B  | 10    | 2023-05-14 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_update_non_partition() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        // Update a partitioned table where the predicate contains only partition column
        // The expectation is that a physical scan of data is not required

        let table = write_batch(table, batch).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .with_update("modified", lit("2023-05-14"))
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 1);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 2);
        assert_eq!(metrics.num_copied_rows, 2);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("modified = '2021-02-03'"));

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-02 |",
            "| A  | 10    | 2023-05-14 |",
            "| A  | 100   | 2023-05-14 |",
            "| B  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_update_partitions() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        let table = write_batch(table, batch.clone()).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 2);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .with_update("modified", lit("2023-05-14"))
            .with_update("id", lit("C"))
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 2);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 2);
        assert_eq!(metrics.num_copied_rows, 0);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-02 |",
            "| C  | 10    | 2023-05-14 |",
            "| C  | 100   | 2023-05-14 |",
            "| B  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Update a partitioned table where the predicate contains a partition column and non-partition column
        let table = setup_table(Some(vec!["modified"])).await;
        let table = write_batch(table, batch).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 2);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate(
                col("modified")
                    .eq(lit("2021-02-03"))
                    .and(col("value").eq(lit(100))),
            )
            .with_update("modified", lit("2023-05-14"))
            .with_update("id", lit("C"))
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 3);
        assert_eq!(metrics.num_added_files, 2);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 1);
        assert_eq!(metrics.num_copied_rows, 1);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-02 |",
            "| A  | 10    | 2021-02-03 |",
            "| B  | 10    | 2021-02-02 |",
            "| C  | 100   | 2023-05-14 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_update_case_sensitive() {
        let schema = StructType::new(vec![
            StructField::new(
                "Id".to_string(),
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "ValUe".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "mOdified".to_string(),
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("Id", DataType::Utf8, true),
            Field::new("ValUe", DataType::Int32, true),
            Field::new("mOdified", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        let table = write_batch(table, batch).await;

        let (table, _metrics) = DeltaOps(table)
            .update()
            .with_predicate("mOdified = '2021-02-03'")
            .with_update("mOdified", "'2023-05-14'")
            .with_update("Id", "'C'")
            .await
            .unwrap();

        let expected = vec![
            "+----+-------+------------+",
            "| Id | ValUe | mOdified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-02 |",
            "| B  | 10    | 2021-02-02 |",
            "| C  | 10    | 2023-05-14 |",
            "| C  | 100   | 2023-05-14 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_update_null() {
        let table = prepare_values_table().await;
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 1);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_update("value", col("value") + lit(1))
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 5);
        assert_eq!(metrics.num_copied_rows, 0);

        let expected = [
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 1     |",
            "| 3     |",
            "| 5     |",
            "+-------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate order operators do not include nulls
        let table = prepare_values_table().await;
        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("value").gt(lit(2)).or(col("value").lt(lit(2))))
            .with_update("value", lit(10))
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 2);
        assert_eq!(metrics.num_copied_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let extra_info = last_commit.info.clone();
        assert_eq!(
            extra_info["operationMetrics"],
            serde_json::to_value(&metrics).unwrap()
        );

        let expected = [
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 2     |",
            "| 10    |",
            "| 10    |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        let table = prepare_values_table().await;
        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate("value is null")
            .with_update("value", "10")
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 2);
        assert_eq!(metrics.num_copied_rows, 3);

        let expected = [
            "+-------+",
            "| value |",
            "+-------+",
            "| 10    |",
            "| 10    |",
            "| 0     |",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_no_updates() {
        // No Update operations are provided
        let table = prepare_values_table().await;
        let (table, metrics) = DeltaOps(table).update().await.unwrap();

        assert_eq!(table.version(), 0);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_copied_rows, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.scan_time_ms, 0);
        assert_eq!(metrics.execution_time_ms, 0);

        // The predicate does not match any records
        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("value").eq(lit(3)))
            .with_update("value", lit(10))
            .await
            .unwrap();

        assert_eq!(table.version(), 0);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_copied_rows, 0);
        assert_eq!(metrics.num_removed_files, 0);
    }

    #[tokio::test]
    async fn test_expected_failures() {
        // The predicate must be deterministic and expression must be valid

        let table = setup_table(None).await;

        let res = DeltaOps(table)
            .update()
            .with_predicate(col("value").eq(cast(
                random() * lit(20.0),
                arrow::datatypes::DataType::Int32,
            )))
            .with_update("value", col("value") + lit(20))
            .await;
        assert!(res.is_err());

        // Expression result types must match the table's schema
        let table = prepare_values_table().await;
        let res = DeltaOps(table)
            .update()
            .with_update("value", lit("a string"))
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_no_cdc_on_older_tables() {
        let table = prepare_values_table().await;
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 1);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )
        .unwrap();
        let table = DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), 1);

        let (table, _metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("value").eq(lit(2)))
            .with_update("value", lit(12))
            .await
            .unwrap();
        assert_eq!(table.version(), 2);

        // NOTE: This currently doesn't really assert anything because cdc_files() is not reading
        // actions correct
        if let Some(state) = table.state.clone() {
            let cdc_files = state.cdc_files();
            assert!(cdc_files.is_ok());
            if let Ok(cdc_files) = cdc_files {
                let cdc_files: Vec<_> = cdc_files.collect();
                assert_eq!(cdc_files.len(), 0);
            }
        } else {
            panic!("I shouldn't exist!");
        }

        // Too close for missiles, switching to guns. Just checking that the data wasn't actually
        // written instead!
        if let Ok(files) = crate::storage::utils::flatten_list_stream(
            &table.object_store(),
            Some(&object_store::path::Path::from("_change_data")),
        )
        .await
        {
            assert_eq!(
                0,
                files.len(),
                "This test should not find any written CDC files! {files:#?}"
            );
        }
    }

    #[tokio::test]
    async fn test_update_cdc_enabled() {
        // Currently you cannot pass EnableChangeDataFeed through `with_configuration_property`
        // so the only way to create a truly CDC enabled table is by shoving the Protocol
        // directly into the actions list
        let actions = vec![Action::Protocol(Protocol::new(1, 4))];
        let table: DeltaTable = DeltaOps::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_actions(actions)
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )
        .unwrap();
        let table = DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), 1);

        let (table, _metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("value").eq(lit(2)))
            .with_update("value", lit(12))
            .await
            .unwrap();
        assert_eq!(table.version(), 2);

        let ctx = SessionContext::new();
        let table = DeltaOps(table)
            .load_cdf()
            .with_session_ctx(ctx.clone())
            .with_starting_version(0)
            .build()
            .await
            .expect("Failed to load CDF");

        let mut batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await
        .expect("Failed to collect batches");

        // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(3)).collect();

        assert_batches_sorted_eq! {[
        "+-------+------------------+-----------------+",
        "| value | _change_type     | _commit_version |",
        "+-------+------------------+-----------------+",
        "| 1     | insert           | 1               |",
        "| 2     | insert           | 1               |",
        "| 2     | update_preimage  | 2               |",
        "| 12    | update_postimage | 2               |",
        "| 3     | insert           | 1               |",
        "+-------+------------------+-----------------+",
            ], &batches }
    }

    #[tokio::test]
    async fn test_update_cdc_enabled_partitions() {
        // Currently you cannot pass EnableChangeDataFeed through `with_configuration_property`
        // so the only way to create a truly CDC enabled table is by shoving the Protocol
        // directly into the actions list
        let actions = vec![Action::Protocol(Protocol::new(1, 4))];
        let table: DeltaTable = DeltaOps::new_in_memory()
            .create()
            .with_column(
                "year",
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
                None,
            )
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_partition_columns(vec!["year"])
            .with_actions(actions)
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("2020"),
                    Some("2020"),
                    Some("2024"),
                ])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            ],
        )
        .unwrap();
        let table = DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), 1);

        let (table, _metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("value").eq(lit(2)))
            .with_update("year", "2024")
            .await
            .unwrap();
        assert_eq!(table.version(), 2);

        let ctx = SessionContext::new();
        let table = DeltaOps(table)
            .load_cdf()
            .with_session_ctx(ctx.clone())
            .with_starting_version(0)
            .build()
            .await
            .expect("Failed to load CDF");

        let mut batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await
        .expect("Failed to collect batches");

        let _ = arrow::util::pretty::print_batches(&batches);

        // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(3)).collect();

        assert_batches_sorted_eq! {[
        "+-------+------------------+-----------------+------+",
        "| value | _change_type     | _commit_version | year |",
        "+-------+------------------+-----------------+------+",
        "| 1     | insert           | 1               | 2020 |",
        "| 2     | insert           | 1               | 2020 |",
        "| 2     | update_preimage  | 2               | 2020 |",
        "| 2     | update_postimage | 2               | 2024 |",
        "| 3     | insert           | 1               | 2024 |",
        "+-------+------------------+-----------------+------+",
            ], &batches }
    }
}
