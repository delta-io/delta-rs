//! Delete records from a Delta Table that statisfy a predicate
//!
//! When a predicate is not provided then all records are deleted from the Delta
//! Table. Otherwise a scan of the Delta table is performed to mark any files
//! that contain records that satisfy the predicate. Once files are determined
//! they are rewritten without the records.
//!
//!
//! Predicates MUST be deterministic otherwise undefined behaviour may occur during the
//! scanning and rewriting phase.
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = DeleteBuilder::new(table.object_store(), table.state)
//!     .with_predicate(col("col1").eq(lit(1)))
//!     .await?;
//! ````

use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::provider_as_source;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use datafusion_common::ScalarValue;
use datafusion_expr::{lit, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode};
use datafusion_physical_plan::metrics::MetricBuilder;
use datafusion_physical_plan::ExecutionPlan;

use futures::future::BoxFuture;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parquet::file::properties::WriterProperties;
use serde::Serialize;

use super::cdc::should_write_cdc;
use super::datafusion_utils::Expression;
use super::transaction::{CommitBuilder, CommitProperties, PROTOCOL};
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::logical::MetricObserver;
use crate::delta_datafusion::physical::{find_metric_node, get_metric, MetricObserverExec};
use crate::delta_datafusion::planner::DeltaPlanner;
use crate::delta_datafusion::{
    find_files, register_store, DataFusionMixins, DeltaScanConfigBuilder, DeltaSessionContext,
    DeltaTableProvider,
};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, Remove};
use crate::logstore::LogStoreRef;
use crate::operations::write::{write_execution_plan, write_execution_plan_cdc, WriterStatsConfig};
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaTable, DeltaTableError};

const SOURCE_COUNT_ID: &str = "delete_source_count";
const SOURCE_COUNT_METRIC: &str = "num_source_rows";

/// Delete Records from the Delta Table.
/// See this module's documentation for more information
pub struct DeleteBuilder {
    /// Which records to delete
    predicate: Option<Expression>,
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Commit properties and configuration
    commit_properties: CommitProperties,
}

#[derive(Default, Debug, Serialize)]
/// Metrics for the Delete Operation
pub struct DeleteMetrics {
    /// Number of files added
    pub num_added_files: usize,
    /// Number of files removed
    pub num_removed_files: usize,
    /// Number of rows removed
    pub num_deleted_rows: usize,
    /// Number of rows copied in the process of deleting files
    pub num_copied_rows: usize,
    /// Time taken to execute the entire operation
    pub execution_time_ms: u64,
    /// Time taken to scan the file for matches
    pub scan_time_ms: u64,
    /// Time taken to rewrite the matched files
    pub rewrite_time_ms: u64,
}

impl super::Operation<()> for DeleteBuilder {}

impl DeleteBuilder {
    /// Create a new [`DeleteBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            predicate: None,
            snapshot,
            log_store,
            state: None,
            commit_properties: CommitProperties::default(),
            writer_properties: None,
        }
    }

    /// A predicate that determines if a record is deleted
    pub fn with_predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }

    /// Additonal information to write to the commit
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Writer properties passed to parquet writer for when files are rewritten
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }
}

#[derive(Clone)]
struct DeleteMetricExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for DeleteMetricExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(metric_observer) = node.as_any().downcast_ref::<MetricObserver>() {
            if metric_observer.id.eq(SOURCE_COUNT_ID) {
                return Ok(Some(MetricObserverExec::try_new(
                    SOURCE_COUNT_ID.into(),
                    physical_inputs,
                    |batch, metrics| {
                        MetricBuilder::new(metrics)
                            .global_counter(SOURCE_COUNT_METRIC)
                            .add(batch.num_rows());
                    },
                )?));
            }
        }
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn excute_non_empty_expr(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    expression: &Expr,
    rewrite: &[Add],
    metrics: &mut DeleteMetrics,
    writer_properties: Option<WriterProperties>,
    partition_scan: bool,
) -> DeltaResult<Vec<Action>> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.
    let mut actions: Vec<Action> = Vec::new();
    let table_partition_cols = snapshot.metadata().partition_columns.clone();

    let delete_planner = DeltaPlanner::<DeleteMetricExtensionPlanner> {
        extension_planner: DeleteMetricExtensionPlanner {},
    };

    let state = SessionStateBuilder::new_from_existing(state.clone())
        .with_query_planner(Arc::new(delete_planner))
        .build();

    let scan_config = DeltaScanConfigBuilder::default()
        .with_file_column(false)
        .with_schema(snapshot.input_schema()?)
        .build(snapshot)?;

    let target_provider = Arc::new(
        DeltaTableProvider::try_new(snapshot.clone(), log_store.clone(), scan_config.clone())?
            .with_files(rewrite.to_vec()),
    );
    let target_provider = provider_as_source(target_provider);
    let source = LogicalPlanBuilder::scan("target", target_provider.clone(), None)?.build()?;

    let source = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: "delete_source_count".into(),
            input: source,
            enable_pushdown: false,
        }),
    });

    let df = DataFrame::new(state.clone(), source);

    let writer_stats_config = WriterStatsConfig::new(
        snapshot.table_config().num_indexed_cols(),
        snapshot
            .table_config()
            .stats_columns()
            .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
    );

    if !partition_scan {
        // Apply the negation of the filter and rewrite files
        let negated_expression = Expr::Not(Box::new(Expr::IsTrue(Box::new(expression.clone()))));

        let filter = df
            .clone()
            .filter(negated_expression)?
            .create_physical_plan()
            .await?;

        let add_actions: Vec<Action> = write_execution_plan(
            Some(snapshot),
            state.clone(),
            filter.clone(),
            table_partition_cols.clone(),
            log_store.object_store(),
            Some(snapshot.table_config().target_file_size() as usize),
            None,
            writer_properties.clone(),
            writer_stats_config.clone(),
            None,
        )
        .await?;

        actions.extend(add_actions);

        let source_count = find_metric_node(SOURCE_COUNT_ID, &filter).ok_or_else(|| {
            DeltaTableError::Generic("Unable to locate expected metric node".into())
        })?;
        let source_count_metrics = source_count.metrics().unwrap();
        let read_records = get_metric(&source_count_metrics, SOURCE_COUNT_METRIC);
        let filter_records = filter.metrics().and_then(|m| m.output_rows()).unwrap_or(0);

        metrics.num_copied_rows = filter_records;
        metrics.num_deleted_rows = read_records - filter_records;
    }

    // CDC logic, simply filters data with predicate and adds the _change_type="delete" as literal column
    if let Ok(true) = should_write_cdc(snapshot) {
        // Create CDC scan
        let change_type_lit = lit(ScalarValue::Utf8(Some("delete".to_string())));
        let cdc_filter = df
            .filter(expression.clone())?
            .with_column("_change_type", change_type_lit)?
            .create_physical_plan()
            .await?;

        let cdc_actions = write_execution_plan_cdc(
            Some(snapshot),
            state.clone(),
            cdc_filter,
            table_partition_cols.clone(),
            log_store.object_store(),
            Some(snapshot.table_config().target_file_size() as usize),
            None,
            writer_properties,
            writer_stats_config,
            None,
        )
        .await?;
        actions.extend(cdc_actions)
    }

    Ok(actions)
}

async fn execute(
    predicate: Option<Expr>,
    log_store: LogStoreRef,
    snapshot: DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    mut commit_properties: CommitProperties,
) -> DeltaResult<(DeltaTableState, DeleteMetrics)> {
    if !&snapshot.load_config().require_files {
        return Err(DeltaTableError::NotInitializedWithFiles("DELETE".into()));
    }

    let exec_start = Instant::now();
    let mut metrics = DeleteMetrics::default();

    let scan_start = Instant::now();
    let candidates = find_files(&snapshot, log_store.clone(), &state, predicate.clone()).await?;
    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis() as u64;

    let predicate = predicate.unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));

    let mut actions = {
        let write_start = Instant::now();
        let add = excute_non_empty_expr(
            &snapshot,
            log_store.clone(),
            &state,
            &predicate,
            &candidates.candidates,
            &mut metrics,
            writer_properties,
            candidates.partition_scan,
        )
        .await?;
        metrics.rewrite_time_ms = Instant::now().duration_since(write_start).as_millis() as u64;
        add
    };
    let remove = candidates.candidates;

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    metrics.num_removed_files = remove.len();
    metrics.num_added_files = actions.len();

    for action in remove {
        actions.push(Action::Remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            deletion_vector: action.deletion_vector,
            tags: None,
            base_row_id: action.base_row_id,
            default_row_commit_version: action.default_row_commit_version,
        }))
    }

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;

    commit_properties
        .app_metadata
        .insert("readVersion".to_owned(), snapshot.version().into());
    commit_properties.app_metadata.insert(
        "operationMetrics".to_owned(),
        serde_json::to_value(&metrics)?,
    );

    // Do not make a commit when there are zero updates to the state
    let operation = DeltaOperation::Delete {
        predicate: Some(fmt_expr_to_sql(&predicate)?),
    };
    if actions.is_empty() {
        return Ok((snapshot.clone(), metrics));
    }

    let commit = CommitBuilder::from(commit_properties)
        .with_actions(actions)
        .build(Some(&snapshot), log_store, operation)
        .await?;
    Ok((commit.snapshot(), metrics))
}

impl std::future::IntoFuture for DeleteBuilder {
    type Output = DeltaResult<(DeltaTable, DeleteMetrics)>;
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

            let predicate = match this.predicate {
                Some(predicate) => match predicate {
                    Expression::DataFusion(expr) => Some(expr),
                    Expression::String(s) => {
                        Some(this.snapshot.parse_predicate_expression(s, &state)?)
                    }
                },
                None => None,
            };

            let (new_snapshot, metrics) = execute(
                predicate,
                this.log_store.clone(),
                this.snapshot,
                state,
                this.writer_properties,
                this.commit_properties,
            )
            .await?;

            Ok((
                DeltaTable::new_with_state(this.log_store, new_snapshot),
                metrics,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::delta_datafusion::cdf::DeltaCdfScan;
    use crate::kernel::DataType as DeltaDataType;
    use crate::operations::collect_sendable_stream;
    use crate::operations::DeltaOps;
    use crate::protocol::*;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::datafusion::write_batch;
    use crate::writer::test_utils::{
        get_arrow_schema, get_delta_schema, get_record_batch, setup_table_with_configuration,
    };
    use crate::DeltaTable;
    use crate::TableProperty;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::ArrayRef;
    use arrow_array::StringArray;
    use arrow_array::StructArray;
    use arrow_buffer::NullBuffer;
    use arrow_schema::DataType;
    use arrow_schema::Fields;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::*;
    use delta_kernel::schema::PrimitiveType;
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

    #[tokio::test]
    async fn test_delete_when_delta_table_is_append_only() {
        let table = setup_table_with_configuration(TableProperty::AppendOnly, Some("true")).await;
        let batch = get_record_batch(None, false);
        // append some data
        let table = write_batch(table, batch).await;
        // delete
        let _err = DeltaOps(table)
            .delete()
            .await
            .expect_err("Remove action is included when Delta table is append-only. Should error");
    }

    #[tokio::test]
    async fn test_delete_default() {
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
        // write some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        let (table, metrics) = DeltaOps(table).delete().await.unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 0);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let _extra_info = last_commit.info.clone();
        // assert_eq!(
        //     extra_info["operationMetrics"],
        //     serde_json::to_value(&metrics).unwrap()
        // );

        // Deletes with no changes to state must not commit
        let (table, metrics) = DeltaOps(table).delete().await.unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);
    }

    #[tokio::test]
    async fn test_delete_on_nonpartition_column() {
        // Delete based on a nonpartition column
        // Only rewrite files that match the predicate
        // Test data designed to force a scan of the underlying data

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

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").eq(lit(1)))
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_files_count(), 2);

        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert!(metrics.scan_time_ms > 0);
        assert_eq!(metrics.num_deleted_rows, 1);
        assert_eq!(metrics.num_copied_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("value = 1"));

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| B  | 10    | 2021-02-02 |",
            "| B  | 20    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_null() {
        // Demonstrate deletion of null

        async fn prepare_table() -> DeltaTable {
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

        // Validate behaviour of greater than
        let table = prepare_table().await;
        let (table, _) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").gt(lit(2)))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 0     |",
            "| 2     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate behaviour of less than
        let table = prepare_table().await;
        let (table, _) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").lt(lit(2)))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate behaviour of less plus not null
        let table = prepare_table().await;
        let (table, _) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").lt(lit(2)).or(col("value").is_null()))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_on_partition_column() {
        // Perform a delete where the predicate only contains partition columns

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 1);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);
        assert!(metrics.scan_time_ms > 0);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_on_mixed_columns() {
        // Test predicates that contain non-partition and partition column
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-04",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 3);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(
                col("modified")
                    .eq(lit("2021-02-04"))
                    .and(col("value").eq(lit(100))),
            )
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 2);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, 1);
        assert_eq!(metrics.num_copied_rows, 0);
        assert!(metrics.scan_time_ms > 0);

        let expected = [
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| B  | 20    | 2021-02-03 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_nested() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        // Test Delete with a predicate that references struct fields
        // See #2019
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new(
                "props",
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Utf8, true)])),
                true,
            ),
        ]));

        let struct_array = StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Utf8, true)]),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                Some("2021-02-01"),
                Some("2021-02-02"),
                None,
                None,
            ])) as ArrayRef],
            Some(NullBuffer::from_iter(vec![true, true, true, false])),
        );

        let data = vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ];
        let batches = vec![RecordBatch::try_new(schema.clone(), data).unwrap()];

        let table = DeltaOps::new_in_memory().write(batches).await.unwrap();

        let (table, _metrics) = DeltaOps(table)
            .delete()
            .with_predicate("props['a'] = '2021-02-02'")
            .await
            .unwrap();

        let expected = [
            "+----+-----------------+",
            "| id | props           |",
            "+----+-----------------+",
            "| A  | {a: 2021-02-01} |",
            "| C  | {a: }           |",
            "| D  |                 |",
            "+----+-----------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_failure_nondeterministic_query() {
        // Deletion requires a deterministic predicate

        let table = setup_table(None).await;

        let res = DeltaOps(table)
            .delete()
            .with_predicate(col("value").eq(cast(
                random() * lit(20.0),
                arrow::datatypes::DataType::Int32,
            )))
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_delete_cdc_enabled() {
        let table: DeltaTable = DeltaOps::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
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
            .delete()
            .with_predicate(col("value").eq(lit(2)))
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
        "+-------+--------------+-----------------+",
        "| value | _change_type | _commit_version |",
        "+-------+--------------+-----------------+",
        "| 1     | insert       | 1               |",
        "| 2     | delete       | 2               |",
        "| 2     | insert       | 1               |",
        "| 3     | insert       | 1               |",
        "+-------+--------------+-----------------+",
        ], &batches }
    }

    #[tokio::test]
    async fn test_delete_cdc_enabled_partitioned() {
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
            .delete()
            .with_predicate(col("value").eq(lit(2)))
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
        "+-------+--------------+-----------------+------+",
        "| value | _change_type | _commit_version | year |",
        "+-------+--------------+-----------------+------+",
        "| 1     | insert       | 1               | 2020 |",
        "| 2     | delete       | 2               | 2020 |",
        "| 2     | insert       | 1               | 2020 |",
        "| 3     | insert       | 1               | 2024 |",
        "+-------+--------------+-----------------+------+",
        ], &batches }
    }

    async fn collect_batches(
        num_partitions: usize,
        stream: DeltaCdfScan,
        ctx: SessionContext,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let mut batches = vec![];
        for p in 0..num_partitions {
            let data: Vec<RecordBatch> =
                collect_sendable_stream(stream.execute(p, ctx.task_ctx())?).await?;
            batches.extend_from_slice(&data);
        }
        Ok(batches)
    }
}
