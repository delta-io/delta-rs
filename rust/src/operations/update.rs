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
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use arrow::datatypes::Schema as ArrowSchema;
use arrow_array::RecordBatch;
use arrow_cast::CastOptions;
use arrow_schema::{Field, SchemaRef};
use datafusion::{
    execution::context::SessionState,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        projection::ProjectionExec,
        ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
    },
    prelude::SessionContext,
};
use datafusion_common::Result as DataFusionResult;
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::{case, col, lit, when, Expr};
use datafusion_physical_expr::{
    create_physical_expr,
    expressions::{self},
    PhysicalExpr,
};
use futures::{future::BoxFuture, Stream, StreamExt};
use parquet::file::properties::WriterProperties;
use serde_json::{Map, Value};

use crate::{
    action::{Action, DeltaOperation, Remove},
    delta_datafusion::{find_files, parquet_scan_from_actions, register_store},
    storage::{DeltaObjectStore, ObjectStoreRef},
    table_state::DeltaTableState,
    DeltaResult, DeltaTable, DeltaTableError,
};

use super::{transaction::commit, write::write_execution_plan};

/// Used to represent user input of either a Datafusion expression or string expression
pub enum Expression {
    /// Datafusion Expression
    DataFusion(Expr),
    /// String Expression
    String(String),
}

impl From<Expr> for Expression {
    fn from(val: Expr) -> Self {
        Expression::DataFusion(val)
    }
}

impl From<&str> for Expression {
    fn from(val: &str) -> Self {
        Expression::String(val.to_string())
    }
}
impl From<String> for Expression {
    fn from(val: String) -> Self {
        Expression::String(val)
    }
}

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
    object_store: Arc<DeltaObjectStore>,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<Map<String, serde_json::Value>>,
    /// CastOptions determines how data types that do not match the underlying table are handled
    /// By default an error is returned
    cast_options: CastOptions,
}

#[derive(Default)]
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
    pub execution_time_ms: u128,
    /// Time taken to scan the files for matches.
    pub scan_time_ms: u128,
}

impl UpdateBuilder {
    /// Create a new ['UpdateBuilder']
    pub fn new(object_store: ObjectStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            predicate: None,
            updates: HashMap::new(),
            snapshot,
            object_store,
            state: None,
            writer_properties: None,
            app_metadata: None,
            cast_options: CastOptions { safe: false },
        }
    }

    /// Which records to update
    pub fn with_predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// Perform an additonal update expression during the operaton
    pub fn with_update<S: Into<Column>, E: Into<Expression>>(
        mut self,
        column: S,
        expression: E,
    ) -> Self {
        self.updates.insert(column.into(), expression.into());
        self
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.app_metadata = Some(Map::from_iter(metadata));
        self
    }

    /// Writer properties passed to parquet writer for when fiiles are rewritten
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Specify the cast options to use when casting columns that do not match the table's schema.
    pub fn with_cast_options(mut self, cast_options: CastOptions) -> Self {
        self.cast_options = cast_options;
        self
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    predicate: Option<Expression>,
    updates: HashMap<Column, Expression>,
    object_store: ObjectStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    app_metadata: Option<Map<String, Value>>,
    cast_options: CastOptions,
) -> DeltaResult<((Vec<Action>, i64), UpdateMetrics)> {
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
    let mut version = snapshot.version();

    if updates.is_empty() {
        return Ok(((Vec::new(), version), metrics));
    }

    let predicate = match predicate {
        Some(predicate) => match predicate {
            Expression::DataFusion(expr) => Some(expr),
            Expression::String(s) => Some(snapshot.parse_predicate_expression(s)?),
        },
        None => None,
    };

    let mut _updates = HashMap::new();
    for (key, expr) in updates {
        let expr = match expr {
            Expression::DataFusion(e) => e,
            Expression::String(s) => snapshot.parse_predicate_expression(s)?,
        };
        _updates.insert(key, expr);
    }
    let updates = _updates;

    let current_metadata = snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?;
    let table_partition_cols = current_metadata.partition_columns.clone();
    let schema = snapshot.arrow_schema()?;

    let scan_start = Instant::now();
    let candidates = find_files(
        snapshot,
        object_store.clone(),
        schema.clone(),
        &state,
        predicate.clone(),
    )
    .await?;
    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_micros();

    if candidates.candidates.is_empty() {
        return Ok(((Vec::new(), version), metrics));
    }

    let predicate = predicate.unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));

    let execution_props = state.execution_props();
    // For each rewrite evaluate the predicate and then modify each expression
    // to either compute the new value or obtain the old one then write these batches
    let parquet_scan = parquet_scan_from_actions(
        snapshot,
        object_store.clone(),
        &candidates.candidates,
        &schema,
        None,
        &state,
        None,
        None,
    )
    .await?;

    // Create a projection for a new column with the predicate evaluated
    let input_schema = snapshot.input_schema()?;
    let mut fields = Vec::new();
    for field in input_schema.fields.iter() {
        fields.push(field.to_owned());
    }
    fields.push(Arc::new(Field::new(
        "__delta_rs_update_predicate",
        arrow_schema::DataType::Boolean,
        true,
    )));

    // Recreate the schemas with the new column included
    let input_schema = Arc::new(ArrowSchema::new(fields));
    let input_dfschema: DFSchema = input_schema.clone().as_ref().clone().try_into()?;

    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let scan_schema = parquet_scan.schema();
    for (i, field) in scan_schema.fields().into_iter().enumerate() {
        expressions.push((
            Arc::new(expressions::Column::new(field.name(), i)),
            field.name().to_owned(),
        ));
    }

    // Take advantage of how null counts are tracked in arrow arrays use the
    // null count to track how many records do NOT statisfy the predicate.  The
    // count is then exposed through the metrics through the `UpdateCountExec`
    // execution plan
    let predicate_null =
        when(predicate.clone(), lit(true)).otherwise(lit(ScalarValue::Boolean(None)))?;
    let predicate_expr = create_physical_expr(
        &predicate_null,
        &input_dfschema,
        &input_schema,
        execution_props,
    )?;
    expressions.push((predicate_expr, "__delta_rs_update_predicate".to_string()));

    let projection_predicate: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(expressions, parquet_scan)?);

    let count_plan = Arc::new(UpdateCountExec::new(projection_predicate.clone()));

    // Perform another projection but instead calculate updated values based on
    // the predicate value.  If the predicate is true then evalute the user
    // provided expression otherwise return the original column value
    //
    // For each update column a new column with a name of __delta_rs_ + `original name` is created
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let scan_schema = count_plan.schema();
    for (i, field) in scan_schema.fields().into_iter().enumerate() {
        expressions.push((
            Arc::new(expressions::Column::new(field.name(), i)),
            field.name().to_owned(),
        ));
    }

    // Maintain a map from the original column name to its temporary column index
    let mut map = HashMap::<String, usize>::new();
    let mut control_columns = HashSet::<String>::new();
    control_columns.insert("__delta_rs_update_predicate".to_owned());

    for (column, expr) in updates {
        let expr = case(col("__delta_rs_update_predicate"))
            .when(lit(true), expr.to_owned())
            .otherwise(col(column.to_owned()))?;
        let predicate_expr =
            create_physical_expr(&expr, &input_dfschema, &input_schema, execution_props)?;
        map.insert(column.name.clone(), expressions.len());
        let c = "__delta_rs_".to_string() + &column.name;
        expressions.push((predicate_expr, c.clone()));
        control_columns.insert(c);
    }

    let projection_update: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(expressions, count_plan.clone())?);

    // Project again to remove __delta_rs columns and rename update columns to their original name
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let scan_schema = projection_update.schema();
    for (i, field) in scan_schema.fields().into_iter().enumerate() {
        if !control_columns.contains(field.name()) {
            match map.get(field.name()) {
                Some(value) => {
                    expressions.push((
                        Arc::new(expressions::Column::new(field.name(), *value)),
                        field.name().to_owned(),
                    ));
                }
                None => {
                    expressions.push((
                        Arc::new(expressions::Column::new(field.name(), i)),
                        field.name().to_owned(),
                    ));
                }
            }
        }
    }

    let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
        expressions,
        projection_update.clone(),
    )?);

    let add_actions = write_execution_plan(
        snapshot,
        state.clone(),
        projection.clone(),
        table_partition_cols.clone(),
        object_store.clone(),
        Some(snapshot.table_config().target_file_size() as usize),
        None,
        writer_properties,
        &cast_options,
    )
    .await?;

    let count_metrics = count_plan.metrics().unwrap();

    metrics.num_updated_rows = count_metrics
        .sum_by_name("num_updated_rows")
        .map(|m| m.as_usize())
        .unwrap_or(0);

    metrics.num_copied_rows = count_metrics
        .sum_by_name("num_copied_rows")
        .map(|m| m.as_usize())
        .unwrap_or(0);

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut actions: Vec<Action> = add_actions.into_iter().map(Action::add).collect();

    metrics.num_added_files = actions.len();
    metrics.num_removed_files = candidates.candidates.len();

    for action in candidates.candidates {
        actions.push(Action::remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            tags: None,
        }))
    }

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_micros();

    let operation = DeltaOperation::Update {
        predicate: Some(predicate.canonical_name()),
    };
    version = commit(
        object_store.as_ref(),
        &actions,
        operation,
        snapshot,
        app_metadata,
    )
    .await?;

    Ok(((actions, version), metrics))
}

impl std::future::IntoFuture for UpdateBuilder {
    type Output = DeltaResult<(DeltaTable, UpdateMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            let state = this.state.unwrap_or_else(|| {
                let session = SessionContext::new();

                // If a user provides their own their DF state then they must register the store themselves
                register_store(this.object_store.clone(), session.runtime_env());

                session.state()
            });

            let ((actions, version), metrics) = execute(
                this.predicate,
                this.updates,
                this.object_store.clone(),
                &this.snapshot,
                state,
                this.writer_properties,
                this.app_metadata,
                this.cast_options,
            )
            .await?;

            this.snapshot
                .merge(DeltaTableState::from_actions(actions, version)?, true, true);
            let table = DeltaTable::new_with_state(this.object_store, this.snapshot);

            Ok((table, metrics))
        })
    }
}

#[derive(Debug)]
struct UpdateCountExec {
    parent: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl UpdateCountExec {
    pub fn new(parent: Arc<dyn ExecutionPlan>) -> Self {
        UpdateCountExec {
            parent,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for UpdateCountExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.parent.schema()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.parent.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        self.parent.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.parent.clone()]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let res = self.parent.execute(partition, context)?;
        Ok(Box::pin(UpdateCountStream {
            schema: self.schema(),
            input: res,
            metrics: self.metrics.clone(),
        }))
    }

    fn statistics(&self) -> datafusion_common::Statistics {
        self.parent.statistics()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        ExecutionPlan::with_new_children(self.parent.clone(), children)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct UpdateCountStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metrics: ExecutionPlanMetricsSet,
}

impl Stream for UpdateCountStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let array = batch.column_by_name("__delta_rs_update_predicate").unwrap();
                let copied_rows = array.null_count();
                let num_updated = array.len() - copied_rows;
                let c1 = MetricBuilder::new(&self.metrics).global_counter("num_updated_rows");
                c1.add(num_updated);

                let c2 = MetricBuilder::new(&self.metrics).global_counter("num_copied_rows");
                c2.add(copied_rows);
                Some(Ok(batch))
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for UpdateCountStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {

    use crate::operations::DeltaOps;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::{get_arrow_schema, get_delta_schema};
    use crate::DeltaTable;
    use crate::{action::*, DeltaResult};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::Int32Array;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::from_slice::FromSlice;
    use datafusion::prelude::*;
    use std::sync::Arc;

    async fn setup_table(partitions: Option<Vec<&str>>) -> DeltaTable {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.get_fields().clone())
            .with_partition_columns(partitions.unwrap_or_default())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        table
    }

    async fn write_batch(table: DeltaTable, batch: RecordBatch) -> DeltaResult<DeltaTable> {
        DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
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
    async fn test_update_no_predicate() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice([1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        let table = write_batch(table, batch).await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_update("modified", lit("2023-05-14"))
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 1);
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
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice([1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        let table = write_batch(table, batch).await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .with_update("modified", lit("2023-05-14"))
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 1);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 2);
        assert_eq!(metrics.num_copied_rows, 2);

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
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice([1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        let table = write_batch(table, batch).await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 2);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .with_update("modified", lit("2023-05-14"))
            .with_update("id", lit("C"))
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 2);
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
    }

    #[tokio::test]
    async fn test_update_null() {
        let table = prepare_values_table().await;
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_file_uris().count(), 1);

        let (table, metrics) = DeltaOps(table)
            .update()
            .with_update("value", col("value") + lit(1))
            .await
            .unwrap();

        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);
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
        assert_eq!(table.get_file_uris().count(), 1);
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_updated_rows, 2);
        assert_eq!(metrics.num_copied_rows, 3);

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
        assert_eq!(table.get_file_uris().count(), 1);
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
}
