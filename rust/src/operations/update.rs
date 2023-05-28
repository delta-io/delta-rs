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
use arrow_schema::Field;
use datafusion::{
    execution::context::SessionState,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{projection::ProjectionExec, ExecutionPlan},
    prelude::SessionContext,
};
use datafusion_common::{tree_node::TreeNode, Column, DFSchema, ScalarValue};
use datafusion_expr::{case, col, lit, Expr};
use datafusion_physical_expr::{create_physical_expr, expressions, PhysicalExpr};
use futures::future::BoxFuture;
use parquet::file::properties::WriterProperties;
use serde_json::{Map, Value};

use crate::{
    action::{Action, Add, DeltaOperation, Remove},
    delta_datafusion::{parquet_scan_from_actions, register_store},
    operations::delete::find_files,
    storage::{DeltaObjectStore, ObjectStoreRef},
    table_state::DeltaTableState,
    DeltaResult, DeltaTable, DeltaTableError,
};

use super::{
    delete::{scan_memory_table, ExprProperties},
    transaction::commit,
    write::write_execution_plan,
};

/// Updates records in the Delta Table.
/// See this module's documentaiton for more information
pub struct UpdateBuilder {
    /// Which records to update
    predicate: Option<Expr>,
    /// How to update columns in a record that match the predicate
    updates: HashMap<Column, Expr>,
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
}

#[derive(Default)]
/// Metrics collected during the Update operation
pub struct UpdateMetrics {
    /// Number of files added.
    pub num_added_files: usize,
    /// Number of files removed.
    pub num_removed_files: usize,
    // Number of rows updated.
    // pub num_updated_rows: usize,
    // Number of rows just copied over in the process of updating files.
    // pub num_copied_rows: usize,
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
        }
    }

    /// Which records to update
    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Overwrite update expressions with the supplied map
    pub fn with_updates(mut self, updates: HashMap<Column, Expr>) -> Self {
        self.updates = updates;
        self
    }

    /// Perform an additonal update expression during the operaton
    pub fn with_update<S: Into<Column>>(mut self, column: S, expression: Expr) -> Self {
        self.updates.insert(column.into(), expression);
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
}

async fn execute(
    predicate: Option<Expr>,
    updates: &HashMap<Column, Expr>,
    object_store: ObjectStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    app_metadata: Option<Map<String, Value>>,
) -> DeltaResult<((Vec<Action>, i64), UpdateMetrics)> {
    // Validate the predicate and update expressions
    //
    // If the predicate is not set then all files needs to be updated.
    // else if only contains partitions columns then perform in memory-scan
    // otherwise scan files for records that statisfy the predicate
    //
    // For files that were identified, scan for record that match the predicate
    // and perform update operations, and then commit add and remove actions to
    // the log

    let exec_start = Instant::now();
    let mut metrics = UpdateMetrics::default();
    let mut version = snapshot.version();

    if updates.is_empty() {
        return Ok(((Vec::new(), version), metrics));
    }

    let current_metadata = snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?;
    let table_partition_cols = current_metadata.partition_columns.clone();
    let schema = snapshot.arrow_schema()?;

    let files_to_rewrite = match &predicate {
        Some(predicate) => {
            let file_schema = Arc::new(ArrowSchema::new(
                schema
                    .fields()
                    .iter()
                    .filter(|f| !table_partition_cols.contains(f.name()))
                    .cloned()
                    .collect::<Vec<_>>(),
            ));

            let input_schema = snapshot.input_schema()?;
            let input_dfschema: DFSchema = input_schema.clone().as_ref().clone().try_into()?;
            let expr = create_physical_expr(
                predicate,
                &input_dfschema,
                &input_schema,
                state.execution_props(),
            )?;

            // Validate the Predicate and determine if it only contains partition columns
            let mut expr_properties = ExprProperties {
                partition_only: true,
                partition_columns: current_metadata.partition_columns.clone(),
                result: Ok(()),
            };

            TreeNode::visit(predicate, &mut expr_properties)?;
            expr_properties.result?;

            let scan_start = Instant::now();
            let candidates = if expr_properties.partition_only {
                scan_memory_table(snapshot, predicate).await?
            } else {
                let pruning_predicate = PruningPredicate::try_new(expr, schema.clone())?;
                let files_to_prune = pruning_predicate.prune(snapshot)?;
                let files: Vec<&Add> = snapshot
                    .files()
                    .iter()
                    .zip(files_to_prune.into_iter())
                    .filter_map(|(action, keep)| if keep { Some(action) } else { None })
                    .collect();

                // Create a new delta scan plan with only files that have a record
                let candidates = find_files(
                    snapshot,
                    object_store.clone(),
                    schema.clone(),
                    file_schema.clone(),
                    files,
                    &state,
                    predicate,
                )
                .await?;
                candidates.into_iter().map(|s| s.to_owned()).collect()
            };
            metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_micros();
            candidates
        }
        None => snapshot.files().to_owned(),
    };

    let predicate = predicate.unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));

    let execution_props = state.execution_props();
    // For each rewrite evaluate the predicate and then modify each expression
    // to either compute the new value or obtain the old one then write these batches
    let parquet_scan = parquet_scan_from_actions(
        snapshot,
        object_store.clone(),
        &files_to_rewrite,
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

    let predicate_expr =
        create_physical_expr(&predicate, &input_dfschema, &input_schema, execution_props)?;
    expressions.push((predicate_expr, "__delta_rs_update_predicate".to_string()));

    // Perform another projection but instead calculate updated values based on
    // the predicate value.  If the predicate is true then evalute the user
    // provided expression otherwise return the original column value
    //
    // For each update column a new column with a name of __delta_rs_ + `original name` is created
    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(expressions, parquet_scan)?);
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let scan_schema = projection.schema();
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

    let projection_predicate: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(expressions, projection.clone())?);

    // Project again to remove __delta_rs columns and rename update columns to their original name
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let scan_schema = projection_predicate.schema();
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
        projection_predicate.clone(),
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
    )
    .await?;

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut actions: Vec<Action> = add_actions.into_iter().map(Action::add).collect();

    metrics.num_added_files = actions.len();
    metrics.num_removed_files = files_to_rewrite.len();

    for action in files_to_rewrite {
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

    // Do not make a commit when there are zero updates to the state
    if !actions.is_empty() {
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
    }

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
                &this.updates,
                this.object_store.clone(),
                &this.snapshot,
                state,
                this.writer_properties,
                this.app_metadata,
            )
            .await?;

            this.snapshot
                .merge(DeltaTableState::from_actions(actions, version)?, true, true);
            let table = DeltaTable::new_with_state(this.object_store, this.snapshot);

            Ok((table, metrics))
        })
    }
}
#[cfg(test)]
mod tests {

    use crate::action::*;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::{get_arrow_schema, get_delta_schema};
    use crate::DeltaTable;
    use arrow::record_batch::RecordBatch;
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

    async fn get_data(table: DeltaTable) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table)).unwrap();
        ctx.sql("select * from test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
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
        // write some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
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
        //assert_eq!(metrics.num_updated_rows, 4);
        //assert_eq!(metrics.num_copied_rows, 0);

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
        let actual = get_data(table).await;
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
        // write some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
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
        //assert_eq!(metrics.num_updated_rows, 4);
        //assert_eq!(metrics.num_copied_rows, 0);

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
        let actual = get_data(table).await;
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
        // write some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
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
        //assert_eq!(metrics.num_updated_rows, 2);
        //assert_eq!(metrics.num_copied_rows, 0);

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

        let actual = get_data(table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_failure_invalid_expressions() {
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
    }
}
