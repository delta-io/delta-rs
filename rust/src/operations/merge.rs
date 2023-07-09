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

use arrow_cast::pretty::pretty_format_batches;
use datafusion::{
    execution::context::SessionState,
    physical_plan::{
        filter::FilterExec,
        joins::{
            utils::{build_join_schema, JoinFilter},
            NestedLoopJoinExec,
        },
        projection::ProjectionExec,
        ExecutionPlan,
    },
    prelude::{DataFrame, SessionContext},
};
use datafusion_common::Column;
use datafusion_expr::{col, conditional_expressions::CaseBuilder, lit, Expr, JoinType};
use datafusion_physical_expr::{create_physical_expr, expressions, PhysicalExpr};
use futures::{future::BoxFuture, StreamExt};
use parquet::file::properties::WriterProperties;
use serde_json::{Map, Value};

use crate::{
    action::{Action, DeltaOperation, Remove},
    delta_datafusion::{parquet_scan_from_actions, register_store},
    operations::write::write_execution_plan,
    storage::{DeltaObjectStore, ObjectStoreRef},
    table_state::DeltaTableState,
    DeltaResult, DeltaTable, DeltaTableError,
};

use super::{transaction::commit, update::Expression};

/// Merge records into a Delta Table.
/// See this module's documentation for more information
pub struct MergeBuilder {
    /// The join predicate
    predicate: Expression,
    /// TODO
    match_operations: Vec<MergeOperation>,
    /// TODO
    not_match_operations: Vec<MergeOperation>,
    /// TODO
    not_match_source_operations: Vec<MergeOperation>,
    //last_match_update: bool,
    //last_match_delete: bool,
    //last_not_matched_insert: bool,
    //last_not_match_source_update: bool,
    //last_not_match_source_delete: bool,
    /// A snapshot of the table's state. AKA the target table in the operation
    snapshot: DeltaTableState,
    /// The source data
    source: DataFrame,
    /// Delta object store for handling data files
    object_store: Arc<DeltaObjectStore>,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<Map<String, serde_json::Value>>,
    /// safe_cast determines how data types that do not match the underlying table are handled
    /// By default an error is returned
    safe_cast: bool,
}

impl MergeBuilder {
    /// Create a new [`MergeBuilder`]
    pub fn new(
        object_store: ObjectStoreRef,
        snapshot: DeltaTableState,
        predicate: Expression,
        source: DataFrame,
    ) -> Self {
        Self {
            predicate,
            source,
            snapshot,
            object_store,
            state: None,
            app_metadata: None,
            writer_properties: None,
            match_operations: Vec::new(),
            not_match_operations: Vec::new(),
            not_match_source_operations: Vec::new(),
            safe_cast: false,
            //last_match_update: false,
            //last_match_delete: false,
            //last_not_matched_insert: false,
            //last_not_match_source_update: false,
            //last_not_match_source_delete: false,
        }
    }

    /// Perform an additonal update expression during the operaton
    pub fn when_matched_update(self) -> MergeOperationBuilder {
        MergeOperationBuilder::new(self, OperationClass::Match, OperationType::Update)
    }
    /// Perform an additonal update expression during the operaton
    pub fn when_matched_delete(self) -> MergeOperationBuilder {
        MergeOperationBuilder::new(self, OperationClass::Match, OperationType::Delete)
    }

    /// Perform an additonal update expression during the operaton
    pub fn when_not_matched_insert(self) -> MergeOperationBuilder {
        MergeOperationBuilder::new(self, OperationClass::TargetNotMatch, OperationType::Insert)
    }

    /// Perform an additonal update expression during the operaton
    pub fn when_not_matched_by_source_update(self) -> MergeOperationBuilder {
        MergeOperationBuilder::new(self, OperationClass::SourceNotMatch, OperationType::Update)
    }

    /// Perform an additonal update expression during the operaton
    pub fn when_not_matched_by_source_delete(self) -> MergeOperationBuilder {
        MergeOperationBuilder::new(self, OperationClass::SourceNotMatch, OperationType::Delete)
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

enum OperationClass {
    Match,
    TargetNotMatch,
    SourceNotMatch,
}

#[derive(Debug)]
enum OperationType {
    Update,
    Delete,
    Insert,
}

/// TODO!
pub struct MergeOperationBuilder {
    /// Which records to update
    predicate: Option<Expression>,
    /// How to update columns in a record that match the predicate
    updates: HashMap<Column, Expression>,
    merge_builder: MergeBuilder,
    class: OperationClass,
    r#type: OperationType,
}

impl MergeOperationBuilder {
    fn new(merge_builder: MergeBuilder, class: OperationClass, r#type: OperationType) -> Self {
        MergeOperationBuilder {
            class,
            r#type,
            predicate: None,
            updates: HashMap::new(),
            merge_builder,
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

    fn _build(mut self) -> MergeBuilder {
        let predicate = match self.predicate {
            Some(predicate) => {
                match predicate {
                    Expression::DataFusion(expr) => Some(expr),
                    //TODO: Have this return a result...
                    Expression::String(s) => Some(
                        self.merge_builder
                            .snapshot
                            .parse_predicate_expression(s)
                            .unwrap(),
                    ),
                }
            }
            None => None,
        };

        let update = MergeOperation {
            predicate: predicate,
            updates: self.updates,
            r#type: self.r#type,
        };

        match self.class {
            OperationClass::Match => self.merge_builder.match_operations.push(update),
            OperationClass::TargetNotMatch => self.merge_builder.not_match_operations.push(update),
            OperationClass::SourceNotMatch => {
                self.merge_builder.not_match_source_operations.push(update)
            }
        }

        self.merge_builder
    }

    /// Perform an additonal update expression during the operaton
    pub fn when_matched_update(self) -> MergeOperationBuilder {
        self._build().when_matched_update()
    }

    /// Perform an additonal update expression during the operaton
    pub fn when_not_matched_insert(self) -> MergeOperationBuilder {
        self._build().when_not_matched_insert()
    }

    /// TODO Add the not match source stuff

    /// TODO
    pub fn build(self) -> MergeBuilder {
        self._build()
    }
}

/// TODO
pub struct MergeOperation {
    /// Which records to update
    predicate: Option<Expr>,
    /// How to update columns in a record that match the predicate
    updates: HashMap<Column, Expression>,
    /// type
    r#type: OperationType,
}

#[derive(Default)]
/// Metrics for the Merge Operation
pub struct MergeMetrics {
    /// Number of rows in the source data
    pub num_source_rows: usize,
    /// Number of rows inserted into the target table
    pub num_target_rows_inserted: usize,
    /// Number of rows updated in the target table
    pub num_target_rows_updated: usize,
    /// Number of rows deleted in the target table
    pub num_target_rows_deleted: usize,
    /// Number of target rows copied
    pub num_target_rows_copied: usize,
    /// Total number of rows written out
    pub num_output_rows: usize,
    /// Number of files added to the sink(target)
    pub num_target_files_added: usize,
    /// Number of files removed from the sink(target)
    pub num_target_files_removed: usize,
    /// Time taken to execute the entire operation
    pub execution_time_ms: u128,
    /// Time taken to scan the files for matches
    pub scan_time_ms: u128,
    /// Time taken to rewrite the matched files
    pub rewrite_time_ms: u128,
}

async fn execute(
    predicate: Expression,
    source: DataFrame,
    object_store: ObjectStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    app_metadata: Option<Map<String, Value>>,
    safe_cast: bool,
    match_operations: Vec<MergeOperation>,
    not_match_target_operations: Vec<MergeOperation>,
    not_match_source_operations: Vec<MergeOperation>,
) -> DeltaResult<((Vec<Action>, i64), MergeMetrics)> {
    let mut metrics = MergeMetrics::default();
    let exec_start = Instant::now();

    let current_metadata = snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?;

    let predicate = match predicate {
        Expression::DataFusion(expr) => expr,
        Expression::String(s) => snapshot.parse_predicate_expression(s)?,
    };

    let schema = snapshot.arrow_schema()?;
    // Try to prune files that are scanned based on partition pruning.
    // Can also be extended to columns too
    let target = parquet_scan_from_actions(
        snapshot,
        object_store.clone(),
        &snapshot.files(),
        &schema,
        None,
        &state,
        None,
        None,
    )
    .await?;

    let source = source.create_physical_plan().await?;

    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let source_schema = source.schema();
    for (i, field) in source_schema.fields().into_iter().enumerate() {
        expressions.push((
            Arc::new(expressions::Column::new(field.name(), i)),
            field.name().to_owned(),
        ));
    }
    expressions.push((
        Arc::new(expressions::Literal::new(true.into())),
        "__delta_rs_source".to_owned(),
    ));
    let source = Arc::new(ProjectionExec::try_new(expressions, source.clone())?);

    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let target_schema = target.schema();
    for (i, field) in target_schema.fields().into_iter().enumerate() {
        expressions.push((
            Arc::new(expressions::Column::new(field.name(), i)),
            field.name().to_owned(),
        ));
    }
    expressions.push((
        Arc::new(expressions::Literal::new(true.into())),
        "__delta_rs_target".to_owned(),
    ));
    let target = Arc::new(ProjectionExec::try_new(expressions, target.clone())?);

    let join_schema = build_join_schema(&source.schema(), &target.schema(), &JoinType::Full);
    let predicate_expr = create_physical_expr(
        &predicate,
        &join_schema.0.clone().try_into()?,
        &join_schema.0,
        state.execution_props(),
    )?;
    let join_filter = JoinFilter::new(predicate_expr, join_schema.1, join_schema.0);
    // How to determine if a record is a left, full, or right join?

    // The left side is coalesced into a single partition
    let join: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinExec::try_new(
        source.clone(),
        target.clone(),
        Some(join_filter),
        &datafusion_expr::JoinType::Full,
    )?);

    // Project to include __delta_rs_operation which indicates which particular operation to perform on the column.
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let schema = join.schema();
    for (i, field) in schema.fields().into_iter().enumerate() {
        expressions.push((
            Arc::new(expressions::Column::new(field.name(), i)),
            field.name().to_owned(),
        ));
    }

    let matched = col("__delta_rs_source")
        .is_true()
        .and(col("__delta_rs_target").is_true());
    let not_matched_target = col("__delta_rs_source")
        .is_true()
        .and(col("__delta_rs_target").is_null());
    let not_matched_source = col("__delta_rs_source")
        .is_null()
        .and(col("__delta_rs_target"))
        .is_true();

    let mut when_expr = Vec::new();
    let mut then_expr = Vec::new();
    let mut ops = Vec::new();

    fn update_case(
        operations: Vec<MergeOperation>,
        ops: &mut Vec<(HashMap<Column, Expression>, bool)>,
        when_expr: &mut Vec<Expr>,
        then_expr: &mut Vec<Expr>,
        base_expr: &Expr,
    ) {
        for action in operations {
            let predicate = match &action.predicate {
                Some(predicate) => base_expr.clone().and(predicate.to_owned()),
                None => base_expr.clone(),
            };

            let delete = match action.r#type {
                OperationType::Delete => true,
                _ => false,
            };

            when_expr.push(predicate);
            then_expr.push(lit(ops.len() as i32));

            ops.push((action.updates, delete));
        }
    }

    update_case(
        match_operations,
        &mut ops,
        &mut when_expr,
        &mut then_expr,
        &matched,
    );

    update_case(
        not_match_target_operations,
        &mut ops,
        &mut when_expr,
        &mut then_expr,
        &not_matched_target,
    );

    update_case(
        not_match_source_operations,
        &mut ops,
        &mut when_expr,
        &mut then_expr,
        &not_matched_source,
    );

    when_expr.push(matched);
    then_expr.push(lit(ops.len() as i32));
    ops.push((HashMap::new(), false));

    when_expr.push(not_matched_target);
    then_expr.push(lit(ops.len() as i32));
    ops.push((HashMap::new(), true));

    when_expr.push(not_matched_source);
    then_expr.push(lit(ops.len() as i32));
    ops.push((HashMap::new(), false));

    let case = CaseBuilder::new(None, when_expr, then_expr, None).end()?;

    let case = create_physical_expr(
        &case,
        &join.schema().as_ref().to_owned().try_into()?,
        &join.schema(),
        state.execution_props(),
    )?;
    expressions.push((case, "__delta_rs_operation".to_owned()));
    let projection = Arc::new(ProjectionExec::try_new(expressions, join.clone())?);

    // Project again and include the original table schema plus a column to mark if row needs to be filtered before write
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let schema = projection.schema();
    for (i, field) in schema.fields().into_iter().enumerate() {
        expressions.push((
            Arc::new(expressions::Column::new(field.name(), i)),
            field.name().to_owned(),
        ));
    }

    let mut projection_map = HashMap::new();
    for field in snapshot.schema().unwrap().get_fields() {
        //TODO: reuse existing vectors and pre-allocate size...
        let mut when_expr = Vec::new();
        let mut then_expr = Vec::new();

        for (idx, (operations, _delete)) in ops.iter().enumerate() {
            let op = operations
                .get(&field.get_name().to_owned().into())
                .map(|expr| {
                    match expr {
                        Expression::DataFusion(expr) => expr.to_owned(),
                        //TODO: Have this return a result...
                        Expression::String(s) => {
                            snapshot.parse_predicate_expression(s).unwrap().to_owned()
                        }
                    }
                })
                .unwrap_or(col(field.get_name()));

            when_expr.push(lit(idx as i32));
            then_expr.push(op);
        }

        let case = CaseBuilder::new(
            Some(Box::new(col("__delta_rs_operation"))),
            when_expr,
            then_expr,
            None,
        )
        .end()?;

        let case = create_physical_expr(
            &case,
            &projection.schema().as_ref().to_owned().try_into()?,
            &projection.schema(),
            state.execution_props(),
        )?;

        projection_map.insert(field.get_name(), expressions.len());
        expressions.push((case, "__delta_rs_c_".to_owned() + field.get_name()));
    }

    let mut when_expr = Vec::new();
    let mut then_expr = Vec::new();
    for (idx, (_operations, delete)) in ops.iter().enumerate() {
        when_expr.push(lit(idx as i32));
        then_expr.push(lit(delete.to_owned()));
    }
    let case = CaseBuilder::new(
        Some(Box::new(col("__delta_rs_operation"))),
        when_expr,
        then_expr,
        None,
    )
    .end()?;

    let case = create_physical_expr(
        &case,
        &projection.schema().as_ref().to_owned().try_into()?,
        &projection.schema(),
        state.execution_props(),
    )?;

    expressions.push((case, "__delta_rs_delete".to_owned()));
    let projection = Arc::new(ProjectionExec::try_new(expressions, projection.clone())?);

    let mut batches = Vec::new();
    let mut b = projection.execute(0, state.task_ctx())?;

    while let Some(r) = b.next().await {
        let b = r?;
        batches.push(b);
    }
    println!("{}", pretty_format_batches(&batches)?);

    // TODO: Filter records after applying updates and deletions. Column used to determine this will change
    let write_predicate = create_physical_expr(
        &(col("__delta_rs_delete").is_false()),
        &projection.schema().as_ref().to_owned().try_into()?,
        &projection.schema(),
        state.execution_props(),
    )?;
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(write_predicate, projection.clone())?);

    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    for (key, value) in projection_map {
        expressions.push((
            Arc::new(expressions::Column::new(
                &("__delta_rs_c_".to_owned() + key),
                value,
            )),
            key.to_owned(),
        ));
    }
    // project filtered records to delta schema
    let projection = Arc::new(ProjectionExec::try_new(expressions, filter.clone())?);

    // write projected records
    let table_partition_cols = current_metadata.partition_columns.clone();
    let add_actions = write_execution_plan(
        snapshot,
        state.clone(),
        projection.clone(),
        table_partition_cols.clone(),
        object_store.clone(),
        Some(snapshot.table_config().target_file_size() as usize),
        None,
        writer_properties,
        safe_cast,
    )
    .await?;

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut actions: Vec<Action> = add_actions.into_iter().map(Action::add).collect();
    for action in snapshot.files() {
        actions.push(Action::remove(Remove {
            path: action.path.clone(),
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values.clone()),
            size: Some(action.size),
            tags: None,
        }))
    }

    let mut version = snapshot.version();

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis();
    // Do not make a commit when there are zero updates to the state
    if !actions.is_empty() {
        let operation = DeltaOperation::Merge {
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

impl std::future::IntoFuture for MergeBuilder {
    type Output = DeltaResult<(DeltaTable, MergeMetrics)>;
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
                this.source,
                this.object_store.clone(),
                &this.snapshot,
                state,
                this.writer_properties,
                this.app_metadata,
                this.safe_cast,
                this.match_operations,
                this.not_match_operations,
                this.not_match_source_operations,
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
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::get_arrow_schema;
    use crate::writer::test_utils::get_delta_schema;
    use crate::DeltaTable;
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::from_slice::FromSlice;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::col;
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

    #[tokio::test]
    async fn test_merge() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "C", "D"])),
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

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from_slice([10, 20, 30])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx
            .read_batch(batch)
            .unwrap()
            .with_column_renamed("id", "id_src")
            .unwrap()
            .with_column_renamed("value", "value_src")
            .unwrap()
            .with_column_renamed("modified", "modified_src")
            .unwrap();

        let (table, _metrics) = DeltaOps(table)
            .merge(source, col("id").eq(col("id_src")))
            .when_matched_update()
            .with_update("value", col("value_src"))
            .with_update("modified", col("modified_src"))
            .build()
            .when_not_matched_insert()
            .with_update("id", col("id_src"))
            .with_update("value", col("value_src"))
            .with_update("modified", col("modified_src"))
            .build()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 1);
        /*
        assert_eq!(metrics.num_target_files_added, 1);
        assert_eq!(metrics.num_target_files_removed, 1);
        assert_eq!(metrics.num_target_rows_copied, 2);
        assert_eq!(metrics.num_target_rows_updated, 2);
        */

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-02 |",
            "| B  | 10    | 2021-02-02 |",
            "| C  | 20    | 2023-07-04 |",
            "| D  | 100   | 2021-02-02 |",
            "| X  | 30    | 2023-07-04 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }
}
