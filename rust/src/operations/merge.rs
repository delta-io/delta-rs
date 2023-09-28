//! Merge data from a source dataset with the target Delta Table based on a join
//! predicate.  A full outer join is performed which results in source and
//! target records that match, source records that do not match, or target
//! records that do not match.
//!
//! Users can specify update, delete, and insert operations for these categories
//! and specify additional predicates for finer control. The order of operations
//! specified matter.  See [`MergeBuilder`] for more information
//!
//! *WARNING* The current implementation rewrites the entire delta table so only
//! use on small to medium sized tables. The solution also cannot take advantage
//! of multiple threads and is limited to a single single thread.
//! Enhancements tracked at #850
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = DeltaOps(table)
//!     .merge(source, col("id").eq(col("source.id")))
//!     .with_source_alias("source")
//!     .when_matched_update(|update| {
//!         update
//!             .update("value", col("source.value") + lit(1))
//!             .update("modified", col("source.modified"))
//!     })?
//!     .when_not_matched_insert(|insert| {
//!         insert
//!             .set("id", col("source.id"))
//!             .set("value", col("source.value"))
//!             .set("modified", col("source.modified"))
//!     })?
//!     .await?
//! ````

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_schema::SchemaRef;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::{
    execution::context::SessionState,
    physical_plan::{
        filter::FilterExec,
        joins::{
            utils::{build_join_schema, JoinFilter},
            NestedLoopJoinExec,
        },
        metrics::{MetricBuilder, MetricsSet},
        projection::ProjectionExec,
        ExecutionPlan,
    },
    prelude::{DataFrame, SessionContext},
};
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::{col, conditional_expressions::CaseBuilder, lit, when, Expr, JoinType};
use datafusion_physical_expr::{create_physical_expr, expressions, PhysicalExpr};
use futures::future::BoxFuture;
use parquet::file::properties::WriterProperties;
use serde_json::{Map, Value};

use super::datafusion_utils::{into_expr, maybe_into_expr, Expression};
use super::transaction::commit;
use crate::delta_datafusion::{parquet_scan_from_actions, register_store};
use crate::operations::datafusion_utils::MetricObserverExec;
use crate::operations::write::write_execution_plan;
use crate::protocol::{Action, DeltaOperation, MergePredicate, Remove};
use crate::storage::{DeltaObjectStore, ObjectStoreRef};
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

const OPERATION_COLUMN: &str = "__delta_rs_operation";
const DELETE_COLUMN: &str = "__delta_rs_delete";
const TARGET_INSERT_COLUMN: &str = "__delta_rs_target_insert";
const TARGET_UPDATE_COLUMN: &str = "__delta_rs_target_update";
const TARGET_DELETE_COLUMN: &str = "__delta_rs_target_delete";
const TARGET_COPY_COLUMN: &str = "__delta_rs_target_copy";

const SOURCE_COUNT_METRIC: &str = "num_source_rows";
const TARGET_COPY_METRIC: &str = "num_copied_rows";
const TARGET_INSERTED_METRIC: &str = "num_target_inserted_rows";
const TARGET_UPDATED_METRIC: &str = "num_target_updated_rows";
const TARGET_DELETED_METRIC: &str = "num_target_deleted_rows";

/// Merge records into a Delta Table.
pub struct MergeBuilder {
    /// The join predicate
    predicate: Expression,
    /// Operations to perform when a source record and target record match
    match_operations: Vec<MergeOperation>,
    /// Operations to perform on source records when they do not pair with a target record
    not_match_operations: Vec<MergeOperation>,
    /// Operations to perform on target records when they do not pair with a source record
    not_match_source_operations: Vec<MergeOperation>,
    ///Prefix the source columns with a user provided prefix
    source_alias: Option<String>,
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
            source_alias: None,
            state: None,
            app_metadata: None,
            writer_properties: None,
            match_operations: Vec::new(),
            not_match_operations: Vec::new(),
            not_match_source_operations: Vec::new(),
            safe_cast: false,
        }
    }

    /// Update a target record when it matches with a source record
    ///
    /// The update expressions can specify both source and target columns.
    ///
    /// Multiple match clasues can be specified and their predicates are
    /// evaluated to determine if the corresponding operation are performed.
    /// Only the first clause that results in an satisfy predicate is executed.
    /// Ther order of match clauses matter.
    ///
    /// #Example
    /// ```rust ignore
    /// let table = open_table("../path/to/table")?;
    /// let (table, metrics) = DeltaOps(table)
    ///     .merge(source, col("id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .when_matched_update(|update| {
    ///         update
    ///             .predicate(col("source.value").lt(lit(0)))
    ///             .update("value", lit(0))
    ///             .update("modified", col("source.modified"))
    ///     })?
    ///     .when_matched_update(|update| {
    ///         update
    ///             .update("value", col("source.value") + lit(1))
    ///             .update("modified", col("source.modified"))
    ///     })?
    ///     .await?
    /// ```
    pub fn when_matched_update<F>(mut self, builder: F) -> DeltaResult<MergeBuilder>
    where
        F: FnOnce(UpdateBuilder) -> UpdateBuilder,
    {
        let builder = builder(UpdateBuilder::default());
        let op = MergeOperation::try_new(
            &self.snapshot,
            builder.predicate,
            builder.updates,
            OperationType::Update,
        )?;
        self.match_operations.push(op);
        Ok(self)
    }

    /// Delete a target record when it matches with a source record
    ///
    /// Multiple match clasues can be specified and their predicates are
    /// evaluated to determine if the corresponding operation are performed.
    /// Only the first clause that results in an satisfy predicate is executed.
    /// Ther order of match clauses matter.
    ///
    /// #Example
    /// ```rust ignore
    /// let table = open_table("../path/to/table")?;
    /// let (table, metrics) = DeltaOps(table)
    ///     .merge(source, col("id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .when_matched_delete(|delete| {
    ///         delete.predicate(col("source.delete"))
    ///     })?
    ///     .await?
    /// ```
    pub fn when_matched_delete<F>(mut self, builder: F) -> DeltaResult<MergeBuilder>
    where
        F: FnOnce(DeleteBuilder) -> DeleteBuilder,
    {
        let builder = builder(DeleteBuilder::default());
        let op = MergeOperation::try_new(
            &self.snapshot,
            builder.predicate,
            HashMap::default(),
            OperationType::Delete,
        )?;
        self.match_operations.push(op);
        Ok(self)
    }

    /// Insert a source record when it does not match with a target record
    ///
    /// Multiple not match clasues can be specified and their predicates are
    /// evaluated to determine if the corresponding operation are performed.
    /// Only the first clause that results in an satisfy predicate is executed.
    /// Ther order of not match clauses matter.
    ///
    /// #Example
    /// ```rust ignore
    /// let table = open_table("../path/to/table")?;
    /// let (table, metrics) = DeltaOps(table)
    ///     .merge(source, col("id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .when_not_matched_insert(|insert| {
    ///         insert
    ///             .set("id", col("source.id"))
    ///             .set("value", col("source.value"))
    ///             .set("modified", col("source.modified"))
    ///     })?
    ///     .await?
    /// ```
    pub fn when_not_matched_insert<F>(mut self, builder: F) -> DeltaResult<MergeBuilder>
    where
        F: FnOnce(InsertBuilder) -> InsertBuilder,
    {
        let builder = builder(InsertBuilder::default());
        let op = MergeOperation::try_new(
            &self.snapshot,
            builder.predicate,
            builder.set,
            OperationType::Insert,
        )?;
        self.not_match_operations.push(op);
        Ok(self)
    }

    /// Update a target record when it does not match with a
    /// source record
    ///
    /// The update expressions can specify only target columns.
    ///
    /// Multiple source not match clasues can be specified and their predicates
    /// are evaluated to determine if the corresponding operation are performed.
    /// Only the first clause that results in an satisfy predicate is executed.
    /// Ther order of source not match clauses matter.
    ///
    /// #Example
    /// ```rust ignore
    /// let table = open_table("../path/to/table")?;
    /// let (table, metrics) = DeltaOps(table)
    ///     .merge(source, col("id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .when_not_matched_by_source_update(|update| {
    ///         update
    ///             .update("active", lit(false))
    ///             .update("to_dt", lit("2023-07-11"))
    ///     })?
    ///     .await?
    /// ```
    pub fn when_not_matched_by_source_update<F>(mut self, builder: F) -> DeltaResult<MergeBuilder>
    where
        F: FnOnce(UpdateBuilder) -> UpdateBuilder,
    {
        let builder = builder(UpdateBuilder::default());
        let op = MergeOperation::try_new(
            &self.snapshot,
            builder.predicate,
            builder.updates,
            OperationType::Update,
        )?;
        self.not_match_source_operations.push(op);
        Ok(self)
    }

    /// Delete a target record when it does not match with a source record
    ///
    /// Multiple source not match clasues can be specified and their predicates
    /// are evaluated to determine if the corresponding operation are performed.
    /// Only the first clause that results in an satisfy predicate is executed.
    /// Ther order of source not match clauses matter.
    ///
    /// #Example
    /// ```rust ignore
    /// let table = open_table("../path/to/table")?;
    /// let (table, metrics) = DeltaOps(table)
    ///     .merge(source, col("id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .when_not_matched_by_source_delete(|delete| {
    ///         delete
    ///     })?
    ///     .await?
    /// ```
    pub fn when_not_matched_by_source_delete<F>(mut self, builder: F) -> DeltaResult<MergeBuilder>
    where
        F: FnOnce(DeleteBuilder) -> DeleteBuilder,
    {
        let builder = builder(DeleteBuilder::default());
        let op = MergeOperation::try_new(
            &self.snapshot,
            builder.predicate,
            HashMap::default(),
            OperationType::Delete,
        )?;
        self.not_match_source_operations.push(op);
        Ok(self)
    }

    /// Rename columns in the source dataset to have a prefix of `alias`.`original column name`
    pub fn with_source_alias<S: ToString>(mut self, alias: S) -> Self {
        self.source_alias = Some(alias.to_string());
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

#[derive(Default)]
/// Builder for update clauses
pub struct UpdateBuilder {
    /// Only update records that match the predicate
    predicate: Option<Expression>,
    /// How to update columns in the target table
    updates: HashMap<Column, Expression>,
}

impl UpdateBuilder {
    /// Perform the update operation when the predicate is satisfied
    pub fn predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// How a column from the target table should be updated.
    /// In the match case the expression may contain both source and target columns.
    /// In the source not match case the expression may only contain target columns
    pub fn update<C: Into<Column>, E: Into<Expression>>(
        mut self,
        column: C,
        expression: E,
    ) -> Self {
        self.updates.insert(column.into(), expression.into());
        self
    }
}

/// Builder for insert clauses
#[derive(Default)]
pub struct InsertBuilder {
    /// Only insert records that match the predicate
    predicate: Option<Expression>,
    /// What value each column is inserted with
    set: HashMap<Column, Expression>,
}

impl InsertBuilder {
    /// Perform the insert operation when the predicate is satisfied
    pub fn predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// Which values to insert into the target tables. If a target column is not
    /// specified then null is inserted.
    pub fn set<C: Into<Column>, E: Into<Expression>>(mut self, column: C, expression: E) -> Self {
        self.set.insert(column.into(), expression.into());
        self
    }
}

/// Builder for delete clauses
#[derive(Default)]
pub struct DeleteBuilder {
    predicate: Option<Expression>,
}

impl DeleteBuilder {
    /// Delete a record when the predicate is satisfied
    pub fn predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }
}

#[derive(Debug, Copy, Clone)]
enum OperationType {
    Update,
    Delete,
    SourceDelete,
    Insert,
    Copy,
}

struct MergeOperation {
    /// Which records to update
    predicate: Option<Expr>,
    /// How to update columns in a record that match the predicate
    operations: HashMap<Column, Expr>,
    r#type: OperationType,
}

impl MergeOperation {
    pub fn try_new(
        snapshot: &DeltaTableState,
        predicate: Option<Expression>,
        operations: HashMap<Column, Expression>,
        r#type: OperationType,
    ) -> DeltaResult<Self> {
        let predicate = maybe_into_expr(predicate, snapshot)?;
        let mut _operations = HashMap::new();

        for (column, expr) in operations {
            _operations.insert(column, into_expr(expr, snapshot)?);
        }

        Ok(MergeOperation {
            predicate,
            operations: _operations,
            r#type,
        })
    }
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
    pub execution_time_ms: u64,
    /// Time taken to scan the files for matches
    pub scan_time_ms: u64,
    /// Time taken to rewrite the matched files
    pub rewrite_time_ms: u64,
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    predicate: Expression,
    source: DataFrame,
    object_store: ObjectStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    app_metadata: Option<Map<String, Value>>,
    safe_cast: bool,
    source_alias: Option<String>,
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

    let schema = snapshot.input_schema()?;

    // TODO: Given the join predicate, remove any expression that involve the
    // source table and keep expressions that only involve the target table.
    // This would allow us to perform statistics/partition pruning E.G
    // Expression source.id = id and to_dt = '9999-12-31' -Becomes-> to_dt =
    // '9999-12-31'
    //
    // If the user specified any not_source_match operations then those
    // predicates also need to be considered when pruning

    let target = parquet_scan_from_actions(
        snapshot,
        object_store.clone(),
        snapshot.files(),
        &schema,
        None,
        &state,
        None,
        None,
    )
    .await?;

    let source = source.create_physical_plan().await?;

    let source_count = Arc::new(MetricObserverExec::new(source, |batch, metrics| {
        MetricBuilder::new(metrics)
            .global_counter(SOURCE_COUNT_METRIC)
            .add(batch.num_rows());
    }));

    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let source_schema = source_count.schema();

    let source_prefix = source_alias
        .map(|mut s| {
            s.push('.');
            s
        })
        .unwrap_or_default();
    for (i, field) in source_schema.fields().into_iter().enumerate() {
        expressions.push((
            Arc::new(expressions::Column::new(field.name(), i)),
            source_prefix.clone() + field.name(),
        ));
    }
    expressions.push((
        Arc::new(expressions::Literal::new(true.into())),
        "__delta_rs_source".to_owned(),
    ));
    let source = Arc::new(ProjectionExec::try_new(expressions, source_count.clone())?);

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

    // TODO: Currently a NestedLoopJoin is used but we should target to support SortMergeJoin
    // This would require rewriting the join predicate to only contain equality between left and right columns and pushing some filters down
    // Ideally it would be nice if the optimizer / planner can pick the best join so maybe explore rewriting the entire operation using logical plans.

    // NLJ requires both sides to have one partition for outer joins
    let target = Arc::new(CoalescePartitionsExec::new(target));
    let source = Arc::new(CoalescePartitionsExec::new(source));

    let join_schema = build_join_schema(&source.schema(), &target.schema(), &JoinType::Full);
    let predicate_expr = create_physical_expr(
        &predicate,
        &join_schema.0.clone().try_into()?,
        &join_schema.0,
        state.execution_props(),
    )?;

    let join_filter = JoinFilter::new(predicate_expr, join_schema.1, join_schema.0);
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

    // Plus 3 for the default operations for each match category
    let operations_size = match_operations.len()
        + not_match_source_operations.len()
        + not_match_target_operations.len()
        + 3;

    let mut when_expr = Vec::with_capacity(operations_size);
    let mut then_expr = Vec::with_capacity(operations_size);
    let mut ops = Vec::with_capacity(operations_size);

    fn update_case(
        operations: Vec<MergeOperation>,
        ops: &mut Vec<(HashMap<Column, Expr>, OperationType)>,
        when_expr: &mut Vec<Expr>,
        then_expr: &mut Vec<Expr>,
        base_expr: &Expr,
    ) -> DeltaResult<Vec<MergePredicate>> {
        let mut predicates = Vec::with_capacity(operations.len());

        for op in operations {
            let predicate = match &op.predicate {
                Some(predicate) => base_expr.clone().and(predicate.to_owned()),
                None => base_expr.clone(),
            };

            when_expr.push(predicate);
            then_expr.push(lit(ops.len() as i32));

            ops.push((op.operations, op.r#type));

            let action_type = match op.r#type {
                OperationType::Update => "update",
                OperationType::Delete => "delete",
                OperationType::Insert => "insert",
                OperationType::SourceDelete => {
                    return Err(DeltaTableError::Generic("Invalid action type".to_string()))
                }
                OperationType::Copy => {
                    return Err(DeltaTableError::Generic("Invalid action type".to_string()))
                }
            };

            let action_type = action_type.to_string();
            let predicate = op.predicate.map(|expr| expr.display_name().unwrap());

            predicates.push(MergePredicate {
                action_type,
                predicate,
            });
        }
        Ok(predicates)
    }

    let match_operations = update_case(
        match_operations,
        &mut ops,
        &mut when_expr,
        &mut then_expr,
        &matched,
    )?;

    let not_match_target_operations = update_case(
        not_match_target_operations,
        &mut ops,
        &mut when_expr,
        &mut then_expr,
        &not_matched_target,
    )?;

    let not_match_source_operations = update_case(
        not_match_source_operations,
        &mut ops,
        &mut when_expr,
        &mut then_expr,
        &not_matched_source,
    )?;

    when_expr.push(matched);
    then_expr.push(lit(ops.len() as i32));
    ops.push((HashMap::new(), OperationType::Copy));

    when_expr.push(not_matched_target);
    then_expr.push(lit(ops.len() as i32));
    ops.push((HashMap::new(), OperationType::SourceDelete));

    when_expr.push(not_matched_source);
    then_expr.push(lit(ops.len() as i32));
    ops.push((HashMap::new(), OperationType::Copy));

    let case = CaseBuilder::new(None, when_expr, then_expr, None).end()?;

    let case = create_physical_expr(
        &case,
        &join.schema().as_ref().to_owned().try_into()?,
        &join.schema(),
        state.execution_props(),
    )?;
    expressions.push((case, OPERATION_COLUMN.to_owned()));
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
        let mut when_expr = Vec::with_capacity(operations_size);
        let mut then_expr = Vec::with_capacity(operations_size);

        for (idx, (operations, _)) in ops.iter().enumerate() {
            let op = operations
                .get(&field.get_name().to_owned().into())
                .map(|expr| expr.to_owned())
                .unwrap_or(col(field.get_name()));

            when_expr.push(lit(idx as i32));
            then_expr.push(op);
        }

        let case = CaseBuilder::new(
            Some(Box::new(col(OPERATION_COLUMN))),
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

    let mut insert_when = Vec::with_capacity(ops.len());
    let mut insert_then = Vec::with_capacity(ops.len());

    let mut update_when = Vec::with_capacity(ops.len());
    let mut update_then = Vec::with_capacity(ops.len());

    let mut target_delete_when = Vec::with_capacity(ops.len());
    let mut target_delete_then = Vec::with_capacity(ops.len());

    let mut delete_when = Vec::with_capacity(ops.len());
    let mut delete_then = Vec::with_capacity(ops.len());

    let mut copy_when = Vec::with_capacity(ops.len());
    let mut copy_then = Vec::with_capacity(ops.len());

    for (idx, (_operations, r#type)) in ops.iter().enumerate() {
        let op = idx as i32;

        // Used to indicate the record should be dropped prior to write
        delete_when.push(lit(op));
        delete_then.push(lit(matches!(
            r#type,
            OperationType::Delete | OperationType::SourceDelete
        )));

        // Use the null count on these arrays to determine how many records satisfy the predicate
        insert_when.push(lit(op));
        insert_then.push(
            when(
                lit(matches!(r#type, OperationType::Insert)),
                lit(ScalarValue::Boolean(None)),
            )
            .otherwise(lit(false))?,
        );

        update_when.push(lit(op));
        update_then.push(
            when(
                lit(matches!(r#type, OperationType::Update)),
                lit(ScalarValue::Boolean(None)),
            )
            .otherwise(lit(false))?,
        );

        target_delete_when.push(lit(op));
        target_delete_then.push(
            when(
                lit(matches!(r#type, OperationType::Delete)),
                lit(ScalarValue::Boolean(None)),
            )
            .otherwise(lit(false))?,
        );

        copy_when.push(lit(op));
        copy_then.push(
            when(
                lit(matches!(r#type, OperationType::Copy)),
                lit(ScalarValue::Boolean(None)),
            )
            .otherwise(lit(false))?,
        );
    }

    fn build_case(
        when: Vec<Expr>,
        then: Vec<Expr>,
        schema: SchemaRef,
        input_dfschema: &DFSchema,
        state: &SessionState,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let case = CaseBuilder::new(
            Some(Box::new(col(OPERATION_COLUMN))),
            when,
            then,
            Some(Box::new(lit(false))),
        )
        .end()?;

        create_physical_expr(&case, input_dfschema, &schema, state.execution_props())
    }

    let schema = projection.schema();
    let input_dfschema = schema.as_ref().to_owned().try_into()?;
    expressions.push((
        build_case(
            delete_when,
            delete_then,
            schema.clone(),
            &input_dfschema,
            &state,
        )?,
        DELETE_COLUMN.to_owned(),
    ));

    expressions.push((
        build_case(
            insert_when,
            insert_then,
            schema.clone(),
            &input_dfschema,
            &state,
        )?,
        TARGET_INSERT_COLUMN.to_owned(),
    ));
    expressions.push((
        build_case(
            update_when,
            update_then,
            schema.clone(),
            &input_dfschema,
            &state,
        )?,
        TARGET_UPDATE_COLUMN.to_owned(),
    ));
    expressions.push((
        build_case(
            target_delete_when,
            target_delete_then,
            schema.clone(),
            &input_dfschema,
            &state,
        )?,
        TARGET_DELETE_COLUMN.to_owned(),
    ));
    expressions.push((
        build_case(
            copy_when,
            copy_then,
            schema.clone(),
            &input_dfschema,
            &state,
        )?,
        TARGET_COPY_COLUMN.to_owned(),
    ));

    let projection = Arc::new(ProjectionExec::try_new(expressions, projection.clone())?);

    let target_count_plan = Arc::new(MetricObserverExec::new(projection, |batch, metrics| {
        MetricBuilder::new(metrics)
            .global_counter(TARGET_INSERTED_METRIC)
            .add(
                batch
                    .column_by_name(TARGET_INSERT_COLUMN)
                    .unwrap()
                    .null_count(),
            );
        MetricBuilder::new(metrics)
            .global_counter(TARGET_UPDATED_METRIC)
            .add(
                batch
                    .column_by_name(TARGET_UPDATE_COLUMN)
                    .unwrap()
                    .null_count(),
            );
        MetricBuilder::new(metrics)
            .global_counter(TARGET_DELETED_METRIC)
            .add(
                batch
                    .column_by_name(TARGET_DELETE_COLUMN)
                    .unwrap()
                    .null_count(),
            );
        MetricBuilder::new(metrics)
            .global_counter(TARGET_COPY_METRIC)
            .add(
                batch
                    .column_by_name(TARGET_COPY_COLUMN)
                    .unwrap()
                    .null_count(),
            );
    }));

    let write_predicate = create_physical_expr(
        &(col(DELETE_COLUMN).is_false()),
        &target_count_plan.schema().as_ref().to_owned().try_into()?,
        &target_count_plan.schema(),
        state.execution_props(),
    )?;
    let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
        write_predicate,
        target_count_plan.clone(),
    )?);

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

    let rewrite_start = Instant::now();
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

    metrics.rewrite_time_ms = Instant::now().duration_since(rewrite_start).as_millis() as u64;

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut actions: Vec<Action> = add_actions.into_iter().map(Action::add).collect();
    metrics.num_target_files_added = actions.len();

    for action in snapshot.files() {
        metrics.num_target_files_removed += 1;
        actions.push(Action::remove(Remove {
            path: action.path.clone(),
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values.clone()),
            deletion_vector: action.deletion_vector.clone(),
            size: Some(action.size),
            tags: None,
        }))
    }

    let mut version = snapshot.version();

    let source_count_metrics = source_count.metrics().unwrap();
    let target_count_metrics = target_count_plan.metrics().unwrap();
    fn get_metric(metrics: &MetricsSet, name: &str) -> usize {
        metrics.sum_by_name(name).map(|m| m.as_usize()).unwrap_or(0)
    }

    metrics.num_source_rows = get_metric(&source_count_metrics, SOURCE_COUNT_METRIC);
    metrics.num_target_rows_inserted = get_metric(&target_count_metrics, TARGET_INSERTED_METRIC);
    metrics.num_target_rows_updated = get_metric(&target_count_metrics, TARGET_UPDATED_METRIC);
    metrics.num_target_rows_deleted = get_metric(&target_count_metrics, TARGET_DELETED_METRIC);
    metrics.num_target_rows_copied = get_metric(&target_count_metrics, TARGET_COPY_METRIC);
    metrics.num_output_rows = metrics.num_target_rows_inserted
        + metrics.num_target_rows_updated
        + metrics.num_target_rows_copied;

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;

    // Do not make a commit when there are zero updates to the state
    if !actions.is_empty() {
        let operation = DeltaOperation::Merge {
            predicate: Some(predicate.canonical_name()),
            matched_predicates: match_operations,
            not_matched_predicates: not_match_target_operations,
            not_matched_by_source_predicates: not_match_source_operations,
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
                this.source_alias,
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

    use crate::operations::DeltaOps;
    use crate::protocol::*;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::get_arrow_schema;
    use crate::writer::test_utils::get_delta_schema;
    use crate::DeltaTable;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::col;
    use datafusion_expr::lit;
    use serde_json::json;
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

    async fn write_data(table: DeltaTable, schema: &Arc<ArrowSchema>) -> DeltaTable {
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-01",
                    "2021-02-01",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();
        // write some data
        DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_merge() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let (mut table, metrics) = DeltaOps(table)
            .merge(source, col("id").eq(col("source.id")))
            .with_source_alias("source")
            .when_matched_update(|update| {
                update
                    .update("value", col("source.value"))
                    .update("modified", col("source.modified"))
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate(col("value").eq(lit(1)))
                    .update("value", col("value") + lit(1))
            })
            .unwrap()
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", col("source.id"))
                    .set("value", col("source.value"))
                    .set("modified", col("source.modified"))
            })
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 1);
        assert_eq!(metrics.num_target_files_added, 1);
        assert_eq!(metrics.num_target_files_removed, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_target_rows_updated, 3);
        assert_eq!(metrics.num_target_rows_inserted, 1);
        assert_eq!(metrics.num_target_rows_deleted, 0);
        assert_eq!(metrics.num_output_rows, 5);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[commit_info.len() - 1];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("id = source.id"));
        assert_eq!(
            parameters["matchedPredicates"],
            json!(r#"[{"actionType":"update"}]"#)
        );
        assert_eq!(
            parameters["notMatchedPredicates"],
            json!(r#"[{"actionType":"insert"}]"#)
        );
        // Todo: Expected this predicate to actually be 'value = 1'. Predicate should contain a valid sql expression
        assert_eq!(
            parameters["notMatchedBySourcePredicates"],
            json!(r#"[{"actionType":"update","predicate":"value = Int32(1)"}]"#)
        );

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 2     | 2021-02-01 |",
            "| B  | 10    | 2021-02-02 |",
            "| C  | 20    | 2023-07-04 |",
            "| D  | 100   | 2021-02-02 |",
            "| X  | 30    | 2023-07-04 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_merge_partitions() {
        /* Validate the join predicate works with partition columns */
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 2);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let (table, metrics) = DeltaOps(table)
            .merge(
                source,
                col("id")
                    .eq(col("source.id"))
                    .and(col("modified").eq(lit("2021-02-02"))),
            )
            .with_source_alias("source")
            .when_matched_update(|update| {
                update
                    .update("value", col("source.value"))
                    .update("modified", col("source.modified"))
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate(col("value").eq(lit(1)))
                    .update("value", col("value") + lit(1))
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate(col("modified").eq(lit("2021-02-01")))
                    .update("value", col("value") - lit(1))
            })
            .unwrap()
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", col("source.id"))
                    .set("value", col("source.value"))
                    .set("modified", col("source.modified"))
            })
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 3);
        assert_eq!(metrics.num_target_files_added, 3);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_target_rows_updated, 3);
        assert_eq!(metrics.num_target_rows_inserted, 2);
        assert_eq!(metrics.num_target_rows_deleted, 0);
        assert_eq!(metrics.num_output_rows, 6);
        assert_eq!(metrics.num_source_rows, 3);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 2     | 2021-02-01 |",
            "| B  | 9     | 2021-02-01 |",
            "| B  | 10    | 2021-02-02 |",
            "| C  | 20    | 2023-07-04 |",
            "| D  | 100   | 2021-02-02 |",
            "| X  | 30    | 2023-07-04 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_merge_delete_matched() {
        // Validate behaviours of match delete

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 2);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let (mut table, metrics) = DeltaOps(table)
            .merge(source, col("id").eq(col("source.id")))
            .with_source_alias("source")
            .when_matched_delete(|delete| delete)
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 2);
        assert_eq!(metrics.num_target_files_added, 2);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 2);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 2);
        assert_eq!(metrics.num_output_rows, 2);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[commit_info.len() - 1];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("id = source.id"));
        assert_eq!(
            parameters["matchedPredicates"],
            json!(r#"[{"actionType":"delete"}]"#)
        );

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-01 |",
            "| D  | 100   | 2021-02-02 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Test match delete again but with a predicate
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 2);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let (mut table, metrics) = DeltaOps(table)
            .merge(source, col("id").eq(col("source.id")))
            .with_source_alias("source")
            .when_matched_delete(|delete| delete.predicate(col("source.value").lt_eq(lit(10))))
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 2);
        assert_eq!(metrics.num_target_files_added, 2);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 3);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 1);
        assert_eq!(metrics.num_output_rows, 3);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[commit_info.len() - 1];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("id = source.id"));
        assert_eq!(
            parameters["matchedPredicates"],
            json!(r#"[{"actionType":"delete","predicate":"source.value <= Int32(10)"}]"#)
        );

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-01 |",
            "| C  | 10    | 2021-02-02 |",
            "| D  | 100   | 2021-02-02 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_merge_delete_not_matched() {
        // Validate behaviours of not match delete

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 2);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let (mut table, metrics) = DeltaOps(table)
            .merge(source, col("id").eq(col("source.id")))
            .with_source_alias("source")
            .when_not_matched_by_source_delete(|delete| delete)
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 2);
        assert_eq!(metrics.num_target_files_added, 2);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 2);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 2);
        assert_eq!(metrics.num_output_rows, 2);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[commit_info.len() - 1];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("id = source.id"));
        assert_eq!(
            parameters["notMatchedBySourcePredicates"],
            json!(r#"[{"actionType":"delete"}]"#)
        );

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| B  | 10    | 2021-02-01 |",
            "| C  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 2);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let (mut table, metrics) = DeltaOps(table)
            .merge(source, col("id").eq(col("source.id")))
            .with_source_alias("source")
            .when_not_matched_by_source_delete(|delete| {
                delete.predicate(col("modified").gt(lit("2021-02-01")))
            })
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 2);
        assert_eq!(metrics.num_target_files_added, 2);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 3);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 1);
        assert_eq!(metrics.num_output_rows, 3);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[commit_info.len() - 1];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("id = source.id"));
        assert_eq!(
            parameters["notMatchedBySourcePredicates"],
            json!(r#"[{"actionType":"delete","predicate":"modified > Utf8(\"2021-02-01\")"}]"#)
        );

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-01 |",
            "| B  | 10    | 2021-02-01 |",
            "| C  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }
}
