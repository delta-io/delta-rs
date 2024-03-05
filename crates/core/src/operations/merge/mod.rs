//! Merge data from a source dataset with the target Delta Table based on a join
//! predicate.  A full outer join is performed which results in source and
//! target records that match, source records that do not match, or target
//! records that do not match.
//!
//! Users can specify update, delete, and insert operations for these categories
//! and specify additional predicates for finer control. The order of operations
//! specified matter.  See [`MergeBuilder`] for more information
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = DeltaOps(table)
//!     .merge(source, col("target.id").eq(col("source.id")))
//!     .with_source_alias("source")
//!     .with_target_alias("target")
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
use std::time::Instant;

use async_trait::async_trait;
use datafusion::datasource::provider_as_source;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::{QueryPlanner, SessionConfig};
use datafusion::logical_expr::build_join_schema;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::{
    execution::context::SessionState,
    physical_plan::{
        metrics::{MetricBuilder, MetricsSet},
        ExecutionPlan,
    },
    prelude::{DataFrame, SessionContext},
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion_expr::expr::Placeholder;
use datafusion_expr::{col, conditional_expressions::CaseBuilder, lit, when, Expr, JoinType};
use datafusion_expr::{
    BinaryExpr, Distinct, Extension, Filter, LogicalPlan, LogicalPlanBuilder, Operator, Projection,
    UserDefinedLogicalNode, UNNAMED_TABLE,
};
use futures::future::BoxFuture;
use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use serde_json::Value;

use self::barrier::{MergeBarrier, MergeBarrierExec};

use super::datafusion_utils::{into_expr, maybe_into_expr, Expression};
use super::transaction::{commit, PROTOCOL};
use crate::delta_datafusion::expr::{fmt_expr_to_sql, parse_predicate_expression};
use crate::delta_datafusion::logical::MetricObserver;
use crate::delta_datafusion::physical::{find_metric_node, MetricObserverExec};
use crate::delta_datafusion::{
    execute_plan_to_batch, register_store, DeltaColumn, DeltaScanConfigBuilder, DeltaSessionConfig,
    DeltaTableProvider,
};
use crate::kernel::Action;
use crate::logstore::LogStoreRef;
use crate::operations::merge::barrier::find_barrier_node;
use crate::operations::write::write_execution_plan;
use crate::protocol::{DeltaOperation, MergePredicate};
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

mod barrier;

const SOURCE_COLUMN: &str = "__delta_rs_source";
const TARGET_COLUMN: &str = "__delta_rs_target";

const OPERATION_COLUMN: &str = "__delta_rs_operation";
const DELETE_COLUMN: &str = "__delta_rs_delete";
pub(crate) const TARGET_INSERT_COLUMN: &str = "__delta_rs_target_insert";
pub(crate) const TARGET_UPDATE_COLUMN: &str = "__delta_rs_target_update";
pub(crate) const TARGET_DELETE_COLUMN: &str = "__delta_rs_target_delete";
pub(crate) const TARGET_COPY_COLUMN: &str = "__delta_rs_target_copy";

const SOURCE_COUNT_METRIC: &str = "num_source_rows";
const TARGET_COUNT_METRIC: &str = "num_target_rows";
const TARGET_COPY_METRIC: &str = "num_copied_rows";
const TARGET_INSERTED_METRIC: &str = "num_target_inserted_rows";
const TARGET_UPDATED_METRIC: &str = "num_target_updated_rows";
const TARGET_DELETED_METRIC: &str = "num_target_deleted_rows";

const SOURCE_COUNT_ID: &str = "merge_source_count";
const TARGET_COUNT_ID: &str = "merge_target_count";
const OUTPUT_COUNT_ID: &str = "merge_output_count";

/// Merge records into a Delta Table.
pub struct MergeBuilder {
    /// The join predicate
    predicate: Expression,
    /// Operations to perform when a source record and target record match
    match_operations: Vec<MergeOperationConfig>,
    /// Operations to perform on source records when they do not pair with a target record
    not_match_operations: Vec<MergeOperationConfig>,
    /// Operations to perform on target records when they do not pair with a source record
    not_match_source_operations: Vec<MergeOperationConfig>,
    ///Prefix the source columns with a user provided prefix
    source_alias: Option<String>,
    ///Prefix target columns with a user provided prefix
    target_alias: Option<String>,
    /// A snapshot of the table's state. AKA the target table in the operation
    snapshot: DeltaTableState,
    /// The source data
    source: DataFrame,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
    /// safe_cast determines how data types that do not match the underlying table are handled
    /// By default an error is returned
    safe_cast: bool,
}

impl MergeBuilder {
    /// Create a new [`MergeBuilder`]
    pub fn new<E: Into<Expression>>(
        log_store: LogStoreRef,
        snapshot: DeltaTableState,
        predicate: E,
        source: DataFrame,
    ) -> Self {
        let predicate = predicate.into();
        Self {
            predicate,
            source,
            snapshot,
            log_store,
            source_alias: None,
            target_alias: None,
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
    ///     .merge(source, col("target.id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .with_target_alias("target")
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
        let op =
            MergeOperationConfig::new(builder.predicate, builder.updates, OperationType::Update)?;
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
    ///     .merge(source, col("target.id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .with_target_alias("target")
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
        let op = MergeOperationConfig::new(
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
    ///     .merge(source, col("target.id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .with_target_alias("target")
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
        let op = MergeOperationConfig::new(builder.predicate, builder.set, OperationType::Insert)?;
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
    ///     .merge(source, col("target.id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .with_target_alias("target")
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
        let op =
            MergeOperationConfig::new(builder.predicate, builder.updates, OperationType::Update)?;
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
    ///     .merge(source, col("target.id").eq(col("source.id")))
    ///     .with_source_alias("source")
    ///     .with_target_alias("target")
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
        let op = MergeOperationConfig::new(
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

    /// Rename columns in the target dataset to have a prefix of `alias`.`original column name`
    pub fn with_target_alias<S: ToString>(mut self, alias: S) -> Self {
        self.target_alias = Some(alias.to_string());
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
        self.app_metadata = Some(HashMap::from_iter(metadata));
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
    pub fn update<C: Into<DeltaColumn>, E: Into<Expression>>(
        mut self,
        column: C,
        expression: E,
    ) -> Self {
        self.updates.insert(column.into().into(), expression.into());
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
    pub fn set<C: Into<DeltaColumn>, E: Into<Expression>>(
        mut self,
        column: C,
        expression: E,
    ) -> Self {
        self.set.insert(column.into().into(), expression.into());
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

//Encapsute the User's Merge configuration for later processing
struct MergeOperationConfig {
    /// Which records to update
    predicate: Option<Expression>,
    /// How to update columns in a record that match the predicate
    operations: HashMap<Column, Expression>,
    r#type: OperationType,
}

struct MergeOperation {
    /// Which records to update
    predicate: Option<Expr>,
    /// How to update columns in a record that match the predicate
    operations: HashMap<Column, Expr>,
    r#type: OperationType,
}

impl MergeOperation {
    fn try_from(
        config: MergeOperationConfig,
        schema: &DFSchema,
        state: &SessionState,
        target_alias: &Option<String>,
    ) -> DeltaResult<MergeOperation> {
        let mut ops = HashMap::with_capacity(config.operations.capacity());

        for (column, expression) in config.operations.into_iter() {
            // Normalize the column name to contain the target alias. If a table reference was provided ensure it's the target.
            let column = match target_alias {
                Some(alias) => {
                    let r = TableReference::bare(alias.to_owned());
                    match column {
                        Column {
                            relation: None,
                            name,
                        } => Column {
                            relation: Some(r),
                            name,
                        },
                        Column {
                            relation: Some(TableReference::Bare { table }),
                            name,
                        } => {
                            if table.eq(alias) {
                                Column {
                                    relation: Some(r),
                                    name,
                                }
                            } else {
                                return Err(DeltaTableError::Generic(
                                    format!("Table alias '{table}' in column reference '{table}.{name}' unknown. Hint: You must reference the Delta Table with alias '{alias}'.")
                                ));
                            }
                        }
                        _ => {
                            return Err(DeltaTableError::Generic(
                                "Column must reference column in Delta table".into(),
                            ))
                        }
                    }
                }
                None => column,
            };
            ops.insert(column, into_expr(expression, schema, state)?);
        }

        Ok(MergeOperation {
            predicate: maybe_into_expr(config.predicate, schema, state)?,
            operations: ops,
            r#type: config.r#type,
        })
    }
}

impl MergeOperationConfig {
    pub fn new(
        predicate: Option<Expression>,
        operations: HashMap<Column, Expression>,
        r#type: OperationType,
    ) -> DeltaResult<Self> {
        Ok(MergeOperationConfig {
            predicate,
            operations,
            r#type,
        })
    }
}

#[derive(Default, Serialize, Debug)]
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

struct MergeMetricExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for MergeMetricExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
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

            if metric_observer.id.eq(TARGET_COUNT_ID) {
                return Ok(Some(MetricObserverExec::try_new(
                    TARGET_COUNT_ID.into(),
                    physical_inputs,
                    |batch, metrics| {
                        MetricBuilder::new(metrics)
                            .global_counter(TARGET_COUNT_METRIC)
                            .add(batch.num_rows());
                    },
                )?));
            }

            if metric_observer.id.eq(OUTPUT_COUNT_ID) {
                return Ok(Some(MetricObserverExec::try_new(
                    OUTPUT_COUNT_ID.into(),
                    physical_inputs,
                    |batch, metrics| {
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
                    },
                )?));
            }
        }

        if let Some(barrier) = node.as_any().downcast_ref::<MergeBarrier>() {
            let schema = barrier.input.schema();
            return Ok(Some(Arc::new(MergeBarrierExec::new(
                physical_inputs.first().unwrap().clone(),
                barrier.file_column.clone(),
                planner.create_physical_expr(&barrier.expr, schema, session_state)?,
            ))));
        }

        Ok(None)
    }
}

/// Takes the predicate provided and does two things:
///
/// 1. for any relations between a source column and a target column, if the target column is a
/// partition column, then replace source with a placeholder matching the name of the partition
/// columns
///
/// 2. for any other relation with a source column, remove them.
///
/// For example, for the predicate:
///
/// `source.date = target.date and source.id = target.id and frob > 42`
///
/// where `date` is a partition column, would result in the expr:
///
/// `$date = target.date and frob > 42`
///
/// This leaves us with a predicate that we can push into delta scan after expanding it out to
/// a conjunction between the disinct partitions in the source input.
///
/// TODO: A futher improvement here might be for non-partition columns to be replaced with min/max
/// checks, so the above example could become:
///
/// `$date = target.date and target.id between 12345 and 99999 and frob > 42`
fn generalize_filter(
    predicate: Expr,
    partition_columns: &Vec<String>,
    source_name: &TableReference,
    target_name: &TableReference,
    placeholders: &mut HashMap<String, Expr>,
) -> Option<Expr> {
    #[derive(Debug)]
    enum ReferenceTableCheck {
        HasReference(String),
        NoReference,
        Unknown,
    }
    impl ReferenceTableCheck {
        fn has_reference(&self) -> bool {
            matches!(self, ReferenceTableCheck::HasReference(_))
        }
    }
    fn references_table(expr: &Expr, table: &TableReference) -> ReferenceTableCheck {
        let res = match expr {
            Expr::Alias(alias) => references_table(&alias.expr, table),
            Expr::Column(col) => col
                .relation
                .as_ref()
                .map(|rel| {
                    if rel == table {
                        ReferenceTableCheck::HasReference(col.name.to_owned())
                    } else {
                        ReferenceTableCheck::NoReference
                    }
                })
                .unwrap_or(ReferenceTableCheck::NoReference),
            Expr::Negative(neg) => references_table(neg, table),
            Expr::Cast(cast) => references_table(&cast.expr, table),
            Expr::TryCast(try_cast) => references_table(&try_cast.expr, table),
            Expr::ScalarFunction(func) => {
                if func.args.len() == 1 {
                    references_table(&func.args[0], table)
                } else {
                    ReferenceTableCheck::Unknown
                }
            }
            Expr::IsNull(inner) => references_table(inner, table),
            Expr::Literal(_) => ReferenceTableCheck::NoReference,
            _ => ReferenceTableCheck::Unknown,
        };
        res
    }

    match predicate {
        Expr::BinaryExpr(binary) => {
            if references_table(&binary.right, source_name).has_reference() {
                if let ReferenceTableCheck::HasReference(left_target) =
                    references_table(&binary.left, target_name)
                {
                    if partition_columns.contains(&left_target) {
                        let placeholder_name = format!("{left_target}_{}", placeholders.len());

                        let placeholder = Expr::Placeholder(datafusion_expr::expr::Placeholder {
                            id: placeholder_name.clone(),
                            data_type: None,
                        });
                        let replaced = Expr::BinaryExpr(BinaryExpr {
                            left: binary.left,
                            op: binary.op,
                            right: placeholder.into(),
                        });

                        placeholders.insert(placeholder_name, *binary.right);

                        return Some(replaced);
                    }
                }
                return None;
            }
            if references_table(&binary.left, source_name).has_reference() {
                if let ReferenceTableCheck::HasReference(right_target) =
                    references_table(&binary.right, target_name)
                {
                    if partition_columns.contains(&right_target) {
                        let placeholder_name = format!("{right_target}_{}", placeholders.len());

                        let placeholder = Expr::Placeholder(datafusion_expr::expr::Placeholder {
                            id: placeholder_name.clone(),
                            data_type: None,
                        });
                        let replaced = Expr::BinaryExpr(BinaryExpr {
                            right: binary.right,
                            op: binary.op,
                            left: placeholder.into(),
                        });

                        placeholders.insert(placeholder_name, *binary.left);

                        return Some(replaced);
                    }
                }
                return None;
            }

            let left = generalize_filter(
                *binary.left,
                partition_columns,
                source_name,
                target_name,
                placeholders,
            );
            let right = generalize_filter(
                *binary.right,
                partition_columns,
                source_name,
                target_name,
                placeholders,
            );

            match (left, right) {
                (None, None) => None,
                (None, Some(one_side)) | (Some(one_side), None) => {
                    // in the case of an AND clause, it's safe to generalize the filter down to just one side of the AND.
                    // this is because this filter will be more permissive than the actual predicate, so we know that
                    // we will catch all data that could be matched by the predicate. For OR this is not the case - we
                    // could potentially eliminate one side of the predicate and the filter would only match half the
                    // cases that would have satisfied the match predicate.
                    match binary.op {
                        Operator::And => Some(one_side),
                        Operator::Or => None,
                        _ => None,
                    }
                }
                (Some(l), Some(r)) => Expr::BinaryExpr(BinaryExpr {
                    left: l.into(),
                    op: binary.op,
                    right: r.into(),
                })
                .into(),
            }
        }
        other => match references_table(&other, source_name) {
            ReferenceTableCheck::HasReference(col) => {
                let placeholder_name = format!("{col}_{}", placeholders.len());

                let placeholder = Expr::Placeholder(datafusion_expr::expr::Placeholder {
                    id: placeholder_name.clone(),
                    data_type: None,
                });

                placeholders.insert(placeholder_name, other);

                Some(placeholder)
            }
            ReferenceTableCheck::NoReference => Some(other),
            ReferenceTableCheck::Unknown => None,
        },
    }
}

fn replace_placeholders(expr: Expr, placeholders: &HashMap<String, ScalarValue>) -> Expr {
    expr.transform(&|expr| match expr {
        Expr::Placeholder(Placeholder { id, .. }) => {
            let value = placeholders[&id].clone();
            // Replace the placeholder with the value
            Ok(Transformed::Yes(Expr::Literal(value)))
        }
        _ => Ok(Transformed::No(expr)),
    })
    .unwrap()
}

async fn try_construct_early_filter(
    join_predicate: Expr,
    table_snapshot: &DeltaTableState,
    session_state: &SessionState,
    source: &LogicalPlan,
    source_name: &TableReference<'_>,
    target_name: &TableReference<'_>,
) -> DeltaResult<Option<Expr>> {
    let table_metadata = table_snapshot.metadata();
    let partition_columns = &table_metadata.partition_columns;

    if partition_columns.is_empty() {
        return Ok(None);
    }

    let mut placeholders = HashMap::default();

    match generalize_filter(
        join_predicate,
        partition_columns,
        source_name,
        target_name,
        &mut placeholders,
    ) {
        None => Ok(None),
        Some(filter) => {
            if placeholders.is_empty() {
                // if we haven't recognised any partition-based predicates in the join predicate, return our reduced filter
                Ok(Some(filter))
            } else {
                // if we have some recognised partitions, then discover the distinct set of partitions in the source data and
                // make a new filter, which expands out the placeholders for each distinct partition (and then OR these together)
                let distinct_partitions = LogicalPlan::Distinct(Distinct::All(
                    LogicalPlan::Projection(Projection::try_new(
                        placeholders
                            .into_iter()
                            .map(|(alias, expr)| expr.alias(alias))
                            .collect_vec(),
                        source.clone().into(),
                    )?)
                    .into(),
                ));

                let execution_plan = session_state
                    .create_physical_plan(&distinct_partitions)
                    .await?;

                let items = execute_plan_to_batch(session_state, execution_plan).await?;

                let placeholder_names = items
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_owned())
                    .collect_vec();

                let expr = (0..items.num_rows())
                    .map(|i| {
                        let replacements = placeholder_names
                            .iter()
                            .map(|placeholder| {
                                let col = items.column_by_name(placeholder).unwrap();
                                let value = ScalarValue::try_from_array(col, i)?;
                                DeltaResult::Ok((placeholder.to_owned(), value))
                            })
                            .try_collect()?;
                        Ok(replace_placeholders(filter.clone(), &replacements))
                    })
                    .collect::<DeltaResult<Vec<_>>>()?
                    .into_iter()
                    .reduce(Expr::or);

                Ok(expr)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    predicate: Expression,
    source: DataFrame,
    log_store: LogStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    app_metadata: Option<HashMap<String, Value>>,
    safe_cast: bool,
    source_alias: Option<String>,
    target_alias: Option<String>,
    match_operations: Vec<MergeOperationConfig>,
    not_match_target_operations: Vec<MergeOperationConfig>,
    not_match_source_operations: Vec<MergeOperationConfig>,
) -> DeltaResult<((Vec<Action>, i64, Option<DeltaOperation>), MergeMetrics)> {
    let mut metrics = MergeMetrics::default();
    let exec_start = Instant::now();

    let current_metadata = snapshot.metadata();

    // TODO: Given the join predicate, remove any expression that involve the
    // source table and keep expressions that only involve the target table.
    // This would allow us to perform statistics/partition pruning E.G
    // Expression source.id = id and to_dt = '9999-12-31' -Becomes-> to_dt =
    // '9999-12-31'
    //
    // If the user specified any not_source_match operations then those
    // predicates also need to be considered when pruning

    let source_name = match &source_alias {
        Some(alias) => TableReference::bare(alias.to_string()),
        None => TableReference::bare(UNNAMED_TABLE),
    };

    let target_name = match &target_alias {
        Some(alias) => TableReference::bare(alias.to_string()),
        None => TableReference::bare(UNNAMED_TABLE),
    };

    // This is only done to provide the source columns with a correct table reference. Just renaming the columns does not work
    let source = LogicalPlanBuilder::scan(
        source_name.clone(),
        provider_as_source(source.into_view()),
        None,
    )?
    .build()?;

    let source = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: SOURCE_COUNT_ID.into(),
            input: source,
            enable_pushdown: false,
        }),
    });

    let scan_config = DeltaScanConfigBuilder::default()
        .with_file_column(true)
        .build(snapshot)?;

    let file_column = Arc::new(scan_config.file_column_name.clone().unwrap());

    let target_provider = Arc::new(DeltaTableProvider::try_new(
        snapshot.clone(),
        log_store.clone(),
        scan_config,
    )?);

    let target_provider = provider_as_source(target_provider);

    let target = LogicalPlanBuilder::scan(target_name.clone(), target_provider, None)?.build()?;

    let source_schema = source.schema();
    let target_schema = target.schema();
    let join_schema_df = build_join_schema(source_schema, target_schema, &JoinType::Full)?;
    let predicate = match predicate {
        Expression::DataFusion(expr) => expr,
        Expression::String(s) => parse_predicate_expression(&join_schema_df, s, &state)?,
    };

    let state = state.with_query_planner(Arc::new(MergePlanner {}));

    let target = {
        // Attempt to construct an early filter that we can apply to the Add action list and the delta scan.
        // In the case where there are partition columns in the join predicate, we can scan the source table
        // to get the distinct list of partitions affected and constrain the search to those.

        if !not_match_source_operations.is_empty() {
            // It's only worth trying to create an early filter where there are no `when_not_matched_source` operators, since
            // that implies a full scan
            target
        } else if let Some(filter) = try_construct_early_filter(
            predicate.clone(),
            snapshot,
            &state,
            &source,
            &source_name,
            &target_name,
        )
        .await?
        {
            LogicalPlan::Filter(Filter::try_new(filter, target.into())?)
        } else {
            target
        }
    };

    let source = DataFrame::new(state.clone(), source);
    let source = source.with_column(SOURCE_COLUMN, lit(true))?;

    // Not match operations imply a full scan of the target table is required
    let enable_pushdown =
        not_match_source_operations.is_empty() && not_match_target_operations.is_empty();
    let target = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: TARGET_COUNT_ID.into(),
            input: target,
            enable_pushdown,
        }),
    });
    let target = DataFrame::new(state.clone(), target);
    let target = target.with_column(TARGET_COLUMN, lit(true))?;

    let join = source.join(target, JoinType::Full, &[], &[], Some(predicate.clone()))?;
    let join_schema_df = join.schema().to_owned();

    let match_operations: Vec<MergeOperation> = match_operations
        .into_iter()
        .map(|op| MergeOperation::try_from(op, &join_schema_df, &state, &target_alias))
        .collect::<Result<Vec<MergeOperation>, DeltaTableError>>()?;

    let not_match_target_operations: Vec<MergeOperation> = not_match_target_operations
        .into_iter()
        .map(|op| MergeOperation::try_from(op, &join_schema_df, &state, &target_alias))
        .collect::<Result<Vec<MergeOperation>, DeltaTableError>>()?;

    let not_match_source_operations: Vec<MergeOperation> = not_match_source_operations
        .into_iter()
        .map(|op| MergeOperation::try_from(op, &join_schema_df, &state, &target_alias))
        .collect::<Result<Vec<MergeOperation>, DeltaTableError>>()?;

    let matched = col(SOURCE_COLUMN)
        .is_true()
        .and(col(TARGET_COLUMN).is_true());
    let not_matched_target = col(SOURCE_COLUMN)
        .is_true()
        .and(col(TARGET_COLUMN).is_null());
    let not_matched_source = col(SOURCE_COLUMN)
        .is_null()
        .and(col(TARGET_COLUMN))
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
            let predicate = op
                .predicate
                .map(|expr| fmt_expr_to_sql(&expr))
                .transpose()?;

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

    let projection = join.with_column(OPERATION_COLUMN, case)?;

    let mut new_columns = vec![];
    let mut write_projection = Vec::new();

    for delta_field in snapshot.schema().fields() {
        let mut when_expr = Vec::with_capacity(operations_size);
        let mut then_expr = Vec::with_capacity(operations_size);

        let qualifier = match &target_alias {
            Some(alias) => Some(TableReference::Bare {
                table: alias.to_owned().into(),
            }),
            None => TableReference::none(),
        };
        let name = delta_field.name();
        let column = Column::new(qualifier.clone(), name);

        for (idx, (operations, _)) in ops.iter().enumerate() {
            let op = operations
                .get(&column)
                .map(|expr| expr.to_owned())
                .unwrap_or_else(|| col(column.clone()));

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

        let name = "__delta_rs_c_".to_owned() + delta_field.name();
        write_projection.push(
            Expr::Column(Column::from_qualified_name_ignore_case(name.clone()))
                .alias(delta_field.name()),
        );
        new_columns.push((name, case));
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

    fn build_case(when: Vec<Expr>, then: Vec<Expr>) -> DataFusionResult<Expr> {
        CaseBuilder::new(
            Some(Box::new(col(OPERATION_COLUMN))),
            when,
            then,
            Some(Box::new(lit(false))),
        )
        .end()
    }

    new_columns.push((
        DELETE_COLUMN.to_owned(),
        build_case(delete_when, delete_then)?,
    ));
    new_columns.push((
        TARGET_INSERT_COLUMN.to_owned(),
        build_case(insert_when, insert_then)?,
    ));
    new_columns.push((
        TARGET_UPDATE_COLUMN.to_owned(),
        build_case(update_when, update_then)?,
    ));
    new_columns.push((
        TARGET_DELETE_COLUMN.to_owned(),
        build_case(target_delete_when, target_delete_then)?,
    ));
    new_columns.push((
        TARGET_COPY_COLUMN.to_owned(),
        build_case(copy_when, copy_then)?,
    ));

    let new_columns = {
        let plan = projection.into_unoptimized_plan();
        let mut fields: Vec<Expr> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| col(f.qualified_column()))
            .collect();

        fields.extend(new_columns.into_iter().map(|(name, ex)| ex.alias(name)));

        LogicalPlanBuilder::from(plan).project(fields)?.build()?
    };

    let distrbute_expr = col(file_column.as_str());

    let merge_barrier = LogicalPlan::Extension(Extension {
        node: Arc::new(MergeBarrier {
            input: new_columns,
            expr: distrbute_expr,
            file_column,
        }),
    });

    let operation_count = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: OUTPUT_COUNT_ID.into(),
            input: merge_barrier,
            enable_pushdown: false,
        }),
    });

    let operation_count = DataFrame::new(state.clone(), operation_count);
    let filtered = operation_count.filter(col(DELETE_COLUMN).is_false())?;

    let project = filtered.select(write_projection)?;
    let merge_final = &project.into_unoptimized_plan();

    let write = state.create_physical_plan(merge_final).await?;

    let err = || DeltaTableError::Generic("Unable to locate expected metric node".into());
    let source_count = find_metric_node(SOURCE_COUNT_ID, &write).ok_or_else(err)?;
    let op_count = find_metric_node(OUTPUT_COUNT_ID, &write).ok_or_else(err)?;
    let barrier = find_barrier_node(&write).ok_or_else(err)?;

    // write projected records
    let table_partition_cols = current_metadata.partition_columns.clone();

    let rewrite_start = Instant::now();
    let add_actions = write_execution_plan(
        Some(snapshot),
        state.clone(),
        write,
        table_partition_cols.clone(),
        log_store.object_store(),
        Some(snapshot.table_config().target_file_size() as usize),
        None,
        writer_properties,
        safe_cast,
        None,
    )
    .await?;

    metrics.rewrite_time_ms = Instant::now().duration_since(rewrite_start).as_millis() as u64;

    let mut actions: Vec<Action> = add_actions.clone();
    metrics.num_target_files_added = actions.len();

    let survivors = barrier
        .as_any()
        .downcast_ref::<MergeBarrierExec>()
        .unwrap()
        .survivors();

    {
        let lock = survivors.lock().unwrap();
        for action in snapshot.log_data() {
            if lock.contains(action.path().as_ref()) {
                metrics.num_target_files_removed += 1;
                actions.push(action.remove_action(true).into());
            }
        }
    }

    let mut version = snapshot.version();

    let source_count_metrics = source_count.metrics().unwrap();
    let target_count_metrics = op_count.metrics().unwrap();
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

    let mut app_metadata = match app_metadata {
        Some(meta) => meta,
        None => HashMap::new(),
    };

    app_metadata.insert("readVersion".to_owned(), snapshot.version().into());

    if let Ok(map) = serde_json::to_value(&metrics) {
        app_metadata.insert("operationMetrics".to_owned(), map);
    }

    // Do not make a commit when there are zero updates to the state
    let operation = DeltaOperation::Merge {
        predicate: Some(fmt_expr_to_sql(&predicate)?),
        matched_predicates: match_operations,
        not_matched_predicates: not_match_target_operations,
        not_matched_by_source_predicates: not_match_source_operations,
    };
    if !actions.is_empty() {
        version = commit(
            log_store.as_ref(),
            &actions,
            operation.clone(),
            Some(snapshot),
            Some(app_metadata),
        )
        .await?;
    }
    let op = (!actions.is_empty()).then_some(operation);
    Ok(((actions, version, op), metrics))
}

// TODO: Abstract MergePlanner into DeltaPlanner to support other delta operations in the future.
struct MergePlanner {}

#[async_trait]
impl QueryPlanner for MergePlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let planner = Arc::new(Box::new(DefaultPhysicalPlanner::with_extension_planners(
            vec![Arc::new(MergeMetricExtensionPlanner {})],
        )));
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

impl std::future::IntoFuture for MergeBuilder {
    type Output = DeltaResult<(DeltaTable, MergeMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            PROTOCOL.can_write_to(&this.snapshot)?;

            let state = this.state.unwrap_or_else(|| {
                let config: SessionConfig = DeltaSessionConfig::default().into();
                let session = SessionContext::new_with_config(config);

                // If a user provides their own their DF state then they must register the store themselves
                register_store(this.log_store.clone(), session.runtime_env());

                session.state()
            });

            let ((actions, version, operation), metrics) = execute(
                this.predicate,
                this.source,
                this.log_store.clone(),
                &this.snapshot,
                state,
                this.writer_properties,
                this.app_metadata,
                this.safe_cast,
                this.source_alias,
                this.target_alias,
                this.match_operations,
                this.not_match_operations,
                this.not_match_source_operations,
            )
            .await?;

            if let Some(op) = &operation {
                this.snapshot.merge(actions, op, version)?;
            }
            let table = DeltaTable::new_with_state(this.log_store, this.snapshot);

            Ok((table, metrics))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::DataType;
    use crate::kernel::PrimitiveType;
    use crate::kernel::StructField;
    use crate::operations::merge::generalize_filter;
    use crate::operations::merge::try_construct_early_filter;
    use crate::operations::DeltaOps;
    use crate::protocol::*;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::get_arrow_schema;
    use crate::writer::test_utils::get_delta_schema;
    use crate::writer::test_utils::setup_table_with_configuration;
    use crate::DeltaConfigKey;
    use crate::DeltaTable;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::datasource::provider_as_source;
    use datafusion::prelude::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_common::Column;
    use datafusion_common::ScalarValue;
    use datafusion_common::TableReference;
    use datafusion_expr::col;
    use datafusion_expr::expr::Placeholder;
    use datafusion_expr::lit;
    use datafusion_expr::Expr;
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_expr::Operator;
    use itertools::Itertools;
    use serde_json::json;
    use std::collections::HashMap;
    use std::ops::Neg;
    use std::sync::Arc;

    use super::MergeMetrics;

    async fn setup_table(partitions: Option<Vec<&str>>) -> DeltaTable {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().clone())
            .with_partition_columns(partitions.unwrap_or_default())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        table
    }

    // TODO(ion): property keys are not passed through or translated as table features.. fix this as well
    #[tokio::test]
    async fn test_merge_when_delta_table_is_append_only() {
        let schema = get_arrow_schema(&None);
        let table = setup_table_with_configuration(DeltaConfigKey::AppendOnly, Some("true")).await;
        // append some data
        let table = write_data(table, &schema).await;
        // merge
        let _err = DeltaOps(table)
            .merge(merge_source(schema), col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_by_source_delete(|delete| delete)
            .unwrap()
            .await
            .expect_err("Remove action is included when Delta table is append-only. Should error");
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

    fn merge_source(schema: Arc<ArrowSchema>) -> DataFrame {
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
        ctx.read_batch(batch).unwrap()
    }

    async fn setup() -> (DeltaTable, DataFrame) {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        (table, merge_source(schema))
    }

    async fn assert_merge(table: DeltaTable, metrics: MergeMetrics) {
        assert_eq!(table.version(), 2);
        assert!(table.get_files_count() >= 1);
        assert!(metrics.num_target_files_added >= 1);
        assert_eq!(metrics.num_target_files_removed, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_target_rows_updated, 3);
        assert_eq!(metrics.num_target_rows_inserted, 1);
        assert_eq!(metrics.num_target_rows_deleted, 0);
        assert_eq!(metrics.num_output_rows, 5);
        assert_eq!(metrics.num_source_rows, 3);

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
    async fn test_merge() {
        let (table, source) = setup().await;

        let (table, metrics) = DeltaOps(table)
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("value", col("source.value"))
                    .update("modified", col("source.modified"))
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate(col("target.value").eq(lit(1)))
                    .update("value", col("target.value") + lit(1))
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

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("target.id = source.id"));
        assert_eq!(
            parameters["matchedPredicates"],
            json!(r#"[{"actionType":"update"}]"#)
        );
        assert_eq!(
            parameters["notMatchedPredicates"],
            json!(r#"[{"actionType":"insert"}]"#)
        );
        assert_eq!(
            parameters["notMatchedBySourcePredicates"],
            json!(r#"[{"actionType":"update","predicate":"target.value = 1"}]"#)
        );

        assert_merge(table, metrics).await;
    }

    #[tokio::test]
    async fn test_merge_str() {
        // Validate that users can use string predicates
        // Also validates that update and set operations can contain the target alias
        let (table, source) = setup().await;

        let (table, metrics) = DeltaOps(table)
            .merge(source, "target.id = source.id")
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("target.value", "source.value")
                    .update("modified", "source.modified")
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate("target.value = 1")
                    .update("value", "target.value + cast(1 as int)")
            })
            .unwrap()
            .when_not_matched_insert(|insert| {
                insert
                    .set("target.id", "source.id")
                    .set("value", "source.value")
                    .set("modified", "source.modified")
            })
            .unwrap()
            .await
            .unwrap();

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("target.id = source.id"));
        assert_eq!(
            parameters["matchedPredicates"],
            json!(r#"[{"actionType":"update"}]"#)
        );
        assert_eq!(
            parameters["notMatchedPredicates"],
            json!(r#"[{"actionType":"insert"}]"#)
        );
        assert_eq!(
            parameters["notMatchedBySourcePredicates"],
            json!(r#"[{"actionType":"update","predicate":"target.value = 1"}]"#)
        );

        assert_merge(table, metrics).await;
    }

    #[tokio::test]
    async fn test_merge_no_alias() {
        // Validate merge can be used without specifying an alias
        let (table, source) = setup().await;

        let source = source
            .with_column_renamed("id", "source_id")
            .unwrap()
            .with_column_renamed("value", "source_value")
            .unwrap()
            .with_column_renamed("modified", "source_modified")
            .unwrap();

        let (table, metrics) = DeltaOps(table)
            .merge(source, "id = source_id")
            .when_matched_update(|update| {
                update
                    .update("value", "source_value")
                    .update("modified", "source_modified")
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update.predicate("value = 1").update("value", "value + 1")
            })
            .unwrap()
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", "source_id")
                    .set("value", "source_value")
                    .set("modified", "source_modified")
            })
            .unwrap()
            .await
            .unwrap();

        assert_merge(table, metrics).await;
    }

    #[tokio::test]
    async fn test_merge_with_alias_mix() {
        // Validate merge can be used with an alias and unambiguous column references
        // I.E users should be able to specify an alias and still reference columns without using that alias when there is no ambiguity
        let (table, source) = setup().await;

        let source = source
            .with_column_renamed("id", "source_id")
            .unwrap()
            .with_column_renamed("value", "source_value")
            .unwrap()
            .with_column_renamed("modified", "source_modified")
            .unwrap();

        let (table, metrics) = DeltaOps(table)
            .merge(source, "id = source_id")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("value", "source_value")
                    .update("modified", "source_modified")
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate("value = 1")
                    .update("value", "target.value + 1")
            })
            .unwrap()
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", "source_id")
                    .set("target.value", "source_value")
                    .set("modified", "source_modified")
            })
            .unwrap()
            .await
            .unwrap();

        assert_merge(table, metrics).await;
    }

    #[tokio::test]
    async fn test_merge_failures() {
        // Validate target columns cannot be from the source
        let (table, source) = setup().await;
        let res = DeltaOps(table)
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("source.value", "source.value")
                    .update("modified", "source.modified")
            })
            .unwrap()
            .await;
        assert!(res.is_err());

        // Validate failure when aliases are the same
        let (table, source) = setup().await;
        let res = DeltaOps(table)
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("source")
            .when_matched_update(|update| {
                update
                    .update("target.value", "source.value")
                    .update("modified", "source.modified")
            })
            .unwrap()
            .await;
        assert!(res.is_err())
    }

    #[tokio::test]
    async fn test_merge_partitions() {
        /* Validate the join predicate works with table partitions */
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 2);

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
                col("target.id")
                    .eq(col("source.id"))
                    .and(col("target.modified").eq(lit("2021-02-02"))),
            )
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("value", col("source.value"))
                    .update("modified", col("source.modified"))
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate(col("target.value").eq(lit(1)))
                    .update("value", col("target.value") + lit(1))
            })
            .unwrap()
            .when_not_matched_by_source_update(|update| {
                update
                    .predicate(col("target.modified").eq(lit("2021-02-01")))
                    .update("value", col("target.value") - lit(1))
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
        assert!(table.get_files_count() >= 3);
        assert!(metrics.num_target_files_added >= 3);
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
    async fn test_merge_partitions_skipping() {
        /* Validate the join predicate can be used for skipping partitions */
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["id"])).await;

        let table = write_data(table, &schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 4);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![999, 999, 999])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2023-07-04",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let (table, metrics) = DeltaOps(table)
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("value", col("source.value"))
                    .update("modified", col("source.modified"))
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
        assert!(table.get_files_count() >= 3);
        assert_eq!(metrics.num_target_files_added, 3);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 0);
        assert_eq!(metrics.num_target_rows_updated, 2);
        assert_eq!(metrics.num_target_rows_inserted, 1);
        assert_eq!(metrics.num_target_rows_deleted, 0);
        assert_eq!(metrics.num_output_rows, 3);
        assert_eq!(metrics.num_source_rows, 3);

        let expected = vec![
            "+-------+------------+----+",
            "| value | modified   | id |",
            "+-------+------------+----+",
            "| 1     | 2021-02-01 | A  |",
            "| 100   | 2021-02-02 | D  |",
            "| 999   | 2023-07-04 | B  |",
            "| 999   | 2023-07-04 | C  |",
            "| 999   | 2023-07-04 | X  |",
            "+-------+------------+----+",
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
        assert_eq!(table.get_files_count(), 2);

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
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_delete(|delete| delete)
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert!(table.get_files_count() >= 2);
        assert_eq!(metrics.num_target_files_added, 2);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 2);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 2);
        assert_eq!(metrics.num_output_rows, 2);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        let extra_info = last_commit.info.clone();
        assert_eq!(
            extra_info["operationMetrics"],
            serde_json::to_value(&metrics).unwrap()
        );
        assert_eq!(parameters["predicate"], json!("target.id = source.id"));
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
        assert_eq!(table.get_files_count(), 2);

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
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_delete(|delete| delete.predicate(col("source.value").lt_eq(lit(10))))
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert!(table.get_files_count() >= 2);
        assert_eq!(metrics.num_target_files_added, 1);
        assert_eq!(metrics.num_target_files_removed, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 1);
        assert_eq!(metrics.num_output_rows, 1);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("target.id = source.id"));
        assert_eq!(
            parameters["matchedPredicates"],
            json!(r#"[{"actionType":"delete","predicate":"source.value <= 10"}]"#)
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
        assert_eq!(table.get_files_count(), 2);

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
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_by_source_delete(|delete| delete)
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 2);
        assert_eq!(metrics.num_target_files_added, 2);
        assert_eq!(metrics.num_target_files_removed, 2);
        assert_eq!(metrics.num_target_rows_copied, 2);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 2);
        assert_eq!(metrics.num_output_rows, 2);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("target.id = source.id"));
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
        assert_eq!(table.get_files_count(), 2);

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
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_by_source_delete(|delete| {
                delete.predicate(col("target.modified").gt(lit("2021-02-01")))
            })
            .unwrap()
            .await
            .unwrap();

        assert_eq!(table.version(), 2);
        assert!(metrics.num_target_files_added == 1);
        assert_eq!(metrics.num_target_files_removed, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 0);
        assert_eq!(metrics.num_target_rows_deleted, 1);
        assert_eq!(metrics.num_output_rows, 1);
        assert_eq!(metrics.num_source_rows, 3);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("target.id = source.id"));
        assert_eq!(
            parameters["notMatchedBySourcePredicates"],
            json!(r#"[{"actionType":"delete","predicate":"target.modified > '2021-02-01'"}]"#)
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

    #[tokio::test]
    async fn test_merge_empty_table() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

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
                col("target.id")
                    .eq(col("source.id"))
                    .and(col("target.modified").eq(lit("2021-02-02"))),
            )
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("value", col("source.value"))
                    .update("modified", col("source.modified"))
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

        assert_eq!(table.version(), 1);
        assert!(table.get_files_count() >= 2);
        assert!(metrics.num_target_files_added >= 2);
        assert_eq!(metrics.num_target_files_removed, 0);
        assert_eq!(metrics.num_target_rows_copied, 0);
        assert_eq!(metrics.num_target_rows_updated, 0);
        assert_eq!(metrics.num_target_rows_inserted, 3);
        assert_eq!(metrics.num_target_rows_deleted, 0);
        assert_eq!(metrics.num_output_rows, 3);
        assert_eq!(metrics.num_source_rows, 3);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| B  | 10    | 2021-02-02 |",
            "| C  | 20    | 2023-07-04 |",
            "| X  | 30    | 2023-07-04 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_merge_case_sensitive() {
        let schema = vec![
            StructField::new(
                "Id".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "vAlue".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "mOdifieD".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
        ];

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("Id", ArrowDataType::Utf8, true),
            Field::new("vAlue", ArrowDataType::Int32, true),
            Field::new("mOdifieD", ArrowDataType::Utf8, true),
        ]));

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(schema)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema.clone()),
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

        let table = write_data(table, &arrow_schema).await;
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        let (table, _metrics) = DeltaOps(table)
            .merge(source, "target.Id = source.Id")
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_insert(|insert| {
                insert
                    .set("Id", "source.Id")
                    .set("vAlue", "source.vAlue + 1")
                    .set("mOdifieD", "source.mOdifieD")
            })
            .unwrap()
            .await
            .unwrap();

        let expected = vec![
            "+----+-------+------------+",
            "| Id | vAlue | mOdifieD   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-01 |",
            "| B  | 10    | 2021-02-01 |",
            "| C  | 10    | 2021-02-02 |",
            "| D  | 100   | 2021-02-02 |",
            "| X  | 31    | 2023-07-04 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_generalize_filter_with_partitions() {
        let source = TableReference::parse_str("source");
        let target = TableReference::parse_str("target");

        let parsed_filter = col(Column::new(source.clone().into(), "id"))
            .eq(col(Column::new(target.clone().into(), "id")));

        let mut placeholders = HashMap::default();

        let generalized = generalize_filter(
            parsed_filter,
            &vec!["id".to_owned()],
            &source,
            &target,
            &mut placeholders,
        )
        .unwrap();

        let expected_filter = Expr::Placeholder(Placeholder {
            id: "id_0".to_owned(),
            data_type: None,
        })
        .eq(col(Column::new(target.clone().into(), "id")));

        assert_eq!(generalized, expected_filter);
    }

    #[tokio::test]
    async fn test_generalize_filter_with_partitions_nulls() {
        let source = TableReference::parse_str("source");
        let target = TableReference::parse_str("target");

        let source_id = col(Column::new(source.clone().into(), "id"));
        let target_id = col(Column::new(target.clone().into(), "id"));

        // source.id = target.id OR (source.id is null and target.id is null)
        let parsed_filter = (source_id.clone().eq(target_id.clone()))
            .or(source_id.clone().is_null().and(target_id.clone().is_null()));

        let mut placeholders = HashMap::default();

        let generalized = generalize_filter(
            parsed_filter,
            &vec!["id".to_owned()],
            &source,
            &target,
            &mut placeholders,
        )
        .unwrap();

        // id_1 = target.id OR (id_2 and target.id is null)
        let expected_filter = Expr::Placeholder(Placeholder {
            id: "id_0".to_owned(),
            data_type: None,
        })
        .eq(target_id.clone())
        .or(Expr::Placeholder(Placeholder {
            id: "id_1".to_owned(),
            data_type: None,
        })
        .and(target_id.clone().is_null()));

        assert!(placeholders.len() == 2);

        let captured_expressions = placeholders.values().collect_vec();

        assert!(captured_expressions.contains(&&source_id));
        assert!(captured_expressions.contains(&&source_id.is_null()));

        assert_eq!(generalized, expected_filter);
    }

    #[tokio::test]
    async fn test_generalize_filter_with_partitions_captures_expression() {
        // Check that when generalizing the filter, the placeholder map captures the expression needed to make the statement the same
        // when the distinct values are substitiuted in
        let source = TableReference::parse_str("source");
        let target = TableReference::parse_str("target");

        let parsed_filter = col(Column::new(source.clone().into(), "id"))
            .neg()
            .eq(col(Column::new(target.clone().into(), "id")));

        let mut placeholders = HashMap::default();

        let generalized = generalize_filter(
            parsed_filter,
            &vec!["id".to_owned()],
            &source,
            &target,
            &mut placeholders,
        )
        .unwrap();

        let expected_filter = Expr::Placeholder(Placeholder {
            id: "id_0".to_owned(),
            data_type: None,
        })
        .eq(col(Column::new(target.clone().into(), "id")));

        assert_eq!(generalized, expected_filter);

        assert_eq!(placeholders.len(), 1);

        let placeholder_expr = &placeholders["id_0"];

        let expected_placeholder = col(Column::new(source.clone().into(), "id")).neg();

        assert_eq!(placeholder_expr, &expected_placeholder);
    }

    #[tokio::test]
    async fn test_generalize_filter_keeps_static_target_references() {
        let source = TableReference::parse_str("source");
        let target = TableReference::parse_str("target");

        // source.id = target.id and target.id = 'C'
        let parsed_filter = col(Column::new(source.clone().into(), "id"))
            .eq(col(Column::new(target.clone().into(), "id")))
            .and(col(Column::new(target.clone().into(), "id")).eq(lit("C")));

        let mut placeholders = HashMap::default();

        let generalized = generalize_filter(
            parsed_filter,
            &vec!["id".to_owned()],
            &source,
            &target,
            &mut placeholders,
        )
        .unwrap();

        // id_0 = target.id and target.id = 'C'
        let expected_filter = Expr::Placeholder(Placeholder {
            id: "id_0".to_owned(),
            data_type: None,
        })
        .eq(col(Column::new(target.clone().into(), "id")))
        .and(col(Column::new(target.clone().into(), "id")).eq(lit("C")));

        assert_eq!(generalized, expected_filter);
    }

    #[tokio::test]
    async fn test_generalize_filter_removes_source_references() {
        let source = TableReference::parse_str("source");
        let target = TableReference::parse_str("target");

        let parsed_filter = col(Column::new(source.clone().into(), "id"))
            .eq(col(Column::new(target.clone().into(), "id")))
            .and(col(Column::new(source.clone().into(), "id")).eq(lit("C")));

        let mut placeholders = HashMap::default();

        let generalized = generalize_filter(
            parsed_filter,
            &vec!["id".to_owned()],
            &source,
            &target,
            &mut placeholders,
        )
        .unwrap();

        let expected_filter = Expr::Placeholder(Placeholder {
            id: "id_0".to_owned(),
            data_type: None,
        })
        .eq(col(Column::new(target.clone().into(), "id")));

        assert_eq!(generalized, expected_filter);
    }

    #[tokio::test]
    async fn test_try_construct_early_filter_with_partitions_expands() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["id"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

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

        let source_name = TableReference::parse_str("source");
        let target_name = TableReference::parse_str("target");

        let source = LogicalPlanBuilder::scan(
            source_name.clone(),
            provider_as_source(source.into_view()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_predicate = col(Column {
            relation: Some(source_name.clone()),
            name: "id".to_owned(),
        })
        .eq(col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        }));

        let pred = try_construct_early_filter(
            join_predicate,
            table.snapshot().unwrap(),
            &ctx.state(),
            &source,
            &source_name,
            &target_name,
        )
        .await
        .unwrap();

        assert!(pred.is_some());

        let split_pred = {
            fn split(expr: Expr, parts: &mut Vec<(String, String)>) {
                match expr {
                    Expr::BinaryExpr(ex) if ex.op == Operator::Or => {
                        split(*ex.left, parts);
                        split(*ex.right, parts);
                    }
                    Expr::BinaryExpr(ex) if ex.op == Operator::Eq => {
                        let col = match *ex.right {
                            Expr::Column(col) => col.name,
                            ex => panic!("expected column in pred, got {ex}!"),
                        };

                        let value = match *ex.left {
                            Expr::Literal(ScalarValue::Utf8(Some(value))) => value,
                            ex => panic!("expected value in predicate, got {ex}!"),
                        };

                        parts.push((col, value))
                    }

                    expr => panic!("expected either = or OR, got {expr}"),
                }
            }

            let mut parts = vec![];
            split(pred.unwrap(), &mut parts);
            parts.sort();
            parts
        };

        let expected_pred_parts = [
            ("id".to_owned(), "B".to_owned()),
            ("id".to_owned(), "C".to_owned()),
            ("id".to_owned(), "X".to_owned()),
        ];

        assert_eq!(split_pred, expected_pred_parts);
    }
}
