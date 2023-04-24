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

use crate::action::DeltaOperation;
use crate::delta::DeltaResult;
use crate::delta_datafusion::logical_expr_to_physical_expr;
use crate::delta_datafusion::parquet_scan_from_actions;
use crate::delta_datafusion::partitioned_file_from_action;
use crate::delta_datafusion::register_store;
use crate::operations::transaction::commit;
use crate::operations::write::write_execution_plan;
use crate::storage::DeltaObjectStore;
use crate::storage::ObjectStoreRef;
use crate::table_state::DeltaTableState;
use crate::DeltaTable;
use crate::DeltaTableError;

use crate::action::{Action, Add, Remove};
use arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::DFSchema;
use datafusion_expr::Volatility;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use parquet::file::properties::WriterProperties;
use serde_json::Map;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Delete Records from the Delta Table.
/// See this module's documentaiton for more information
pub struct DeleteBuilder {
    /// Which records to delete
    predicate: Option<Expr>,
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    store: Arc<DeltaObjectStore>,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Default, Debug)]
/// Metrics for the Delete Operation
pub struct DeleteMetrics {
    /// Number of files added
    pub num_added_files: usize,
    /// Number of files removed
    pub num_removed_files: usize,
    /// Number of rows removed
    pub num_deleted_rows: Option<usize>,
    /// Number of rows copied in the process of deleting files
    pub num_copied_rows: Option<usize>,
    /// Time taken to execute the entire operation
    pub execution_time_ms: u128,
    /// Time taken to scan the file for matches
    pub scan_time_ms: u128,
    /// Time taken to rewrite the matched files
    pub rewrite_time_ms: u128,
}

/// Determine which files contain a record that statisfies the predicate
async fn find_files<'a>(
    snapshot: &DeltaTableState,
    store: ObjectStoreRef,
    schema: &ArrowSchema,
    file_schema: Arc<ArrowSchema>,
    candidates: Vec<&'a Add>,
    state: &SessionState,
    expression: &Expr,
    task_ctx: Arc<TaskContext>,
) -> DeltaResult<Vec<&'a Add>> {
    // This solution is temporary until Datafusion can expose which path a file came from
    let mut files = Vec::new();
    for action in candidates {
        let mut file_group: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
        let part = partitioned_file_from_action(action, &schema);
        file_group
            .entry(part.partition_values.clone())
            .or_default()
            .push(part);

        let parquet_scan = parquet_scan_from_actions(
            snapshot,
            store.clone(),
            &[action.to_owned()],
            &schema,
            None,
            &state,
            None,
            None,
        )
        .await?;

        let physical_schema = file_schema.clone();
        let logical_schema: DFSchema = file_schema.as_ref().clone().try_into()?;
        let execution_props = ExecutionProps::new();

        let predicate_expr = create_physical_expr(
            expression,
            &logical_schema,
            &physical_schema,
            &execution_props,
        )?;
        let filter = Arc::new(FilterExec::try_new(predicate_expr, parquet_scan)?);
        let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(filter, 0, Some(1)));

        for i in 0..limit.output_partitioning().partition_count() {
            let mut stream = limit.execute(i, task_ctx.clone())?;
            if let Some(_) = stream.next().await {
                files.push(action);
                break;
            }
        }
    }
    return Ok(files);
}

struct ExprProperties {
    partition_only: bool,
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
fn validate_expr(
    expr: &Expr,
    partition_columns: &Vec<String>,
    properties: &mut ExprProperties,
) -> DeltaResult<()> {
    // TODO: We can likely relax the volatility to STABLE. Would require further
    // research to confirm the same value is generated during the scan and
    // rewrite phases.
    match expr {
        Expr::ScalarVariable(_, _) | Expr::Literal(_) => (),
        Expr::Alias(expr, _) => validate_expr(expr, partition_columns, properties)?,
        Expr::Column(c) => {
            if !partition_columns.contains(&c.name) {
                properties.partition_only = false;
            }
            ()
        }
        Expr::BinaryExpr(bin) => {
            validate_expr(&bin.left, partition_columns, properties)?;
            validate_expr(&bin.right, partition_columns, properties)?;
        }
        Expr::Like(like) => {
            validate_expr(&like.expr, partition_columns, properties)?;
        }
        Expr::ILike(like) => {
            validate_expr(&like.expr, partition_columns, properties)?;
        }
        Expr::SimilarTo(like) => {
            validate_expr(&like.expr, partition_columns, properties)?;
        }
        Expr::Not(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Negative(expr) => validate_expr(expr, partition_columns, properties)?,
        Expr::GetIndexedField(index_field) => {
            validate_expr(&index_field.expr, partition_columns, properties)?
        }
        Expr::Between(between) => {
            validate_expr(&between.expr, partition_columns, properties)?;
            validate_expr(&between.low, partition_columns, properties)?;
            validate_expr(&between.high, partition_columns, properties)?;
        }
        Expr::Case(case) => {
            if let Some(expr) = &case.expr {
                validate_expr(expr, partition_columns, properties)?;
            }

            if let Some(expr) = &case.else_expr {
                validate_expr(expr, partition_columns, properties)?;
            }

            for (when, then) in &case.when_then_expr {
                validate_expr(when, partition_columns, properties)?;
                validate_expr(then, partition_columns, properties)?;
            }

            ()
        }
        Expr::Cast(cast) => validate_expr(&cast.expr, partition_columns, properties)?,
        Expr::TryCast(try_cast) => validate_expr(&try_cast.expr, partition_columns, properties)?,
        Expr::Sort(_) => {
            return Err(DeltaTableError::Generic(
                "Sort expression is not allowed Delete operation".to_string(),
            ))
        }
        Expr::ScalarFunction { fun, args } => match fun.volatility() {
            Volatility::Immutable => {
                for e in args {
                    validate_expr(e, partition_columns, properties)?;
                }
                ()
            }
            _ => {
                return Err(DeltaTableError::Generic(
                    "Nondeterministic functions are not allowed in delete expression".to_string(),
                ))
            }
        },
        Expr::ScalarUDF { fun, args } => match fun.signature.volatility {
            Volatility::Immutable => {
                for e in args {
                    validate_expr(e, partition_columns, properties)?;
                }
                ()
            }
            _ => {
                return Err(DeltaTableError::Generic(
                    "Nondeterministic functions are not allowed in delete expression".to_string(),
                ))
            }
        },
        Expr::AggregateFunction(_) => {
            return Err(DeltaTableError::Generic(
                "Aggregate Function is not allowed in delete expression".to_string(),
            ))
        }
        Expr::WindowFunction(_) => {
            return Err(DeltaTableError::Generic(
                "Window Function is not allowed in delete expression".to_string(),
            ))
        }
        Expr::AggregateUDF { .. } => {
            return Err(DeltaTableError::Generic(
                "Aggregates UDF is not allowed in delete expression".to_string(),
            ))
        }
        Expr::InList { expr, list, .. } => {
            validate_expr(expr, partition_columns, properties)?;
            for e in list {
                validate_expr(e, partition_columns, properties)?;
            }
            ()
        }
        Expr::Exists { .. } => {
            return Err(DeltaTableError::Generic(
                "Exists expression is not allowed in delete expression".to_string(),
            ))
        }
        Expr::InSubquery { .. } => {
            return Err(DeltaTableError::Generic(
                "Subquery expression is not allowed in delete expression".to_string(),
            ))
        }
        Expr::ScalarSubquery(_) => {
            return Err(DeltaTableError::Generic(
                "Subquery expression is not allowed in delete expression".to_string(),
            ))
        }
        Expr::Wildcard => {
            return Err(DeltaTableError::Generic(
                "Wildcard expression is not allowed in delete expression".to_string(),
            ))
        }
        Expr::QualifiedWildcard { .. } => {
            return Err(DeltaTableError::Generic(
                "Qualified Wildcard expression is not allowed in delete expression".to_string(),
            ))
        }
        Expr::GroupingSet(_) => {
            return Err(DeltaTableError::Generic(
                "Group By expression is not allowed in delete expression".to_string(),
            ))
        }
        Expr::Placeholder { .. } => {
            return Err(DeltaTableError::Generic(
                "Placeholder expression is not allowed in delete expression".to_string(),
            ))
        }
        Expr::OuterReferenceColumn(_, _) => {
            return Err(DeltaTableError::Generic(
                "OuterReferenceColumn is not allowed in delete expression".to_string(),
            ))
        }
    }

    Ok(())
}

impl DeleteBuilder {
    /// TODO
    pub fn new(object_store: ObjectStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            predicate: None,
            snapshot,
            store: object_store,
            state: None,
            app_metadata: None,
            writer_properties: None,
        }
    }

    /// A predicate that determines if a record is deleted
    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Parse the provided query into a Datafusion expression
    pub fn with_str_predicate(
        mut self,
        predicate: impl AsRef<str>,
    ) -> Result<Self, DeltaTableError> {
        let expr = self.snapshot.parse_predicate_expression(predicate)?;
        self.predicate = Some(expr);

        Ok(self)
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
}

async fn excute_non_empty_expr(
    snapshot: &DeltaTableState,
    object_store: ObjectStoreRef,
    state: &SessionState,
    expression: &Expr,
    metrics: &mut DeleteMetrics,
    writer_properties: WriterProperties,
    expr_properties: ExprProperties,
) -> DeltaResult<(Vec<Add>, Vec<Add>)> {
    let task_ctx = Arc::new(TaskContext::from(state));

    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.

    let schema = snapshot.arrow_schema().unwrap();

    //TODO: Move this to a function.
    let table_partition_cols = snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)
        .unwrap()
        .partition_columns
        .clone();
    let file_schema = Arc::new(ArrowSchema::new(
        schema
            .fields()
            .iter()
            .filter(|f| !table_partition_cols.contains(f.name()))
            .cloned()
            .collect(),
    ));
    let expr = logical_expr_to_physical_expr(&expression, &schema);

    let pruning_predicate = PruningPredicate::try_new(expr, schema.clone())?;
    let files_to_prune = pruning_predicate.prune(snapshot).unwrap();
    let files: Vec<&Add> = snapshot
        .files()
        .iter()
        .zip(files_to_prune.into_iter())
        .filter_map(|(action, keep)| if keep { Some(action) } else { None })
        .collect();

    // The expression only contains partition columns so the scan can be skipped.
    // This assumes pruning works correctly for partitions
    if expr_properties.partition_only {
        return Ok((
            Vec::new(),
            files.into_iter().map(|x| x.to_owned()).collect(),
        ));
    }

    let scan_start = Instant::now();
    // Create a new delta scan plan with only files that have a record
    let rewrite = find_files(
        snapshot,
        object_store.clone(),
        &schema,
        file_schema.clone(),
        files,
        &state,
        expression,
        task_ctx.clone(),
    )
    .await?;

    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis();

    let rewrite: Vec<Add> = rewrite.into_iter().map(|s| s.to_owned()).collect();
    let parquet_scan = parquet_scan_from_actions(
        snapshot,
        object_store.clone(),
        &rewrite,
        &schema,
        None,
        &state,
        None,
        None,
    )
    .await?;

    // Apply the negation of the filter and rewrite files
    let negated_expression = Expr::Not(Box::new(expression.clone()));
    let physical_schema = file_schema.clone();
    let logical_schema: DFSchema = file_schema.as_ref().clone().try_into()?;
    let execution_props = ExecutionProps::new();

    let predicate_expr = create_physical_expr(
        &negated_expression,
        &logical_schema,
        &physical_schema,
        &execution_props,
    )?;
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, parquet_scan.clone())?);

    let write_start = Instant::now();

    let add_actions = write_execution_plan(
        snapshot,
        state.clone(),
        filter.clone(),
        table_partition_cols.clone(),
        object_store.clone(),
        None,
        None,
        Some(writer_properties),
    )
    .await?;
    metrics.rewrite_time_ms = Instant::now().duration_since(write_start).as_millis();

    let read_records = parquet_scan.metrics().unwrap().output_rows().unwrap();
    let filter_records = filter.metrics().unwrap().output_rows().unwrap();
    metrics.num_copied_rows = Some(filter_records);
    metrics.num_deleted_rows = Some(read_records - filter_records);

    Ok((add_actions, rewrite))
}

async fn execute(
    predicate: Option<Expr>,
    object_store: ObjectStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: WriterProperties,
    app_metadata: Option<Map<String, Value>>,
) -> DeltaResult<DeleteMetrics> {
    // TODO:
    // Metrics for records written and deleted
    let mut metrics = DeleteMetrics::default();
    let exec_start = Instant::now();

    let (add_actions, to_delete) = match &predicate {
        Some(expr) => {
            let mut expr_properties = ExprProperties {
                partition_only: true,
            };

            let current_metadata = snapshot
                .current_metadata()
                .ok_or(DeltaTableError::NoMetadata)?;

            validate_expr(
                expr,
                &current_metadata.partition_columns,
                &mut expr_properties,
            )?;
            excute_non_empty_expr(
                snapshot,
                object_store.clone(),
                &state,
                expr,
                &mut metrics,
                writer_properties,
                expr_properties,
            )
            .await?
        }
        None => (Vec::<Add>::new(), snapshot.files().to_owned()),
    };

    let predicate = predicate.unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut actions: Vec<Action> = add_actions.into_iter().map(|a| Action::add(a)).collect();
    metrics.num_removed_files = to_delete.len();
    metrics.num_added_files = actions.len();

    for action in to_delete {
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
        let operation = DeltaOperation::Delete {
            predicate: Some(predicate.canonical_name()),
        };
        commit(
            object_store.as_ref(),
            &actions,
            operation,
            snapshot,
            app_metadata,
        )
        .await?;
    }

    Ok(metrics)
}

impl std::future::IntoFuture for DeleteBuilder {
    type Output = DeltaResult<(DeltaTable, DeleteMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let writer_properties = this
                .writer_properties
                .unwrap_or_else(|| WriterProperties::builder().build());

            let state = this.state.unwrap_or_else(|| {
                let session = SessionContext::new();

                // If a user provides their own their DF state then they must register the store themselves
                register_store(this.store.clone(), session.runtime_env().clone());

                session.state()
            });

            let metrics = execute(
                this.predicate,
                this.store.clone(),
                &this.snapshot,
                state,
                writer_properties,
                None,
            )
            .await?;
            let mut table = DeltaTable::new_with_state(this.store, this.snapshot);
            table.update().await?;
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
        return table;
    }

    #[tokio::test]
    async fn test_delete_default() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(&["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice(&[1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice(&[
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

        let (table, metrics) = DeltaOps(table).delete().await.unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 0);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);

        // Scan and rewrite is not required
        assert_eq!(metrics.scan_time_ms, 0);
        assert_eq!(metrics.rewrite_time_ms, 0);

        // Deletes with no changes to state must not commit
        let (table, metrics) = DeltaOps(table).delete().await.unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);
    }

    #[tokio::test]
    async fn test_delete_on_nonpartiton_column() {
        // Delete based on a nonpartition column
        // Only rewrite files that match the predicate
        // Test data designed to force a scan of the underlying data

        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(&["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice(&[1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice(&[
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
        assert_eq!(table.get_file_uris().count(), 1);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(&["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice(&[0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice(&[
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
        assert_eq!(table.get_file_uris().count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").eq(lit(1)))
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_file_uris().count(), 2);

        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, Some(1));
        assert_eq!(metrics.num_copied_rows, Some(3));
    }

    #[tokio::test]
    async fn test_delete_on_partition_column() {
        // Perform a delete where the predicate only contains partition columns

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(&["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice(&[0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice(&[
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
        assert_eq!(table.get_file_uris().count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 1);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);
    }

    #[tokio::test]
    async fn test_failure_nondeterministic_query() {
        // Deletion requires a deterministic predicate
        // Currently Datafusion does not provide an easy way to determine that. See arrow-datafusion/issues/3618

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
}
