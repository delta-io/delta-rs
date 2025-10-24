use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Field, Schema as ArrowSchema};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::logical_expr::{col, Expr, Volatility};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::ExecutionPlan;
use itertools::Itertools;
use tracing::*;

use crate::delta_datafusion::{
    df_logical_schema, get_path_column, DeltaScanBuilder, DeltaScanConfigBuilder, PATH_COLUMN,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Add, EagerSnapshot};
use crate::logstore::LogStoreRef;

#[derive(Debug, Hash, Eq, PartialEq)]
/// Representing the result of the [find_files] function.
pub(crate) struct FindFiles {
    /// A list of `Add` objects that match the given predicate
    pub candidates: Vec<Add>,
    /// Was a physical read to the datastore required to determine the candidates
    pub partition_scan: bool,
}

/// Finds files in a snapshot that match the provided predicate.
#[instrument(
    skip_all,
    fields(
        version = snapshot.version(),
        has_predicate = predicate.is_some(),
        partition_scan = field::Empty,
        candidate_count = field::Empty
    )
)]
pub(crate) async fn find_files(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    predicate: Option<Expr>,
) -> DeltaResult<FindFiles> {
    let current_metadata = snapshot.metadata();

    match &predicate {
        Some(predicate) => {
            // Validate the Predicate and determine if it only contains partition columns
            let mut expr_properties = FindFilesExprProperties {
                partition_only: true,
                partition_columns: current_metadata.partition_columns().clone(),
                result: Ok(()),
            };

            TreeNode::visit(predicate, &mut expr_properties)?;
            expr_properties.result?;

            if expr_properties.partition_only {
                let candidates = scan_memory_table(snapshot, predicate).await?;
                let result = FindFiles {
                    candidates,
                    partition_scan: true,
                };
                Span::current().record("partition_scan", result.partition_scan);
                Span::current().record("candidate_count", result.candidates.len());
                Ok(result)
            } else {
                let candidates =
                    find_files_scan(snapshot, log_store, session, predicate.to_owned()).await?;

                let result = FindFiles {
                    candidates,
                    partition_scan: false,
                };
                Span::current().record("partition_scan", result.partition_scan);
                Span::current().record("candidate_count", result.candidates.len());
                Ok(result)
            }
        }
        None => {
            let result = FindFiles {
                candidates: snapshot.log_data().iter().map(|f| f.add_action()).collect(),
                partition_scan: true,
            };
            Span::current().record("partition_scan", result.partition_scan);
            Span::current().record("candidate_count", result.candidates.len());
            Ok(result)
        }
    }
}

struct FindFilesExprProperties {
    pub partition_columns: Vec<String>,

    pub partition_only: bool,
    pub result: DeltaResult<()>,
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
impl TreeNodeVisitor<'_> for FindFilesExprProperties {
    type Node = Expr;

    fn f_down(&mut self, expr: &Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
        // TODO: We can likely relax the volatility to STABLE. Would require further
        // research to confirm the same value is generated during the scan and
        // rewrite phases.

        match expr {
            Expr::Column(c) => {
                if !self.partition_columns.contains(&c.name) {
                    self.partition_only = false;
                }
            }
            Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::Alias(_)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::InList { .. }
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_) => (),
            Expr::ScalarFunction(scalar_function) => {
                match scalar_function.func.signature().volatility {
                    Volatility::Immutable => (),
                    _ => {
                        self.result = Err(DeltaTableError::Generic(format!(
                            "Find files predicate contains nondeterministic function {}",
                            scalar_function.func.name()
                        )));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            _ => {
                self.result = Err(DeltaTableError::Generic(format!(
                    "Find files predicate contains unsupported expression {expr}"
                )));
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
) -> DeltaResult<Vec<Add>> {
    // Given RecordBatches that contains `__delta_rs_path` perform a hash join
    // with actions to obtain original add actions

    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());

        let iter: Box<dyn Iterator<Item = Option<&str>>> = if dict_array {
            let array = get_path_column(&batch, path_column)?;
            Box::new(array.into_iter())
        } else {
            let array = batch
                .column_by_name(path_column)
                .ok_or_else(err)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(err)?;
            Box::new(array.into_iter())
        };

        for path in iter {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{path_column} cannot be null"
            )))?;

            match actions.remove(path) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::Generic(
                        "Unable to map __delta_rs_path to action.".to_owned(),
                    ))
                }
            }
        }
    }
    Ok(files)
}

/// Determine which files contain a record that satisfies the predicate
#[instrument(
    skip_all,
    fields(
        version = snapshot.version(),
        total_files = field::Empty,
        matching_files = field::Empty
    )
)]
async fn find_files_scan(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    let candidate_map: HashMap<String, Add> = snapshot
        .log_data()
        .iter()
        .map(|f| f.add_action())
        .map(|add| {
            let path = add.path.clone();
            (path, add)
        })
        .collect();

    Span::current().record("total_files", candidate_map.len());

    let scan_config = DeltaScanConfigBuilder::default()
        .with_file_column(true)
        .build(snapshot)?;

    let logical_schema = df_logical_schema(snapshot, &scan_config.file_column_name, None)?;

    // Identify which columns we need to project
    let mut used_columns = expression
        .column_refs()
        .into_iter()
        .map(|column| logical_schema.index_of(&column.name))
        .collect::<Result<Vec<usize>, ArrowError>>()?;
    // Add path column
    used_columns.push(logical_schema.index_of(scan_config.file_column_name.as_ref().unwrap())?);

    let scan = DeltaScanBuilder::new(snapshot, log_store, session)
        .with_filter(Some(expression.clone()))
        .with_projection(Some(&used_columns))
        .with_scan_config(scan_config)
        .build()
        .await?;
    let scan = Arc::new(scan);

    let config = &scan.config;
    let input_dfschema = scan.logical_schema.as_ref().to_owned().try_into()?;

    let predicate_expr = session
        .create_physical_expr(Expr::IsTrue(Box::new(expression.clone())), &input_dfschema)?;

    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);
    let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filter, 1));

    let task_ctx = Arc::new(TaskContext::from(session));
    let path_batches = datafusion::physical_plan::collect(limit, task_ctx).await?;

    let result = join_batches_with_add_actions(
        path_batches,
        candidate_map,
        config.file_column_name.as_ref().unwrap(),
        true,
    )?;

    Span::current().record("matching_files", result.len());
    Ok(result)
}

async fn scan_memory_table(snapshot: &EagerSnapshot, predicate: &Expr) -> DeltaResult<Vec<Add>> {
    let actions = snapshot
        .log_data()
        .iter()
        .map(|f| f.add_action())
        .collect_vec();

    let batch = snapshot.add_actions_table(true)?;
    let schema = batch.schema();
    let mut arrays = Vec::with_capacity(schema.fields().len());
    let mut fields = Vec::with_capacity(schema.fields().len());

    arrays.push(
        batch
            .column_by_name("path")
            .ok_or(DeltaTableError::Generic(
                "Column with name `path` does not exist".to_owned(),
            ))?
            .to_owned(),
    );
    fields.push(Field::new(PATH_COLUMN, ArrowDataType::Utf8, false));

    for field in schema.fields() {
        if field.name().starts_with("partition.") {
            let name = field.name().strip_prefix("partition.").unwrap();

            arrays.push(batch.column_by_name(field.name()).unwrap().to_owned());
            fields.push(Field::new(
                name,
                field.data_type().to_owned(),
                field.is_nullable(),
            ));
        }
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let ctx = SessionContext::new();
    let mut df = ctx.read_table(Arc::new(mem_table))?;
    df = df
        .filter(predicate.to_owned())?
        .select(vec![col(PATH_COLUMN)])?;
    let batches = df.collect().await?;

    let map = actions
        .into_iter()
        .map(|action| {
            let path = action.path.clone();
            (path, action)
        })
        .collect::<HashMap<String, Add>>();

    join_batches_with_add_actions(batches, map, PATH_COLUMN, false)
}
