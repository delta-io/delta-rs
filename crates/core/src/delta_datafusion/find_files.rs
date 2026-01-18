use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode as _, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::datasource::provider_as_source;
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, Volatility, col};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::lit;
use futures::TryStreamExt;
use itertools::Itertools;
use tracing::*;

use crate::delta_datafusion::table_provider::next::SnapshotWrapper;
use crate::delta_datafusion::{
    DeltaScanNext, PATH_COLUMN, get_path_column, update_datafusion_session,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;
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
        has_predicate = predicate.is_some(),
        partition_scan = field::Empty,
        candidate_count = field::Empty
    )
)]
pub(crate) async fn find_files(
    snapshot: SnapshotWrapper,
    log_store: LogStoreRef,
    session: &dyn Session,
    predicate: Option<Expr>,
) -> DeltaResult<FindFiles> {
    let result = match &predicate {
        Some(predicate) => {
            // Validate the Predicate and determine if it only contains partition columns
            let mut expr_properties = FindFilesExprProperties {
                partition_only: true,
                partition_columns: snapshot
                    .table_configuration()
                    .metadata()
                    .partition_columns(),
                result: Ok(()),
            };
            predicate.visit(&mut expr_properties)?;
            expr_properties.result?;

            FindFiles {
                partition_scan: expr_properties.partition_only,
                candidates: find_files_scan(snapshot, log_store, session, predicate.clone())
                    .await?,
            }
        }
        None => FindFiles {
            candidates: snapshot
                .file_views(&log_store, None)
                .map_ok(|f| f.add_action())
                .try_collect()
                .await?,
            partition_scan: true,
        },
    };

    Span::current().record("partition_scan", result.partition_scan);
    Span::current().record("candidate_count", result.candidates.len());

    Ok(result)
}

struct FindFilesExprProperties<'a> {
    pub partition_columns: &'a [String],
    pub partition_only: bool,
    pub result: DeltaResult<()>,
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
impl TreeNodeVisitor<'_> for FindFilesExprProperties<'_> {
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

// Given RecordBatches that contains `__delta_rs_path` perform a hash join
// with actions to obtain original add actions
fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
) -> DeltaResult<Vec<Add>> {
    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let iter = get_path_column(&batch, PATH_COLUMN)?.into_iter();
        for path in iter {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{PATH_COLUMN} cannot be null"
            )))?;
            match actions.remove(path) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::Generic(
                        "Unable to map __delta_rs_path to action.".to_owned(),
                    ));
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
        total_files = field::Empty,
        matching_files = field::Empty
    )
)]
async fn find_files_scan(
    snapshot: SnapshotWrapper,
    log_store: LogStoreRef,
    session: &dyn Session,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    update_datafusion_session(&log_store, session, None)?;

    let table_root = snapshot.table_configuration().table_root();
    let to_url = |path: String| {
        Ok::<_, DeltaTableError>(
            table_root
                .join(&path)
                .map_err(|e| DeltaTableError::Generic(e.to_string()))?
                .to_string(),
        )
    };

    // let kernel_predicate = to_predicate(&expression).ok().map(Arc::new);
    let candidate_map: HashMap<_, _> = snapshot
        .file_views(&log_store, None)
        .map_ok(|f| {
            let add = f.add_action();
            (f.path_raw().to_string(), add)
        })
        .try_collect()
        .await?;
    let candidate_map: HashMap<_, _> = candidate_map
        .into_iter()
        .map(|(path, add)| Ok::<_, DeltaTableError>((to_url(path)?, add)))
        .try_collect()?;

    Span::current().record("total_files", candidate_map.len());

    let exec = create_execution_plan(snapshot, session, expression).await?;

    let path_batches = collect(exec, session.task_ctx()).await?;
    let result = join_batches_with_add_actions(path_batches, candidate_map)?;

    Span::current().record("matching_files", result.len());
    Ok(result)
}

async fn create_execution_plan(
    snapshot: SnapshotWrapper,
    session: &dyn Session,
    expression: Expr,
) -> DeltaResult<Arc<dyn ExecutionPlan>> {
    let mut builder = DeltaScanNext::builder().with_file_column(PATH_COLUMN);
    match snapshot {
        SnapshotWrapper::EagerSnapshot(s) => {
            builder = builder.with_eager_snapshot(s);
        }
        SnapshotWrapper::Snapshot(s) => {
            builder = builder.with_snapshot(s);
        }
    }
    let provider = builder.await?;

    // Scan the table with the predicate applied to identify files with matching rows.
    // We are only interested if any matching row exists in the file, so we
    // can use a simple first_value aggregate to minimize data movement.
    let logical_plan = LogicalPlanBuilder::scan("source", provider_as_source(provider), None)?
        .filter(expression.is_true())?
        .aggregate([col(PATH_COLUMN)], [first_value(lit(1_i32), vec![])])?
        .build()?;

    Ok(session.create_physical_plan(&logical_plan).await?)
}
