//! Translates a kernel [`Plan`] into a DataFusion `LogicalPlan` and runs it.
//!
//! [`Plan::nodes`] is topologically sorted: a node's inputs always come earlier in the slice.
//! [`to_logical_plan`] walks the nodes in order, builds one `LogicalPlan` per node from its
//! already translated inputs ([`operator`]), and returns the plan of the terminal (last) node.
//! Expressions and predicates inside operators go through [`crate::expressions`].

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use delta_kernel::DeltaResult;
use delta_kernel::plans::PlanResult;
use delta_kernel::plans::ir::plan::Plan;

use crate::{DataFusionPlanExecutor, ExecutorError};

pub mod operator;
pub mod scan;

/// Translates `plan`, runs it on a session built from the executor's task context, and streams
/// the terminal node's rows as [`PlanResult::Data`].
pub fn execute(executor: &DataFusionPlanExecutor, plan: Plan) -> DeltaResult<PlanResult> {
    let logical = to_logical_plan(executor, &plan)?;
    let ctx = executor.task_ctx();
    let mut config = ctx.session_config().clone();

    // A scan gives every file its own partition. Splitting a file further by byte range would
    // interleave its rows, and the kernel's handler contract wants them in file order.
    config.options_mut().optimizer.repartition_file_scans = false;
    let session = SessionContext::new_with_config_rt(config, ctx.runtime_env());
    let stream = crate::runtime::block_on(executor.handle(), async move {
        session
            .execute_logical_plan(logical)
            .await?
            .execute_stream()
            .await
    })
    .map_err(ExecutorError::from)?;

    Ok(PlanResult::Data(crate::stream::into_engine_data(
        executor.handle(),
        stream,
    )))
}

/// Translates `plan` into the `LogicalPlan` of its terminal node.
///
/// # Errors
/// Returns an error if `plan` has no nodes, a node references an input that does not precede it,
/// or lowering any individual node fails.
pub fn to_logical_plan(
    executor: &DataFusionPlanExecutor,
    plan: &Plan,
) -> crate::Result<LogicalPlan> {
    // A node is identified by its index, and `nodes` is topologically ordered: every input index is
    // strictly less than the node's own, so a single forward pass leaves each node's inputs already
    // lowered by the time it is reached.
    let mut lowered: Vec<Arc<LogicalPlan>> = Vec::with_capacity(plan.nodes.len());

    for (node_index, node) in plan.nodes.iter().enumerate() {
        let op = &node.op;
        let available = lowered.len();
        let invalid_input = |input_index| {
            DataFusionError::Plan(format!(
                "node {node_index} ({}) references input {input_index}, but only {available} \
                 prior node(s) are available",
                operator::name(op)
            ))
        };
        let inputs: Vec<_> = node
            .inputs
            .iter()
            .map(|&input_index| {
                lowered
                    .get(input_index)
                    .map(Arc::clone)
                    .ok_or_else(|| invalid_input(input_index))
            })
            .collect::<Result<_, _>>()?;
        lowered.push(Arc::new(operator::to_logical_plan(executor, op, &inputs)?));
    }

    // The terminal node is the last one: no other node consumes it, and its rows are the plan's
    // output.
    match lowered.pop() {
        Some(terminal) => Ok(Arc::unwrap_or_clone(terminal)),
        None => Err(DataFusionError::Plan("cannot lower a plan with no nodes".to_string()).into()),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use delta_kernel::PlanBuilder;
    use delta_kernel::expressions::{
        Expression as KernelExpr, Predicate as KernelPredicate, Scalar, column_expr,
    };
    use delta_kernel::plans::ir::nodes::{Filter, Project, Values};
    use delta_kernel::plans::ir::plan::PlanNode;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use rstest::rstest;
    use tokio::runtime::Handle;

    use super::*;

    // === Shared helpers ===

    /// An executor over a default session and the test runtime.
    fn executor() -> DataFusionPlanExecutor {
        DataFusionPlanExecutor::new(SessionContext::new().task_ctx(), Handle::current())
    }

    /// A single-field `long` schema named `a`.
    fn test_schema() -> StructType {
        StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap()
    }

    /// A `Values` node over [`test_schema`] holding `rows` one-column rows.
    fn values_node(rows: Vec<Vec<Scalar>>) -> PlanNode {
        PlanNode::new(Values::new(test_schema(), rows), vec![])
    }

    async fn execute(plan: &Plan) -> crate::Result<Vec<RecordBatch>> {
        Ok(SessionContext::new()
            .execute_logical_plan(to_logical_plan(&executor(), plan)?)
            .await?
            .collect()
            .await?)
    }

    // === Tests ===

    #[tokio::test]
    async fn empty_plan_is_rejected() {
        let err = to_logical_plan(&executor(), &Plan { nodes: vec![] }).unwrap_err();
        assert!(err.to_string().contains("no nodes"), "{err}");
    }

    #[tokio::test]
    async fn terminal_node_is_the_plans_output() {
        // Two independent sources; the last node is the terminal one, so its rows are the output.
        let plan = Plan {
            nodes: vec![
                values_node(vec![vec![1i64.into()]]),
                values_node(vec![vec![2i64.into()], vec![3i64.into()]]),
            ],
        };
        let batches = execute(&plan).await.unwrap();
        assert_batches_eq!(
            &["+---+", "| a |", "+---+", "| 2 |", "| 3 |", "+---+"],
            &batches
        );
    }

    #[tokio::test]
    async fn filter_plan_executes() {
        let plan = PlanBuilder::values(
            Arc::new(test_schema()),
            vec![vec![Scalar::null(DataType::LONG)], vec![2i64.into()]],
        )
        .unwrap()
        .filter(KernelPredicate::is_null(column_expr!("a")))
        .unwrap()
        .build()
        .unwrap();
        let batches = execute(&plan).await.unwrap();
        assert_batches_eq!(&["+---+", "| a |", "+---+", "|   |", "+---+"], &batches);
    }

    #[tokio::test]
    async fn values_project_filter_composes_through_declared_project_schema() {
        let project_schema = Arc::new(
            StructType::try_new([StructField::nullable("projected", DataType::LONG)]).unwrap(),
        );
        let project = Project {
            expr: KernelExpr::struct_from([column_expr!("a")]).into(),
            schema: project_schema.clone(),
        };
        let filter = Filter {
            predicate: KernelPredicate::is_not_null(column_expr!("projected")).into(),
        };
        let plan = Plan {
            nodes: vec![
                values_node(vec![vec![1i64.into()]]),
                PlanNode::new(project, vec![0]),
                PlanNode::new(filter, vec![1]),
            ],
        };

        let batches = execute(&plan).await.unwrap();
        assert_batches_eq!(
            &[
                "+-----------+",
                "| projected |",
                "+-----------+",
                "| 1         |",
                "+-----------+",
            ],
            &batches
        );
    }

    #[rstest]
    #[case::self_reference(0)]
    #[case::forward_reference(1)]
    #[case::far_out_of_range(42)]
    #[tokio::test]
    async fn invalid_input_reference_is_rejected(#[case] input_index: usize) {
        let node = PlanNode::new(Values::new(test_schema(), vec![]), vec![input_index]);
        let err = to_logical_plan(&executor(), &Plan { nodes: vec![node] }).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("node 0 (values)"), "{message}");
        assert!(
            message.contains(&format!("references input {input_index}")),
            "{message}"
        );
        assert!(message.contains("0 prior node(s)"), "{message}");
    }
}
