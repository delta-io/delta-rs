//! Translates a kernel [`Plan`] into a DataFusion `LogicalPlan` and runs it.
//!
//! [`Plan::nodes`] is topologically sorted: a node's inputs always come earlier in the slice.
//! [`to_logical_plan`] walks the nodes in order, builds one `LogicalPlan` per node from its
//! already translated inputs ([`operator`]), and returns the plan of the terminal (last) node.
//! Expressions and predicates inside operators go through [`crate::expressions`].

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use delta_kernel::DeltaResult;
use delta_kernel::plans::PlanResult;
use delta_kernel::plans::ir::plan::Plan;

use crate::{DataFusionPlanExecutor, Error};

pub mod operator;

/// Translates `plan`, runs it on a session built from the executor's task context, and streams
/// the terminal node's rows as [`PlanResult::Data`].
pub fn execute(executor: &DataFusionPlanExecutor, plan: Plan) -> DeltaResult<PlanResult> {
    let logical = to_logical_plan(executor, &plan)?;
    let ctx = executor.task_ctx();
    let session =
        SessionContext::new_with_config_rt(ctx.session_config().clone(), ctx.runtime_env());
    let stream = crate::runtime::block_on(executor.handle(), async move {
        session
            .execute_logical_plan(logical)
            .await?
            .execute_stream()
            .await
    })
    .map_err(Error::from)?;
    Ok(PlanResult::Data(crate::stream::into_engine_data(
        executor.handle(),
        stream,
    )))
}

/// Translates `plan` into the `LogicalPlan` of its terminal node.
pub fn to_logical_plan(
    executor: &DataFusionPlanExecutor,
    plan: &Plan,
) -> crate::Result<LogicalPlan> {
    let mut plans: Vec<LogicalPlan> = Vec::with_capacity(plan.nodes.len());
    for node in &plan.nodes {
        let inputs: Vec<&LogicalPlan> = node.inputs.iter().map(|i| &plans[*i]).collect();
        plans.push(operator::to_logical_plan(executor, &node.op, &inputs)?);
    }
    plans.pop().ok_or_else(|| Error::unsupported("empty plan"))
}
