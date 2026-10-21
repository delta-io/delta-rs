//! Compiles a kernel [`Plan`] into a DataFusion `LogicalPlan` and runs it.
//!
//! [`Plan::nodes`] is topologically sorted: a node's inputs always come earlier in the slice.
//! [`compile`] walks the nodes in order, builds one `LogicalPlan` per node from its already
//! compiled inputs ([`operator`]), and returns the plan of the terminal (last) node.
//! Expressions and predicates inside operators go through [`expression`].

use datafusion::logical_expr::LogicalPlan;
use delta_kernel::DeltaResult;
use delta_kernel::plans::PlanResult;
use delta_kernel::plans::ir::plan::Plan;

use crate::{DataFusionPlanExecutor, Error};

pub mod expression;
pub mod operator;

/// Compiles `plan`, runs it on the executor's session, and streams the terminal node's rows as
/// [`PlanResult::Data`].
pub fn execute(executor: &DataFusionPlanExecutor, plan: Plan) -> DeltaResult<PlanResult> {
    let logical = compile(executor, &plan)?;
    let stream = executor.handle().block_on(async {
        executor
            .session()
            .execute_logical_plan(logical)
            .await?
            .execute_stream()
            .await
    });
    let stream = stream.map_err(Error::from)?;
    Ok(PlanResult::Data(crate::stream::into_engine_data(
        executor.handle().clone(),
        stream,
    )))
}

/// Compiles `plan` into the `LogicalPlan` of its terminal node.
pub fn compile(executor: &DataFusionPlanExecutor, plan: &Plan) -> crate::Result<LogicalPlan> {
    let mut compiled: Vec<LogicalPlan> = Vec::with_capacity(plan.nodes.len());
    for node in &plan.nodes {
        let inputs: Vec<&LogicalPlan> = node.inputs.iter().map(|i| &compiled[*i]).collect();
        compiled.push(operator::compile(executor, &node.op, &inputs)?);
    }
    compiled
        .pop()
        .ok_or_else(|| Error::unsupported("empty plan"))
}
