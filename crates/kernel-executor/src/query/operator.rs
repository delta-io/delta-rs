//! Compiles one kernel [`Operator`] into a DataFusion `LogicalPlan`.
//!
//! Each kernel operator has one function here. The function receives the already compiled
//! inputs in the order [`PlanNode::inputs`] lists them.
//!
//! [`PlanNode::inputs`]: delta_kernel::plans::ir::plan::PlanNode::inputs

use datafusion::logical_expr::LogicalPlan;
use delta_kernel::plans::ir::nodes::{
    Aggregate, DynamicScan, Filter, Operator, Project, ScanJson, ScanParquet, SemiJoin, UnionAll,
    Values,
};

use crate::{DataFusionPlanExecutor, Error};

/// Dispatches `op` to its compile function.
pub fn compile(
    executor: &DataFusionPlanExecutor,
    op: &Operator,
    inputs: &[&LogicalPlan],
) -> crate::Result<LogicalPlan> {
    match op {
        Operator::ScanParquet(op) => scan_parquet(executor, op),
        Operator::ScanJson(op) => scan_json(executor, op),
        Operator::Values(op) => values(op),
        Operator::Project(op) => project(op, inputs),
        Operator::Filter(op) => filter(op, inputs),
        Operator::DynamicScan(op) => dynamic_scan(executor, op, inputs),
        Operator::Aggregate(op) => aggregate(op, inputs),
        Operator::SemiJoin(op) => semi_join(op, inputs),
        Operator::UnionAll(op) => union_all(op, inputs),
    }
}

/// Source: reads Parquet files from the executor's object store.
fn scan_parquet(
    _executor: &DataFusionPlanExecutor,
    _op: &ScanParquet,
) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator scan_parquet"))
}

/// Source: reads newline-delimited JSON files from the executor's object store.
fn scan_json(_executor: &DataFusionPlanExecutor, _op: &ScanJson) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator scan_json"))
}

/// Source: literal rows.
fn values(_op: &Values) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator values"))
}

/// Unary: evaluates expressions over the input rows.
fn project(_op: &Project, _inputs: &[&LogicalPlan]) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator project"))
}

/// Unary: keeps the input rows that satisfy a predicate.
fn filter(_op: &Filter, _inputs: &[&LogicalPlan]) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator filter"))
}

/// Unary: scans the files named by the input rows.
fn dynamic_scan(
    _executor: &DataFusionPlanExecutor,
    _op: &DynamicScan,
    _inputs: &[&LogicalPlan],
) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator dynamic_scan"))
}

/// Unary: groups the input rows and evaluates aggregates per group.
fn aggregate(_op: &Aggregate, _inputs: &[&LogicalPlan]) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator aggregate"))
}

/// Binary: keeps probe rows with (semi) or without (anti) a key match in the build side.
fn semi_join(_op: &SemiJoin, _inputs: &[&LogicalPlan]) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator semi_join"))
}

/// N-ary: concatenates the rows of all inputs.
fn union_all(_op: &UnionAll, _inputs: &[&LogicalPlan]) -> crate::Result<LogicalPlan> {
    Err(Error::unsupported("operator union_all"))
}
