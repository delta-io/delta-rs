use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};
use datafusion::{catalog::Session, common::DFSchema};

use crate::delta_datafusion::DeltaScanExec;
use crate::delta_datafusion::table_provider::next::KernelScanPlan;
use crate::{DeltaResult, delta_datafusion::expr::parse_predicate_expression};

/// Used to represent user input of either a Datafusion expression or string expression
#[derive(Debug, Clone)]
pub enum Expression {
    /// Datafusion Expression
    DataFusion(Expr),
    /// String Expression
    String(String),
}

impl From<Expr> for Expression {
    fn from(val: Expr) -> Self {
        Expression::DataFusion(val)
    }
}

impl From<&str> for Expression {
    fn from(val: &str) -> Self {
        Expression::String(val.to_string())
    }
}
impl From<String> for Expression {
    fn from(val: String) -> Self {
        Expression::String(val)
    }
}

pub(crate) fn into_expr(
    expr: Expression,
    schema: &DFSchema,
    session: &dyn Session,
) -> DeltaResult<Expr> {
    match expr {
        Expression::DataFusion(expr) => Ok(expr),
        Expression::String(s) => parse_predicate_expression(schema, s, session),
    }
}

pub(crate) fn maybe_into_expr(
    expr: Option<Expression>,
    schema: &DFSchema,
    session: &dyn Session,
) -> DeltaResult<Option<Expr>> {
    Ok(match expr {
        Some(predicate) => Some(into_expr(predicate, schema, session)?),
        None => None,
    })
}

/// Extracts fields from the parquet scan
#[derive(Default)]
pub(crate) struct DeltaScanVisitor {
    pub(crate) delta_plan: Option<KernelScanPlan>,
}

impl DeltaScanVisitor {
    fn pre_visit_delta_scan(&mut self, delta_scan_exec: &DeltaScanExec) -> Result<bool> {
        self.delta_plan = Some(delta_scan_exec.delta_plan().clone());
        Ok(true)
    }
}

impl ExecutionPlanVisitor for DeltaScanVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if let Some(delta_scan_exec) = plan.as_any().downcast_ref::<DeltaScanExec>() {
            return self.pre_visit_delta_scan(delta_scan_exec);
        };

        Ok(true)
    }
}
