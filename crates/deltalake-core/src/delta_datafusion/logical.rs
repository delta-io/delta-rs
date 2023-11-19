//! Logical Operations for DataFusion

use datafusion_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

// Metric Observer is used to update DataFusion metrics from a record batch.
// See MetricObserverExec for the physical implementation

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct MetricObserver {
    // id is preserved during conversion to physical node
    pub id: String,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for MetricObserver {
    // Predicate push down is not supported for this node. Try to limit usage
    // near the end of plan.
    fn name(&self) -> &str {
        "MetricObserver"
    }

    fn inputs(&self) -> Vec<&datafusion_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MetricObserver id={}", &self.id)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion_expr::Expr],
        inputs: &[datafusion_expr::LogicalPlan],
    ) -> Self {
        MetricObserver {
            id: self.id.clone(),
            input: inputs[0].clone(),
        }
    }
}
