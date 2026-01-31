//! Logical Operations for DataFusion

use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::Field;
use datafusion::common::{Column, Result};
use datafusion::logical_expr::{
    Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion::prelude::{Expr, col};
use itertools::Itertools;

use crate::delta_datafusion::data_validation::DataValidation;

pub(crate) trait LogicalPlanBuilderExt: Sized {
    fn with_column(self, name: &str, expr: Expr) -> Result<Self>;
    fn drop_columns(self, cols: impl IntoIterator<Item = impl ColumnReference>) -> Result<Self>;
    /// Validate all data produced by the plan coforms to the passed predicates.
    fn validate(self, validations: impl IntoIterator<Item = Expr>) -> Result<Self>;
}

pub(crate) trait ColumnReference: Sized {
    fn col_name(&self) -> &str;
}

impl ColumnReference for &str {
    fn col_name(&self) -> &str {
        self
    }
}

impl ColumnReference for Field {
    fn col_name(&self) -> &str {
        self.name().as_str()
    }
}

impl<T: ColumnReference> ColumnReference for Arc<T> {
    fn col_name(&self) -> &str {
        T::col_name(self)
    }
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn with_column(self, name: &str, expr: Expr) -> Result<Self> {
        let mut projection = self
            .schema()
            .iter()
            .map(|(_, f)| col(Column::from_name(f.col_name())))
            .collect_vec();
        projection.push(expr.alias(name));
        self.project(projection)
    }

    fn drop_columns(self, cols: impl IntoIterator<Item = impl ColumnReference>) -> Result<Self> {
        let away_names = cols
            .into_iter()
            .map(|c| c.col_name().to_string())
            .collect::<HashSet<_>>();
        let projection = self
            .schema()
            .iter()
            .filter_map(|(_, f)| {
                (!away_names.contains(f.name())).then(|| col(Column::from_name(f.col_name())))
            })
            .collect_vec();
        self.project(projection)
    }

    fn validate(self, validations: impl IntoIterator<Item = Expr>) -> Result<Self> {
        let plan = LogicalPlan::Extension(Extension {
            node: DataValidation::try_new(self.build()?, validations)?,
        });
        Ok(LogicalPlanBuilder::new(plan))
    }
}

pub(crate) trait LogicalPlanExt: Sized {
    fn into_builder(self) -> LogicalPlanBuilder;
}

impl LogicalPlanExt for LogicalPlan {
    fn into_builder(self) -> LogicalPlanBuilder {
        LogicalPlanBuilder::new(self)
    }
}

impl LogicalPlanExt for Arc<LogicalPlan> {
    fn into_builder(self) -> LogicalPlanBuilder {
        LogicalPlanBuilder::new_from_arc(self)
    }
}

// Metric Observer is used to update DataFusion metrics from a record batch.
// See MetricObserverExec for the physical implementation

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd)]
pub(crate) struct MetricObserver {
    // id is preserved during conversion to physical node
    pub id: String,
    pub input: LogicalPlan,
    pub enable_pushdown: bool,
}

impl UserDefinedLogicalNodeCore for MetricObserver {
    // Predicate push down is not supported for this node. Try to limit usage
    // near the end of plan.
    fn name(&self) -> &str {
        "MetricObserver"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        if self.enable_pushdown {
            HashSet::new()
        } else {
            self.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        }
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MetricObserver id={}", self.id)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<datafusion::logical_expr::LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(MetricObserver {
            id: self.id.clone(),
            input: inputs[0].clone(),
            enable_pushdown: self.enable_pushdown,
        })
    }
}
