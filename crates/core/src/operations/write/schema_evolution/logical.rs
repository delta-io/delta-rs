use std::cmp::Ordering;

use datafusion_common::{DFSchemaRef, Result as DataFusionResult};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd)]
pub(crate) struct SchemaEvolution {
    pub input: LogicalPlan,
    pub new_schema: SchemaWrapper,
    pub add_missing_columns: bool,
    pub safe_cast: bool,
}


#[derive(Debug, Hash, Eq, PartialEq, Clone)] // Remove PartialOrd from here
pub struct SchemaWrapper {
    pub inner: DFSchemaRef,
}

impl PartialOrd for SchemaWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Define a meaningful comparison, e.g., based on the number of fields in the schema
        let self_fields = self.inner.fields().len();
        let other_fields = other.inner.fields().len();

        self_fields.partial_cmp(&other_fields)
    }
}

impl UserDefinedLogicalNodeCore for SchemaEvolution {
    fn name(&self) -> &str {
        "SchemaEvolution"
    }

    fn inputs(&self) -> Vec<&datafusion_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        &self.new_schema.inner
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SchemaEvolution")
    }

    fn from_template(
        &self,
        exprs: &[datafusion_expr::Expr],
        inputs: &[datafusion_expr::LogicalPlan],
    ) -> Self {
        self.with_exprs_and_inputs(exprs.to_vec(), inputs.to_vec())
            .unwrap()
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion_expr::Expr>,
        inputs: Vec<datafusion_expr::LogicalPlan>,
    ) -> DataFusionResult<Self> {
        Ok(SchemaEvolution {
            input: inputs[0].clone(),
            new_schema: self.new_schema.clone(),
            add_missing_columns: self.add_missing_columns,
            safe_cast: self.safe_cast,
        })
    }
}