use std::collections::HashSet;

use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct FindFilesNode {
    pub id: String,
    pub input: LogicalPlan,
    pub predicates: Vec<Expr>,
    pub files: Vec<String>,
    pub schema: DFSchemaRef,
}

impl UserDefinedLogicalNodeCore for FindFilesNode {
    fn name(&self) -> &str {
        "FindFiles"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "FindFiles id={}, predicate={:?}, files={:?}",
            &self.id, self.predicates, self.files
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}
