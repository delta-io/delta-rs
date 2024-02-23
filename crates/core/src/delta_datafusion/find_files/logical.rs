use std::collections::HashSet;
use std::path::Path;

use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::delta_datafusion::find_files::only_file_path_schema;
use crate::kernel::EagerSnapshot;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct FindFilesNode {
    id: String,
    predicate: Expr,
    files: Vec<String>,
    schema: DFSchemaRef,
}

impl FindFilesNode {
    pub fn new(
        id: String,
        eager_snapshot: EagerSnapshot,
        predicate: Expr,
    ) -> datafusion_common::Result<Self> {
        let files: Vec<String> = eager_snapshot
            .files()
            .map(|f| f.object_store_path().to_string())
            .collect();


        Ok(Self {
            id,
            predicate,
            files,
            schema: only_file_path_schema().to_dfschema_ref()?,
        })
    }

    pub fn predicate(&self) -> &Expr {
        &self.predicate
    }

    pub fn files(&self) -> Vec<String> {
        self.files.clone()
    }
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
            &self.id, self.predicate, self.files
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}
