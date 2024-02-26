use std::collections::HashSet;

use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::delta_datafusion::find_files::ONLY_FILES_DF_SCHEMA;
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct FindFilesNode {
    id: String,
    predicate: Expr,
    files: Vec<String>,
    schema: DFSchemaRef,
    version: i64,
}

impl FindFilesNode {
    pub fn new(
        id: String,
        eager_snapshot: DeltaTableState,
        log_store: LogStoreRef,
        predicate: Expr,
    ) -> datafusion_common::Result<Self> {
        let files: Vec<String> = eager_snapshot
            .file_paths_iter()
            .map(|f| log_store.to_uri(&f))
            .collect();

        Ok(Self {
            id,
            predicate,
            files,
            schema: ONLY_FILES_DF_SCHEMA.clone(),
            version: eager_snapshot.version(),
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
            "FindFiles id={}, predicate={:?}, version={:?}",
            &self.id, self.predicate, self.version
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}
