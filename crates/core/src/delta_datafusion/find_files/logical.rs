use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::delta_datafusion::find_files::ONLY_FILES_DF_SCHEMA;
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;

#[derive(Debug, Clone)]
pub struct FindFilesNode {
    id: String,
    predicate: Expr,
    table_state: DeltaTableState,
    log_store: LogStoreRef,
    version: i64,
}

impl FindFilesNode {
    pub fn new(
        id: String,
        table_state: DeltaTableState,
        log_store: LogStoreRef,
        predicate: Expr,
    ) -> datafusion_common::Result<Self> {
        let version = table_state.version();
        Ok(Self {
            id,
            predicate,
            log_store,
            table_state,

            version,
        })
    }

    pub fn predicate(&self) -> Expr {
        self.predicate.clone()
    }

    pub fn state(&self) -> DeltaTableState {
        self.table_state.clone()
    }

    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }
}

impl Eq for FindFilesNode {}

impl PartialEq<Self> for FindFilesNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for FindFilesNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.id.as_bytes());
        state.finish();
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
        &ONLY_FILES_DF_SCHEMA
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
            "FindFiles[id={}, predicate=\"{}\", version={}]",
            &self.id, self.predicate, self.version,
        )
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        self.with_exprs_and_inputs(exprs.to_vec(), inputs.to_vec())
            .unwrap()
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        Ok(self.clone())
    }
}
