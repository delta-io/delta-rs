//! A DataFusion implementation of the delta-kernel [`PlanExecutor`].
//!
//! The kernel does no I/O or data processing itself. It builds an [`Operation`] and hands it to
//! the executor. [`DataFusionPlanExecutor`] dispatches on the operation:
//!
//! - [`Operation::IoOperation`] goes to [`io`], which runs a single request against the
//!   executor's [`ObjectStore`].
//! - [`Operation::QueryPlan`] goes to [`query`], which compiles the kernel [`Plan`] into a
//!   DataFusion `LogicalPlan`, runs it on the executor's [`SessionContext`], and returns the
//!   batches through [`stream`] as [`PlanResult::Data`].
//!
//! `execute_op` is synchronous, so the executor blocks on the tokio [`Handle`] it was built with.
//! Call it from a thread that does not drive that runtime.
//!
//! [`Plan`]: delta_kernel::plans::ir::plan::Plan

#![warn(missing_docs)]

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use delta_kernel::DeltaResult;
use delta_kernel::plans::{Operation, PlanExecutor, PlanResult};
use object_store::ObjectStore;
use tokio::runtime::Handle;

pub mod error;
pub mod io;
pub mod query;
pub mod stream;

pub use error::{Error, Result};

/// Executes kernel declarative plans on DataFusion.
///
/// Build one per table (or per object store) and pass it to the kernel wherever a
/// `&dyn PlanExecutor` is expected.
#[derive(Clone)]
pub struct DataFusionPlanExecutor {
    ctx: SessionContext,
    store: Arc<dyn ObjectStore>,
    handle: Handle,
}

impl std::fmt::Debug for DataFusionPlanExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionPlanExecutor")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl DataFusionPlanExecutor {
    /// Creates an executor that runs plans on `ctx`, reads files from `store`, and blocks on
    /// `handle` for the async parts.
    pub fn new(ctx: SessionContext, store: Arc<dyn ObjectStore>, handle: Handle) -> Self {
        Self { ctx, store, handle }
    }

    /// The DataFusion session plans run on.
    pub fn session(&self) -> &SessionContext {
        &self.ctx
    }

    /// The object store I/O operations and scans read from.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The tokio runtime the executor blocks on.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl PlanExecutor for DataFusionPlanExecutor {
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult> {
        match op {
            Operation::IoOperation(op) => io::execute(self, op),
            Operation::QueryPlan(plan) => query::execute(self, plan),
        }
    }
}
