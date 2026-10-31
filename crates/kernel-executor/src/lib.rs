//! A DataFusion implementation of the delta-kernel [`PlanExecutor`].
//!
//! The kernel does no I/O or data processing itself. It builds an [`Operation`] and hands it to
//! the executor. [`DataFusionPlanExecutor`] dispatches on the operation:
//!
//! - [`Operation::IoOperation`] goes to [`io`], which runs a single request through the
//!   executor's [`DataFusionStorageHandler`] or [`DataFusionFileFormatHandler`]. Both resolve
//!   the object store for a URL through the DataFusion session's object store registry.
//! - [`Operation::QueryPlan`] goes to [`query`], which translates the kernel [`Plan`] into a
//!   DataFusion `LogicalPlan`, runs it on a session built from the executor's [`TaskContext`],
//!   and returns the batches through [`stream`] as [`PlanResult::Data`].
//!
//! The kernel reaches the executor through [`Engine::plan_executor`]. Wrap it in the kernel's
//! `PlanBasedEngine` to route the handler-based `Engine` methods through the executor too.
//!
//! `execute_op` is synchronous. The executor spawns its async work on the tokio [`Handle`] it
//! was built with and waits for results over channels ([`runtime`]), so it can be called from a
//! runtime worker or from a plain thread. The runtime must be multi-threaded, or driven by a
//! thread other than the caller.
//!
//! [`Plan`]: delta_kernel::plans::ir::plan::Plan
//! [`Engine::plan_executor`]: delta_kernel::Engine::plan_executor

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::TaskContext;
use delta_kernel::DeltaResult;
use delta_kernel::plans::{Operation, PlanExecutor, PlanResult};
use tokio::runtime::Handle;

pub mod error;
pub mod expressions;
pub mod io;
pub mod query;
mod runtime;
pub mod stream;

pub use error::{Error, Result};
pub use io::file_formats::DataFusionFileFormatHandler;
pub use io::storage::{AsObjectStoreUrl, DataFusionStorageHandler};

/// Executes kernel declarative plans on DataFusion.
///
/// Build one per DataFusion session and pass it to the kernel wherever a `&dyn PlanExecutor`
/// is expected.
#[derive(Clone)]
pub struct DataFusionPlanExecutor {
    ctx: Arc<TaskContext>,
    handle: Handle,
    storage: Arc<DataFusionStorageHandler>,
    file_formats: Arc<DataFusionFileFormatHandler>,
}

impl std::fmt::Debug for DataFusionPlanExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionPlanExecutor")
            .field("session_id", &self.ctx.session_id())
            .finish_non_exhaustive()
    }
}

impl DataFusionPlanExecutor {
    /// Creates an executor from a DataFusion [`Session`], reusing its task context and the
    /// current tokio runtime handle.
    pub fn new_from_session(session: &dyn Session) -> Arc<Self> {
        Self::new(session.task_ctx(), Handle::current()).into()
    }

    /// Creates an executor from a [`TaskContext`], using the current tokio runtime handle.
    /// Useful inside physical operators where only the task context is available.
    pub fn new_from_context(ctx: Arc<TaskContext>) -> Arc<Self> {
        Self::new(ctx, Handle::current()).into()
    }

    /// Creates an executor from an explicit [`TaskContext`] and tokio runtime [`Handle`].
    ///
    /// The other constructors delegate here; call this directly to bind the executor to a
    /// specific runtime rather than the ambient one.
    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        let storage = Arc::new(DataFusionStorageHandler::new(ctx.clone(), handle.clone()));
        let file_formats = Arc::new(DataFusionFileFormatHandler::new(
            ctx.clone(),
            handle.clone(),
        ));
        Self {
            ctx,
            handle,
            storage,
            file_formats,
        }
    }

    /// The DataFusion task context: session config and runtime environment (object store
    /// registry) plans and I/O run against.
    pub fn task_ctx(&self) -> &Arc<TaskContext> {
        &self.ctx
    }

    /// The tokio runtime the executor blocks on.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// The storage handler I/O operations run through.
    pub fn storage(&self) -> &Arc<DataFusionStorageHandler> {
        &self.storage
    }

    /// The JSON and Parquet handler file reads run through.
    pub fn file_formats(&self) -> &Arc<DataFusionFileFormatHandler> {
        &self.file_formats
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
