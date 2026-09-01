use std::sync::Arc;

use datafusion::catalog::Session;
#[cfg(feature = "datafusion-declarative-scan")]
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::TaskContext;
#[cfg(feature = "datafusion-declarative-scan")]
use datafusion::execution::context::SessionState;
#[cfg(feature = "datafusion-declarative-scan")]
use datafusion::physical_plan::{SendableRecordBatchStream, execute_stream};
#[cfg(feature = "datafusion-declarative-scan")]
use datafusion_executor::{DataFusionExecutor, to_datafusion_plan};
#[cfg(feature = "datafusion-declarative-scan")]
use delta_kernel::PlanExecutor;
#[cfg(feature = "datafusion-declarative-scan")]
use delta_kernel::plans::ir::plan::Plan;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};
use tokio::runtime::Handle;

pub(crate) use self::expressions::*;
use self::file_formats::DataFusionFileFormatHandler;
pub use self::storage::AsObjectStoreUrl;
use self::storage::DataFusionStorageHandler;
use crate::kernel::ARROW_HANDLER;

mod expressions;
mod file_formats;
mod storage;

/// A Datafusion based Kernel Engine
#[derive(Clone)]
pub struct DataFusionEngine {
    storage: Arc<DataFusionStorageHandler>,
    formats: Arc<DataFusionFileFormatHandler>,
    #[cfg(feature = "datafusion-declarative-scan")]
    plan_executor: Arc<dyn PlanExecutor>,
    #[cfg(feature = "datafusion-declarative-scan")]
    session_state: Option<SessionState>,
}

impl DataFusionEngine {
    /// Create an engine from a DataFusion [`Session`], reusing its task context and the
    /// current Tokio runtime handle. This is the convenient entry point when wiring the
    /// kernel engine into an active query session.
    pub fn new_from_session(session: &dyn Session) -> Arc<Self> {
        #[cfg(feature = "datafusion-declarative-scan")]
        {
            let handle = Handle::current();
            let mut engine = Self::new(session.task_ctx(), handle.clone());
            if let Some(state) = session.as_any().downcast_ref::<SessionState>() {
                engine.session_state = Some(state.clone());
                engine.plan_executor = Arc::new(DataFusionExecutor::new_with_state(
                    state.clone(),
                    engine.storage.clone(),
                    handle,
                ));
            }
            Arc::new(engine)
        }

        #[cfg(not(feature = "datafusion-declarative-scan"))]
        {
            Self::new(session.task_ctx(), Handle::current()).into()
        }
    }

    /// Create an engine directly from a DataFusion [`TaskContext`], using the current Tokio
    /// runtime handle. Useful inside physical operators where only the task context is
    /// available.
    pub fn new_from_context(ctx: Arc<TaskContext>) -> Arc<Self> {
        Self::new(ctx, Handle::current()).into()
    }

    /// Create an engine from an explicit [`TaskContext`] and Tokio runtime [`Handle`].
    ///
    /// The other constructors delegate here; call this directly when you need to bind the
    /// engine to a specific runtime handle rather than the ambient one.
    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        let storage = Arc::new(DataFusionStorageHandler::new(ctx.clone(), handle.clone()));
        let formats = Arc::new(DataFusionFileFormatHandler::new(ctx, handle));
        Self {
            storage,
            formats,
            #[cfg(feature = "datafusion-declarative-scan")]
            plan_executor: Arc::new(()),
            #[cfg(feature = "datafusion-declarative-scan")]
            session_state: None,
        }
    }

    #[cfg(feature = "datafusion-declarative-scan")]
    pub(crate) fn can_execute_declarative_plan(&self) -> bool {
        self.session_state.is_some()
    }

    /// Execute a kernel plan directly through this engine's DataFusion session.
    #[cfg(feature = "datafusion-declarative-scan")]
    pub(crate) async fn execute_declarative_plan(
        &self,
        plan: &Plan,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut session_state = self.session_state.clone().ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "declarative plans require a DataFusion SessionState".to_string(),
            )
        })?;
        let options = session_state.config_mut().options_mut();
        // DataFusion 55's leaf-expression pushdown can retain a struct alias on multiple nested
        // projections and produce duplicate unqualified fields. Kernel scan sources already
        // declare their exact nested columns, so disable it only for this plan's cloned state.
        options.optimizer.enable_leaf_expression_pushdown = false;
        // Let EnsureRequirements use any available row-count estimates to avoid repartitioning
        // inputs that are unlikely to fill even one output batch.
        options
            .execution
            .use_row_number_estimates_to_optimize_partitioning = true;

        let logical_plan = to_datafusion_plan(plan)?;
        let task_ctx = session_state.task_ctx();
        let physical_plan = session_state.create_physical_plan(&logical_plan).await?;
        execute_stream(physical_plan, task_ctx)
    }
}

impl Engine for DataFusionEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        ARROW_HANDLER.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.formats.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.formats.clone()
    }

    #[cfg(feature = "datafusion-declarative-scan")]
    fn plan_executor(&self) -> Arc<dyn PlanExecutor> {
        self.plan_executor.clone()
    }
}
