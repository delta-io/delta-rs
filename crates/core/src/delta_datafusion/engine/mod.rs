use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::TaskContext;
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
}

impl DataFusionEngine {
    pub fn new_from_session(session: &dyn Session) -> Arc<Self> {
        Self::new(session.task_ctx(), Handle::current()).into()
    }

    pub fn new_from_context(ctx: Arc<TaskContext>) -> Arc<Self> {
        Self::new(ctx, Handle::current()).into()
    }

    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        let storage = Arc::new(DataFusionStorageHandler::new(ctx.clone(), handle.clone()));
        let formats = Arc::new(DataFusionFileFormatHandler::new(ctx, handle));
        Self { storage, formats }
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
}
