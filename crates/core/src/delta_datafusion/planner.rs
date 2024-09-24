//! Custom planners for datafusion so that you can convert custom nodes, can be used
//! to trace custom metrics in an operation
//!
//! # Example
//!
//! #[derive(Clone)]
//! struct MergeMetricExtensionPlanner {}
//!
//! #[async_trait]
//! impl ExtensionPlanner for MergeMetricExtensionPlanner {
//!     async fn plan_extension(
//!         &self,
//!         planner: &dyn PhysicalPlanner,
//!         node: &dyn UserDefinedLogicalNode,
//!         _logical_inputs: &[&LogicalPlan],
//!         physical_inputs: &[Arc<dyn ExecutionPlan>],
//!         session_state: &SessionState,
//!     ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {}
//!
//! let merge_planner = DeltaPlanner::<MergeMetricExtensionPlanner> {
//!     extension_planner: MergeMetricExtensionPlanner {}
//! };
//!
//! let state = state.with_query_planner(Arc::new(merge_planner));
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::{
    execution::{context::QueryPlanner, session_state::SessionState},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner},
};
use datafusion_expr::LogicalPlan;

use crate::delta_datafusion::DataFusionResult;

/// Deltaplanner
pub struct DeltaPlanner<T: ExtensionPlanner> {
    /// custom extension planner
    pub extension_planner: T,
}

#[async_trait]
impl<T: ExtensionPlanner + Send + Sync + 'static + Clone> QueryPlanner for DeltaPlanner<T> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let planner = Arc::new(Box::new(DefaultPhysicalPlanner::with_extension_planners(
            vec![Arc::new(self.extension_planner.clone())],
        )));
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
