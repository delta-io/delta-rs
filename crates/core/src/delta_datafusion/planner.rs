//! Custom planners for datafusion so that you can convert custom nodes, can be used
//! to trace custom metrics in an operation
//!
//! # Example
//!
//! #[derive(Clone)]
//! struct MergeMetricExtensionPlanner {}
//!
//! #[macro@async_trait]
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
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::{
    execution::{context::QueryPlanner, session_state::SessionState},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner},
};

use crate::delta_datafusion::DataFusionResult;
use crate::delta_datafusion::data_validation::DataValidationExtensionPlanner;
use crate::operations::delete::DeleteMetricExtensionPlanner;
use crate::operations::merge::MergeMetricExtensionPlanner;
use crate::operations::update::UpdateMetricExtensionPlanner;
use crate::operations::write::metrics::WriteMetricExtensionPlanner;

const DELTA_EXTENSION_PLANNERS: LazyLock<Vec<Arc<dyn ExtensionPlanner + Send + Sync>>> =
    LazyLock::new(|| {
        vec![
            MergeMetricExtensionPlanner::new(),
            WriteMetricExtensionPlanner::new(),
            DeleteMetricExtensionPlanner::new(),
            UpdateMetricExtensionPlanner::new(),
            DataValidationExtensionPlanner::new(),
        ]
    });

const DELTA_PLANNER: LazyLock<Arc<DeltaPlanner>> = LazyLock::new(|| Arc::new(DeltaPlanner));

/// Deltaplanner
#[derive(Debug)]
pub struct DeltaPlanner;

impl DeltaPlanner {
    pub fn new() -> Arc<Self> {
        DELTA_PLANNER.clone()
    }
}

#[async_trait]
impl QueryPlanner for DeltaPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let planner = Arc::new(Box::new(DefaultPhysicalPlanner::with_extension_planners(
            vec![DeltaExtensionPlanner::new()],
        )));
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

pub struct DeltaExtensionPlanner;

impl DeltaExtensionPlanner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ExtensionPlanner for DeltaExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        for ext_planner in DELTA_EXTENSION_PLANNERS.iter() {
            if let Some(plan) = ext_planner
                .plan_extension(
                    planner,
                    node,
                    logical_inputs,
                    physical_inputs,
                    session_state,
                )
                .await?
            {
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }
}
