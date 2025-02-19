use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    execution::SessionState,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_plan::{metrics::MetricBuilder, ExecutionPlan};

use crate::delta_datafusion::{logical::MetricObserver, physical::MetricObserverExec};

pub(crate) const SOURCE_COUNT_ID: &str = "write_source_count";
pub(crate) const SOURCE_COUNT_METRIC: &str = "num_source_rows";

#[derive(Clone, Debug)]
pub(crate) struct WriteMetricExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for WriteMetricExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(metric_observer) = node.as_any().downcast_ref::<MetricObserver>() {
            if metric_observer.id.eq(SOURCE_COUNT_ID) {
                return Ok(Some(MetricObserverExec::try_new(
                    SOURCE_COUNT_ID.into(),
                    physical_inputs,
                    |batch, metrics| {
                        MetricBuilder::new(metrics)
                            .global_counter(SOURCE_COUNT_METRIC)
                            .add(batch.num_rows());
                    },
                )?));
            }
        }
        Ok(None)
    }
}
