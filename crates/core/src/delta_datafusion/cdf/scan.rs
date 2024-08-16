use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};

/// Physical execution of a scan
#[derive(Debug, Clone)]
pub struct DeltaCdfScan {
    plan: Arc<dyn ExecutionPlan>,
}

impl DeltaCdfScan {
    /// Creates a new scan
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

impl DisplayAs for DeltaCdfScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ExecutionPlan for DeltaCdfScan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema().clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.plan.clone().with_new_children(_children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.plan.execute(partition, context)
    }
}
