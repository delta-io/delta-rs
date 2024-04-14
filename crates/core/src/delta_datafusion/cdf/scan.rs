use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_physical_expr::{Partitioning, PhysicalSortExpr};
use lazy_static::lazy_static;

/// Change type column name
pub const CHANGE_TYPE_COL: &str = "_change_type";
/// Commit version column name
pub const COMMIT_VERSION_COL: &str = "_commit_version";
/// Commit Timestamp column name
pub const COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

lazy_static! {
    pub static ref CDC_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        )
    ];
    pub static ref ADD_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(CHANGE_TYPE_COL, DataType::Utf8, true),
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        ),
    ];
}

/// Physical execution of a scan
#[derive(Debug)]
pub struct DeltaCdfScan {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl DeltaCdfScan {
    /// Creates a new scan
    pub fn new(plan: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        Self { plan, schema }
    }
}

impl DisplayAs for DeltaCdfScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ExecutionPlan for DeltaCdfScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.plan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.plan.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
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
