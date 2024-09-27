use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};

use crate::delta_datafusion::find_files::{
    scan_table_by_files, scan_table_by_partitions, ONLY_FILES_SCHEMA,
};
use crate::delta_datafusion::FindFilesExprProperties;
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;

pub struct FindFilesExec {
    predicate: Expr,
    state: DeltaTableState,
    log_store: LogStoreRef,
    plan_properties: PlanProperties,
}

impl FindFilesExec {
    pub fn new(state: DeltaTableState, log_store: LogStoreRef, predicate: Expr) -> Result<Self> {
        Ok(Self {
            predicate,
            log_store,
            state,
            plan_properties: PlanProperties::new(
                EquivalenceProperties::new(ONLY_FILES_SCHEMA.clone()),
                Partitioning::RoundRobinBatch(num_cpus::get()),
                ExecutionMode::Bounded,
            ),
        })
    }
}

struct FindFilesStream<'a> {
    mem_stream: BoxStream<'a, Result<RecordBatch>>,
}

impl<'a> FindFilesStream<'a> {
    pub fn new(mem_stream: BoxStream<'a, Result<RecordBatch>>) -> Result<Self> {
        Ok(Self { mem_stream })
    }
}

impl<'a> RecordBatchStream for FindFilesStream<'a> {
    fn schema(&self) -> SchemaRef {
        ONLY_FILES_SCHEMA.clone()
    }
}

impl<'a> Stream for FindFilesStream<'a> {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().mem_stream.poll_next_unpin(cx)
    }
}

impl Debug for FindFilesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FindFilesExec[predicate=\"{}\"]", self.predicate)
    }
}

impl DisplayAs for FindFilesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FindFilesExec[predicate=\"{}\"]", self.predicate)
    }
}

impl ExecutionPlan for FindFilesExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        ONLY_FILES_SCHEMA.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "Children cannot be replaced in FindFilesExec".to_string(),
            ));
        }

        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let current_metadata = self.state.metadata();
        let mut expr_properties = FindFilesExprProperties {
            partition_only: true,
            partition_columns: current_metadata.partition_columns.clone(),
            result: Ok(()),
        };

        TreeNode::visit(&self.predicate, &mut expr_properties)?;
        expr_properties.result?;

        if expr_properties.partition_only {
            let actions_table = self.state.add_actions_table(true)?;
            let predicate = self.predicate.clone();
            let schema = actions_table.schema();
            let mem_stream =
                MemoryStream::try_new(vec![actions_table.clone()], schema.clone(), None)?
                    .and_then(move |batch| scan_table_by_partitions(batch, predicate.clone()))
                    .boxed();

            Ok(Box::pin(FindFilesStream::new(mem_stream)?))
        } else {
            let ctx = SessionContext::new();
            let state = ctx.state();
            let table_state = self.state.clone();
            let predicate = self.predicate.clone();
            let output_files =
                scan_table_by_files(table_state, self.log_store.clone(), state, predicate);

            let mem_stream = output_files.into_stream().boxed();
            Ok(Box::pin(FindFilesStream::new(mem_stream)?))
        }
    }
}
