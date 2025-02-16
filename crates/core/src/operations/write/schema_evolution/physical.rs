use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_physical_expr::{Distribution, PhysicalExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use futures::Stream;
use futures::StreamExt;
use std::task::ready;

use crate::operations::cast::cast_record_batch;

#[derive(Debug)]
pub struct SchemaEvolutionExec {
    input: Arc<dyn ExecutionPlan>,
    new_schema: SchemaRef,
    add_missing_columns: bool,
    safe_cast: bool,
}

impl SchemaEvolutionExec {
    /// Create a new SchemaEvolutionExec Node
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        new_schema: SchemaRef,
        add_missing_columns: bool,
        safe_cast: bool,
    ) -> Self {
        SchemaEvolutionExec {
            input,
            new_schema,
            add_missing_columns,
            // I wonder since we use logical plans, this should be the default,
            // because if we didn't add new columns, why are we still casting.
            // DataFusion can logically coerce these types now
            safe_cast,
        }
    }
}

impl DisplayAs for SchemaEvolutionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SchemaEvolution",)?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for SchemaEvolutionExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.new_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "SchemaEvolutionExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(SchemaEvolutionExec::new(
            children[0].clone(),
            self.new_schema.clone(),
            self.add_missing_columns,
            self.safe_cast,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        dbg!(input.schema());
        dbg!(self.schema());
        let stream = SchemaEvolutionStream::new(
            input,
            self.schema(),
            self.add_missing_columns,
            self.safe_cast,
        );
        Ok(Box::pin(stream))
    }
}

struct SchemaEvolutionStream {
    input: SendableRecordBatchStream,
    new_schema: SchemaRef,
    add_missing: bool,
    safe_cast: bool,
}

impl SchemaEvolutionStream {
    pub fn new(
        input: SendableRecordBatchStream,
        new_schema: SchemaRef,
        add_missing: bool,
        safe_cast: bool,
    ) -> Self {
        SchemaEvolutionStream {
            new_schema,
            input,
            add_missing,
            safe_cast,
        }
    }
}

impl Stream for SchemaEvolutionStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll;

        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let casted_batch = cast_record_batch(
                        &batch,
                        self.new_schema.clone(),
                        self.safe_cast,
                        self.add_missing,
                    )?;

                    poll = Poll::Ready(Some(Ok(casted_batch)));
                    break;
                }
                value => {
                    poll = Poll::Ready(value);
                    break;
                }
            }
        }
        poll
    }
}

impl RecordBatchStream for SchemaEvolutionStream {
    fn schema(&self) -> SchemaRef {
        self.new_schema.clone()
    }
}