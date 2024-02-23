use std::any::Any;
use std::fmt::{Debug, Formatter};

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_expr::Expr;
use datafusion_physical_expr::{Partitioning, PhysicalSortExpr};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::delta_datafusion::find_files::{only_file_path_schema, scan_memory_table_batch};

pub struct FindFilesExec {
    files: Vec<String>,
    predicate: Expr,
}

impl FindFilesExec {
    pub fn new(files: Vec<String>, predicate: Expr) -> Result<Self> {
        Ok(Self { files, predicate })
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
        only_file_path_schema()
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
        write!(f, "FindFilesExec[schema={:?}, files={:?}]", 1, 2)
    }
}

impl DisplayAs for FindFilesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FindFilesExec[schema={:?}, files={:?}]", 1, 2)
    }
}

impl ExecutionPlan for FindFilesExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        only_file_path_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(0)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let array = Arc::new(StringArray::from(self.files.clone()));
        let record_batch = RecordBatch::try_new(only_file_path_schema(), vec![array])?;
        let predicate = self.predicate.clone();
        let mem_stream =
            MemoryStream::try_new(vec![record_batch.clone()], only_file_path_schema(), None)?
                .and_then(move |batch| scan_memory_table_batch(batch, predicate.clone()))
                .boxed();

        Ok(Box::pin(FindFilesStream::new(mem_stream)?))
    }
}
