//! Merge Barrier determines which files have modifications during the merge operation
//!
//! For every unique path in the input stream, a barrier is established. If any
//! single record for a file contains any delete, update, or insert operations
//! then the barrier for the file is opened and can be sent downstream.
//! To determine if a file contains zero changes, the input stream is
//! exhausted. Afterwards, records are then dropped.
//!
//! Bookkeeping is maintained to determine which files have modifications so
//! they can be removed from the delta log.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use arrow_array::{
    builder::UInt64Builder, types::UInt16Type, ArrayRef, DictionaryArray, RecordBatch, StringArray,
};
use arrow_schema::SchemaRef;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::{Distribution, PhysicalExpr};
use futures::{Stream, StreamExt};

use crate::{
    operations::merge::{TARGET_DELETE_COLUMN, TARGET_INSERT_COLUMN, TARGET_UPDATE_COLUMN},
    DeltaTableError,
};

#[derive(Debug)]
/// Physical Node for the MergeBarrier
/// Batches to this node must be repartitioned on col('deleta_rs_path').
/// Each record batch then undergoes further partitioning based on the file column to it's corresponding barrier
pub struct MergeBarrierExec {
    input: Arc<dyn ExecutionPlan>,
    file_column: Arc<String>,
    survivors: Arc<Mutex<Vec<String>>>,
    expr: Arc<dyn PhysicalExpr>,
}

impl MergeBarrierExec {
    /// Create a new MergeBarrierExec Node
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        file_column: Arc<String>,
        expr: Arc<dyn PhysicalExpr>,
        survivors: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        MergeBarrierExec {
            input,
            file_column,
            survivors,
            expr,
        }
    }

    /// Files that have modifications to them and need to removed from the delta log
    pub fn survivors(&self) -> Arc<Mutex<Vec<String>>> {
        self.survivors.clone()
    }
}

impl ExecutionPlan for MergeBarrierExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> datafusion_physical_expr::Partitioning {
        self.input.output_partitioning()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(vec![self.expr.clone()]); 1]
    }

    fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MergeBarrierExec::new(
            children[0].clone(),
            self.file_column.clone(),
            self.expr.clone(),
            self.survivors.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(MergeBarrierStream::new(
            input,
            self.schema(),
            self.survivors.clone(),
            self.file_column.clone(),
        )))
    }
}

impl DisplayAs for MergeBarrierExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MergeBarrier",)?;
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
enum State {
    Feed,
    Drain,
    Finalize,
    Abort,
    Done,
}

#[derive(Debug)]
enum PartitionBarrierState {
    Closed,
    Open,
}

#[derive(Debug)]
struct MergeBarrierPartition {
    state: PartitionBarrierState,
    buffer: Vec<RecordBatch>,
    file_name: Option<String>,
}

impl MergeBarrierPartition {
    pub fn new(file_name: Option<String>) -> Self {
        MergeBarrierPartition {
            state: PartitionBarrierState::Closed,
            buffer: Vec::new(),
            file_name,
        }
    }

    pub fn feed(&mut self, batch: RecordBatch) {
        match self.state {
            PartitionBarrierState::Closed => {
                let delete_count = get_count(&batch, TARGET_DELETE_COLUMN);
                let update_count = get_count(&batch, TARGET_UPDATE_COLUMN);
                let insert_count = get_count(&batch, TARGET_INSERT_COLUMN);
                self.buffer.push(batch);

                if insert_count > 0 || update_count > 0 || delete_count > 0 {
                    self.state = PartitionBarrierState::Open;
                }
            }
            PartitionBarrierState::Open => {
                self.buffer.push(batch);
            }
        }
    }

    pub fn drain(&mut self) -> Option<RecordBatch> {
        match self.state {
            PartitionBarrierState::Closed => None,
            PartitionBarrierState::Open => self.buffer.pop(),
        }
    }
}

struct MergeBarrierStream {
    schema: SchemaRef,
    state: State,
    input: SendableRecordBatchStream,
    file_column: Arc<String>,
    survivors: Arc<Mutex<Vec<String>>>,
    map: HashMap<String, usize>,
    file_partitions: Vec<MergeBarrierPartition>,
}

impl MergeBarrierStream {
    pub fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        survivors: Arc<Mutex<Vec<String>>>,
        file_column: Arc<String>,
    ) -> Self {
        // Always allocate for a null bucket at index 0;
        let file_partitions = vec![MergeBarrierPartition::new(None)];

        MergeBarrierStream {
            schema,
            state: State::Feed,
            input,
            file_column,
            survivors,
            file_partitions,
            map: HashMap::new(),
        }
    }
}

fn get_count(batch: &RecordBatch, column: &str) -> usize {
    batch.column_by_name(column).unwrap().null_count()
}

impl Stream for MergeBarrierStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                State::Feed => {
                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let path_column = &self.file_column;
                            let file_dictionary = batch
                                .column_by_name(path_column)
                                .unwrap()
                                .as_any()
                                .downcast_ref::<DictionaryArray<UInt16Type>>()
                                .ok_or(DeltaTableError::Generic(format!(
                                    "Unable to downcast column {}",
                                    path_column
                                )))?
                                .downcast_dict::<StringArray>()
                                .ok_or(DeltaTableError::Generic(format!(
                                    "Unable to downcast column {}",
                                    path_column
                                )))?;

                            // For each record batch, the key for a file path is not stable.
                            // We can iterate through the dictionary and lookup the correspond string for each record and then lookup the correct `file_partition` for that value.
                            // However this approach exposes the cost of hashing so we want to minimize that as much as possible.
                            // A map from an arrow dictionary key to the correct index of `file_partition` is created for each batch that's processed.
                            // This ensures we only need to hash each file path at most once per batch.
                            let mut key_map = Vec::new();

                            for file_name in file_dictionary.values().into_iter() {
                                let key = match file_name {
                                    Some(name) => {
                                        if !self.map.contains_key(name) {
                                            let key = self.file_partitions.len();
                                            let part_stream =
                                                MergeBarrierPartition::new(Some(name.to_string()));
                                            self.file_partitions.push(part_stream);
                                            self.map.insert(name.to_string(), key);
                                        }
                                        // Safe unwrap due to the above
                                        *self.map.get(name).unwrap()
                                    }
                                    None => 0,
                                };
                                key_map.push(key)
                            }

                            let mut indices: Vec<_> = (0..(self.file_partitions.len()))
                                .map(|_| UInt64Builder::with_capacity(batch.num_rows()))
                                .collect();

                            for (idx, key) in file_dictionary.keys().iter().enumerate() {
                                match key {
                                    Some(value) => {
                                        indices[key_map[value as usize]].append_value(idx as u64)
                                    }
                                    None => indices[0].append_value(idx as u64),
                                }
                            }

                            let batches: Vec<Result<(usize, RecordBatch), DataFusionError>> =
                                indices
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(partition, mut indices)| {
                                        let indices = indices.finish();
                                        (!indices.is_empty()).then_some((partition, indices))
                                    })
                                    .map(move |(partition, indices)| {
                                        // Produce batches based on indices
                                        let columns = batch
                                            .columns()
                                            .iter()
                                            .map(|c| {
                                                arrow::compute::take(c.as_ref(), &indices, None)
                                                    .map_err(DataFusionError::ArrowError)
                                            })
                                            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;

                                        let batch =
                                            RecordBatch::try_new(batch.schema(), columns).unwrap();

                                        Ok((partition, batch))
                                    })
                                    .collect();

                            for batch in batches {
                                match batch {
                                    Ok((partition, batch)) => {
                                        self.file_partitions[partition].feed(batch);
                                    }
                                    Err(err) => {
                                        self.state = State::Abort;
                                        return Poll::Ready(Some(Err(err)));
                                    }
                                }
                            }

                            self.state = State::Drain;
                            continue;
                        }
                        Poll::Ready(Some(Err(err))) => {
                            self.state = State::Abort;
                            return Poll::Ready(Some(Err(err)));
                        }
                        Poll::Ready(None) => {
                            self.state = State::Finalize;
                            continue;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                State::Drain => {
                    for part in &mut self.file_partitions {
                        if let Some(batch) = part.drain() {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }

                    self.state = State::Feed;
                    continue;
                }
                State::Finalize => {
                    for part in &mut self.file_partitions {
                        if let Some(batch) = part.drain() {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }

                    {
                        let mut lock = self.survivors.lock().unwrap();
                        for part in &self.file_partitions {
                            match part.state {
                                PartitionBarrierState::Closed => {}
                                PartitionBarrierState::Open => {
                                    if let Some(file_name) = &part.file_name {
                                        lock.push(file_name.to_owned())
                                    }
                                }
                            }
                        }
                    }

                    self.state = State::Done;
                    continue;
                }
                State::Abort => return Poll::Ready(None),
                State::Done => return Poll::Ready(None),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.input.size_hint().1)
    }
}

impl RecordBatchStream for MergeBarrierStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct MergeBarrier {
    pub input: LogicalPlan,
    pub expr: Expr,
    pub file_column: Arc<String>,
}

impl UserDefinedLogicalNodeCore for MergeBarrier {
    fn name(&self) -> &str {
        "MergeBarrier"
    }

    fn inputs(&self) -> Vec<&datafusion_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![self.expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeBarrier")
    }

    fn from_template(
        &self,
        exprs: &[datafusion_expr::Expr],
        inputs: &[datafusion_expr::LogicalPlan],
    ) -> Self {
        MergeBarrier {
            input: inputs[0].clone(),
            file_column: self.file_column.clone(),
            expr: exprs[0].clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    // TODO:
    // Need to check if when null exist in dictionary keys
    // Need to check nulls when in dictionary values
    async fn test_barrier() {}
}
