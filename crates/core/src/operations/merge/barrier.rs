//! Merge Barrier determines which files have modifications during the merge operation
//!
//! For every unique path in the input stream, a barrier is established. If any
//! single record for a file contains any delete, update, or insert operations
//! then the barrier for the file is opened and can be sent downstream.
//! To determine if a file contains zero changes, the input stream is
//! exhausted. Afterwards, records are then dropped.
//!
//! Bookkeeping is maintained to determine which files have modifications, so
//! they can be removed from the delta log.

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use arrow_array::{builder::UInt64Builder, ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::{Distribution, PhysicalExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

use crate::{
    delta_datafusion::get_path_column,
    operations::merge::{TARGET_DELETE_COLUMN, TARGET_INSERT_COLUMN, TARGET_UPDATE_COLUMN},
    DeltaTableError,
};

pub(crate) type BarrierSurvivorSet = Arc<Mutex<HashSet<String>>>;

#[derive(Debug)]
/// Physical Node for the MergeBarrier
/// Batches to this node must be repartitioned on col('deleta_rs_path').
/// Each record batch then undergoes further partitioning based on the file column to it's corresponding barrier
pub struct MergeBarrierExec {
    input: Arc<dyn ExecutionPlan>,
    file_column: Arc<String>,
    survivors: BarrierSurvivorSet,
    expr: Arc<dyn PhysicalExpr>,
}

impl MergeBarrierExec {
    /// Create a new MergeBarrierExec Node
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        file_column: Arc<String>,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        MergeBarrierExec {
            input,
            file_column,
            survivors: Arc::new(Mutex::new(HashSet::new())),
            expr,
        }
    }

    /// Files that have modifications to them and need to removed from the delta log
    pub fn survivors(&self) -> BarrierSurvivorSet {
        self.survivors.clone()
    }
}

impl ExecutionPlan for MergeBarrierExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(vec![self.expr.clone()]); 1]
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
                "MergeBarrierExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(MergeBarrierExec::new(
            children[0].clone(),
            self.file_column.clone(),
            self.expr.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
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

    pub fn feed(&mut self, batch: RecordBatch) -> DataFusionResult<()> {
        match self.state {
            PartitionBarrierState::Closed => {
                let delete_count = get_count(&batch, TARGET_DELETE_COLUMN)?;
                let update_count = get_count(&batch, TARGET_UPDATE_COLUMN)?;
                let insert_count = get_count(&batch, TARGET_INSERT_COLUMN)?;
                self.buffer.push(batch);

                if insert_count > 0 || update_count > 0 || delete_count > 0 {
                    self.state = PartitionBarrierState::Open;
                }
            }
            PartitionBarrierState::Open => {
                self.buffer.push(batch);
            }
        }
        Ok(())
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
    survivors: BarrierSurvivorSet,
    map: HashMap<String, usize>,
    file_partitions: Vec<MergeBarrierPartition>,
}

impl MergeBarrierStream {
    pub fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        survivors: BarrierSurvivorSet,
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

fn get_count(batch: &RecordBatch, column: &str) -> DataFusionResult<usize> {
    batch
        .column_by_name(column)
        .map(|array| array.null_count())
        .ok_or_else(|| {
            DataFusionError::External(Box::new(DeltaTableError::Generic(
                "Required operation column is missing".to_string(),
            )))
        })
}

impl Stream for MergeBarrierStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                State::Feed => {
                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let file_dictionary = get_path_column(&batch, &self.file_column)?;

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
                                                    .map_err(|err| {
                                                        DataFusionError::ArrowError(err, None)
                                                    })
                                            })
                                            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;

                                        // This unwrap is safe since the processed batched has the same schema
                                        let batch =
                                            RecordBatch::try_new(batch.schema(), columns).unwrap();

                                        Ok((partition, batch))
                                    })
                                    .collect();

                            for batch in batches {
                                match batch {
                                    Ok((partition, batch)) => {
                                        self.file_partitions[partition].feed(batch)?;
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
                        let mut lock = self.survivors.lock().map_err(|_| {
                            DataFusionError::External(Box::new(DeltaTableError::Generic(
                                "MergeBarrier mutex is poisoned".to_string(),
                            )))
                        })?;
                        for part in &self.file_partitions {
                            match part.state {
                                PartitionBarrierState::Closed => {}
                                PartitionBarrierState::Open => {
                                    if let Some(file_name) = &part.file_name {
                                        lock.insert(file_name.to_owned());
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
        self.with_exprs_and_inputs(exprs.to_vec(), inputs.to_vec())
            .unwrap()
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion_expr::Expr>,
        inputs: Vec<datafusion_expr::LogicalPlan>,
    ) -> DataFusionResult<Self> {
        Ok(MergeBarrier {
            input: inputs[0].clone(),
            file_column: self.file_column.clone(),
            expr: exprs[0].clone(),
        })
    }
}

pub(crate) fn find_node<T: 'static>(
    parent: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    //! Used to locate a Node::<T> after the planner converts the logical node
    if parent.as_any().downcast_ref::<T>().is_some() {
        return Some(parent.to_owned());
    }

    for child in &parent.children() {
        let res = find_node::<T>(child);
        if res.is_some() {
            return res;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use crate::operations::merge::MergeBarrierExec;
    use crate::operations::merge::{
        TARGET_DELETE_COLUMN, TARGET_INSERT_COLUMN, TARGET_UPDATE_COLUMN,
    };
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_array::RecordBatch;
    use arrow_array::StringArray;
    use arrow_array::{DictionaryArray, UInt16Array};
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_physical_expr::expressions::Column;
    use futures::StreamExt;
    use std::sync::Arc;

    use super::BarrierSurvivorSet;

    #[tokio::test]
    async fn test_barrier() {
        // Validate that files without modifications are dropped and that files with changes passthrough
        // File 0: No Changes
        // File 1: Contains an update
        // File 2: Contains a delete
        // null (id: 3): is a insert

        let schema = get_schema();
        let keys = UInt16Array::from(vec![Some(0), Some(1), Some(2), None]);
        let values = StringArray::from(vec![Some("file0"), Some("file1"), Some("file2")]);
        let dict = DictionaryArray::new(keys, Arc::new(values));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["0", "1", "2", "3"])),
                Arc::new(dict),
                //insert column
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(false),
                    None,
                ])),
                //update column
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    None,
                    Some(false),
                    Some(false),
                ])),
                //delete column
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    None,
                    Some(false),
                ])),
            ],
        )
        .unwrap();

        let (actual, survivors) = execute(vec![batch]).await;
        let expected = vec![
            "+----+-----------------+--------------------------+--------------------------+--------------------------+",
            "| id | __delta_rs_path | __delta_rs_target_insert | __delta_rs_target_update | __delta_rs_target_delete |",
            "+----+-----------------+--------------------------+--------------------------+--------------------------+",
            "| 1  | file1           | false                    |                          | false                    |",
            "| 2  | file2           | false                    | false                    |                          |",
            "| 3  |                 |                          | false                    | false                    |",
            "+----+-----------------+--------------------------+--------------------------+--------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);

        let s = survivors.lock().unwrap();
        assert!(!s.contains(&"file0".to_string()));
        assert!(s.contains(&"file1".to_string()));
        assert!(s.contains(&"file2".to_string()));
        assert_eq!(s.len(), 2);
    }

    #[tokio::test]
    async fn test_barrier_changing_indicies() {
        // Validate implementation can handle different dictionary indicies between batches

        let schema = get_schema();
        let mut batches = vec![];

        // Batch 1
        let keys = UInt16Array::from(vec![Some(0), Some(1)]);
        let values = StringArray::from(vec![Some("file0"), Some("file1")]);
        let dict = DictionaryArray::new(keys, Arc::new(values));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["0", "1"])),
                Arc::new(dict),
                //insert column
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                ])),
                //update column
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                ])),
                //delete column
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                ])),
            ],
        )
        .unwrap();
        batches.push(batch);
        // Batch 2

        let keys = UInt16Array::from(vec![Some(0), Some(1)]);
        let values = StringArray::from(vec![Some("file1"), Some("file0")]);
        let dict = DictionaryArray::new(keys, Arc::new(values));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["2", "3"])),
                Arc::new(dict),
                //insert column
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                ])),
                //update column
                Arc::new(arrow::array::BooleanArray::from(vec![None, Some(false)])),
                //delete column
                Arc::new(arrow::array::BooleanArray::from(vec![Some(false), None])),
            ],
        )
        .unwrap();
        batches.push(batch);

        let (actual, _survivors) = execute(batches).await;
        let expected = vec!
            [
                "+----+-----------------+--------------------------+--------------------------+--------------------------+",
                "| id | __delta_rs_path | __delta_rs_target_insert | __delta_rs_target_update | __delta_rs_target_delete |",
                "+----+-----------------+--------------------------+--------------------------+--------------------------+",
                "| 0  | file0           | false                    | false                    | false                    |",
                "| 1  | file1           | false                    | false                    | false                    |",
                "| 2  | file1           | false                    |                          | false                    |",
                "| 3  | file0           | false                    | false                    |                          |",
                "+----+-----------------+--------------------------+--------------------------+--------------------------+",
            ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_barrier_null_paths() {
        // Arrow dictionaries are interesting since a null value can be either in the keys of the dict or in the values.
        // Validate they can be processed without issue

        let schema = get_schema();
        let keys = UInt16Array::from(vec![Some(0), None, Some(1)]);
        let values = StringArray::from(vec![Some("file1"), None]);
        let dict = DictionaryArray::new(keys, Arc::new(values));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["1", "2", "3"])),
                Arc::new(dict),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    None,
                    None,
                ])),
                Arc::new(arrow::array::BooleanArray::from(vec![false, false, false])),
                Arc::new(arrow::array::BooleanArray::from(vec![false, false, false])),
            ],
        )
        .unwrap();

        let (actual, _) = execute(vec![batch]).await;
        let expected = vec![
            "+----+-----------------+--------------------------+--------------------------+--------------------------+",
            "| id | __delta_rs_path | __delta_rs_target_insert | __delta_rs_target_update | __delta_rs_target_delete |",
            "+----+-----------------+--------------------------+--------------------------+--------------------------+",
            "| 2  |                 |                          | false                    | false                    |",
            "| 3  |                 |                          | false                    | false                    |",
            "+----+-----------------+--------------------------+--------------------------+--------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    async fn execute(input: Vec<RecordBatch>) -> (Vec<RecordBatch>, BarrierSurvivorSet) {
        let schema = get_schema();
        let repartition = Arc::new(Column::new("__delta_rs_path", 2));
        let exec = Arc::new(MemoryExec::try_new(&[input], schema.clone(), None).unwrap());

        let task_ctx = Arc::new(TaskContext::default());
        let merge =
            MergeBarrierExec::new(exec, Arc::new("__delta_rs_path".to_string()), repartition);

        let survivors = merge.survivors();
        let coalsece = CoalesceBatchesExec::new(Arc::new(merge), 100);
        let mut stream = coalsece.execute(0, task_ctx).unwrap();
        (vec![stream.next().await.unwrap().unwrap()], survivors)
    }

    fn get_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Utf8, true),
            Field::new(
                "__delta_rs_path",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt16),
                    Box::new(ArrowDataType::Utf8),
                ),
                true,
            ),
            Field::new(TARGET_INSERT_COLUMN, ArrowDataType::Boolean, true),
            Field::new(TARGET_UPDATE_COLUMN, ArrowDataType::Boolean, true),
            Field::new(TARGET_DELETE_COLUMN, ArrowDataType::Boolean, true),
        ]))
    }
}
