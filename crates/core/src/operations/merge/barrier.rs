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
//!
//! DV mode collects changed row positions and forwards batches without per-file buffering.
//! Downstream duplicate validation and filtering decide which rows are written.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array, builder::UInt64Builder};
use arrow::datatypes::SchemaRef;
use dashmap::{DashMap, DashSet};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{Distribution, PhysicalExpr};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, RecordBatchStream, ReplaceChildrenOptions,
    SendableRecordBatchStream, apply_expression_roots,
};
use futures::{Stream, StreamExt};
use roaring::RoaringTreemap;

use crate::{
    DeltaTableError,
    delta_datafusion::get_path_column,
    operations::merge::{
        TARGET_DELETE_COLUMN, TARGET_INSERT_COLUMN, TARGET_ROW_ORDINAL_IN_FILE_COLUMN,
        TARGET_UPDATE_COLUMN,
    },
};

pub(crate) type BarrierSurvivorSet = Arc<DashSet<String>>;
pub(crate) type DeletedRows = Arc<DashMap<String, RoaringTreemap>>;

#[derive(Debug)]
/// Physical Node for the MergeBarrier
/// Batches to this node must be repartitioned on col('deleta_rs_path').
/// Each record batch then undergoes further partitioning based on the file column to it's corresponding barrier
pub struct MergeBarrierExec {
    input: Arc<dyn ExecutionPlan>,
    file_column: Arc<String>,
    survivors: BarrierSurvivorSet,
    expr: Arc<dyn PhysicalExpr>,
    deleted_rows: Option<DeletedRows>,
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
            survivors: Arc::new(DashSet::new()),
            expr,
            deleted_rows: None,
        }
    }

    pub(super) fn with_deletion_vectors(mut self, enabled: bool) -> Self {
        self.deleted_rows = enabled.then(|| Arc::new(DashMap::new()));
        self
    }

    pub(super) fn deleted_rows(&self) -> Option<DeletedRows> {
        self.deleted_rows.clone()
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

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::KeyPartitioned(vec![Arc::clone(
            &self.expr,
        )])])
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "MergeBarrierExec wrong number of children".to_string(),
            ));
        }
        let mut replacement = MergeBarrierExec::new(
            children[0].clone(),
            self.file_column.clone(),
            self.expr.clone(),
        );
        replacement.deleted_rows = self.deleted_rows.clone();
        Ok(Arc::new(replacement))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(MergeBarrierStream::new(
            input,
            self.schema(),
            self.survivors.clone(),
            self.file_column.clone(),
            self.deleted_rows.clone(),
        )))
    }

    fn apply_expressions(
        &self,
        expr_rewriter: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        apply_expression_roots([&self.expr], expr_rewriter)
    }
}

impl DisplayAs for MergeBarrierExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
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
    deleted_rows: Option<DeletedRows>,
}

impl MergeBarrierStream {
    pub fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        survivors: BarrierSurvivorSet,
        file_column: Arc<String>,
        deleted_rows: Option<DeletedRows>,
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
            deleted_rows,
        }
    }
}

fn collect_deleted_rows(
    batch: &RecordBatch,
    file_column: &str,
    deleted_rows: &DeletedRows,
) -> DataFusionResult<()> {
    let files = get_path_column(batch, file_column)?;
    let ordinals = batch
        .column_by_name(TARGET_ROW_ORDINAL_IN_FILE_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            DataFusionError::Execution("DV MERGE requires physical UInt64 row indexes".into())
        })?;
    let updates = batch.column_by_name(TARGET_UPDATE_COLUMN).ok_or_else(|| {
        DataFusionError::Execution("DV MERGE requires the target update marker".into())
    })?;
    let deletes = batch.column_by_name(TARGET_DELETE_COLUMN).ok_or_else(|| {
        DataFusionError::Execution("DV MERGE requires the target delete marker".into())
    })?;
    let mut batch_deletions: HashMap<&str, RoaringTreemap> = HashMap::new();
    for (row, key) in files.keys().iter().enumerate() {
        if !updates.is_null(row) && !deletes.is_null(row) {
            continue;
        }
        let key = key.ok_or_else(|| {
            DataFusionError::Execution("Changed target row has no physical file".into())
        })? as usize;
        if files.values().is_null(key) || ordinals.is_null(row) {
            return Err(DataFusionError::Execution(
                "Changed target row has no physical file or row index".into(),
            ));
        }
        let ordinal = ordinals.value(row);
        if ordinal > i64::MAX as u64 {
            return Err(DataFusionError::Execution(format!(
                "DV row index {ordinal} exceeds i64::MAX"
            )));
        }
        batch_deletions
            .entry(files.values().value(key))
            .or_default()
            .insert(ordinal);
    }
    for (path, rows) in batch_deletions {
        *deleted_rows.entry(path.to_owned()).or_default() |= rows;
    }
    Ok(())
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
                            if let Some(deleted_rows) = &self.deleted_rows {
                                if let Err(error) =
                                    collect_deleted_rows(&batch, &self.file_column, deleted_rows)
                                {
                                    self.state = State::Abort;
                                    return Poll::Ready(Some(Err(error)));
                                }
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            let file_dictionary = get_path_column(&batch, &self.file_column)?;

                            // For each record batch, the key for a file path is not stable.
                            // We can iterate through the dictionary and lookup the correspond string for each record and then lookup the correct `file_partition` for that value.
                            // However this approach exposes the cost of hashing so we want to minimize that as much as possible.
                            // A map from an arrow dictionary key to the correct index of `file_partition` is created for each batch that's processed.
                            // This ensures we only need to hash each file path at most once per batch.
                            let mut key_map = Vec::with_capacity(file_dictionary.len());

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

                            let mut indices: Vec<_> =
                                Vec::with_capacity(self.file_partitions.len());
                            for _ in 0..self.file_partitions.len() {
                                indices.push(UInt64Builder::with_capacity(batch.num_rows()));
                            }

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
                                                Ok(arrow::compute::take(
                                                    c.as_ref(),
                                                    &indices,
                                                    None,
                                                )?)
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
                        for part in &self.file_partitions {
                            match part.state {
                                PartitionBarrierState::Closed => {}
                                PartitionBarrierState::Open => {
                                    if let Some(file_name) = &part.file_name {
                                        self.survivors.insert(file_name.to_owned());
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

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd)]
pub(crate) struct MergeBarrier {
    pub input: LogicalPlan,
    pub expr: Expr,
    pub file_column: Arc<String>,
    pub deletion_vectors: bool,
}

impl UserDefinedLogicalNodeCore for MergeBarrier {
    fn name(&self) -> &str {
        "MergeBarrier"
    }

    fn inputs(&self) -> Vec<&datafusion::logical_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![self.expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeBarrier")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<datafusion::logical_expr::LogicalPlan>,
    ) -> DataFusionResult<Self> {
        Ok(MergeBarrier {
            input: inputs[0].clone(),
            file_column: self.file_column.clone(),
            expr: exprs[0].clone(),
            deletion_vectors: self.deletion_vectors,
        })
    }
}

pub(crate) fn find_node<T: ExecutionPlan>(
    parent: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    //! Used to locate a Node::<T> after the planner converts the logical node
    if parent.downcast_ref::<T>().is_some() {
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
    use super::{BarrierSurvivorSet, MergeBarrierStream, collect_deleted_rows};
    use crate::operations::merge::MergeBarrierExec;
    use crate::operations::merge::TARGET_ROW_ORDINAL_IN_FILE_COLUMN;
    use crate::operations::merge::{
        TARGET_DELETE_COLUMN, TARGET_INSERT_COLUMN, TARGET_UPDATE_COLUMN,
    };
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_array::RecordBatch;
    use arrow_array::StringArray;
    use arrow_array::{BooleanArray, DictionaryArray, UInt16Array, UInt64Array};
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use dashmap::DashMap;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    #[allow(deprecated)]
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{Stream, StreamExt};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    fn deletion_batch(
        paths: Vec<Option<&str>>,
        ordinals: Vec<Option<u64>>,
        updates: Vec<Option<bool>>,
    ) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "__delta_rs_path",
                ArrowDataType::Dictionary(ArrowDataType::UInt16.into(), ArrowDataType::Utf8.into()),
                true,
            ),
            Field::new(
                TARGET_ROW_ORDINAL_IN_FILE_COLUMN,
                ArrowDataType::UInt64,
                true,
            ),
            Field::new(TARGET_UPDATE_COLUMN, ArrowDataType::Boolean, true),
            Field::new(TARGET_DELETE_COLUMN, ArrowDataType::Boolean, true),
            Field::new(TARGET_INSERT_COLUMN, ArrowDataType::Boolean, true),
        ]));
        let mut dictionary =
            arrow_array::builder::StringDictionaryBuilder::<arrow::datatypes::UInt16Type>::new();
        for path in paths {
            match path {
                Some(path) => {
                    dictionary.append(path).unwrap();
                }
                None => dictionary.append_null(),
            }
        }
        let count = ordinals.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(dictionary.finish()),
                Arc::new(UInt64Array::from(ordinals)),
                Arc::new(BooleanArray::from(updates)),
                Arc::new(BooleanArray::from(vec![false; count])),
                Arc::new(BooleanArray::from(vec![false; count])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn deleted_rows_preserve_physical_indexes_and_dictionary_identity() {
        let deleted = Arc::new(DashMap::new());
        collect_deleted_rows(
            &deletion_batch(
                vec![Some("a"), Some("b"), Some("a"), None],
                vec![Some(0), Some(7), Some(0), None],
                vec![None, None, None, Some(false)],
            ),
            "__delta_rs_path",
            &deleted,
        )
        .unwrap();
        collect_deleted_rows(
            &deletion_batch(
                vec![Some("b"), Some("a")],
                vec![Some(9), Some(3)],
                vec![None, None],
            ),
            "__delta_rs_path",
            &deleted,
        )
        .unwrap();
        assert_eq!(
            deleted.get("a").unwrap().iter().collect::<Vec<_>>(),
            vec![0, 3]
        );
        assert_eq!(
            deleted.get("b").unwrap().iter().collect::<Vec<_>>(),
            vec![7, 9]
        );
    }

    #[test]
    fn invalid_changed_row_does_not_partially_update_deleted_rows() {
        let deleted = Arc::new(DashMap::new());
        let batch = deletion_batch(
            vec![Some("a"), Some("a")],
            vec![Some(1), Some(i64::MAX as u64 + 1)],
            vec![None, None],
        );
        assert!(collect_deleted_rows(&batch, "__delta_rs_path", &deleted).is_err());
        assert!(deleted.is_empty());
        let batch = deletion_batch(vec![None], vec![Some(1)], vec![None]);
        assert!(collect_deleted_rows(&batch, "__delta_rs_path", &deleted).is_err());
        assert!(deleted.is_empty());
    }

    #[test]
    fn dv_stream_returns_batches_without_buffering_untouched_files() {
        let paths: Vec<_> = (0..64)
            .map(|index| format!("file-{index}.parquet"))
            .collect();
        let batches: Vec<_> = paths
            .iter()
            .enumerate()
            .map(|(index, path)| {
                deletion_batch(
                    vec![Some(path)],
                    vec![Some(7)],
                    vec![if index == paths.len() - 1 {
                        None
                    } else {
                        Some(false)
                    }],
                )
            })
            .collect();
        let schema = batches[0].schema();
        let input = futures::stream::iter(batches.clone().into_iter().map(Ok))
            .chain(futures::stream::pending());
        let input = Box::pin(RecordBatchStreamAdapter::new(schema.clone(), input));
        let deleted_rows = Arc::new(DashMap::new());
        let mut stream = MergeBarrierStream::new(
            input,
            schema,
            BarrierSurvivorSet::default(),
            Arc::new("__delta_rs_path".into()),
            Some(deleted_rows.clone()),
        );
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        for expected in &batches {
            let actual = match Pin::new(&mut stream).poll_next(&mut context) {
                Poll::Ready(Some(Ok(batch))) => batch,
                result => {
                    panic!("Expected one batch without waiting for the input to finish: {result:?}")
                }
            };
            assert_eq!(&actual, expected);
            assert!(stream.map.is_empty());
            assert!(
                stream
                    .file_partitions
                    .iter()
                    .all(|file| file.buffer.is_empty())
            );
        }
        assert!(Pin::new(&mut stream).poll_next(&mut context).is_pending());
        assert_eq!(deleted_rows.len(), 1);
        assert_eq!(
            deleted_rows
                .get(paths.last().unwrap())
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![7]
        );
    }

    #[test]
    fn dv_stream_rejects_invalid_positions_before_forwarding() {
        let batch = deletion_batch(vec![Some("file.parquet")], vec![None], vec![None]);
        let schema = batch.schema();
        let input = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter([Ok(batch)]),
        ));
        let deleted_rows = Arc::new(DashMap::new());
        let mut stream = MergeBarrierStream::new(
            input,
            schema,
            BarrierSurvivorSet::default(),
            Arc::new("__delta_rs_path".into()),
            Some(deleted_rows.clone()),
        );
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        assert!(matches!(
            Pin::new(&mut stream).poll_next(&mut context),
            Poll::Ready(Some(Err(_)))
        ));
        assert!(matches!(
            Pin::new(&mut stream).poll_next(&mut context),
            Poll::Ready(None)
        ));
        assert!(deleted_rows.is_empty());
    }

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

        assert!(!survivors.contains(&"file0".to_string()));
        assert!(survivors.contains(&"file1".to_string()));
        assert!(survivors.contains(&"file2".to_string()));
        assert_eq!(survivors.len(), 2);
    }

    #[tokio::test]
    async fn test_barrier_changing_indices() {
        // Validate implementation can handle different dictionary indices between batches

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
        let expected = vec![
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
        let exec = MemorySourceConfig::try_new_exec(&[input], schema.clone(), None).unwrap();

        let task_ctx = Arc::new(TaskContext::default());
        let merge =
            MergeBarrierExec::new(exec, Arc::new("__delta_rs_path".to_string()), repartition);

        let survivors = merge.survivors();
        #[allow(deprecated)]
        let coalescence = CoalesceBatchesExec::new(Arc::new(merge), 100);
        let mut stream = coalescence.execute(0, task_ctx).unwrap();
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
