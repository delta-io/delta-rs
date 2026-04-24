use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, Int32Array, RecordBatch, UInt64Array};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::Distribution;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

use crate::DeltaTableError;
use crate::delta_datafusion::get_path_column;
use crate::operations::merge::{MatchParticipationClass, TARGET_MATCH_CARDINALITY_CLASS_COLUMN};

#[derive(Debug)]
pub(crate) struct MergeValidationExec {
    input: Arc<dyn ExecutionPlan>,
    file_expr: Arc<dyn PhysicalExpr>,
    file_column: Arc<String>,
    row_ordinal_column: Arc<String>,
}

impl MergeValidationExec {
    pub(super) fn new(
        input: Arc<dyn ExecutionPlan>,
        file_expr: Arc<dyn PhysicalExpr>,
        file_column: Arc<String>,
        row_ordinal_column: Arc<String>,
    ) -> Self {
        Self {
            input,
            file_expr,
            file_column,
            row_ordinal_column,
        }
    }
}

impl ExecutionPlan for MergeValidationExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(vec![self.file_expr.clone()]); 1]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "MergeValidationExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.file_expr.clone(),
            Arc::clone(&self.file_column),
            Arc::clone(&self.row_ordinal_column),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(MergeValidationStream::new(
            input,
            self.schema(),
            Arc::clone(&self.file_column),
            Arc::clone(&self.row_ordinal_column),
        )))
    }
}

impl DisplayAs for MergeValidationExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MergeValidation")?;
                Ok(())
            }
        }
    }
}

#[derive(Default)]
struct TargetMatchState {
    matched_action_count: i32,
    unconditional_delete_count: i32,
}

struct MergeValidationStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    file_column: Arc<String>,
    row_ordinal_column: Arc<String>,
    file_id_by_path: HashMap<String, usize>,
    file_paths: Vec<String>,
    target_row_state: HashMap<(usize, u64), TargetMatchState>,
}

impl MergeValidationStream {
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        file_column: Arc<String>,
        row_ordinal_column: Arc<String>,
    ) -> Self {
        Self {
            schema,
            input,
            file_column,
            row_ordinal_column,
            file_id_by_path: HashMap::new(),
            file_paths: Vec::new(),
            target_row_state: HashMap::new(),
        }
    }

    fn intern_file_id(&mut self, file_path: &str) -> usize {
        if let Some(file_id) = self.file_id_by_path.get(file_path) {
            *file_id
        } else {
            let file_id = self.file_paths.len();
            let file_path = file_path.to_string();
            self.file_id_by_path.insert(file_path.clone(), file_id);
            self.file_paths.push(file_path);
            file_id
        }
    }

    fn validate_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        let file_dictionary =
            get_path_column(batch, self.file_column.as_str()).map_err(DataFusionError::from)?;
        let file_keys = file_dictionary.keys();
        let mut key_map = Vec::with_capacity(file_dictionary.values().len());
        for file_name in file_dictionary.values().into_iter() {
            key_map.push(file_name.map(|path| self.intern_file_id(path)));
        }

        let row_ordinal_col = batch
            .column_by_name(self.row_ordinal_column.as_str())
            .ok_or_else(|| {
                DataFusionError::External(Box::new(DeltaTableError::Generic(format!(
                    "Required column {} is missing",
                    self.row_ordinal_column
                ))))
            })?;

        let row_ordinal_array = row_ordinal_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::External(Box::new(DeltaTableError::Generic(format!(
                    "Column {} is not UInt64",
                    self.row_ordinal_column
                ))))
            })?;

        let cardinality_class_col = batch
            .column_by_name(TARGET_MATCH_CARDINALITY_CLASS_COLUMN)
            .ok_or_else(|| {
                DataFusionError::External(Box::new(DeltaTableError::Generic(
                    "Required column __delta_rs_match_cardinality_class is missing".to_string(),
                )))
            })?;

        let cardinality_class_array = cardinality_class_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::External(Box::new(DeltaTableError::Generic(
                    "Column __delta_rs_match_cardinality_class is not Int32".to_string(),
                )))
            })?;

        for row in 0..batch.num_rows() {
            if file_keys.is_null(row)
                || row_ordinal_array.is_null(row)
                || cardinality_class_array.is_null(row)
            {
                continue;
            }

            let file_key = file_keys.value(row) as usize;
            let Some(file_id) = key_map[file_key] else {
                continue;
            };

            let row_ordinal = row_ordinal_array.value(row);
            let cardinality_class = cardinality_class_array.value(row);

            let state = self
                .target_row_state
                .entry((file_id, row_ordinal))
                .or_default();

            if cardinality_class == MatchParticipationClass::MatchedAction as i32 {
                state.matched_action_count += 1;
            } else if cardinality_class
                == MatchParticipationClass::MatchedUnconditionalDelete as i32
            {
                state.unconditional_delete_count += 1;
            }

            if state.matched_action_count > 1
                || (state.matched_action_count > 0 && state.unconditional_delete_count > 0)
            {
                let file_path = self.file_paths[file_id].as_str();
                return Err(DataFusionError::External(Box::new(
                    DeltaTableError::Generic(format!(
                        "MERGE matched a target row with multiple source rows that satisfy duplicate relevant WHEN MATCHED clauses (file: {}, row ordinal in file: {})",
                        file_path, row_ordinal
                    )),
                )));
            }
        }

        Ok(())
    }
}

impl Stream for MergeValidationStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if let Err(err) = self.validate_batch(&batch) {
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for MergeValidationStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd)]
pub(crate) struct MergeValidation {
    pub(super) input: LogicalPlan,
    pub(super) file_expr: Expr,
    pub(super) file_column: Arc<String>,
    pub(super) row_ordinal_column: Arc<String>,
}

impl UserDefinedLogicalNodeCore for MergeValidation {
    fn name(&self) -> &str {
        "MergeValidation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.file_expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeValidation")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        Ok(Self {
            input: inputs[0].clone(),
            file_expr: exprs[0].clone(),
            file_column: Arc::clone(&self.file_column),
            row_ordinal_column: Arc::clone(&self.row_ordinal_column),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, Int32Array, RecordBatch, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, UInt16Type};
    use arrow_array::builder::StringDictionaryBuilder;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use crate::delta_datafusion::PATH_COLUMN;
    use crate::operations::merge::TARGET_ROW_ORDINAL_IN_FILE_COLUMN;

    fn batch(target_indices: Vec<u64>, classes: Vec<i32>) -> RecordBatch {
        batch_with_files(
            vec!["a.parquet"; target_indices.len()],
            target_indices,
            classes,
        )
    }

    fn batch_with_files(
        file_paths: Vec<&str>,
        target_ordinals: Vec<u64>,
        classes: Vec<i32>,
    ) -> RecordBatch {
        let mut file_paths_builder = StringDictionaryBuilder::<UInt16Type>::new();
        for file_path in file_paths {
            file_paths_builder.append_value(file_path);
        }
        let file_paths: DictionaryArray<UInt16Type> = file_paths_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                PATH_COLUMN,
                DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
                false,
            ),
            Field::new(TARGET_ROW_ORDINAL_IN_FILE_COLUMN, DataType::UInt64, false),
            Field::new(
                TARGET_MATCH_CARDINALITY_CLASS_COLUMN,
                DataType::Int32,
                false,
            ),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(file_paths),
                Arc::new(UInt64Array::from(target_ordinals)),
                Arc::new(Int32Array::from(classes)),
            ],
        )
        .expect("failed to build test batch")
    }

    fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
        let stream = futures::stream::iter(Vec::<DataFusionResult<RecordBatch>>::new());
        Box::pin(RecordBatchStreamAdapter::new(schema, Box::pin(stream)))
    }

    fn validate(b: &RecordBatch) -> DataFusionResult<()> {
        validate_batches(std::slice::from_ref(b))
    }

    fn validate_batches(batches: &[RecordBatch]) -> DataFusionResult<()> {
        let schema = batches
            .first()
            .expect("expected at least one batch")
            .schema();
        let mut s = MergeValidationStream::new(
            empty_stream(schema.clone()),
            schema,
            Arc::new(PATH_COLUMN.to_string()),
            Arc::new(TARGET_ROW_ORDINAL_IN_FILE_COLUMN.to_string()),
        );
        for batch in batches {
            s.validate_batch(batch)?;
        }
        Ok(())
    }

    #[test]
    fn test_duplicate_fails() {
        let b = batch(
            vec![10, 10],
            vec![
                MatchParticipationClass::MatchedAction as i32,
                MatchParticipationClass::MatchedAction as i32,
            ],
        );

        let err = validate(&b).expect_err("expected duplicate violation");
        assert!(err.to_string().contains(
            "duplicate relevant WHEN MATCHED clauses (file: a.parquet, row ordinal in file: 10)"
        ));
    }

    #[test]
    fn test_no_duplicate_passes() {
        let b = batch(
            vec![10, 11],
            vec![
                MatchParticipationClass::MatchedAction as i32,
                MatchParticipationClass::MatchedAction as i32,
            ],
        );

        validate(&b).expect("should not fail for non-duplicate matches");
    }

    #[test]
    fn test_unconditional_duplicate_deletes_passes() {
        let b = batch(
            vec![30, 30],
            vec![
                MatchParticipationClass::MatchedUnconditionalDelete as i32,
                MatchParticipationClass::MatchedUnconditionalDelete as i32,
            ],
        );

        validate(&b).expect("unconditional duplicate deletes should be allowed");
    }

    #[test]
    fn test_unconditional_and_conditional_duplicate_deletes_fails() {
        let b = batch(
            vec![40, 40],
            vec![
                MatchParticipationClass::MatchedUnconditionalDelete as i32,
                MatchParticipationClass::MatchedAction as i32,
            ],
        );

        let err = validate(&b).expect_err("expected duplicate violation");
        assert!(err.to_string().contains(
            "duplicate relevant WHEN MATCHED clauses (file: a.parquet, row ordinal in file: 40)"
        ));
    }

    #[test]
    fn test_matched_noop_rows_do_not_count_as_duplicates() {
        let b = batch(
            vec![10, 10],
            vec![
                MatchParticipationClass::MatchedNoop as i32,
                MatchParticipationClass::MatchedNoop as i32,
            ],
        );

        validate(&b).expect("matched no op rows should not be treated as duplicates");
    }

    #[test]
    fn test_one_matched_action_and_one_matched_noop_passes() {
        let b = batch(
            vec![10, 10],
            vec![
                MatchParticipationClass::MatchedNoop as i32,
                MatchParticipationClass::MatchedAction as i32,
            ],
        );

        validate(&b).expect("matched noop + one matched action should pass");
    }

    #[test]
    fn test_same_row_ordinal_in_different_files_does_not_collide() {
        let b = batch_with_files(
            vec!["a.parquet", "b.parquet"],
            vec![1, 1],
            vec![
                MatchParticipationClass::MatchedAction as i32,
                MatchParticipationClass::MatchedAction as i32,
            ],
        );

        validate(&b).expect("row ordinals must be scoped by file");
    }

    #[test]
    fn test_duplicate_across_batches_with_changed_dictionary_indices_fails() {
        let first = batch_with_files(
            vec!["a.parquet"],
            vec![7],
            vec![MatchParticipationClass::MatchedAction as i32],
        );
        let second = batch_with_files(
            vec!["b.parquet", "a.parquet"],
            vec![99, 7],
            vec![
                MatchParticipationClass::MatchedNoop as i32,
                MatchParticipationClass::MatchedAction as i32,
            ],
        );

        let err = validate_batches(&[first, second]).expect_err("expected duplicate violation");
        assert!(err.to_string().contains(
            "duplicate relevant WHEN MATCHED clauses (file: a.parquet, row ordinal in file: 7)"
        ));
    }

    #[test]
    fn test_different_files_across_batches_with_same_dictionary_index_do_not_collide() {
        let first = batch_with_files(
            vec!["a.parquet"],
            vec![7],
            vec![MatchParticipationClass::MatchedAction as i32],
        );
        let second = batch_with_files(
            vec!["b.parquet"],
            vec![7],
            vec![MatchParticipationClass::MatchedAction as i32],
        );

        validate_batches(&[first, second])
            .expect("dictionary keys must be remapped per batch before cross batch validation");
    }
}
