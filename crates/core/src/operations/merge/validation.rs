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
use crate::operations::merge::{
    CardinalityClass, TARGET_MATCH_CARDINALITY_CLASS_COLUMN, TARGET_ROW_INDEX_COLUMN,
};

#[derive(Debug)]
pub(crate) struct MergeValidationExec {
    input: Arc<dyn ExecutionPlan>,
    row_index_expr: Arc<dyn PhysicalExpr>,
}

impl MergeValidationExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            input,
            row_index_expr: expr,
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

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(vec![self.row_index_expr.clone()]); 1]
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
            self.row_index_expr.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(MergeValidationStream::new(input, self.schema())))
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

struct MergeValidationStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    target_row_state: HashMap<u64, (i32, i32)>,
}

impl MergeValidationStream {
    fn new(input: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        Self {
            schema,
            input,
            target_row_state: HashMap::new(),
        }
    }

    fn validate_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        let target_row_index_col =
            batch
                .column_by_name(TARGET_ROW_INDEX_COLUMN)
                .ok_or_else(|| {
                    DataFusionError::External(Box::new(DeltaTableError::Generic(
                        "Required column __delta_rs_target_row_index is missing".to_string(),
                    )))
                })?;

        let target_row_index_array = target_row_index_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::External(Box::new(DeltaTableError::Generic(
                    "Column __delta_rs_target_row_index is not UInt64".to_string(),
                )))
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
            if !target_row_index_array.is_null(row) && !cardinality_class_array.is_null(row) {
                let target_row_index = target_row_index_array.value(row);
                let cardinality_class = cardinality_class_array.value(row);

                let (candidate_count, invalidating_count) = self
                    .target_row_state
                    .entry(target_row_index)
                    .or_insert((0, 0));

                if cardinality_class != CardinalityClass::Ignore as i32 {
                    *candidate_count += 1;
                }

                if cardinality_class == CardinalityClass::DuplicateInvalidating as i32 {
                    *invalidating_count += 1;
                }

                if *candidate_count > 1 && *invalidating_count > 0 {
                    return Err(DataFusionError::External(Box::new(
                        DeltaTableError::Generic(format!(
                            "Merge matched a single target row (index: {}) with multiple source rows",
                            target_row_index
                        )),
                    )));
                }
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
    pub input: LogicalPlan,
    pub expr: Expr,
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
        vec![self.expr.clone()]
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
            expr: exprs[0].clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    fn batch(target_indices: Vec<u64>, classes: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TARGET_ROW_INDEX_COLUMN, DataType::UInt64, false),
            Field::new(
                TARGET_MATCH_CARDINALITY_CLASS_COLUMN,
                DataType::Int32,
                false,
            ),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(target_indices)),
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
        let mut s = MergeValidationStream::new(empty_stream(b.schema()), b.schema());
        s.validate_batch(b)
    }

    #[test]
    fn test_duplicate_fails() {
        let b = batch(
            vec![10, 10],
            vec![
                CardinalityClass::DuplicateInvalidating as i32,
                CardinalityClass::DuplicateInvalidating as i32,
            ],
        );

        let err = validate(&b).expect_err("expected duplicate violation");
        assert!(
            err.to_string()
                .contains("Merge matched a single target row (index: 10)")
        );
    }

    #[test]
    fn test_no_duplicate_passes() {
        let b = batch(
            vec![10, 11],
            vec![
                CardinalityClass::DuplicateInvalidating as i32,
                CardinalityClass::DuplicateInvalidating as i32,
            ],
        );

        validate(&b).expect("should not fail for non-duplicate matches");
    }

    #[test]
    fn test_unconditional_duplicate_deletes_passes() {
        let b = batch(
            vec![30, 30],
            vec![
                CardinalityClass::MatchedUnconditionalDelete as i32,
                CardinalityClass::MatchedUnconditionalDelete as i32,
            ],
        );

        validate(&b).expect("unconditional duplicate deletes should be allowed");
    }

    #[test]
    fn test_unconditional_and_conditional_duplicate_deletes_fails() {
        let b = batch(
            vec![40, 40],
            vec![
                CardinalityClass::MatchedUnconditionalDelete as i32,
                CardinalityClass::DuplicateInvalidating as i32,
            ],
        );

        let err = validate(&b).expect_err("expected duplicate violation");
        assert!(
            err.to_string().contains(
                "Merge matched a single target row (index: 40) with multiple source rows"
            )
        );
    }
}
