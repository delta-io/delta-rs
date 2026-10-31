//! Negative-zero normalizing wrapper around the kernel's [`ArrowEvaluationHandler`].
//!
//! Arrow's comparison kernels order floats with the IEEE 754 `totalOrder` predicate, under
//! which `-0.0 < 0.0`. SQL semantics (and delta-spark) treat `-0.0 = 0.0`. Without
//! normalization, the kernel's data-skipping filter prunes files whose float min/max stats
//! are `-0.0` for predicates such as `f = 0`, silently dropping rows (delta-rs#4990).
//!
//! Predicate literals are normalized when the query filter is converted to a kernel
//! predicate (see `datafusion_scalar_to_scalar`); this handler normalizes the *data* side by
//! rewriting `-0.0` to `0.0` in the float columns of each batch before predicate evaluation.

use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, RecordBatch, StructArray,
};
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{ExpressionRef, PredicateRef, Scalar};
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{
    DeltaResult, EngineData, EvaluationHandler, ExpressionEvaluator, PredicateEvaluator,
};

use crate::kernel::ARROW_HANDLER;

/// An [`EvaluationHandler`] that delegates to the kernel's [`ArrowEvaluationHandler`] but
/// normalizes `-0.0` to `0.0` in float data before predicate evaluation, restoring SQL
/// comparison semantics over arrow's `totalOrder` comparison kernels.
#[derive(Debug)]
pub(crate) struct NormalizingEvaluationHandler;

impl EvaluationHandler for NormalizingEvaluationHandler {
    fn new_expression_evaluator(
        &self,
        input_schema: SchemaRef,
        expression: ExpressionRef,
        output_type: DataType,
    ) -> DeltaResult<Arc<dyn ExpressionEvaluator>> {
        ARROW_HANDLER.new_expression_evaluator(input_schema, expression, output_type)
    }

    fn new_predicate_evaluator(
        &self,
        input_schema: SchemaRef,
        predicate: PredicateRef,
    ) -> DeltaResult<Arc<dyn PredicateEvaluator>> {
        let inner = ARROW_HANDLER.new_predicate_evaluator(input_schema, predicate)?;
        Ok(Arc::new(NormalizingPredicateEvaluator { inner }))
    }

    fn null_row(&self, output_schema: SchemaRef) -> DeltaResult<Box<dyn EngineData>> {
        ARROW_HANDLER.null_row(output_schema)
    }

    fn create_many(
        &self,
        schema: SchemaRef,
        rows: &[&[Scalar]],
    ) -> DeltaResult<Box<dyn EngineData>> {
        ARROW_HANDLER.create_many(schema, rows)
    }
}

/// Wraps a [`PredicateEvaluator`] to rewrite `-0.0` to `0.0` in the float columns of each
/// input batch before delegating.
struct NormalizingPredicateEvaluator {
    inner: Arc<dyn PredicateEvaluator>,
}

impl std::fmt::Debug for NormalizingPredicateEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NormalizingPredicateEvaluator")
            .finish_non_exhaustive()
    }
}

impl PredicateEvaluator for NormalizingPredicateEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        let Some(arrow_data) = batch.any_ref().downcast_ref::<ArrowEngineData>() else {
            return self.inner.evaluate(batch);
        };
        match normalize_batch_zeros(arrow_data.record_batch()) {
            Some(normalized) => self.inner.evaluate(&ArrowEngineData::new(normalized)),
            None => self.inner.evaluate(batch),
        }
    }
}

/// Returns a copy of `batch` with every float `-0.0` rewritten to `0.0`, or `None` when the
/// batch contains no negative zeros (the common case, avoiding reallocation).
fn normalize_batch_zeros(batch: &RecordBatch) -> Option<RecordBatch> {
    let mut changed = false;
    let columns = batch
        .columns()
        .iter()
        .map(|column| match normalize_array_zeros(column) {
            Some(normalized) => {
                changed = true;
                normalized
            }
            None => column.clone(),
        })
        .collect::<Vec<_>>();
    if !changed {
        return None;
    }
    RecordBatch::try_new(batch.schema(), columns).ok()
}

/// Returns a copy of `array` with every float `-0.0` rewritten to `0.0`, recursing into
/// struct columns, or `None` when the array contains no negative zeros.
fn normalize_array_zeros(array: &ArrayRef) -> Option<ArrayRef> {
    match array.data_type() {
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>()?;
            if !arr
                .values()
                .iter()
                .any(|v| *v == 0.0 && v.is_sign_negative())
            {
                return None;
            }
            let normalized = Float32Array::from_iter(
                arr.iter()
                    .map(|v| v.map(|x| if x == 0.0 { 0.0 } else { x })),
            );
            Some(Arc::new(normalized))
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            if !arr
                .values()
                .iter()
                .any(|v| *v == 0.0 && v.is_sign_negative())
            {
                return None;
            }
            let normalized = Float64Array::from_iter(
                arr.iter()
                    .map(|v| v.map(|x| if x == 0.0 { 0.0 } else { x })),
            );
            Some(Arc::new(normalized))
        }
        ArrowDataType::Struct(_) => {
            let arr = array.as_any().downcast_ref::<StructArray>()?;
            let mut changed = false;
            let columns = arr
                .columns()
                .iter()
                .map(|column| match normalize_array_zeros(column) {
                    Some(normalized) => {
                        changed = true;
                        normalized
                    }
                    None => column.clone(),
                })
                .collect::<Vec<_>>();
            if !changed {
                return None;
            }
            Some(Arc::new(StructArray::new(
                arr.fields().clone(),
                columns,
                arr.nulls().cloned(),
            )))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::arrow::array::Int64Array;
    use delta_kernel::arrow::datatypes::{Field, Fields, Schema};
    use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
    use delta_kernel::expressions::{ColumnName, Expression, Predicate};

    fn stats_batch(max_f: f64, min_f: f64) -> RecordBatch {
        let f_field = Arc::new(Field::new("f", ArrowDataType::Float64, true));
        let struct_f = ArrowDataType::Struct(Fields::from(vec![f_field.clone()]));
        let max_values = StructArray::from(vec![(
            f_field.clone(),
            Arc::new(Float64Array::from(vec![max_f])) as ArrayRef,
        )]);
        let min_values = StructArray::from(vec![(
            f_field.clone(),
            Arc::new(Float64Array::from(vec![min_f])) as ArrayRef,
        )]);
        let stats = StructArray::from(vec![
            (
                Arc::new(Field::new("maxValues", struct_f.clone(), true)),
                Arc::new(max_values) as ArrayRef,
            ),
            (
                Arc::new(Field::new("minValues", struct_f, true)),
                Arc::new(min_values) as ArrayRef,
            ),
        ]);
        let stats_field = Field::new("stats_parsed", stats.data_type().clone(), true);
        let schema = Arc::new(Schema::new(vec![stats_field]));
        RecordBatch::try_new(schema, vec![Arc::new(stats) as ArrayRef]).unwrap()
    }

    /// The kernel's data-skipping predicate for `f = 0` is
    /// `maxValues.f >= 0 AND minValues.f <= 0`; with stats min/max of `-0.0` both
    /// comparisons must hold or the file is wrongly pruned (delta-rs#4990).
    #[test]
    fn skipping_predicate_keeps_negative_zero_stats() {
        let batch = stats_batch(-0.0, -0.0);
        let max_col = Expression::Column(ColumnName::new(["stats_parsed", "maxValues", "f"]));
        let min_col = Expression::Column(ColumnName::new(["stats_parsed", "minValues", "f"]));
        let pred = Predicate::and(
            Predicate::ge(max_col, Scalar::Double(0.0)),
            Predicate::le(min_col, Scalar::Double(0.0)),
        );

        // The raw arrow evaluation prunes the file (totalOrder: -0.0 < 0.0).
        let raw = evaluate_predicate(&pred, &batch, false).unwrap();
        assert!(!raw.value(0), "precondition: raw arrow evaluation prunes");

        // After normalizing -0.0 to 0.0 the file is kept.
        let normalized = normalize_batch_zeros(&batch).expect("batch contains -0.0");
        let kept = evaluate_predicate(&pred, &normalized, false).unwrap();
        assert!(kept.value(0), "normalized stats must keep the file");
    }

    #[test]
    fn normalize_batch_zeros_leaves_other_values_untouched() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("f", ArrowDataType::Float64, true),
                Field::new("i", ArrowDataType::Int64, true),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![Some(-0.0), Some(1.5), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .unwrap();
        let normalized = normalize_batch_zeros(&batch).expect("batch contains -0.0");
        let f = normalized
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(f.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(f.value(1), 1.5);
        assert!(f.is_null(2));
        let i = normalized
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(i.values(), &[1, 2, 3]);
    }

    #[test]
    fn normalize_batch_zeros_returns_none_without_negative_zeros() {
        let batch = stats_batch(0.0, 1.5);
        assert!(normalize_batch_zeros(&batch).is_none());
    }
}
