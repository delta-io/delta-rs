//! Default Expression handler.
//!
//! Expression handling based on arrow-rs compute kernels.

use std::sync::Arc;

use arrow_arith::boolean::{and, is_null, not, or};
use arrow_arith::numeric::{add, div, mul, sub};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};

use crate::kernel::error::{DeltaResult, Error};
use crate::kernel::expressions::{scalars::Scalar, Expression};
use crate::kernel::expressions::{BinaryOperator, UnaryOperator};

// TODO leverage scalars / Datum

impl Scalar {
    /// Convert scalar to arrow array.
    pub fn to_array(&self, num_rows: usize) -> ArrayRef {
        use Scalar::*;
        match self {
            Integer(val) => Arc::new(Int32Array::from(vec![*val; num_rows])),
            Float(val) => Arc::new(Float32Array::from(vec![*val; num_rows])),
            String(val) => Arc::new(StringArray::from(vec![val.clone(); num_rows])),
            Boolean(val) => Arc::new(BooleanArray::from(vec![*val; num_rows])),
            Timestamp(val) => Arc::new(TimestampMicrosecondArray::from(vec![*val; num_rows])),
            Date(val) => Arc::new(Date32Array::from(vec![*val; num_rows])),
            Binary(val) => Arc::new(BinaryArray::from(vec![val.as_slice(); num_rows])),
            Decimal(val, precision, scale) => Arc::new(
                Decimal128Array::from(vec![*val; num_rows])
                    .with_precision_and_scale(*precision, *scale)
                    .unwrap(),
            ),
            Null(_) => todo!(),
        }
    }
}

pub(crate) fn evaluate_expression(
    expression: &Expression,
    batch: &RecordBatch,
) -> DeltaResult<ArrayRef> {
    match expression {
        Expression::Literal(scalar) => Ok(scalar.to_array(batch.num_rows())),
        Expression::Column(name) => batch
            .column_by_name(name)
            .ok_or(Error::MissingColumn(name.clone()))
            .cloned(),
        Expression::UnaryOperation { op, expr } => {
            let arr = evaluate_expression(expr.as_ref(), batch)?;
            match op {
                UnaryOperator::Not => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or(Error::Generic("expected boolean array".to_string()))?;
                    let result = not(arr)?;
                    Ok(Arc::new(result))
                }
                UnaryOperator::IsNull => {
                    let result = is_null(&arr)?;
                    Ok(Arc::new(result))
                }
            }
        }
        Expression::BinaryOperation { op, left, right } => {
            let left_arr = evaluate_expression(left.as_ref(), batch)?;
            let right_arr = evaluate_expression(right.as_ref(), batch)?;
            match op {
                BinaryOperator::Plus => {
                    add(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
                BinaryOperator::Minus => {
                    sub(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
                BinaryOperator::Multiply => {
                    mul(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
                BinaryOperator::Divide => {
                    div(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })
                }
                BinaryOperator::LessThan => {
                    let result = lt(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })?;
                    Ok(Arc::new(result))
                }
                BinaryOperator::LessThanOrEqual => {
                    let result =
                        lt_eq(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                            source: Box::new(err),
                        })?;
                    Ok(Arc::new(result))
                }
                BinaryOperator::GreaterThan => {
                    let result = gt(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })?;
                    Ok(Arc::new(result))
                }
                BinaryOperator::GreaterThanOrEqual => {
                    let result =
                        gt_eq(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                            source: Box::new(err),
                        })?;
                    Ok(Arc::new(result))
                }
                BinaryOperator::Equal => {
                    let result = eq(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })?;
                    Ok(Arc::new(result))
                }
                BinaryOperator::NotEqual => {
                    let result = neq(&left_arr, &right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })?;
                    Ok(Arc::new(result))
                }
                BinaryOperator::And => {
                    let left_arr = evaluate_expression(left.as_ref(), batch)?;
                    let left_arr = left_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or(Error::Generic("expected boolean array".to_string()))?;
                    let right_arr = evaluate_expression(right.as_ref(), batch)?;
                    let right_arr = right_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or(Error::Generic("expected boolean array".to_string()))?;
                    let result = and(left_arr, right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })?;
                    Ok(Arc::new(result))
                }
                BinaryOperator::Or => {
                    let left_arr = evaluate_expression(left.as_ref(), batch)?;
                    let left_arr = left_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or(Error::Generic("expected boolean array".to_string()))?;
                    let right_arr = evaluate_expression(right.as_ref(), batch)?;
                    let right_arr = right_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or(Error::Generic("expected boolean array".to_string()))?;
                    let result = or(left_arr, right_arr).map_err(|err| Error::GenericError {
                        source: Box::new(err),
                    })?;
                    Ok(Arc::new(result))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::ops::{Add, Div, Mul, Sub};

    #[test]
    fn test_binary_op_scalar() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = Expression::Column("a".to_string());

        let expression = Box::new(column.clone().add(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 3, 4]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().sub(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 1, 2]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().mul(Expression::Literal(Scalar::Integer(2))));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        // TODO handle type casting
        let expression = Box::new(column.div(Expression::Literal(Scalar::Integer(1))));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 2, 3]));
        assert_eq!(results.as_ref(), expected.as_ref())
    }

    #[test]
    fn test_binary_op() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(values.clone()), Arc::new(values)],
        )
        .unwrap();
        let column_a = Expression::Column("a".to_string());
        let column_b = Expression::Column("b".to_string());

        let expression = Box::new(column_a.clone().add(column_b.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().sub(column_b.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 0, 0]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().mul(column_b));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 4, 9]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_binary_cmp() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = Expression::Column("a".to_string());
        let lit = Expression::Literal(Scalar::Integer(2));

        let expression = Box::new(column.clone().lt(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().lt_eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().gt(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().gt_eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().eq(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column.clone().ne(lit.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false, true]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_logical() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(BooleanArray::from(vec![false, true])),
            ],
        )
        .unwrap();
        let column_a = Expression::Column("a".to_string());
        let column_b = Expression::Column("b".to_string());

        let expression = Box::new(column_a.clone().and(column_b.clone()));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(
            column_a
                .clone()
                .and(Expression::literal(Scalar::Boolean(true))),
        );
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(column_a.clone().or(column_b));
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = Box::new(
            column_a
                .clone()
                .or(Expression::literal(Scalar::Boolean(false))),
        );
        let results = evaluate_expression(&expression, &batch).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }
}
