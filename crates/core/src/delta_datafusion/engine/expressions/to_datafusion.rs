use std::sync::Arc;

use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{
    DataFusionError, Result as DFResult, ScalarValue, internal_datafusion_err, not_impl_err,
};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions::expr_fn::named_struct;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, expr_fn::ident, lit};
use datafusion::prelude::coalesce;
use delta_kernel::Predicate;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, Expression,
    JunctionPredicate, JunctionPredicateOp, Scalar, UnaryExpression, UnaryExpressionOp,
    UnaryPredicate, UnaryPredicateOp, VariadicExpressionOp,
};
use delta_kernel::schema::DataType;
use itertools::Itertools;

use crate::delta_datafusion::engine::expressions::to_json::to_json;

/// Converts a Delta kernel Expression into a DataFusion Expr.
///
/// Recursively transforms Delta kernel expressions (literals, columns, predicates, etc.)
/// into their DataFusion equivalents, using the provided output type for type inference.
pub(crate) fn to_datafusion_expr(expr: &Expression, output_type: &DataType) -> DFResult<Expr> {
    match expr {
        Expression::Literal(scalar) => to_datafusion_scalar(scalar).map(lit),
        Expression::Column(name) => {
            let mut name_iter = name.iter();
            let base_name = name_iter
                .next()
                .ok_or_else(|| internal_datafusion_err!("Expected at least one column name"))?;
            // Kernel column paths are already exact segments. Using `ident` avoids DataFusion SQL
            // identifier normalization (e.g. lowercasing), and treats `.` as a literal character
            // rather than a qualifier separator. This is required for correctness with camelCase
            // and other non-normalized column names (see #4082).
            Ok(name_iter.fold(ident(base_name), |acc, n| acc.field(n)))
        }
        Expression::Predicate(expr) => predicate_to_df(expr, output_type),
        Expression::Struct(fields) => struct_to_df(fields, output_type),
        Expression::Binary(expr) => binary_to_df(expr, output_type),
        Expression::Unary(expr) => unary_to_df(expr, output_type),
        Expression::Variadic(expr) => {
            let exprs: Vec<_> = expr
                .exprs
                .iter()
                .map(|e| to_datafusion_expr(e, output_type))
                .try_collect()?;
            match expr.op {
                VariadicExpressionOp::Coalesce => Ok(coalesce(exprs)),
            }
        }
        Expression::Opaque(_) => not_impl_err!("Opaque expressions are not yet supported"),
        Expression::Unknown(_) => not_impl_err!("Unknown expressions are not yet supported"),
        Expression::Transform(_) => not_impl_err!("Transform expressions are not yet supported"),
    }
}

/// Converts a Delta kernel Scalar value to a DataFusion ScalarValue.
///
/// Handles type mapping between Delta Lake's scalar types and DataFusion's scalar types,
/// including primitive types, temporal types, structs, and null values.
pub(crate) fn to_datafusion_scalar(scalar: &Scalar) -> DFResult<ScalarValue> {
    Ok(match scalar {
        Scalar::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Scalar::String(value) => ScalarValue::Utf8(Some(value.clone())),
        Scalar::Byte(value) => ScalarValue::Int8(Some(*value)),
        Scalar::Short(value) => ScalarValue::Int16(Some(*value)),
        Scalar::Integer(value) => ScalarValue::Int32(Some(*value)),
        Scalar::Long(value) => ScalarValue::Int64(Some(*value)),
        Scalar::Float(value) => ScalarValue::Float32(Some(*value)),
        Scalar::Double(value) => ScalarValue::Float64(Some(*value)),
        Scalar::Timestamp(value) => {
            ScalarValue::TimestampMicrosecond(Some(*value), Some("UTC".into()))
        }
        Scalar::TimestampNtz(value) => ScalarValue::TimestampMicrosecond(Some(*value), None),
        Scalar::Date(value) => ScalarValue::Date32(Some(*value)),
        Scalar::Binary(value) => ScalarValue::Binary(Some(value.clone())),
        Scalar::Decimal(data) => {
            ScalarValue::Decimal128(Some(data.bits()), data.precision(), data.scale() as i8)
        }
        Scalar::Struct(data) => {
            let fields: Vec<ArrowField> = data
                .fields()
                .iter()
                .map(|f| f.try_into_arrow())
                .try_collect()?;
            let values: Vec<_> = data
                .values()
                .iter()
                .map(to_datafusion_scalar)
                .try_collect()?;
            fields
                .into_iter()
                .zip(values.into_iter())
                .fold(ScalarStructBuilder::new(), |builder, (field, value)| {
                    builder.with_scalar(field, value)
                })
                .build()?
        }
        Scalar::Array(_) => {
            return Err(DataFusionError::NotImplemented(
                "Array scalar values not implemented".into(),
            ));
        }
        Scalar::Map(_) => {
            return Err(DataFusionError::NotImplemented(
                "Map scalar values not implemented".into(),
            ));
        }
        Scalar::Null(data_type) => {
            let data_type: ArrowDataType = data_type
                .try_into_arrow()
                .map_err(|e| DataFusionError::External(e.into()))?;
            ScalarValue::try_from(&data_type)?
        }
    })
}

fn binary_to_df(bin: &BinaryExpression, output_type: &DataType) -> DFResult<Expr> {
    let BinaryExpression { left, op, right } = bin;
    let left_expr = to_datafusion_expr(left, output_type)?;
    let right_expr = to_datafusion_expr(right, output_type)?;
    Ok(match op {
        BinaryExpressionOp::Plus => left_expr + right_expr,
        BinaryExpressionOp::Minus => left_expr - right_expr,
        BinaryExpressionOp::Multiply => left_expr * right_expr,
        BinaryExpressionOp::Divide => left_expr / right_expr,
    })
}

fn unary_to_df(un: &UnaryExpression, output_type: &DataType) -> DFResult<Expr> {
    let UnaryExpression { op, expr } = un;
    let expr = to_datafusion_expr(expr, output_type)?;
    Ok(match op {
        UnaryExpressionOp::ToJson => Expr::ScalarFunction(ScalarFunction {
            func: to_json(),
            args: vec![expr],
        }),
    })
}

fn binary_pred_to_df(bin: &BinaryPredicate, output_type: &DataType) -> DFResult<Expr> {
    let BinaryPredicate { left, op, right } = bin;
    let left_expr = to_datafusion_expr(left, output_type)?;
    let right_expr = to_datafusion_expr(right, output_type)?;

    Ok(match op {
        BinaryPredicateOp::Equal => left_expr.eq(right_expr),
        BinaryPredicateOp::LessThan => left_expr.lt(right_expr),
        BinaryPredicateOp::GreaterThan => left_expr.gt(right_expr),
        BinaryPredicateOp::Distinct => Expr::BinaryExpr(BinaryExpr {
            left: left_expr.into(),
            op: Operator::IsDistinctFrom,
            right: right_expr.into(),
        }),
        BinaryPredicateOp::In => Err(DataFusionError::NotImplemented(
            "IN operator not supported".into(),
        ))?,
    })
}

pub(crate) fn predicate_to_df(predicate: &Predicate, output_type: &DataType) -> DFResult<Expr> {
    match predicate {
        Predicate::BooleanExpression(expr) => to_datafusion_expr(expr, output_type),
        Predicate::Not(expr) => Ok(!(predicate_to_df(expr, output_type)?)),
        Predicate::Unary(expr) => unary_pred_to_df(expr, output_type),
        Predicate::Binary(expr) => binary_pred_to_df(expr, output_type),
        Predicate::Junction(expr) => junction_to_df(expr, output_type),
        Predicate::Opaque(_) => not_impl_err!("Opaque predicates are not yet supported"),
        Predicate::Unknown(_) => not_impl_err!("Unknown predicates are not yet supported"),
    }
}

fn unary_pred_to_df(unary: &UnaryPredicate, output_type: &DataType) -> DFResult<Expr> {
    let UnaryPredicate { op, expr } = unary;
    let df_expr = to_datafusion_expr(expr, output_type)?;
    Ok(match op {
        UnaryPredicateOp::IsNull => df_expr.is_null(),
    })
}

fn junction_to_df(junction: &JunctionPredicate, output_type: &DataType) -> DFResult<Expr> {
    let JunctionPredicate { op, preds } = junction;
    let df_exprs: Vec<_> = preds
        .iter()
        .map(|e| predicate_to_df(e, output_type))
        .try_collect()?;
    match op {
        JunctionPredicateOp::And => Ok(df_exprs
            .into_iter()
            .reduce(|a, b| a.and(b))
            .unwrap_or(lit(true))),
        JunctionPredicateOp::Or => Ok(df_exprs
            .into_iter()
            .reduce(|a, b| a.or(b))
            .unwrap_or(lit(false))),
    }
}

fn struct_to_df(fields: &[Arc<Expression>], output_type: &DataType) -> DFResult<Expr> {
    let DataType::Struct(struct_type) = output_type else {
        return Err(DataFusionError::Execution(
            "expected struct output type".into(),
        ));
    };
    let df_exprs: Vec<_> = fields
        .iter()
        .zip(struct_type.fields())
        .map(|(expr, field)| {
            Ok(vec![
                lit(field.name().to_string()),
                to_datafusion_expr(expr, field.data_type())?,
            ])
        })
        .flatten_ok()
        .try_collect::<_, _, DataFusionError>()?;
    Ok(named_struct(df_exprs))
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use datafusion::logical_expr::{col, expr_fn::ident, lit};
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::expressions::{ArrayData, BinaryExpression, MapData, Scalar, StructData};
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

    use super::*;

    /// Test conversion of primitive scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_primitives() {
        let test_cases = vec![
            (Scalar::Boolean(true), ScalarValue::Boolean(Some(true))),
            (
                Scalar::String("test".to_string()),
                ScalarValue::Utf8(Some("test".to_string())),
            ),
            (Scalar::Integer(42), ScalarValue::Int32(Some(42))),
            (Scalar::Long(42), ScalarValue::Int64(Some(42))),
            (Scalar::Float(42.0), ScalarValue::Float32(Some(42.0))),
            (Scalar::Double(42.0), ScalarValue::Float64(Some(42.0))),
            (Scalar::Byte(42), ScalarValue::Int8(Some(42))),
            (Scalar::Short(42), ScalarValue::Int16(Some(42))),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_scalar(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test conversion of temporal scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_temporal() {
        let test_cases = vec![
            (
                Scalar::Timestamp(1234567890),
                ScalarValue::TimestampMicrosecond(Some(1234567890), Some("UTC".into())),
            ),
            (
                Scalar::TimestampNtz(1234567890),
                ScalarValue::TimestampMicrosecond(Some(1234567890), None),
            ),
            (Scalar::Date(18262), ScalarValue::Date32(Some(18262))),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_scalar(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test conversion of binary and decimal scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_binary_decimal() {
        let binary_data = vec![1, 2, 3];
        let decimal_data = Scalar::decimal(123456789, 10, 2).unwrap();

        let test_cases = vec![
            (
                Scalar::Binary(binary_data.clone()),
                ScalarValue::Binary(Some(binary_data)),
            ),
            (
                decimal_data,
                ScalarValue::Decimal128(Some(123456789), 10, 2),
            ),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_scalar(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test conversion of struct scalar type to DataFusion scalar value
    #[test]
    fn test_scalar_to_df_struct() {
        let result = to_datafusion_scalar(&Scalar::Struct(
            StructData::try_new(
                vec![
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ],
                vec![Scalar::Integer(42), Scalar::String("test".to_string())],
            )
            .unwrap(),
        ))
        .unwrap();

        // Create the expected struct value
        let expected = ScalarStructBuilder::new()
            .with_scalar(
                ArrowField::new("a", ArrowDataType::Int32, true),
                ScalarValue::Int32(Some(42)),
            )
            .with_scalar(
                ArrowField::new("b", ArrowDataType::Utf8, true),
                ScalarValue::Utf8(Some("test".to_string())),
            )
            .build()
            .unwrap();

        assert_eq!(result, expected);
    }

    /// Test conversion of null scalar types to DataFusion scalar values
    #[test]
    fn test_scalar_to_df_null() {
        let test_cases = vec![
            (Scalar::Null(DataType::INTEGER), ScalarValue::Int32(None)),
            (Scalar::Null(DataType::STRING), ScalarValue::Utf8(None)),
            (Scalar::Null(DataType::BOOLEAN), ScalarValue::Boolean(None)),
            (Scalar::Null(DataType::DOUBLE), ScalarValue::Float64(None)),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_scalar(&input).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test error cases for unsupported scalar types (Array and Map)
    #[test]
    fn test_scalar_to_df_errors() {
        let array_data = ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, true),
            vec![Scalar::Integer(1), Scalar::Integer(2)],
        )
        .unwrap();

        let map_data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, true),
            vec![
                (Scalar::String("key1".to_string()), Scalar::Integer(1)),
                (Scalar::String("key2".to_string()), Scalar::Integer(2)),
            ],
        )
        .unwrap();

        let test_cases = vec![
            (
                Scalar::Array(array_data),
                "Array scalar values not implemented",
            ),
            (Scalar::Map(map_data), "Map scalar values not implemented"),
        ];

        for (input, expected_error) in test_cases {
            let result = to_datafusion_scalar(&input);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(expected_error));
        }
    }

    /// Test basic column reference: `test_col`
    #[test]
    fn test_column_expression() {
        let expr = Expression::Column(ColumnName::new(["test_col"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, ident("test_col"));

        let expr = Expression::Column(ColumnName::new(["test_col", "field_1", "field_2"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, ident("test_col").field("field_1").field("field_2"));
    }

    #[test]
    fn test_column_expression_preserves_case() {
        let expr = Expression::Column(ColumnName::new(["submittedAt"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, ident("submittedAt"));
    }

    #[test]
    fn test_column_expression_preserves_dots_in_segments() {
        let expr = Expression::Column(ColumnName::new(["a.b"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, ident("a.b"));
    }

    /// Test various literal values:
    /// - `true` (boolean)
    /// - `"test"` (string)
    /// - `42` (integer)
    /// - `42L` (long)
    /// - `42.0f` (float)
    /// - `42.0` (double)
    /// - `NULL` (null boolean)
    #[test]
    fn test_literal_expressions() {
        // Test various scalar types
        let test_cases = vec![
            (Expression::Literal(Scalar::Boolean(true)), lit(true)),
            (
                Expression::Literal(Scalar::String("test".to_string())),
                lit("test"),
            ),
            (Expression::Literal(Scalar::Integer(42)), lit(42)),
            (Expression::Literal(Scalar::Long(42)), lit(42i64)),
            (Expression::Literal(Scalar::Float(42.0)), lit(42.0f32)),
            (Expression::Literal(Scalar::Double(42.0)), lit(42.0)),
            (
                Expression::Literal(Scalar::Null(DataType::BOOLEAN)),
                lit(ScalarValue::Boolean(None)),
            ),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_expr(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test binary operations:
    /// - `a = 1` (equality)
    /// - `a + b` (addition)
    /// - `a * 2` (multiplication)
    #[test]
    fn test_binary_expressions() {
        let test_cases = vec![
            (
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryExpressionOp::Plus,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                }),
                col("a") + col("b"),
            ),
            (
                Expression::Binary(BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryExpressionOp::Multiply,
                    right: Box::new(Expression::Literal(Scalar::Integer(2))),
                }),
                col("a") * lit(2),
            ),
        ];

        for (input, expected) in test_cases {
            let result = to_datafusion_expr(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test binary operations:
    /// - `a = 1` (equality)
    /// - `a + b` (addition)
    /// - `a * 2` (multiplication)
    #[test]
    fn test_binary_predicate() {
        let test_cases = vec![(
            BinaryPredicate {
                left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                op: BinaryPredicateOp::Equal,
                right: Box::new(Expression::Literal(Scalar::Integer(1))),
            },
            col("a").eq(lit(1)),
        )];

        for (input, expected) in test_cases {
            let result = binary_pred_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test unary operations:
    /// - `a IS NULL` (null check)
    /// - `NOT a` (logical negation)
    #[test]
    fn test_unary_expressions() {
        let test_cases = vec![(
            UnaryPredicate {
                op: UnaryPredicateOp::IsNull,
                expr: Box::new(Expression::Column(ColumnName::new(["a"]))),
            },
            col("a").is_null(),
        )];

        for (input, expected) in test_cases {
            let result = unary_pred_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test junction operations:
    /// - `a AND b` (logical AND)
    /// - `a OR b` (logical OR)
    #[test]
    fn test_junction_expressions() {
        let test_cases = vec![
            (
                JunctionPredicate {
                    op: JunctionPredicateOp::And,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["a"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["b"]))),
                    ],
                },
                col("a").and(col("b")),
            ),
            (
                JunctionPredicate {
                    op: JunctionPredicateOp::Or,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["a"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["b"]))),
                    ],
                },
                col("a").or(col("b")),
            ),
        ];

        for (input, expected) in test_cases {
            let result = junction_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test complex nested expression:
    /// `(a > 1 AND b < 2) OR (c = 3)`
    #[test]
    fn test_complex_nested_expressions() {
        // Test a complex expression: (a > 1 AND b < 2) OR (c = 3)
        let expr = Predicate::Junction(JunctionPredicate {
            op: JunctionPredicateOp::Or,
            preds: vec![
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::And,
                    preds: vec![
                        Predicate::Binary(BinaryPredicate {
                            left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                            op: BinaryPredicateOp::GreaterThan,
                            right: Box::new(Expression::Literal(Scalar::Integer(1))),
                        }),
                        Predicate::Binary(BinaryPredicate {
                            left: Box::new(Expression::Column(ColumnName::new(["b"]))),
                            op: BinaryPredicateOp::LessThan,
                            right: Box::new(Expression::Literal(Scalar::Integer(2))),
                        }),
                    ],
                }),
                Predicate::Binary(BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["c"]))),
                    op: BinaryPredicateOp::Equal,
                    right: Box::new(Expression::Literal(Scalar::Integer(3))),
                }),
            ],
        });

        let result = predicate_to_df(&expr, &DataType::BOOLEAN).unwrap();
        let expected = (col("a").gt(lit(1)).and(col("b").lt(lit(2)))).or(col("c").eq(lit(3)));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_struct_expression() {
        let expr = Expression::Struct(vec![
            Expression::Column(ColumnName::new(["a"])).into(),
            Expression::Column(ColumnName::new(["b"])).into(),
        ]);
        let result = to_datafusion_expr(
            &expr,
            &DataType::Struct(Box::new(
                StructType::try_new(vec![
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::INTEGER),
                ])
                .unwrap(),
            )),
        )
        .unwrap();
        assert_eq!(
            result,
            named_struct(vec![lit("a"), col("a"), lit("b"), col("b")])
        );
    }

    /// Test binary expression conversions:
    /// - Addition: a + b
    /// - Subtraction: a - b
    /// - Multiplication: a * b
    /// - Division: a / b
    #[test]
    fn test_binary_to_df() {
        let test_cases = vec![
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryExpressionOp::Plus,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") + col("b"),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryExpressionOp::Minus,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") - col("b"),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryExpressionOp::Multiply,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") * col("b"),
            ),
            (
                BinaryExpression {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryExpressionOp::Divide,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a") / col("b"),
            ),
        ];

        for (input, expected) in test_cases {
            let result = binary_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test binary expression conversions:
    /// - Equality: a = b
    /// - Inequality: a != b
    /// - Less than: a < b
    /// - Less than or equal: a <= b
    /// - Greater than: a > b
    /// - Greater than or equal: a >= b
    #[test]
    fn test_binary_pred_to_df() {
        let test_cases = vec![
            (
                BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryPredicateOp::Equal,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").eq(col("b")),
            ),
            (
                BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryPredicateOp::LessThan,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").lt(col("b")),
            ),
            (
                BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryPredicateOp::GreaterThan,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                },
                col("a").gt(col("b")),
            ),
        ];

        for (input, expected) in test_cases {
            let result = binary_pred_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }

        let test_cases = vec![
            (
                Predicate::Not(Box::new(Predicate::Binary(BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryPredicateOp::Equal,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                }))),
                col("a").eq(col("b")).not(),
            ),
            (
                Predicate::Not(Box::new(Predicate::Binary(BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryPredicateOp::GreaterThan,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                }))),
                col("a").gt(col("b")).not(),
            ),
            (
                Predicate::Not(Box::new(Predicate::Binary(BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryPredicateOp::LessThan,
                    right: Box::new(Expression::Column(ColumnName::new(["b"]))),
                }))),
                col("a").lt(col("b")).not(),
            ),
        ];

        for (input, expected) in test_cases {
            let result = predicate_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test junction expression conversions:
    /// - Simple AND: a AND b
    /// - Simple OR: a OR b
    /// - Multiple AND: a AND b AND c
    /// - Multiple OR: a OR b OR c
    /// - Empty AND (should return true)
    /// - Empty OR (should return false)
    #[test]
    fn test_junction_to_df() {
        let test_cases = vec![
            // Simple AND
            (
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::And,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["a"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["b"]))),
                    ],
                }),
                col("a").and(col("b")),
            ),
            // Simple OR
            (
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::Or,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["a"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["b"]))),
                    ],
                }),
                col("a").or(col("b")),
            ),
            // Multiple AND
            (
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::And,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["a"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["b"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["c"]))),
                    ],
                }),
                col("a").and(col("b")).and(col("c")),
            ),
            // Multiple OR
            (
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::Or,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["a"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["b"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["c"]))),
                    ],
                }),
                col("a").or(col("b")).or(col("c")),
            ),
            // Empty AND (should return true)
            (
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::And,
                    preds: vec![],
                }),
                lit(true),
            ),
            // Empty OR (should return false)
            (
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::Or,
                    preds: vec![],
                }),
                lit(false),
            ),
        ];

        for (input, expected) in test_cases {
            let result = predicate_to_df(&input, &DataType::BOOLEAN).unwrap();
            assert_eq!(result, expected);
        }
    }

    /// Test to_datafusion_expr with various expression types and combinations:
    /// - Column expressions with nested fields
    /// - Complex unary expressions
    /// - Nested binary expressions
    /// - Mixed junction expressions
    /// - Struct expressions with nested fields
    /// - Complex combinations of all expression types
    #[test]
    fn test_to_datafusion_expr_comprehensive() {
        // Test column expressions with nested fields
        let expr = Expression::Column(ColumnName::new(["struct", "field", "nested"]));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, col("struct").field("field").field("nested"));

        // Test complex unary expressions
        let expr = Expression::Predicate(Box::new(Predicate::Not(Box::new(Predicate::Unary(
            UnaryPredicate {
                op: UnaryPredicateOp::IsNull,
                expr: Box::new(Expression::Column(ColumnName::new(["a"]))),
            },
        )))));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, !col("a").is_null());

        // Test nested binary expressions
        let expr = Expression::Binary(BinaryExpression {
            left: Box::new(Expression::Binary(BinaryExpression {
                left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                op: BinaryExpressionOp::Plus,
                right: Box::new(Expression::Column(ColumnName::new(["b"]))),
            })),
            op: BinaryExpressionOp::Multiply,
            right: Box::new(Expression::Column(ColumnName::new(["c"]))),
        });
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, (col("a") + col("b")) * col("c"));

        // Test mixed junction expressions
        let expr = Expression::Predicate(Box::new(Predicate::Junction(JunctionPredicate {
            op: JunctionPredicateOp::And,
            preds: vec![
                Predicate::Binary(BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["a"]))),
                    op: BinaryPredicateOp::GreaterThan,
                    right: Box::new(Expression::Literal(Scalar::Integer(0))),
                }),
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::Or,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["b"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["c"]))),
                    ],
                }),
            ],
        })));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(result, col("a").gt(lit(0)).and(col("b").or(col("c"))));

        // Test struct expressions with nested fields
        let expr = Expression::Struct(vec![
            Expression::Column(ColumnName::new(["a"])).into(),
            Expression::Binary(BinaryExpression {
                left: Box::new(Expression::Column(ColumnName::new(["b"]))),
                op: BinaryExpressionOp::Plus,
                right: Box::new(Expression::Column(ColumnName::new(["c"]))),
            })
            .into(),
        ]);
        let result = to_datafusion_expr(
            &expr,
            &DataType::Struct(Box::new(
                StructType::try_new(vec![
                    StructField::nullable("a", DataType::INTEGER),
                    StructField::nullable("sum", DataType::INTEGER),
                ])
                .unwrap(),
            )),
        )
        .unwrap();
        assert_eq!(
            result,
            named_struct(vec![lit("a"), col("a"), lit("sum"), col("b") + col("c")])
        );

        // Test complex combination of all expression types
        let expr = Expression::Predicate(Box::new(Predicate::Junction(JunctionPredicate {
            op: JunctionPredicateOp::And,
            preds: vec![
                Predicate::Not(Box::new(Predicate::BooleanExpression(Expression::Column(
                    ColumnName::new(["a"]),
                )))),
                Predicate::Binary(BinaryPredicate {
                    left: Box::new(Expression::Column(ColumnName::new(["b"]))),
                    op: BinaryPredicateOp::Equal,
                    right: Box::new(Expression::Literal(Scalar::Integer(42))),
                }),
                Predicate::Junction(JunctionPredicate {
                    op: JunctionPredicateOp::Or,
                    preds: vec![
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["c"]))),
                        Predicate::BooleanExpression(Expression::Column(ColumnName::new(["d"]))),
                    ],
                }),
            ],
        })));
        let result = to_datafusion_expr(&expr, &DataType::BOOLEAN).unwrap();
        assert_eq!(
            result,
            (!col("a"))
                .and(col("b").eq(lit(42)))
                .and(col("c").or(col("d")))
        );

        // Test error case: empty column name
        let expr = Expression::Column(ColumnName::new::<&str>([]));
        assert!(to_datafusion_expr(&expr, &DataType::BOOLEAN).is_err());
    }
}
