use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode as _};
use datafusion::error::Result;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable as _, cast};
use datafusion::logical_expr_common::operator::Operator;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::scalar::ScalarValue;
use datafusion::{catalog::Session, common::DFSchema};

use crate::{DeltaResult, delta_datafusion::expr::parse_predicate_expression};

/// Used to represent user input of either a Datafusion expression or string expression
#[derive(Debug, Clone)]
pub enum Expression {
    /// Datafusion Expression
    DataFusion(Expr),
    /// String Expression
    String(String),
}

impl Expression {
    pub(crate) fn resolve(&self, session: &dyn Session, schema: DFSchemaRef) -> Result<Expr> {
        self.resolve_with(session, schema, LossyLiterals::WidenColumn)
    }

    pub(crate) fn resolve_with(
        &self,
        session: &dyn Session,
        schema: DFSchemaRef,
        lossy: LossyLiterals,
    ) -> Result<Expr> {
        let expr = match self {
            Expression::DataFusion(expr) => expr.clone(),
            Expression::String(s) => parse_predicate_expression(schema.as_ref(), s, session)?,
        };
        // Coerce literal types to match schema column types for parsed string predicates
        let expr = coerce_predicate_literals_with(expr, schema.as_ref(), lossy)?;
        let execution_props = session.execution_props();
        let context = SimplifyContext::builder()
            .with_schema(schema)
            .with_query_execution_start_time(execution_props.query_execution_start_time)
            .with_config_options(
                execution_props
                    .config_options()
                    .cloned()
                    .unwrap_or_else(|| session.config().options().clone()),
            )
            .build();
        let simplifier = ExprSimplifier::new(context);
        simplifier.simplify(expr)
    }
}

impl From<Expr> for Expression {
    fn from(val: Expr) -> Self {
        Expression::DataFusion(val)
    }
}

impl From<&str> for Expression {
    fn from(val: &str) -> Self {
        Expression::String(val.to_string())
    }
}
impl From<String> for Expression {
    fn from(val: String) -> Self {
        Expression::String(val)
    }
}

pub(crate) fn into_expr(
    expr: Expression,
    schema: &DFSchema,
    session: &dyn Session,
) -> DeltaResult<Expr> {
    Ok(expr.resolve(session, Arc::new(schema.clone()))?)
}

pub(crate) fn maybe_into_expr(
    expr: Option<Expression>,
    schema: &DFSchema,
    session: &dyn Session,
) -> DeltaResult<Option<Expr>> {
    Ok(match expr {
        Some(predicate) => Some(into_expr(predicate, schema, session)?),
        None => None,
    })
}

/// What to do with a comparison whose literal cannot take the column type without losing
/// precision, e.g. `int_col < 8.5` or `decimal_10_2_col = 1.235`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LossyLiterals {
    /// Cast the column to the common comparison type so the predicate keeps its meaning and
    /// every side of the comparison has the same type, e.g. `CAST(int_col AS DOUBLE) < 8.5`.
    /// When the common type is the column's own type, e.g. a `DOUBLE` column against a
    /// `BIGINT`, only the literal is cast.
    WidenColumn,
    /// Leave the comparison as written, e.g. when the expression is persisted as SQL text.
    Keep,
}

/// Coerce literal types in predicates to match the column types from the schema.
///
/// This function walks the expression tree and for comparison expressions
/// (e.g., `column = 3` or `column BETWEEN 1 AND 3`), it ensures that literal
/// values are cast to match the actual column type from the schema. This is
/// necessary because SQL parsers and dynamically constructed expressions often
/// infer literal types independently (e.g., `3` becomes `Int64`), which may not
/// match the schema's column type (e.g., `Int32`).
///
/// A literal is only narrowed to the column type when the cast is exact. Otherwise
/// narrowing would change the predicate (`int_col < 8.5` is not `int_col < 8`), so the
/// column is widened instead and both sides are compared in the common type.
///
/// # Arguments
/// * `expr` - The expression to transform
/// * `schema` - The schema containing column type information
///
/// # Returns
/// The transformed expression with literals properly cast to match column types
pub(crate) fn coerce_predicate_literals(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    coerce_predicate_literals_with(expr, schema, LossyLiterals::WidenColumn)
}

pub(crate) fn coerce_predicate_literals_with(
    expr: Expr,
    schema: &DFSchema,
    lossy: LossyLiterals,
) -> Result<Expr> {
    expr.transform(&|e| {
        match &e {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                // Only coerce for comparison operators
                if !matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                ) {
                    return Ok(Transformed::no(e));
                }

                let (expr, literal, expr_on_left) = match (left.as_ref(), right.as_ref()) {
                    (_, Expr::Literal(..)) => (left.as_ref(), right.as_ref(), true),
                    (Expr::Literal(..), _) => (right.as_ref(), left.as_ref(), false),
                    _ => return Ok(Transformed::no(e)),
                };
                let Some((expr, mut literal)) = coerce_items(expr, &[literal], schema, lossy)?
                else {
                    return Ok(Transformed::no(e));
                };
                let (expr, literal) = (Box::new(expr), Box::new(literal.remove(0)));
                let (left, right) = if expr_on_left {
                    (expr, literal)
                } else {
                    (literal, expr)
                };
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: *op,
                    right,
                })))
            }
            Expr::Between(between) => {
                let bounds = [between.low.as_ref(), between.high.as_ref()];
                let Some((expr, mut bounds)) = coerce_items(&between.expr, &bounds, schema, lossy)?
                else {
                    return Ok(Transformed::no(e));
                };
                let high = Box::new(bounds.remove(1));
                let low = Box::new(bounds.remove(0));
                Ok(Transformed::yes(Expr::Between(
                    datafusion::logical_expr::Between {
                        expr: Box::new(expr),
                        negated: between.negated,
                        low,
                        high,
                    },
                )))
            }
            Expr::InList(in_list) => {
                let items: Vec<&Expr> = in_list.list.iter().collect();
                let Some((expr, list)) = coerce_items(&in_list.expr, &items, schema, lossy)? else {
                    return Ok(Transformed::no(e));
                };
                Ok(Transformed::yes(Expr::InList(InList {
                    expr: Box::new(expr),
                    list,
                    negated: in_list.negated,
                })))
            }
            _ => Ok(Transformed::no(e)),
        }
    })
    .map(|transformed| transformed.data)
}

/// Coerces the literal `items` compared against `expr`, leaving non-literal items unchanged.
fn coerce_items(
    expr: &Expr,
    items: &[&Expr],
    schema: &DFSchema,
    lossy: LossyLiterals,
) -> Result<Option<(Expr, Vec<Expr>)>> {
    let literals: Vec<&ScalarValue> = items
        .iter()
        .filter_map(|item| match item {
            Expr::Literal(value, _) => Some(value),
            _ => None,
        })
        .collect();
    let Some((expr, coerced)) = coerce_comparison(expr, &literals, schema, lossy)? else {
        return Ok(None);
    };
    let mut coerced = coerced.into_iter();
    let items = items
        .iter()
        .map(|item| match item {
            Expr::Literal(..) => coerced
                .next()
                .map_or_else(|| (*item).clone(), |value| Expr::Literal(value, None)),
            _ => (*item).clone(),
        })
        .collect();
    Ok(Some((expr, items)))
}

/// Returns the compared expression and the literals rewritten so that they share one type,
/// or `None` when the comparison should be left as it is.
fn coerce_comparison(
    expr: &Expr,
    literals: &[&ScalarValue],
    schema: &DFSchema,
    lossy: LossyLiterals,
) -> Result<Option<(Expr, Vec<ScalarValue>)>> {
    let expr_type = expr.get_type(schema)?;
    if literals.iter().all(|value| value.data_type() == expr_type) {
        return Ok(None);
    }
    if let Some(narrowed) = literals
        .iter()
        .map(|value| cast_literal_exactly(value, &expr_type))
        .collect::<Option<Vec<_>>>()
    {
        return Ok(Some((expr.clone(), narrowed)));
    }
    if lossy == LossyLiterals::Keep {
        return Ok(None);
    }
    let Some(common_type) = literals.iter().try_fold(expr_type.clone(), |acc, value| {
        comparison_coercion(&acc, &value.data_type())
    }) else {
        return Ok(None);
    };
    // DataFusion compares a float column with an integer in the column's float type,
    // which rounds the literal. Spark compares those in DOUBLE, so do the same where it is wider.
    let common_type = match common_type {
        DataType::Float16 | DataType::Float32 if common_type == expr_type => DataType::Float64,
        common_type => common_type,
    };
    let Ok(widened) = literals
        .iter()
        .map(|value| value.cast_to(&common_type))
        .collect::<Result<Vec<_>>>()
    else {
        return Ok(None);
    };
    let expr = if common_type == expr_type {
        expr.clone()
    } else {
        cast(expr.clone(), common_type)
    };
    Ok(Some((expr, widened)))
}

/// Casts `value` to `target`, or returns `None` if casting the result back does not give
/// the original value (e.g. `8.5` to an integer, `1.235` to `decimal(10, 2)`).
fn cast_literal_exactly(value: &ScalarValue, target: &DataType) -> Option<ScalarValue> {
    if value.data_type() == *target {
        return Some(value.clone());
    }
    let cast_value = value.cast_to(target).ok()?;
    let round_trip = cast_value.cast_to(&value.data_type()).ok()?;
    (round_trip == *value).then_some(cast_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType as ArrowDataType;
    use datafusion::common::ToDFSchema;
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;

    fn setup_test_schema() -> DFSchema {
        use arrow_schema::{Field, Schema};
        let arrow_schema = Schema::new(vec![
            Field::new("int32_col", ArrowDataType::Int32, true),
            Field::new("int64_col", ArrowDataType::Int64, true),
            Field::new("string_col", ArrowDataType::Utf8, true),
            Field::new("bool_col", ArrowDataType::Boolean, true),
            Field::new("float32_col", ArrowDataType::Float32, true),
            Field::new("float64_col", ArrowDataType::Float64, true),
        ]);
        arrow_schema.to_dfschema().unwrap()
    }

    #[test]
    fn test_coerce_int64_to_int32() {
        let schema = setup_test_schema();

        // Int32 column with Int64 literal - should coerce to Int32
        let expr = col("int32_col").eq(lit(42_i64));
        let result = coerce_predicate_literals(expr, &schema).unwrap();

        match result {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                assert_eq!(*left, col("int32_col"));
                assert_eq!(op, Operator::Eq);
                // Right side should be coerced to Int32
                match right.as_ref() {
                    Expr::Literal(val, _) => {
                        assert_eq!(val, &ScalarValue::Int32(Some(42)));
                    }
                    _ => panic!("Expected Literal, got {:?}", right),
                }
            }
            _ => panic!("Expected BinaryExpr, got {:?}", result),
        }
    }

    #[test]
    fn test_coerce_literal_on_left_side() {
        let schema = setup_test_schema();

        // Literal on left side should also be coerced
        let expr = lit(42_i64).eq(col("int32_col"));
        let result = coerce_predicate_literals(expr, &schema).unwrap();

        match result {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                assert_eq!(*right, col("int32_col"));
                assert_eq!(op, Operator::Eq);
                match left.as_ref() {
                    Expr::Literal(val, _) => {
                        assert_eq!(val, &ScalarValue::Int32(Some(42)));
                    }
                    _ => panic!("Expected Literal, got {:?}", left),
                }
            }
            _ => panic!("Expected BinaryExpr, got {:?}", result),
        }
    }

    #[test]
    fn test_all_comparison_operators() {
        let schema = setup_test_schema();

        let operators = vec![
            (col("int32_col").eq(lit(10_i64)), Operator::Eq),
            (col("int32_col").not_eq(lit(10_i64)), Operator::NotEq),
            (col("int32_col").lt(lit(10_i64)), Operator::Lt),
            (col("int32_col").lt_eq(lit(10_i64)), Operator::LtEq),
            (col("int32_col").gt(lit(10_i64)), Operator::Gt),
            (col("int32_col").gt_eq(lit(10_i64)), Operator::GtEq),
        ];

        for (expr, expected_op) in operators {
            let result = coerce_predicate_literals(expr, &schema).unwrap();
            match result {
                Expr::BinaryExpr(BinaryExpr { right, op, .. }) => {
                    assert_eq!(op, expected_op);
                    // All should be coerced to Int32
                    match right.as_ref() {
                        Expr::Literal(val, _) => {
                            assert_eq!(val.data_type(), ArrowDataType::Int32);
                        }
                        _ => panic!("Expected Literal"),
                    }
                }
                _ => panic!("Expected BinaryExpr"),
            }
        }
    }

    #[test]
    fn test_coerce_between_decimal_literals() {
        use arrow_schema::{Field, Schema};

        let schema = Schema::new(vec![Field::new(
            "decimal_col",
            ArrowDataType::Decimal128(6, 1),
            true,
        )])
        .to_dfschema()
        .unwrap();

        let expr = col("decimal_col").between(
            lit(ScalarValue::Decimal128(Some(1505), 4, 1)),
            lit(ScalarValue::Decimal128(Some(1505), 4, 1)),
        );
        let result = coerce_predicate_literals(expr, &schema).unwrap();

        match result {
            Expr::Between(between) => {
                match between.low.as_ref() {
                    Expr::Literal(val, _) => {
                        assert_eq!(val, &ScalarValue::Decimal128(Some(1505), 6, 1));
                    }
                    _ => panic!("Expected Literal in BETWEEN low bound"),
                }
                match between.high.as_ref() {
                    Expr::Literal(val, _) => {
                        assert_eq!(val, &ScalarValue::Decimal128(Some(1505), 6, 1));
                    }
                    _ => panic!("Expected Literal in BETWEEN high bound"),
                }
            }
            _ => panic!("Expected Between expression, got {:?}", result),
        }
    }

    #[test]
    fn test_no_coercion_for_arithmetic_operators() {
        let schema = setup_test_schema();

        // Arithmetic operators should not be affected
        let expr = col("int32_col") + lit(10_i64);
        let result = coerce_predicate_literals(expr.clone(), &schema).unwrap();

        // Should remain unchanged
        assert_eq!(expr, result);
    }

    #[test]
    fn test_no_coercion_for_matching_types() {
        let schema = setup_test_schema();

        // When types already match, no coercion should occur
        let expr = col("int32_col").eq(lit(ScalarValue::Int32(Some(42))));
        let result = coerce_predicate_literals(expr.clone(), &schema).unwrap();

        // Should remain unchanged
        assert_eq!(expr, result);
    }

    #[test]
    fn test_complex_expression_with_and() {
        let schema = setup_test_schema();

        // Complex expression with AND - both sides should be coerced
        let expr = col("int32_col")
            .eq(lit(10_i64))
            .and(col("int32_col").lt(lit(100_i64)));
        let result = coerce_predicate_literals(expr, &schema).unwrap();

        match result {
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                // Check left side (int32_col = 10)
                match left.as_ref() {
                    Expr::BinaryExpr(BinaryExpr {
                        right: lit_expr, ..
                    }) => match lit_expr.as_ref() {
                        Expr::Literal(val, _) => {
                            assert_eq!(val, &ScalarValue::Int32(Some(10)));
                        }
                        _ => panic!("Expected Literal"),
                    },
                    _ => panic!("Expected BinaryExpr"),
                }

                // Check right side (int32_col < 100)
                match right.as_ref() {
                    Expr::BinaryExpr(BinaryExpr {
                        right: lit_expr, ..
                    }) => match lit_expr.as_ref() {
                        Expr::Literal(val, _) => {
                            assert_eq!(val, &ScalarValue::Int32(Some(100)));
                        }
                        _ => panic!("Expected Literal"),
                    },
                    _ => panic!("Expected BinaryExpr"),
                }
            }
            _ => panic!("Expected BinaryExpr for AND"),
        }
    }

    #[test]
    fn test_complex_expression_with_or() {
        let schema = setup_test_schema();

        // Complex expression with OR
        let expr = col("int32_col")
            .eq(lit(10_i64))
            .or(col("int32_col").eq(lit(20_i64)));
        let result = coerce_predicate_literals(expr, &schema).unwrap();

        // Both literals should be coerced to Int32
        match result {
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                match left.as_ref() {
                    Expr::BinaryExpr(BinaryExpr {
                        right: lit_expr, ..
                    }) => match lit_expr.as_ref() {
                        Expr::Literal(val, _) => {
                            assert_eq!(val.data_type(), ArrowDataType::Int32);
                        }
                        _ => panic!("Expected Literal"),
                    },
                    _ => panic!("Expected BinaryExpr"),
                }

                match right.as_ref() {
                    Expr::BinaryExpr(BinaryExpr {
                        right: lit_expr, ..
                    }) => match lit_expr.as_ref() {
                        Expr::Literal(val, _) => {
                            assert_eq!(val.data_type(), ArrowDataType::Int32);
                        }
                        _ => panic!("Expected Literal"),
                    },
                    _ => panic!("Expected BinaryExpr"),
                }
            }
            _ => panic!("Expected BinaryExpr for OR"),
        }
    }

    #[test]
    fn test_string_literal_no_coercion_needed() {
        let schema = setup_test_schema();

        // String literals should match and not need coercion
        let expr = col("string_col").eq(lit("test"));
        let result = coerce_predicate_literals(expr.clone(), &schema).unwrap();

        // Should remain unchanged
        assert_eq!(expr, result);
    }

    #[test]
    fn test_bool_literal_no_coercion_needed() {
        let schema = setup_test_schema();

        // Boolean literals should match and not need coercion
        let expr = col("bool_col").eq(lit(true));
        let result = coerce_predicate_literals(expr.clone(), &schema).unwrap();

        // Should remain unchanged
        assert_eq!(expr, result);
    }

    #[test]
    fn test_float_coercion() {
        let schema = setup_test_schema();

        // Float64 literal with Float32 column should coerce when the value fits exactly
        let expr = col("float32_col").eq(lit(3.25_f64));
        let result = coerce_predicate_literals(expr, &schema).unwrap();

        match result {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => match right.as_ref() {
                Expr::Literal(val, _) => {
                    assert_eq!(val.data_type(), ArrowDataType::Float32);
                }
                _ => panic!("Expected Literal"),
            },
            _ => panic!("Expected BinaryExpr"),
        }
    }

    #[test]
    fn test_non_column_expr_left_side() {
        let schema = setup_test_schema();

        // Expression that's not a simple column should still work
        let expr = (col("int32_col") + col("int32_col")).eq(lit(100_i64));
        let result = coerce_predicate_literals(expr, &schema).unwrap();

        // The literal should be coerced based on the expression type
        match result {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => match right.as_ref() {
                Expr::Literal(val, _) => {
                    assert_eq!(val.data_type(), ArrowDataType::Int32);
                }
                _ => panic!("Expected Literal"),
            },
            _ => panic!("Expected BinaryExpr"),
        }
    }

    #[test]
    fn test_nested_and_or_expression() {
        let schema = setup_test_schema();

        // Nested (A AND B) OR C expression
        let expr = col("int32_col")
            .eq(lit(1_i64))
            .and(col("int32_col").lt(lit(10_i64)))
            .or(col("int32_col").gt(lit(100_i64)));

        let result = coerce_predicate_literals(expr, &schema).unwrap();

        // All three literals should be coerced
        // This is a complex nested structure, just verify it doesn't panic
        // and returns a valid expression
        assert!(matches!(result, Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_no_literal_in_comparison() {
        let schema = setup_test_schema();

        // Column to column comparison should not change
        let expr = col("int32_col").eq(col("int64_col"));
        let result = coerce_predicate_literals(expr.clone(), &schema).unwrap();

        // Should remain unchanged
        assert_eq!(expr, result);
    }

    fn lossy_test_schema() -> DFSchema {
        use arrow_schema::{Field, Schema};
        Schema::new(vec![
            Field::new("int32_col", ArrowDataType::Int32, true),
            Field::new("int64_col", ArrowDataType::Int64, true),
            Field::new("float32_col", ArrowDataType::Float32, true),
            Field::new("float64_col", ArrowDataType::Float64, true),
            Field::new("dec_col", ArrowDataType::Decimal128(10, 2), true),
        ])
        .to_dfschema()
        .unwrap()
    }

    fn dec(value: i128, precision: u8, scale: i8) -> Expr {
        lit(ScalarValue::Decimal128(Some(value), precision, scale))
    }

    #[rstest::rstest]
    #[case::float_to_int(col("int64_col").eq(lit(8.0_f64)), col("int64_col").eq(lit(8_i64)))]
    #[case::wider_decimal(
        col("dec_col").eq(dec(15, 2, 1)),
        col("dec_col").eq(dec(150, 10, 2))
    )]
    #[case::between(
        col("int32_col").between(lit(7_i64), lit(9.0_f64)),
        col("int32_col").between(lit(7_i32), lit(9_i32))
    )]
    fn test_exact_literals_are_narrowed(#[case] expr: Expr, #[case] expected: Expr) {
        let schema = lossy_test_schema();
        for lossy in [LossyLiterals::WidenColumn, LossyLiterals::Keep] {
            let result = coerce_predicate_literals_with(expr.clone(), &schema, lossy).unwrap();
            assert_eq!(result, expected);
        }
    }

    #[rstest::rstest]
    #[case::int_lt_fraction(
        col("int64_col").lt(lit(8.5_f64)),
        cast(col("int64_col"), ArrowDataType::Float64).lt(lit(8.5_f64))
    )]
    #[case::literal_on_left(
        lit(8.5_f64).gt(col("int64_col")),
        lit(8.5_f64).gt(cast(col("int64_col"), ArrowDataType::Float64))
    )]
    #[case::int_eq_almost_int(
        col("int32_col").eq(lit(7.99999_f64)),
        cast(col("int32_col"), ArrowDataType::Float64).eq(lit(7.99999_f64))
    )]
    #[case::float32_vs_float64(
        col("float32_col").lt(lit(2.0000001_f64)),
        cast(col("float32_col"), ArrowDataType::Float64).lt(lit(2.0000001_f64))
    )]
    #[case::float32_vs_int(
        col("float32_col").eq(lit(16_777_217_i64)),
        cast(col("float32_col"), ArrowDataType::Float64).eq(lit(16_777_217.0_f64))
    )]
    #[case::float32_in_list(
        col("float32_col").in_list(vec![lit(16_777_217_i64), lit(1_i64)], false),
        cast(col("float32_col"), ArrowDataType::Float64)
            .in_list(vec![lit(16_777_217.0_f64), lit(1.0_f64)], false)
    )]
    #[case::float64_vs_int(
        col("float64_col").eq(lit((1_i64 << 53) + 1)),
        col("float64_col").eq(lit(9_007_199_254_740_992.0_f64))
    )]
    #[case::decimal_scale(
        col("dec_col").eq(dec(1235, 4, 3)),
        cast(col("dec_col"), ArrowDataType::Decimal128(11, 3)).eq(dec(1235, 11, 3))
    )]
    #[case::out_of_range(
        col("int32_col").eq(lit(1_i64 << 40)),
        cast(col("int32_col"), ArrowDataType::Int64).eq(lit(1_i64 << 40))
    )]
    #[case::nan(
        col("int64_col").eq(lit(f64::NAN)),
        cast(col("int64_col"), ArrowDataType::Float64).eq(lit(f64::NAN))
    )]
    #[case::between(
        col("int64_col").between(lit(7_i64), lit(8.5_f64)),
        cast(col("int64_col"), ArrowDataType::Float64).between(lit(7.0_f64), lit(8.5_f64))
    )]
    #[case::in_list(
        col("int64_col").in_list(vec![lit(8_i64), lit(8.5_f64)], true),
        cast(col("int64_col"), ArrowDataType::Float64)
            .in_list(vec![lit(8.0_f64), lit(8.5_f64)], true)
    )]
    fn test_lossy_literals_widen_the_column(#[case] expr: Expr, #[case] expected: Expr) {
        let schema = lossy_test_schema();
        assert_eq!(
            coerce_predicate_literals(expr.clone(), &schema).unwrap(),
            expected
        );
        assert_eq!(
            coerce_predicate_literals_with(expr.clone(), &schema, LossyLiterals::Keep).unwrap(),
            expr
        );
    }
}
