use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode as _};
use datafusion::error::Result;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable as _};
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
        let expr = match self {
            Expression::DataFusion(expr) => expr.clone(),
            Expression::String(s) => parse_predicate_expression(schema.as_ref(), s, session)?,
        };
        // Coerce literal types to match schema column types for parsed string predicates
        let expr = coerce_predicate_literals(expr, schema.as_ref())?;
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

/// Cast `value` to `target`, unless the cast would change what the literal says.
///
/// A float literal compared with an integer or decimal column must not be cast to the column
/// type: `2.5` would become `2`, and `"id" = 2.5` would then name the rows where `id` is 2.
/// For such casts the literal is left alone, so the comparison keeps the meaning it has in a
/// read and is widened or refused further on, instead of selecting rows that were not named.
/// Returns `None` when the literal should be kept as it is.
fn coerce_literal(value: &ScalarValue, target: &DataType) -> Result<Option<Expr>> {
    let cast = value.cast_to(target)?;
    let narrows_numeric = value.data_type().is_numeric()
        && (target.is_integer()
            || matches!(target, DataType::Decimal128(..) | DataType::Decimal256(..)));
    if narrows_numeric && cast.cast_to(&value.data_type())? != *value {
        return Ok(None);
    }
    Ok(Some(Expr::Literal(cast, None)))
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
/// # Arguments
/// * `expr` - The expression to transform
/// * `schema` - The schema containing column type information
///
/// # Returns
/// The transformed expression with literals properly cast to match column types
pub(crate) fn coerce_predicate_literals(expr: Expr, schema: &DFSchema) -> Result<Expr> {
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

                // Check if we have a Literal pattern
                let (expr, lit_value, expr_on_left) = match (left.as_ref(), right.as_ref()) {
                    (_, Expr::Literal(value, _)) => (left.as_ref(), value, true),
                    (Expr::Literal(value, _), _) => (right.as_ref(), value, false),
                    _ => return Ok(Transformed::no(e)),
                };

                let expr_type = expr.get_type(schema)?;
                if lit_value.data_type() == expr_type {
                    return Ok(Transformed::no(e));
                }

                let Some(lit_expr) = coerce_literal(lit_value, &expr_type)? else {
                    return Ok(Transformed::no(e));
                };
                let new_binary = if expr_on_left {
                    Expr::BinaryExpr(BinaryExpr {
                        left: left.clone(),
                        op: *op,
                        right: Box::new(lit_expr),
                    })
                } else {
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(lit_expr),
                        op: *op,
                        right: right.clone(),
                    })
                };

                Ok(Transformed::yes(new_binary))
            }
            Expr::Between(between) => {
                let expr_type = between.expr.get_type(schema)?;
                let mut changed = false;

                let mut coerce_bound = |bound: &Expr| -> Result<Box<Expr>> {
                    if let Expr::Literal(value, _) = bound
                        && value.data_type() != expr_type
                        && let Some(lit_expr) = coerce_literal(value, &expr_type)?
                    {
                        changed = true;
                        return Ok(Box::new(lit_expr));
                    }
                    Ok(Box::new(bound.clone()))
                };
                let low = coerce_bound(between.low.as_ref())?;
                let high = coerce_bound(between.high.as_ref())?;

                if !changed {
                    return Ok(Transformed::no(e));
                }

                Ok(Transformed::yes(Expr::Between(
                    datafusion::logical_expr::Between {
                        expr: between.expr.clone(),
                        negated: between.negated,
                        low,
                        high,
                    },
                )))
            }
            Expr::InList(in_list) => {
                let expr_type = in_list.expr.get_type(schema)?;
                let mut changed = false;

                let list = in_list
                    .list
                    .iter()
                    .map(|item| match item {
                        Expr::Literal(value, _) if value.data_type() != expr_type => {
                            match coerce_literal(value, &expr_type)? {
                                Some(lit_expr) => {
                                    changed = true;
                                    Ok(lit_expr)
                                }
                                None => Ok(item.clone()),
                            }
                        }
                        _ => Ok(item.clone()),
                    })
                    .collect::<Result<Vec<_>>>()?;

                if !changed {
                    return Ok(Transformed::no(e));
                }

                Ok(Transformed::yes(Expr::InList(InList {
                    expr: in_list.expr.clone(),
                    list,
                    negated: in_list.negated,
                })))
            }
            _ => Ok(Transformed::no(e)),
        }
    })
    .map(|transformed| transformed.data)
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

        // Float64 literal with Float32 column should coerce
        let expr = col("float32_col").eq(lit(3.15_f64));
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

    #[test]
    fn test_fractional_literal_is_not_truncated_to_integer_column() {
        let schema = setup_test_schema();

        let predicates = [
            col("int64_col").eq(lit(2.5_f64)),
            col("int64_col").lt(lit(2.5_f64)),
            lit(2.5_f64).gt(col("int32_col")),
            col("int32_col").between(lit(1.5_f64), lit(2.5_f64)),
            col("int32_col").in_list(vec![lit(2.5_f64), lit(3.5_f64)], false),
        ];
        for expr in predicates {
            let result = coerce_predicate_literals(expr.clone(), &schema).unwrap();
            assert_eq!(expr, result, "a fractional literal must keep its value");
        }
    }

    #[test]
    fn test_whole_float_literal_is_still_coerced_to_integer_column() {
        let schema = setup_test_schema();

        let result = coerce_predicate_literals(col("int64_col").eq(lit(2.0_f64)), &schema).unwrap();
        assert_eq!(result, col("int64_col").eq(lit(2_i64)));
    }

    #[test]
    fn test_only_the_lossy_literal_of_a_list_is_kept() {
        let schema = setup_test_schema();

        let expr = col("int64_col").in_list(vec![lit(2.0_f64), lit(2.5_f64)], false);
        let result = coerce_predicate_literals(expr, &schema).unwrap();
        assert_eq!(
            result,
            col("int64_col").in_list(vec![lit(2_i64), lit(2.5_f64)], false)
        );
    }
}
