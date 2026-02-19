use datafusion::common::{Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, ColumnName,
    DecimalData, Expression, JunctionPredicate, JunctionPredicateOp, Predicate, Scalar,
    UnaryPredicate, UnaryPredicateOp,
};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType};

use crate::kernel::scalars::ScalarExt;

/// Converts a DataFusion expression to a Delta predicate.
///
/// If the expression converts to a Delta predicate, returns it directly.
/// Otherwise, wraps the expression as a boolean expression predicate.
pub(crate) fn to_delta_predicate(expr: &Expr) -> Result<Predicate> {
    match to_delta_expression(expr)? {
        Expression::Predicate(pred) => Ok(pred.as_ref().clone()),
        expr => Ok(Predicate::BooleanExpression(expr)),
    }
}

/// Converts a DataFusion expression to a Delta kernel expression.
pub(crate) fn to_delta_expression(expr: &Expr) -> Result<Expression> {
    match expr {
        Expr::Column(column) => Ok(Expression::Column(ColumnName::from_naive_str_split(
            &column.name,
        ))),
        Expr::Literal(scalar, _meta) => {
            Ok(Expression::Literal(datafusion_scalar_to_scalar(scalar)?))
        }
        Expr::BinaryExpr(BinaryExpr {
            op: op @ (Operator::And | Operator::Or),
            ..
        }) => {
            let preds = flatten_junction_expr(expr, *op)?;
            Ok(Expression::Predicate(Box::new(Predicate::Junction(
                JunctionPredicate {
                    op: to_junction_op(*op),
                    preds,
                },
            ))))
        }
        Expr::BinaryExpr(BinaryExpr {
            op: op @ (Operator::Eq | Operator::Lt | Operator::Gt | Operator::IsDistinctFrom),
            left,
            right,
        }) => Ok(Expression::Predicate(Box::new(Predicate::Binary(
            BinaryPredicate {
                left: Box::new(to_delta_expression(left.as_ref())?),
                op: to_binary_predicate_op(*op)?,
                right: Box::new(to_delta_expression(right.as_ref())?),
            },
        )))),
        Expr::BinaryExpr(BinaryExpr {
            op: op @ (Operator::NotEq | Operator::LtEq | Operator::GtEq),
            left,
            right,
        }) => {
            let inverted = match op {
                Operator::NotEq => Operator::Eq,
                Operator::LtEq => Operator::Gt,
                Operator::GtEq => Operator::Lt,
                _ => unreachable!(),
            };
            Ok(Expression::Predicate(Box::new(Predicate::Not(Box::new(
                Predicate::Binary(BinaryPredicate {
                    left: Box::new(to_delta_expression(left.as_ref())?),
                    op: to_binary_predicate_op(inverted)?,
                    right: Box::new(to_delta_expression(right.as_ref())?),
                }),
            )))))
        }
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::IsNotDistinctFrom,
            left,
            right,
        }) => Ok(Expression::Predicate(Box::new(Predicate::Not(Box::new(
            Predicate::Binary(BinaryPredicate {
                left: Box::new(to_delta_expression(left.as_ref())?),
                op: to_binary_predicate_op(Operator::IsDistinctFrom)?,
                right: Box::new(to_delta_expression(right.as_ref())?),
            }),
        ))))),
        Expr::BinaryExpr(BinaryExpr { op, left, right }) => {
            Ok(Expression::Binary(BinaryExpression {
                left: Box::new(to_delta_expression(left.as_ref())?),
                op: to_binary_op(*op)?,
                right: Box::new(to_delta_expression(right.as_ref())?),
            }))
        }
        Expr::IsNull(expr) => Ok(Expression::Predicate(Box::new(Predicate::Unary(
            UnaryPredicate {
                op: UnaryPredicateOp::IsNull,
                expr: Box::new(to_delta_expression(expr.as_ref())?),
            },
        )))),
        Expr::IsNotNull(expr) => Ok(Expression::Predicate(Box::new(Predicate::Not(Box::new(
            Predicate::Unary(UnaryPredicate {
                op: UnaryPredicateOp::IsNull,
                expr: Box::new(to_delta_expression(expr.as_ref())?),
            }),
        ))))),
        Expr::Not(expr) => Ok(Expression::Predicate(Box::new(Predicate::Not(Box::new(
            Predicate::BooleanExpression(to_delta_expression(expr.as_ref())?),
        ))))),
        Expr::Between(between) => {
            let expr = to_delta_expression(&between.expr)?;
            let expression = Predicate::Junction(JunctionPredicate {
                op: JunctionPredicateOp::Or,
                preds: vec![
                    Predicate::Binary(BinaryPredicate {
                        left: Box::new(expr.clone()),
                        op: BinaryPredicateOp::LessThan,
                        right: Box::new(to_delta_expression(&between.low)?),
                    }),
                    Predicate::Binary(BinaryPredicate {
                        left: Box::new(expr),
                        op: BinaryPredicateOp::GreaterThan,
                        right: Box::new(to_delta_expression(&between.high)?),
                    }),
                ],
            });
            if between.negated {
                Ok(Expression::Predicate(Box::new(expression)))
            } else {
                Ok(Expression::Predicate(Box::new(Predicate::Not(Box::new(
                    expression,
                )))))
            }
        }
        Expr::ScalarFunction(scalar_fn) => {
            if scalar_fn.name() == "get_field" {
                if scalar_fn.args.len() != 2 {
                    return plan_err!(
                        "get_field function requires exactly 2 arguments, got {}",
                        scalar_fn.args.len()
                    );
                }

                let field_name = match &scalar_fn.args[1] {
                    Expr::Literal(name, _) => name.to_string(),
                    other => other.schema_name().to_string(),
                };

                if let Expression::Column(ref col_name) = to_delta_expression(&scalar_fn.args[0])? {
                    return Ok(Expression::Column(
                        col_name.join(&ColumnName::from_naive_str_split(field_name)),
                    ));
                }
            }
            plan_err!(
                "Scalar function not supported in Delta Kernel expressions: {:?}",
                scalar_fn
            )
        }
        _ => plan_err!("Cannot convert to kernel expression: {:?}", expr),
    }
}

pub(crate) fn datafusion_scalar_to_scalar(scalar: &ScalarValue) -> Result<Scalar> {
    match scalar {
        ScalarValue::Boolean(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Boolean(*value)),
            None => Ok(Scalar::Null(DataType::BOOLEAN)),
        },
        ScalarValue::Utf8(maybe_value)
        | ScalarValue::LargeUtf8(maybe_value)
        | ScalarValue::Utf8View(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::String(value.clone())),
            None => Ok(Scalar::Null(DataType::STRING)),
        },
        ScalarValue::Int8(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Byte(*value)),
            None => Ok(Scalar::Null(DataType::BYTE)),
        },
        ScalarValue::Int16(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Short(*value)),
            None => Ok(Scalar::Null(DataType::SHORT)),
        },
        ScalarValue::Int32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Integer(*value)),
            None => Ok(Scalar::Null(DataType::INTEGER)),
        },
        ScalarValue::Int64(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Long(*value)),
            None => Ok(Scalar::Null(DataType::LONG)),
        },
        ScalarValue::Float32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Float(*value)),
            None => Ok(Scalar::Null(DataType::FLOAT)),
        },
        ScalarValue::Float64(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Double(*value)),
            None => Ok(Scalar::Null(DataType::DOUBLE)),
        },
        ScalarValue::TimestampMicrosecond(maybe_value, Some(_)) => match maybe_value {
            Some(value) => Ok(Scalar::Timestamp(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP)),
        },
        ScalarValue::TimestampMicrosecond(maybe_value, None) => match maybe_value {
            Some(value) => Ok(Scalar::TimestampNtz(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP_NTZ)),
        },
        ScalarValue::Date32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Date(*value)),
            None => Ok(Scalar::Null(DataType::DATE)),
        },
        ScalarValue::Binary(maybe_value)
        | ScalarValue::LargeBinary(maybe_value)
        | ScalarValue::BinaryView(maybe_value)
        | ScalarValue::FixedSizeBinary(_, maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Binary(value.clone())),
            None => Ok(Scalar::Null(DataType::BINARY)),
        },
        ScalarValue::Decimal128(maybe_value, precision, scale) => match maybe_value {
            Some(value) => Ok(Scalar::Decimal(
                DecimalData::try_new(
                    *value,
                    DecimalType::try_new(*precision, *scale as u8)
                        .map_err(|e| plan_datafusion_err!("{e}"))?,
                )
                .map_err(|e| plan_datafusion_err!("{e}"))?,
            )),
            None => Ok(Scalar::Null(DataType::Primitive(PrimitiveType::Decimal(
                DecimalType::try_new(*precision, *scale as u8)
                    .map_err(|e| plan_datafusion_err!("{e}"))?,
            )))),
        },
        ScalarValue::Struct(data) => Ok(Scalar::from_array(data.as_ref(), 0)
            .ok_or_else(|| plan_datafusion_err!("Struct to kernel scalar conversion failed."))?),
        ScalarValue::Dictionary(_, value) => datafusion_scalar_to_scalar(value.as_ref()),
        _ => plan_err!("Cannot convert to kernel scalar value: {:?}", scalar),
    }
}

fn to_binary_predicate_op(op: Operator) -> Result<BinaryPredicateOp> {
    match op {
        Operator::Eq => Ok(BinaryPredicateOp::Equal),
        Operator::Lt => Ok(BinaryPredicateOp::LessThan),
        Operator::Gt => Ok(BinaryPredicateOp::GreaterThan),
        Operator::IsDistinctFrom => Ok(BinaryPredicateOp::Distinct),
        _ => plan_err!("Operator not supported in Delta Kernel: {:?}", op),
    }
}

fn to_binary_op(op: Operator) -> Result<BinaryExpressionOp> {
    match op {
        Operator::Plus => Ok(BinaryExpressionOp::Plus),
        Operator::Minus => Ok(BinaryExpressionOp::Minus),
        Operator::Multiply => Ok(BinaryExpressionOp::Multiply),
        Operator::Divide => Ok(BinaryExpressionOp::Divide),
        _ => plan_err!("Operator not supported in Delta Kernel: {:?}", op),
    }
}

/// Helper function to flatten nested AND/OR expressions into a single junction expression
fn flatten_junction_expr(expr: &Expr, target_op: Operator) -> Result<Vec<Predicate>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { op, left, right }) if *op == target_op => {
            let mut left_exprs = flatten_junction_expr(left.as_ref(), target_op)?;
            let mut right_exprs = flatten_junction_expr(right.as_ref(), target_op)?;
            left_exprs.append(&mut right_exprs);
            Ok(left_exprs)
        }
        _ => {
            let delta_expr = to_delta_predicate(expr)?;
            Ok(vec![delta_expr])
        }
    }
}

fn to_junction_op(op: Operator) -> JunctionPredicateOp {
    match op {
        Operator::And => JunctionPredicateOp::And,
        Operator::Or => JunctionPredicateOp::Or,
        _ => unimplemented!("Unsupported operator: {:?}", op),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        functions::core::expr_ext::FieldAccessor,
        logical_expr::{col, lit},
    };
    use delta_kernel::expressions::{BinaryExpressionOp, JunctionPredicateOp, Scalar};

    fn assert_junction_expr(
        expr: &Expr,
        expected_op: JunctionPredicateOp,
        expected_children: usize,
    ) {
        let delta_expr = to_delta_expression(expr).unwrap();
        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Junction(junction) => {
                    assert_eq!(junction.op, expected_op);
                    assert_eq!(junction.preds.len(), expected_children);
                }
                _ => panic!("Expected Junction predicate, got {:?}", predicate),
            },
            _ => panic!("Expected Junction expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_simple_and() {
        let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        assert_junction_expr(&expr, JunctionPredicateOp::And, 2);
    }

    #[test]
    fn test_simple_or() {
        let expr = col("a").eq(lit(1)).or(col("b").eq(lit(2)));
        assert_junction_expr(&expr, JunctionPredicateOp::Or, 2);
    }

    #[test]
    fn test_field_access() {
        let expr = col("a").field("b");
        assert_eq!(
            to_delta_expression(&expr).unwrap(),
            Expression::Column(ColumnName::from_naive_str_split("a.b"))
        );
        let expr = col("a").field("b").field("c");
        assert_eq!(
            to_delta_expression(&expr).unwrap(),
            Expression::Column(ColumnName::from_naive_str_split("a.b.c"))
        );

        let expr = col("a").field("b").field("c").eq(lit(10));
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Binary(binary) => {
                    assert_eq!(binary.op, BinaryPredicateOp::Equal);
                    match binary.left.as_ref() {
                        Expression::Column(name) => {
                            assert_eq!(name.to_string(), "a.b.c")
                        }
                        _ => panic!("Expected Column expression in left operand"),
                    }
                    match *binary.right.as_ref() {
                        Expression::Literal(Scalar::Integer(value)) => assert_eq!(value, 10),
                        _ => panic!("Expected Integer literal in right operand"),
                    }
                }
                _ => panic!("Expected Binary predicate, got {:?}", predicate),
            },
            _ => panic!("Expected Binary expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_nested_and() {
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionPredicateOp::And, 4);
    }

    #[test]
    fn test_nested_or() {
        let expr = col("a")
            .eq(lit(1))
            .or(col("b").eq(lit(2)))
            .or(col("c").eq(lit(3)))
            .or(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionPredicateOp::Or, 4);
    }

    #[test]
    fn test_mixed_nested_and_or() {
        // (a AND b) OR (c AND d)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c").eq(lit(3)).and(col("d").eq(lit(4)));
        let expr = left.or(right);

        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Junction(junction) => {
                    assert_eq!(junction.op, JunctionPredicateOp::Or);
                    assert_eq!(junction.preds.len(), 2);

                    // Check that both children are AND junctions
                    for child in &junction.preds {
                        match child {
                            Predicate::Junction(binary) => {
                                assert_eq!(binary.op, JunctionPredicateOp::And);
                            }
                            _ => panic!("Expected Binary expression in child: {:?}", child),
                        }
                    }
                }
                _ => panic!("Expected Junction predicate, got {:?}", predicate),
            },
            _ => panic!("Expected Junction expression"),
        }
    }

    #[test]
    fn test_deeply_nested_and() {
        // (((a AND b) AND c) AND d)
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionPredicateOp::And, 4);
    }

    #[test]
    fn test_complex_expression() {
        // (a AND b) OR ((c AND d) AND e)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c")
            .eq(lit(3))
            .and(col("d").eq(lit(4)))
            .and(col("e").eq(lit(5)));
        let expr = left.or(right);

        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Junction(junction) => {
                    assert_eq!(junction.op, JunctionPredicateOp::Or);
                    assert_eq!(junction.preds.len(), 2);

                    // First child should be an AND with 2 expressions
                    match &junction.preds[0] {
                        Predicate::Junction(child_junction) => {
                            assert_eq!(child_junction.op, JunctionPredicateOp::And);
                            assert_eq!(child_junction.preds.len(), 2);
                        }
                        _ => panic!("Expected Junction expression in first child"),
                    }

                    // Second child should be an AND with 3 expressions
                    match &junction.preds[1] {
                        Predicate::Junction(child_junction) => {
                            assert_eq!(child_junction.op, JunctionPredicateOp::And);
                            assert_eq!(child_junction.preds.len(), 3);
                        }
                        _ => panic!("Expected Junction expression in second child"),
                    }
                }
                _ => panic!("Expected Junction predicate, got {:?}", predicate),
            },
            _ => panic!("Expected Junction expression"),
        }
    }

    #[test]
    fn test_column_expression() {
        let expr = col("test_column");
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Column(name) => assert_eq!(&name.to_string(), "test_column"),
            _ => panic!("Expected Column expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_literal_expressions() {
        // Test boolean literal
        let expr = lit(true);
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::Boolean(value)) => assert!(value),
            _ => panic!("Expected Boolean literal, got {:?}", delta_expr),
        }

        // Test string literal
        let expr = lit("test");
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::String(value)) => assert_eq!(value, "test"),
            _ => panic!("Expected String literal, got {:?}", delta_expr),
        }

        // Test integer literal
        let expr = lit(42i32);
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::Integer(value)) => assert_eq!(value, 42),
            _ => panic!("Expected Integer literal, got {:?}", delta_expr),
        }

        // Test decimal literal
        let expr = lit(ScalarValue::Decimal128(Some(12345), 10, 2));
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::Decimal(data)) => {
                assert_eq!(data.bits(), 12345);
                assert_eq!(data.precision(), 10);
                assert_eq!(data.scale(), 2);
            }
            _ => panic!("Expected Decimal literal, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_binary_expressions() {
        // Test comparison operators
        let test_cases = vec![
            (col("a").eq(lit(1)), BinaryPredicateOp::Equal),
            (col("a").lt(lit(1)), BinaryPredicateOp::LessThan),
            (col("a").gt(lit(1)), BinaryPredicateOp::GreaterThan),
        ];

        for (expr, expected_op) in test_cases {
            let delta_expr = to_delta_expression(&expr).unwrap();
            match delta_expr {
                Expression::Predicate(predicate) => match predicate.as_ref() {
                    Predicate::Binary(binary) => {
                        assert_eq!(binary.op, expected_op);
                        match binary.left.as_ref() {
                            Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                            _ => panic!("Expected Column expression in left operand"),
                        }
                        match *binary.right.as_ref() {
                            Expression::Literal(Scalar::Integer(value)) => assert_eq!(value, 1),
                            _ => panic!("Expected Integer literal in right operand"),
                        }
                    }
                    _ => panic!("Expected Binary predicate, got {:?}", predicate),
                },
                _ => panic!("Expected Binary expression, got {:?}", delta_expr),
            }
        }

        // Test arithmetic operators
        let test_cases = vec![
            (col("a") + lit(1), BinaryExpressionOp::Plus),
            (col("a") - lit(1), BinaryExpressionOp::Minus),
            (col("a") * lit(1), BinaryExpressionOp::Multiply),
            (col("a") / lit(1), BinaryExpressionOp::Divide),
        ];

        for (expr, expected_op) in test_cases {
            let delta_expr = to_delta_expression(&expr).unwrap();
            match delta_expr {
                Expression::Binary(binary) => {
                    assert_eq!(binary.op, expected_op);
                    match binary.left.as_ref() {
                        Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                        _ => panic!("Expected Column expression in left operand"),
                    }
                    match *binary.right.as_ref() {
                        Expression::Literal(Scalar::Integer(value)) => assert_eq!(value, 1),
                        _ => panic!("Expected Integer literal in right operand"),
                    }
                }
                _ => panic!("Expected Binary expression, got {:?}", delta_expr),
            }
        }
    }

    #[test]
    fn test_unary_expressions() {
        // Test IS NULL
        let expr = col("a").is_null();
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Unary(unary) => {
                    assert_eq!(unary.op, UnaryPredicateOp::IsNull);
                    match unary.expr.as_ref() {
                        Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                        _ => panic!("Expected Column expression in operand"),
                    }
                }
                _ => panic!("Expected Unary predicate, got {:?}", predicate),
            },
            _ => panic!("Expected Unary expression, got {:?}", delta_expr),
        }

        // Test NOT
        let expr = !col("a");
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Not(unary) => match unary.as_ref() {
                    Predicate::BooleanExpression(expr) => match expr {
                        Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                        _ => panic!("Expected Column expression in operand"),
                    },
                    _ => panic!("Expected Boolean expression in operand"),
                },
                _ => panic!("Expected Unary predicate, got {:?}", predicate),
            },
            _ => panic!("Expected Unary expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_null_literals() {
        let test_cases = vec![
            (lit(ScalarValue::Boolean(None)), DataType::BOOLEAN),
            (lit(ScalarValue::Utf8(None)), DataType::STRING),
            (lit(ScalarValue::Int32(None)), DataType::INTEGER),
            (lit(ScalarValue::Float64(None)), DataType::DOUBLE),
        ];

        for (expr, expected_type) in test_cases {
            let delta_expr = to_delta_expression(&expr).unwrap();
            match delta_expr {
                Expression::Literal(Scalar::Null(data_type)) => {
                    assert_eq!(data_type, expected_type);
                }
                _ => panic!("Expected Null literal, got {:?}", delta_expr),
            }
        }
    }

    #[test]
    fn test_between_expressions() {
        // Test BETWEEN (not negated) - should be equivalent to: NOT (x < low OR x > high)
        let expr = col("x").between(lit(10), lit(20));
        let delta_expr = to_delta_expression(&expr).unwrap();

        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Not(not_pred) => match not_pred.as_ref() {
                    Predicate::Junction(junction) => {
                        assert_eq!(junction.op, JunctionPredicateOp::Or);
                        assert_eq!(junction.preds.len(), 2);

                        // First predicate should be x < 10
                        match &junction.preds[0] {
                            Predicate::Binary(binary) => {
                                assert_eq!(binary.op, BinaryPredicateOp::LessThan);
                                match binary.left.as_ref() {
                                    Expression::Column(name) => assert_eq!(name.to_string(), "x"),
                                    _ => panic!("Expected Column expression in left operand"),
                                }
                                match binary.right.as_ref() {
                                    Expression::Literal(Scalar::Integer(value)) => {
                                        assert_eq!(*value, 10)
                                    }
                                    _ => panic!("Expected Integer literal in right operand"),
                                }
                            }
                            _ => panic!("Expected Binary predicate for first condition"),
                        }

                        // Second predicate should be x > 20
                        match &junction.preds[1] {
                            Predicate::Binary(binary) => {
                                assert_eq!(binary.op, BinaryPredicateOp::GreaterThan);
                                match binary.left.as_ref() {
                                    Expression::Column(name) => assert_eq!(name.to_string(), "x"),
                                    _ => panic!("Expected Column expression in left operand"),
                                }
                                match binary.right.as_ref() {
                                    Expression::Literal(Scalar::Integer(value)) => {
                                        assert_eq!(*value, 20)
                                    }
                                    _ => panic!("Expected Integer literal in right operand"),
                                }
                            }
                            _ => panic!("Expected Binary predicate for second condition"),
                        }
                    }
                    _ => panic!("Expected Junction predicate inside NOT"),
                },
                _ => panic!("Expected NOT predicate for BETWEEN, got {:?}", predicate),
            },
            _ => panic!("Expected Predicate expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_not_between_expressions() {
        // Test NOT BETWEEN (negated) - should be equivalent to: x < low OR x > high
        let expr = col("y").not_between(lit(5), lit(15));
        let delta_expr = to_delta_expression(&expr).unwrap();

        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Junction(junction) => {
                    assert_eq!(junction.op, JunctionPredicateOp::Or);
                    assert_eq!(junction.preds.len(), 2);

                    // First predicate should be y < 5
                    match &junction.preds[0] {
                        Predicate::Binary(binary) => {
                            assert_eq!(binary.op, BinaryPredicateOp::LessThan);
                            match binary.left.as_ref() {
                                Expression::Column(name) => assert_eq!(name.to_string(), "y"),
                                _ => panic!("Expected Column expression in left operand"),
                            }
                            match binary.right.as_ref() {
                                Expression::Literal(Scalar::Integer(value)) => {
                                    assert_eq!(*value, 5)
                                }
                                _ => panic!("Expected Integer literal in right operand"),
                            }
                        }
                        _ => panic!("Expected Binary predicate for first condition"),
                    }

                    // Second predicate should be y > 15
                    match &junction.preds[1] {
                        Predicate::Binary(binary) => {
                            assert_eq!(binary.op, BinaryPredicateOp::GreaterThan);
                            match binary.left.as_ref() {
                                Expression::Column(name) => assert_eq!(name.to_string(), "y"),
                                _ => panic!("Expected Column expression in left operand"),
                            }
                            match binary.right.as_ref() {
                                Expression::Literal(Scalar::Integer(value)) => {
                                    assert_eq!(*value, 15)
                                }
                                _ => panic!("Expected Integer literal in right operand"),
                            }
                        }
                        _ => panic!("Expected Binary predicate for second condition"),
                    }
                }
                _ => panic!(
                    "Expected Junction predicate for NOT BETWEEN, got {:?}",
                    predicate
                ),
            },
            _ => panic!("Expected Predicate expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_between_with_expressions() {
        // Test BETWEEN with expressions as bounds: col("a") + 1 BETWEEN col("low") AND col("high")
        let expr = (col("a") + lit(1)).between(col("low"), col("high"));
        let delta_expr = to_delta_expression(&expr).unwrap();

        match delta_expr {
            Expression::Predicate(predicate) => match predicate.as_ref() {
                Predicate::Not(not_pred) => match not_pred.as_ref() {
                    Predicate::Junction(junction) => {
                        assert_eq!(junction.op, JunctionPredicateOp::Or);
                        assert_eq!(junction.preds.len(), 2);

                        // Verify the expression being tested is (a + 1)
                        for pred in &junction.preds {
                            match pred {
                                Predicate::Binary(binary) => match binary.left.as_ref() {
                                    Expression::Binary(bin_expr) => {
                                        assert_eq!(bin_expr.op, BinaryExpressionOp::Plus);
                                        match bin_expr.left.as_ref() {
                                            Expression::Column(name) => {
                                                assert_eq!(name.to_string(), "a")
                                            }
                                            _ => panic!("Expected Column 'a' in binary expression"),
                                        }
                                    }
                                    _ => panic!("Expected Binary expression for (a + 1)"),
                                },
                                _ => panic!("Expected Binary predicate"),
                            }
                        }
                    }
                    _ => panic!("Expected Junction predicate inside NOT"),
                },
                _ => panic!("Expected NOT predicate for BETWEEN"),
            },
            _ => panic!("Expected Predicate expression"),
        }
    }
}
