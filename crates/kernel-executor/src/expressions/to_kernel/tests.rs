use super::*;
use datafusion::{
    common::Column,
    functions::core::expr_ext::FieldAccessor,
    logical_expr::{binary_expr, col, lit},
};
use delta_kernel::expressions::{BinaryExpressionOp, JunctionPredicateOp, Scalar};

fn assert_junction_expr(expr: &Expr, expected_op: JunctionPredicateOp, expected_children: usize) {
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

fn assert_string_equality_predicate(predicate: &Predicate, expected_column: &str, expected: &str) {
    match predicate {
        Predicate::Binary(binary) => {
            assert_eq!(binary.op, BinaryPredicateOp::Equal);
            match binary.left.as_ref() {
                Expression::Column(name) => assert_eq!(name.to_string(), expected_column),
                other => panic!("Expected column expression, got {:?}", other),
            }
            match binary.right.as_ref() {
                Expression::Literal(Scalar::String(value)) => assert_eq!(value, expected),
                other => panic!("Expected string literal, got {:?}", other),
            }
        }
        other => panic!("Expected binary predicate, got {:?}", other),
    }
}

fn assert_string_inequality_predicate(
    predicate: &Predicate,
    expected_column: &str,
    expected: &str,
) {
    match predicate {
        Predicate::Not(inner) => {
            assert_string_equality_predicate(inner.as_ref(), expected_column, expected)
        }
        other => panic!("Expected NOT predicate, got {:?}", other),
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
fn test_in_list_rewrites_to_or_of_equalities() {
    let expr = col("part").in_list(vec![lit("a"), lit("c")], false);
    let delta_predicate = to_delta_predicate(&expr).unwrap();

    match delta_predicate {
        Predicate::Junction(junction) => {
            assert_eq!(junction.op, JunctionPredicateOp::Or);
            assert_eq!(junction.preds.len(), 2);
            assert_string_equality_predicate(&junction.preds[0], "part", "a");
            assert_string_equality_predicate(&junction.preds[1], "part", "c");
        }
        other => panic!("Expected OR junction, got {:?}", other),
    }
}

#[test]
fn test_not_in_list_rewrites_to_and_of_inequalities() {
    let expr = col("part").in_list(vec![lit("a"), lit("c")], true);
    let delta_predicate = to_delta_predicate(&expr).unwrap();

    match delta_predicate {
        Predicate::Junction(junction) => {
            assert_eq!(junction.op, JunctionPredicateOp::And);
            assert_eq!(junction.preds.len(), 2);
            assert_string_inequality_predicate(&junction.preds[0], "part", "a");
            assert_string_inequality_predicate(&junction.preds[1], "part", "c");
        }
        other => panic!("Expected AND junction, got {:?}", other),
    }
}

#[test]
fn test_empty_in_list_rewrites_to_boolean_constant() {
    let expr = col("part").in_list(Vec::<Expr>::new(), false);
    assert_eq!(
        to_delta_predicate(&expr).unwrap(),
        Predicate::BooleanExpression(Expression::Literal(Scalar::Boolean(false)))
    );

    let negated = col("part").in_list(Vec::<Expr>::new(), true);
    assert_eq!(
        to_delta_predicate(&negated).unwrap(),
        Predicate::BooleanExpression(Expression::Literal(Scalar::Boolean(true)))
    );
}

#[test]
fn test_normalize_delta_predicate_expr_leaves_non_literal_in_list_unchanged() {
    let expr = col("part").in_list(vec![lit("a"), col("other")], false);

    assert_eq!(normalize_delta_predicate_expr(&expr).unwrap(), expr);
}

#[test]
fn test_normalize_delta_predicate_expr_leaves_not_in_list_with_null_unchanged() {
    let expr = col("part").in_list(vec![lit("a"), lit(ScalarValue::Utf8(None))], true);

    assert_eq!(normalize_delta_predicate_expr(&expr).unwrap(), expr);
}

#[test]
fn test_field_access() {
    let expr = col("a").field("b");
    assert_eq!(
        to_delta_expression(&expr).unwrap(),
        Expression::Column(ColumnName::new(["a", "b"]))
    );
    let expr = col("a").field("b").field("c");
    assert_eq!(
        to_delta_expression(&expr).unwrap(),
        Expression::Column(ColumnName::new(["a", "b", "c"]))
    );
    // The simplifier merges nested accesses into one call.
    let expr = datafusion::functions::core::get_field().call(vec![col("a"), lit("b"), lit("c")]);
    assert_eq!(
        to_delta_expression(&expr).unwrap(),
        Expression::Column(ColumnName::new(["a", "b", "c"]))
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
fn test_column_expression_preserves_dots_in_name() {
    let expr = Expr::Column(Column::from_name("a.b"));
    assert_eq!(
        to_delta_expression(&expr).unwrap(),
        Expression::Column(ColumnName::new(["a.b"]))
    );
}

#[test]
fn test_field_access_preserves_dots_in_field_name_segment() {
    let expr = col("a").field("b.c");
    assert_eq!(
        to_delta_expression(&expr).unwrap(),
        Expression::Column(ColumnName::new(["a", "b.c"]))
    );
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

#[test]
fn test_is_exact_partition_predicate() {
    let partition_columns = ["p".to_string(), "q".to_string()];
    let exact = |expr: Expr| is_exact_partition_predicate(&expr, &partition_columns);
    let null_bool = || lit(ScalarValue::Boolean(None));

    assert!(exact(col("p").eq(lit(1))));
    assert!(exact(lit(1).lt(col("p"))));
    assert!(exact(col("p").not_eq(lit(1)).and(col("q").gt_eq(lit("a")))));
    assert!(exact(col("p").is_null().or(col("q").is_not_null())));
    assert!(exact(col("p").in_list(vec![lit(1), lit(2)], true)));

    assert!(!exact(col("x").eq(lit(1))));
    assert!(!exact(col("p").eq(col("q"))));
    assert!(!exact((col("p") + lit(1)).eq(lit(2))));
    assert!(!exact(binary_expr(
        col("p").eq(lit(1)),
        Operator::IsNotDistinctFrom,
        lit(true)
    )));
    assert!(!exact(
        col("q").gt(lit("a")).is_null().or(col("q").lt_eq(lit("a")))
    ));
    assert!(!exact(null_bool()));
    assert!(!exact(null_bool().and(col("p").eq(lit(1)))));
    assert!(!exact(col("p").eq(lit(1)).or(col("x").eq(lit(1)))));
    assert!(!exact(col("p").eq(lit(ScalarValue::Int32(None)))));
    assert!(!exact(
        col("p").in_list(vec![lit(1), lit(ScalarValue::Int32(None))], true)
    ));
    assert!(!exact(col("p").in_list(vec![], false)));
}

/// A struct literal, set and NULL, round trips through the DataFusion scalar.
#[test]
fn test_struct_scalar_round_trip() {
    let fields = vec![
        StructField::nullable("a", DataType::INTEGER),
        StructField::nullable("b", DataType::STRING),
    ];
    let set = Scalar::Struct(
        StructData::try_new(
            fields.clone(),
            vec![Scalar::Integer(1), Scalar::Null(DataType::STRING)],
        )
        .unwrap(),
    );
    let null = Scalar::Null(DataType::try_struct_type(fields).unwrap());
    for scalar in [set, null] {
        let df_scalar = crate::expressions::to_datafusion_scalar(&scalar).unwrap();
        assert_eq!(datafusion_scalar_to_scalar(&df_scalar).unwrap(), scalar);
    }
}
