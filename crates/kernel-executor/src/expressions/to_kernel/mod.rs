use datafusion::arrow::array::{Array as _, StructArray};
use datafusion::common::tree_node::{Transformed, TreeNode as _};
use datafusion::common::{Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::expressions::{
    BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp, ColumnName,
    DecimalData, Expression, JunctionPredicate, JunctionPredicateOp, Predicate, Scalar, StructData,
    UnaryPredicate, UnaryPredicateOp,
};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType, StructField};

/// Converts a DataFusion expression to a Delta predicate.
///
/// If the expression converts to a Delta predicate, returns it directly.
/// Otherwise, wraps the expression as a boolean expression predicate.
pub fn to_delta_predicate(expr: &Expr) -> Result<Predicate> {
    match to_delta_expression(&normalize_delta_predicate_expr(expr)?)? {
        Expression::Predicate(pred) => Ok(pred.as_ref().clone()),
        expr => Ok(Predicate::BooleanExpression(expr)),
    }
}

/// Returns true when kernel file skipping selects exactly the files whose partition values
/// satisfy `expr` under SQL semantics.
///
/// Kernel file skipping is best effort: it keeps every file for any term it cannot evaluate
/// (arithmetic, `IS NOT DISTINCT FROM`, `IS NULL` over a comparison, NULL literals, ...). Only
/// comparisons between a partition column and a non-null literal, `IS [NOT] NULL` on a partition
/// column and `IN` lists of non-null literals, combined with `AND` / `OR`, are evaluated exactly.
pub fn is_exact_partition_predicate(expr: &Expr, partition_columns: &[String]) -> bool {
    let is_partition_column =
        |expr: &Expr| matches!(expr, Expr::Column(c) if partition_columns.contains(&c.name));
    let is_non_null_literal = |expr: &Expr| matches!(expr, Expr::Literal(v, _) if !v.is_null());

    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And | Operator::Or,
            right,
        }) => {
            is_exact_partition_predicate(left, partition_columns)
                && is_exact_partition_predicate(right, partition_columns)
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op:
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq,
            right,
        }) => {
            (is_partition_column(left) && is_non_null_literal(right))
                || (is_non_null_literal(left) && is_partition_column(right))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => is_partition_column(inner),
        Expr::InList(in_list) => {
            is_partition_column(&in_list.expr)
                && !in_list.list.is_empty()
                && in_list.list.iter().all(is_non_null_literal)
        }
        _ => false,
    }
}

fn normalize_delta_predicate_expr(expr: &Expr) -> Result<Expr> {
    let transformed = expr.clone().transform_up(|expr| match expr {
        Expr::InList(in_list) => Ok(match rewrite_in_list_expr_for_kernel(&in_list) {
            Some(lowered) => Transformed::yes(lowered),
            None => Transformed::no(Expr::InList(in_list)),
        }),
        other => Ok(Transformed::no(other)),
    })?;

    Ok(transformed.data)
}

/// Rewrites supported `IN` / `NOT IN` expressions into conjunctions or
/// disjunctions that can be converted into Delta kernel predicates.
///
/// Returns `None` when any list item cannot be rewritten without changing the
/// meaning of the predicate. In that case callers should leave the original
/// `Expr::InList` unchanged and let downstream conversion decide how to handle it.
fn rewrite_in_list_expr_for_kernel(in_list: &InList) -> Option<Expr> {
    if in_list.list.is_empty() {
        return Some(Expr::Literal(
            ScalarValue::Boolean(Some(in_list.negated)),
            None,
        ));
    }

    let list = in_list
        .list
        .iter()
        .map(|item| match item {
            Expr::Literal(value, _) if !value.is_null() => Some(item.clone()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;

    let lowered_terms = list.into_iter().map(|item| {
        if in_list.negated {
            in_list.expr.as_ref().clone().not_eq(item)
        } else {
            in_list.expr.as_ref().clone().eq(item)
        }
    });

    if in_list.negated {
        conjunction(lowered_terms)
    } else {
        disjunction(lowered_terms)
    }
}

/// Converts a DataFusion expression to a Delta kernel expression.
pub fn to_delta_expression(expr: &Expr) -> Result<Expression> {
    match expr {
        Expr::Column(column) => Ok(Expression::Column(ColumnName::new([column.name.as_str()]))),
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
            // `get_field(a, 'b', 'c')` is `a.b.c`: each argument after the first is one level.
            if scalar_fn.name() == "get_field" {
                let Some((base, field_names)) = scalar_fn.args.split_first() else {
                    return plan_err!("get_field function requires arguments");
                };
                if field_names.is_empty() {
                    return plan_err!("get_field function requires a field name");
                }

                let field_names = field_names.iter().map(|name| match name {
                    Expr::Literal(name, _) => name.to_string(),
                    other => other.schema_name().to_string(),
                });

                if let Expression::Column(ref col_name) = to_delta_expression(base)? {
                    return Ok(Expression::Column(
                        col_name.join(&ColumnName::new(field_names)),
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

pub fn datafusion_scalar_to_scalar(scalar: &ScalarValue) -> Result<Scalar> {
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
        #[cfg(feature = "nanosecond-timestamps")]
        ScalarValue::TimestampNanosecond(maybe_value, Some(_)) => match maybe_value {
            Some(value) => Ok(Scalar::TimestampNanos(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP_NANOS)),
        },
        #[cfg(feature = "nanosecond-timestamps")]
        ScalarValue::TimestampNanosecond(maybe_value, None) => match maybe_value {
            Some(value) => Ok(Scalar::TimestampNanosNtz(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP_NANOS_NTZ)),
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
        ScalarValue::Struct(data) => struct_scalar(data),
        ScalarValue::Dictionary(_, value) => datafusion_scalar_to_scalar(value.as_ref()),
        _ => plan_err!("Cannot convert to kernel scalar value: {:?}", scalar),
    }
}

/// The kernel scalar of the one-row struct array `data`.
fn struct_scalar(data: &StructArray) -> Result<Scalar> {
    if data.is_null(0) {
        return Ok(Scalar::Null(data.data_type().try_into_kernel()?));
    }
    let fields = data
        .fields()
        .iter()
        .map(|field| field.as_ref().try_into_kernel())
        .collect::<std::result::Result<Vec<StructField>, _>>()?;
    let values = data
        .columns()
        .iter()
        .map(|column| datafusion_scalar_to_scalar(&ScalarValue::try_from_array(column, 0)?))
        .collect::<Result<Vec<_>>>()?;
    let data = StructData::try_new(fields, values).map_err(|e| plan_datafusion_err!("{e}"))?;
    Ok(Scalar::Struct(data))
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
mod tests;
