use std::sync::Arc;

use arrow::array::AsArray as _;
use arrow::datatypes::UInt16Type;
use arrow_array::StringArray;
use datafusion::common::tree_node::{Transformed, TreeNode as _};
use datafusion::common::{DFSchemaRef, HashSet};
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{
    BinaryExpr, Expr, ExprSchemable as _, LogicalPlan, LogicalPlanBuilder,
};
use datafusion::logical_expr_common::operator::Operator;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, simplify_predicates};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor, execute_stream};
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use datafusion::{catalog::Session, common::DFSchema};
use datafusion_datasource::file_scan_config::wrap_partition_value_in_dict;
use delta_kernel::Predicate;
use futures::TryStreamExt as _;
use itertools::Itertools as _;

use crate::delta_datafusion::engine::to_delta_predicate;
use crate::delta_datafusion::logical::LogicalPlanBuilderExt as _;
use crate::delta_datafusion::table_provider::next::KernelScanPlan;
use crate::delta_datafusion::{
    DeltaScanExec, DeltaScanNext, FILE_ID_COLUMN_DEFAULT, FindFilesExprProperties,
};
use crate::kernel::EagerSnapshot;
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
        let context = SimplifyContext::new(session.execution_props()).with_schema(schema);
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

/// Extracts fields from the parquet scan
#[derive(Default)]
pub(crate) struct DeltaScanVisitor {
    pub(crate) delta_plan: Option<KernelScanPlan>,
}

impl DeltaScanVisitor {
    fn pre_visit_delta_scan(&mut self, delta_scan_exec: &DeltaScanExec) -> Result<bool> {
        self.delta_plan = Some(delta_scan_exec.delta_plan().clone());
        Ok(true)
    }
}

impl ExecutionPlanVisitor for DeltaScanVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if let Some(delta_scan_exec) = plan.as_any().downcast_ref::<DeltaScanExec>() {
            return self.pre_visit_delta_scan(delta_scan_exec);
        };

        Ok(true)
    }
}

/// Coerce literal types in predicates to match the column types from the schema.
///
/// This function walks the expression tree and for binary comparison expressions
/// (e.g., `column = 3`), it ensures that literal values are cast to match the
/// actual column type from the schema. This is necessary because SQL parsers
/// typically infer literal types independently (e.g., `3` becomes `Int64`),
/// which may not match the schema's column type (e.g., `Int32`).
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

                let lit_expr = Expr::Literal(lit_value.cast_to(&expr_type)?, None);
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
            _ => Ok(Transformed::no(e)),
        }
    })
    .map(|transformed| transformed.data)
}

/// Create a table scan plan for reading data from
/// all files which contain data matching a predicate.
///
/// This is useful for DML where we need to re-write files by
/// updating/removing some records so we require all records
/// but only from matching files.
///
///
/// ## Returns
///
/// If no files contaon matching data, `None` is returned.
///
/// Otherwise:
/// - The logical plan for reading data from matched files only.
/// - The set of matched file URLs.
/// - The potentially simplified predicate applied when matching data.
/// - A kernel predicate (best effort) which can be used to filter log replays.
pub(crate) async fn scan_for_rewrite(
    session: &dyn Session,
    snapshot: &EagerSnapshot,
    predicate: Expr,
) -> Result<Option<(LogicalPlan, HashSet<String>, Expr, Arc<Predicate>)>> {
    let skipping_pred = simplify_predicates(split_conjunction_owned(predicate))?;

    // validate that the expressions contain no illegal variants
    // that are not eligible for file skipping, e.g. volatile functions.
    for term in &skipping_pred {
        let mut visitor = FindFilesExprProperties::default();
        term.visit(&mut visitor)?;
        visitor.result?
    }

    // convert to a delta predicate that can be applied to kernel scans.
    // This is a best effort predicate and downstream code needs to also
    // apply the explicit file selection so we can ignore errors in the
    // conversion.
    let delta_predicate = Arc::new(Predicate::and_from(
        skipping_pred
            .iter()
            .flat_map(|p| to_delta_predicate(p).ok()),
    ));

    let predicate = conjunction(skipping_pred.clone()).unwrap_or(lit(true));

    // Scan the delta table with a dedicated predicate applied for file skipping
    // and with the source file path exosed as column.
    let table_source = provider_as_source(
        DeltaScanNext::builder()
            .with_eager_snapshot(snapshot.clone())
            .with_file_skipping_predicates(skipping_pred.clone())
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .await?,
    );

    // the kernel scan only provides a best effort file skipping, in this case
    // we want to determine the file we certainly need to rewrite. For this
    // we perform an initial aggreagte scan to see if we can quickly find
    // at least one matching record in the files.
    let files_plan = LogicalPlanBuilder::scan("scan", table_source.clone(), None)?
        .filter(predicate.clone())?
        .aggregate(
            [col(FILE_ID_COLUMN_DEFAULT)],
            [first_value(lit(1_i32), vec![])],
        )?
        .build()?;
    let files_exec = session.create_physical_plan(&files_plan).await?;
    let valid_files: HashSet<_> = execute_stream(files_exec, session.task_ctx())?
        .map_ok(|f| {
            let dict_arr = f.column(0).as_dictionary::<UInt16Type>();
            let typed_dict = dict_arr.downcast_dict::<StringArray>().unwrap();
            typed_dict
                .values()
                .iter()
                .flatten()
                .map(|s| s.to_string())
                .collect_vec()
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect();

    if valid_files.is_empty() {
        return Ok(None);
    }

    // Crate a table scan limiting the data to that originating from valid files.
    let file_list = valid_files
        .iter()
        .cloned()
        .map(|v| lit(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(v)))))
        .collect_vec();
    let plan = LogicalPlanBuilder::scan("source", table_source, None)?
        .filter(col(FILE_ID_COLUMN_DEFAULT).in_list(file_list, false))?
        .drop_columns([FILE_ID_COLUMN_DEFAULT])?
        .build()?;

    Ok(Some((plan, valid_files, predicate, delta_predicate)))
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
}
