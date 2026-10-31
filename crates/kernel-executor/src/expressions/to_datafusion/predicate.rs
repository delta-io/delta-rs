//! Converts a kernel [`Predicate`] into a boolean-valued DataFusion `Expr`.

use datafusion::functions_nested::expr_fn::array_has;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, lit};
use delta_kernel::Predicate;
use delta_kernel::expressions::{
    ArrayData, BinaryPredicate, BinaryPredicateOp, ColumnName, Expression, JunctionPredicate,
    JunctionPredicateOp, Scalar, UnaryPredicate, UnaryPredicateOp,
};
use delta_kernel::schema::{DataType, StructType};
use delta_kernel::{DeltaResult, Error as KernelError};
use itertools::Itertools;

use super::expression::to_datafusion_expr;
use super::scalar::to_datafusion_scalar;

/// Converts a kernel Predicate into a boolean-valued DataFusion Expr, checking column references
/// against `input_schema`.
///
/// # Errors
/// Returns [`KernelError::unsupported`] for engine-defined (`Opaque`) or `Unknown` predicates.
/// Also propagates errors from child expressions, such as an unresolved column or an interval
/// literal (which has no Arrow equivalent).
pub fn to_datafusion_predicate_expr(
    predicate: &Predicate,
    input_schema: &StructType,
) -> DeltaResult<Expr> {
    match predicate {
        Predicate::BooleanExpression(expr) => to_datafusion_expr(expr, input_schema, None),
        Predicate::Not(expr) => Ok(!(to_datafusion_predicate_expr(expr, input_schema)?)),
        Predicate::Unary(expr) => unary_pred_to_df(expr, input_schema),
        Predicate::Binary(expr) => binary_pred_to_df(expr, input_schema),
        Predicate::Junction(expr) => junction_to_df(expr, input_schema),
        Predicate::Opaque(_) => Err(KernelError::unsupported(
            "cannot convert an engine-defined Opaque predicate",
        )),
        Predicate::Unknown(name) => Err(KernelError::unsupported(format!(
            "cannot convert Unknown predicate {name:?}"
        ))),
    }
}

fn unary_pred_to_df(unary: &UnaryPredicate, input_schema: &StructType) -> DeltaResult<Expr> {
    let UnaryPredicate { op, expr } = unary;
    let df_expr = to_datafusion_expr(expr, input_schema, None)?;
    Ok(match op {
        UnaryPredicateOp::IsNull => df_expr.is_null(),
    })
}

fn binary_pred_to_df(bin: &BinaryPredicate, input_schema: &StructType) -> DeltaResult<Expr> {
    let BinaryPredicate { left, op, right } = bin;
    if let BinaryPredicateOp::In = op {
        return in_to_df_predicate_expr(left, right, input_schema);
    }
    let left_expr = to_datafusion_expr(left, input_schema, None)?;
    let right_expr = to_datafusion_expr(right, input_schema, None)?;

    Ok(match op {
        BinaryPredicateOp::Equal => left_expr.eq(right_expr),
        BinaryPredicateOp::LessThan => left_expr.lt(right_expr),
        BinaryPredicateOp::GreaterThan => left_expr.gt(right_expr),
        BinaryPredicateOp::Distinct => Expr::BinaryExpr(BinaryExpr {
            left: left_expr.into(),
            op: Operator::IsDistinctFrom,
            right: right_expr.into(),
        }),
        BinaryPredicateOp::In => unreachable!("handled above"),
    })
}

/// Lowers an `IN` predicate. Kernel models `x IN (..)` as `Binary(In, value, elements)`, where
/// `value` is a literal and `elements` is either a literal array (`1 IN (1, 2)`) or an array-typed
/// column (`1 IN col`). A literal array lowers to DataFusion's [`InList`], whose trailing flag
/// negates the test; a column lowers to `array_has(col, value)`. Kernel has no negated `IN`
/// (`NOT IN` arrives as `Not(Binary(In, ..))`, handled by the caller's `Not` arm), so the `InList`
/// flag is always `false` here.
///
/// This accepts exactly the shapes kernel's Arrow evaluator accepts: both of its arms require a
/// literal left operand, so a column left operand is rejected here as it is there.
///
/// Kernel and DataFusion agree once the result is forced non-null. Kernel compares elements with
/// logical (SQL) equality and reports membership as a plain boolean: a null matches nothing (not
/// even another null), so `NULL IN (1, 2)` and `NULL IN (1, NULL)` are both false, and the result
/// is never null. DataFusion's [`InList`] and `array_has` both keep SQL three-valued logic, so a
/// null on either side yields null. This lowering wraps the test in `IS TRUE`, collapsing that
/// null to false to match kernel.
///
/// `IS TRUE` sits inside the predicate, not around it, because the caller's `Not` arm may negate
/// the result and a leftover null would change which rows a filter keeps. Kernel reads
/// `NOT NULL IN (1, 2)` as `NOT false` = true, but an unguarded DataFusion `NOT NULL` stays null
/// and drops the row.
///
/// # Errors
/// Returns [`KernelError::unsupported`] if the left operand is not a literal, or the right operand
/// is neither a literal array nor an array-typed column.
fn in_to_df_predicate_expr(
    value: &Expression,
    list: &Expression,
    input_schema: &StructType,
) -> DeltaResult<Expr> {
    // Kernel's Arrow evaluator accepts `IN` only with a literal left operand (a column left
    // operand errors), so reject anything else to match it exactly.
    let Expression::Literal(_) = value else {
        return Err(KernelError::unsupported(
            "converting an IN predicate requires a literal left-hand side",
        ));
    };
    let value = to_datafusion_expr(value, input_schema, None)?;
    let membership = match list {
        Expression::Literal(Scalar::Array(array)) => in_list_expr(value, array)?,
        Expression::Column(name) => array_has_expr(value, list, name, input_schema)?,
        _ => {
            return Err(KernelError::unsupported(
                "converting an IN predicate requires a literal array or an array-typed column on \
                 the right-hand side",
            ));
        }
    };
    Ok(membership.is_true())
}
/// Builds `value IN (<elements>)` from a literal array. Null elements stay in the list: they can
/// never match under logical equality, and the caller's `IS TRUE` collapses the null they would
/// otherwise contribute down to false.
fn in_list_expr(value: Expr, array: &ArrayData) -> DeltaResult<Expr> {
    let elements: Vec<Expr> = array
        .array_elements()
        .iter()
        .map(|scalar| Ok(lit(to_datafusion_scalar(scalar)?)))
        .collect::<DeltaResult<_>>()?;
    let in_expr = Expr::InList(InList::new(Box::new(value), elements, false));
    Ok(in_expr)
}
/// Builds `array_has(<column>, value)` for `value IN <list column>`, which kernel evaluates
/// element-wise. `column` is the kernel column expression and `name` its resolved name. Note the
/// flipped argument order: haystack first, needle second.
///
/// # Errors
/// Returns [`KernelError::unsupported`] if `name` does not resolve to an array-typed column.
fn array_has_expr(
    value: Expr,
    column: &Expression,
    name: &ColumnName,
    input_schema: &StructType,
) -> DeltaResult<Expr> {
    let DataType::Array(_) = input_schema.field_at(name)?.data_type else {
        return Err(KernelError::unsupported(
            "converting an IN predicate against a column requires an array-typed column",
        ));
    };
    Ok(array_has(
        to_datafusion_expr(column, input_schema, None)?,
        value,
    ))
}

/// Lowers a junction (`And`/`Or`) by converting each child and folding them with
/// [`fold_terms`]. An empty junction lowers `AND` to `true` and `OR` to `false`, keeping kernel
/// semantics.
fn junction_to_df(junction: &JunctionPredicate, input_schema: &StructType) -> DeltaResult<Expr> {
    let JunctionPredicate { op, preds } = junction;
    let df_exprs: Vec<_> = preds
        .iter()
        .map(|e| to_datafusion_predicate_expr(e, input_schema))
        .try_collect()?;
    match op {
        JunctionPredicateOp::And => Ok(fold_terms(df_exprs, Expr::and, lit(true))),
        JunctionPredicateOp::Or => Ok(fold_terms(df_exprs, Expr::or, lit(false))),
    }
}

/// Junctions up to this many terms keep the left-deep shape a hand-written chain has.
const BALANCED_FOLD_THRESHOLD: usize = 256;

/// Folds terms left-associatively, or pairwise past [`BALANCED_FOLD_THRESHOLD`] so tree depth is
/// O(log n): a left-deep tree of thousands of terms overflows the stack when walked.
fn fold_terms(mut terms: Vec<Expr>, combine: fn(Expr, Expr) -> Expr, empty: Expr) -> Expr {
    if terms.len() <= BALANCED_FOLD_THRESHOLD {
        return terms.into_iter().reduce(combine).unwrap_or(empty);
    }
    while terms.len() > 1 {
        let mut next = Vec::with_capacity(terms.len().div_ceil(2));
        let mut iter = terms.into_iter();
        while let Some(a) = iter.next() {
            next.push(match iter.next() {
                Some(b) => combine(a, b),
                None => a,
            });
        }
        terms = next;
    }
    terms.pop().unwrap_or(empty)
}

#[cfg(test)]
mod predicate_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, AsArray, Int64Array, ListArray, RecordBatch, new_null_array,
    };
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use datafusion::common::DFSchema;
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::expressions::{
        Expression as KernelExpr, Predicate as KernelPred, column_expr, lit, null_lit,
    };
    use delta_kernel::schema::{ArrayType, DataType, StructField};
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    /// Columns these tests resolve against: top-level `a`, `b`, `c` (all `long`) and `list`
    /// (an array of nullable `long`, the right-hand side of a list-column `IN`).
    fn test_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
            StructField::nullable("list", ArrayType::new(DataType::LONG, true)),
        ])
        .unwrap()
    }

    /// Lowers a predicate and returns its DataFusion `Display` string.
    fn lower(pred: KernelPred) -> String {
        to_datafusion_predicate_expr(&pred, &test_schema())
            .unwrap()
            .to_string()
    }

    /// Lowers a predicate, runs it over one all-null row, and asserts the result is not null.
    fn evaluate(pred: KernelPred) -> bool {
        let df_expr = to_datafusion_predicate_expr(&pred, &test_schema()).unwrap();
        let arrow_schema: ArrowSchema = (&test_schema()).try_into_arrow().unwrap();
        let arrow_schema = Arc::new(arrow_schema);
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            arrow_schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), 1))
                .collect(),
        )
        .unwrap();

        let df_schema = DFSchema::try_from(arrow_schema).unwrap();
        let physical = SessionContext::new()
            .create_physical_expr(df_expr, &df_schema)
            .unwrap();
        let result = physical
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let result = result.as_boolean();

        assert_eq!(result.null_count(), 0, "predicate evaluated to null");
        result.value(0)
    }
    /// A literal `Scalar::Array` of longs.
    fn long_array(values: impl IntoIterator<Item = i64>) -> KernelExpr {
        let elements: Vec<Scalar> = values.into_iter().map(Scalar::Long).collect();
        let array = ArrayData::try_new(ArrayType::new(DataType::LONG, false), elements).unwrap();
        lit(Scalar::Array(array))
    }
    /// A literal `Scalar::Array` of longs where `None` becomes a null element.
    fn nullable_long_array(values: impl IntoIterator<Item = Option<i64>>) -> KernelExpr {
        let elements: Vec<Scalar> = values
            .into_iter()
            .map(|v| match v {
                Some(n) => Scalar::Long(n),
                None => Scalar::Null(DataType::LONG),
            })
            .collect();
        let array = ArrayData::try_new(ArrayType::new(DataType::LONG, true), elements).unwrap();
        lit(Scalar::Array(array))
    }
    // === Tests ===

    #[rstest]
    // Primitive comparisons lower to a native binary op.
    #[case::eq(column_expr!("a").eq(lit(1i64)), "a = Int64(1)")]
    #[case::lt(column_expr!("a").lt(lit(1i64)), "a < Int64(1)")]
    #[case::gt(column_expr!("a").gt(lit(1i64)), "a > Int64(1)")]
    #[case::distinct(
        column_expr!("a").distinct(lit(1i64)),
        "a IS DISTINCT FROM Int64(1)"
    )]
    // Kernel has no <=/>=/!= ops: each is `Not` of a comparison, so it renders negated.
    #[case::ne(column_expr!("a").ne(lit(1i64)), "NOT a = Int64(1)")]
    #[case::le(column_expr!("a").le(lit(1i64)), "NOT a > Int64(1)")]
    #[case::ge(column_expr!("a").ge(lit(1i64)), "NOT a < Int64(1)")]
    // Unary.
    #[case::is_null(column_expr!("a").is_null(), "a IS NULL")]
    #[case::is_not_null(column_expr!("a").is_not_null(), "NOT a IS NULL")]
    // IN / NOT IN. Kernel requires a literal needle. `IS TRUE` turns DataFusion's null into
    // kernel's false, and sits inside the `Not` so a null value gives `NOT false` instead of
    // `NOT NULL`.
    #[case::in_list(
        KernelPred::binary(BinaryPredicateOp::In, lit(1i64), long_array([1, 2, 3])),
        "Int64(1) IN ([Int64(1), Int64(2), Int64(3)]) IS TRUE"
    )]
    #[case::not_in(
        KernelPred::not(KernelPred::binary(BinaryPredicateOp::In, lit(1i64), long_array([1, 2]))),
        "NOT Int64(1) IN ([Int64(1), Int64(2)]) IS TRUE"
    )]
    #[case::null_needle_in_list(
        KernelPred::binary(
            BinaryPredicateOp::In,
            null_lit(DataType::LONG),
            long_array([1, 2]),
        ),
        "Int64(NULL) IN ([Int64(1), Int64(2)]) IS TRUE"
    )]
    // A null element stays in the list; it can never match under logical equality, and `IS TRUE`
    // collapses the null it contributes to false.
    #[case::null_element_in_list(
        KernelPred::binary(
            BinaryPredicateOp::In,
            lit(1i64),
            nullable_long_array([Some(1), None]),
        ),
        "Int64(1) IN ([Int64(1), Int64(NULL)]) IS TRUE"
    )]
    // A literal against a list column lowers to `array_has(list, value)` (haystack first).
    #[case::in_list_column(
        KernelPred::binary(BinaryPredicateOp::In, lit(1i64), column_expr!("list")),
        "array_has(list, Int64(1)) IS TRUE"
    )]
    // Junctions fold left-associatively.
    #[case::and(
        KernelPred::and(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL AND b IS NULL"
    )]
    #[case::or(
        KernelPred::or(column_expr!("a").is_null(), column_expr!("b").is_null()),
        "a IS NULL OR b IS NULL"
    )]
    #[case::multi_and(
        KernelPred::and_from([
            column_expr!("a").is_null(),
            column_expr!("b").is_null(),
            column_expr!("c").is_null(),
        ]),
        "a IS NULL AND b IS NULL AND c IS NULL"
    )]
    // A bare boolean expression goes straight to the expression converter.
    #[case::boolean_expression(KernelPred::from_expr(column_expr!("a")), "a")]
    // Nested predicates. DataFusion's Display omits parens around the junction under a `Not`, but
    // the `Expr` tree is still `Not(And(..))`.
    #[case::not_of_junction(
        KernelPred::not(KernelPred::and(column_expr!("a").is_null(), column_expr!("b").is_null())),
        "NOT a IS NULL AND b IS NULL"
    )]
    #[case::junction_of_junction(
        KernelPred::or(
            KernelPred::and(column_expr!("a").is_null(), column_expr!("b").is_null()),
            column_expr!("c").is_null(),
        ),
        "a IS NULL AND b IS NULL OR c IS NULL"
    )]
    #[case::and_of_comparisons(
        KernelPred::and(
            column_expr!("a").eq(lit(1i64)),
            column_expr!("b").gt(lit(2i64)),
        ),
        "a = Int64(1) AND b > Int64(2)"
    )]
    fn predicate_lowers_to_expected(#[case] kernel: KernelPred, #[case] expected: &str) {
        assert_eq!(lower(kernel), expected);
    }

    #[test]
    fn long_junction_lowers_without_stack_overflow() {
        // Pin the stack to the 2 MiB default so RUST_MIN_STACK cannot hide a regression.
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let n = 10_000i64;
                let terms = || (0..n).map(|i| column_expr!("a").eq(lit(i)));
                let expected = |op| (0..n).map(|i| format!("a = Int64({i})")).join(op);
                // Display recurses over the whole tree, and checks every term is kept in order.
                assert_eq!(lower(KernelPred::or_from(terms())), expected(" OR "));
                assert_eq!(lower(KernelPred::and_from(terms())), expected(" AND "));
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[rstest]
    // Engine-defined and unknown predicates have no DataFusion equivalent.
    #[case::unknown(KernelPred::Unknown("mystery".into()))]
    // Kernel's evaluator rejects a column needle, so we do too, whether the elements are a literal
    // array or a list column.
    #[case::column_needle_in_literal_array(
        KernelPred::binary(BinaryPredicateOp::In, column_expr!("a"), long_array([1, 2]))
    )]
    #[case::column_needle_in_list_column(
        KernelPred::binary(BinaryPredicateOp::In, column_expr!("a"), column_expr!("list"))
    )]
    // A literal needle against a non-array column (`b` is `long`) cannot be lowered.
    #[case::in_non_array_column(
        KernelPred::binary(BinaryPredicateOp::In, lit(1i64), column_expr!("b"))
    )]
    fn unsupported_predicate_is_an_error(#[case] pred: KernelPred) {
        to_datafusion_predicate_expr(&pred, &test_schema()).unwrap_err();
    }

    /// `IN` answers true or false but never null, so it still works under a `NOT`. Membership uses
    /// logical (SQL) equality, so a null matches nothing -- not even another null -- matching
    /// kernel's evaluator.
    #[rstest]
    #[case::present(Some(2), &[Some(1), Some(2)], true)]
    #[case::absent(Some(9), &[Some(1), Some(2)], false)]
    #[case::null_needle_without_null_element(None, &[Some(1), Some(2)], false)]
    #[case::null_needle_with_null_element(None, &[Some(1), None], false)]
    #[case::present_alongside_null_element(Some(1), &[Some(1), None], true)]
    #[case::absent_alongside_null_element(Some(9), &[Some(1), None], false)]
    #[case::only_null_element(None, &[None], false)]
    fn in_predicate_matches_membership_and_never_nulls(
        #[case] needle: Option<i64>,
        #[case] elements: &[Option<i64>],
        #[case] expected: bool,
    ) {
        let in_pred = KernelPred::binary(
            BinaryPredicateOp::In,
            match needle {
                Some(n) => lit(n),
                None => null_lit(DataType::LONG),
            },
            nullable_long_array(elements.to_vec()),
        );

        assert_eq!(evaluate(in_pred.clone()), expected);
        assert_eq!(evaluate(KernelPred::not(in_pred)), !expected);
    }
    /// A literal `IN <list column>` lowers to `array_has` and follows the same never-null,
    /// logical-equality rule as the literal-array form: a null needle or a null element matches
    /// nothing, and the result is always a plain boolean, so `NOT IN` is its exact complement.
    #[rstest]
    #[case::present(Some(2), &[Some(1), Some(2)], true)]
    #[case::absent(Some(9), &[Some(1), Some(2)], false)]
    #[case::null_needle_without_null_element(None, &[Some(1), Some(2)], false)]
    #[case::null_needle_with_null_element(None, &[Some(1), None], false)]
    #[case::present_alongside_null_element(Some(1), &[Some(1), None], true)]
    #[case::absent_alongside_null_element(Some(9), &[Some(1), None], false)]
    #[case::only_null_element(None, &[None], false)]
    fn in_list_column_matches_membership_and_never_nulls(
        #[case] needle: Option<i64>,
        #[case] elements: &[Option<i64>],
        #[case] expected: bool,
    ) {
        let in_pred = KernelPred::binary(
            BinaryPredicateOp::In,
            match needle {
                Some(n) => lit(n),
                None => null_lit(DataType::LONG),
            },
            column_expr!("list"),
        );
        // Column `list` carries the elements; `a`/`b`/`c` are unused.
        let batch = list_column_batch(elements);

        assert_eq!(evaluate_over(in_pred.clone(), &batch), expected);
        assert_eq!(evaluate_over(KernelPred::not(in_pred), &batch), !expected);
    }
    /// Lowers `pred` against [`test_schema`], evaluates it over the single-row `batch`, and asserts
    /// the one result is not null.
    fn evaluate_over(pred: KernelPred, batch: &RecordBatch) -> bool {
        let df_expr = to_datafusion_predicate_expr(&pred, &test_schema()).unwrap();
        let df_schema = DFSchema::try_from(batch.schema()).unwrap();
        let result = SessionContext::new()
            .create_physical_expr(df_expr, &df_schema)
            .unwrap()
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let result = result.as_boolean();

        assert_eq!(result.len(), 1, "expected a single-row result");
        assert_eq!(result.null_count(), 0, "predicate evaluated to null");
        result.value(0)
    }
    /// A one-row batch whose single `list` column holds `elements`.
    fn list_column_batch(elements: &[Option<i64>]) -> RecordBatch {
        // Reuse the schema's own `list` field so the built array matches the name and metadata
        // kernel synthesizes (an `element` child field, possibly with field-id metadata).
        let arrow_schema: ArrowSchema = (&test_schema()).try_into_arrow().unwrap();
        let list_field = arrow_schema.field_with_name("list").unwrap().clone();
        let ArrowDataType::List(element_field) = list_field.data_type().clone() else {
            unreachable!("`list` is declared as an array type");
        };
        let list = ListArray::new(
            element_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, elements.len() as i32])),
            Arc::new(Int64Array::from(elements.to_vec())),
            None,
        );
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![list_field])),
            vec![Arc::new(list)],
        )
        .unwrap()
    }
}
