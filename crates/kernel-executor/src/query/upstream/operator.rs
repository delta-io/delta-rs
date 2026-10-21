//! Conversion from a kernel [`Operator`](KernelOperator) to a DataFusion
//! [`LogicalPlan`](DFLogicalPlan) node.
//!
//! DataFusion has no single "operator" type: each relational operator is its own `LogicalPlan`
//! variant holding its inputs, so lowering an operator *is* building the plan node that wraps its
//! already-lowered inputs. Each node uses its validating constructor: filters use
//! [`DFFilter::try_new`], aggregates use [`DFAggregate::try_new_with_schema`], and values use
//! [`LogicalPlanBuilder`].
//!
//! This module lowers one operator at a time; the walk that feeds it each node's inputs is
//! [`crate::plan`].

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::{DFSchema, DataFusionError, NullEquality, Result as DFResult};
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::logical_expr::{
    col as df_col, lit as df_lit, Aggregate as DFAggregate, EmptyRelation, Expr as DFExpr,
    ExprFunctionExt, ExprSchemable, Filter as DFFilter, Join as DFJoin, JoinConstraint, JoinType,
    LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, Projection as DFProjection, Union as DFUnion,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::{ColumnName as KernelColumnName, Scalar as KernelScalar};
use delta_kernel::plans::ir::nodes::{
    Agg as KernelAgg, Aggregate as KernelAggregate, Filter as KernelFilter,
    Operator as KernelOperator, Project as KernelProject, SemiJoin as KernelSemiJoin,
    UnionAll as KernelUnionAll, Values as KernelValues,
};
use delta_kernel::schema::{StructField, StructType};

use crate::expression::to_df_struct_columns;
use crate::predicate::to_df_predicate_expr;
use crate::scalar::to_df_scalar;
use crate::utils::column_to_df_expr;

/// Lowers one kernel [`Operator`](KernelOperator) over its already-lowered inputs.
///
/// # Errors
/// Returns an error if the operator has the wrong number of inputs, has no DataFusion lowering
/// yet, or if lowering its payload fails.
pub(crate) fn lower_operator(
    op: &KernelOperator,
    inputs: &[Arc<DFLogicalPlan>],
) -> DFResult<DFLogicalPlan> {
    let input_count_error = |expected: usize| {
        DataFusionError::Plan(format!(
            "{op} expects {expected} input(s), but received {}",
            inputs.len()
        ))
    };
    match op {
        KernelOperator::Values(values) => {
            let [] = inputs else {
                return Err(input_count_error(0));
            };
            lower_values(values)
        }
        KernelOperator::Project(project) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_project(project, input)
        }
        KernelOperator::Filter(filter) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_filter(filter, input)
        }
        KernelOperator::Aggregate(aggregate) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_aggregate(aggregate, input)
        }
        KernelOperator::SemiJoin(semi_join) => {
            let [probe, build] = inputs else {
                return Err(input_count_error(2));
            };
            lower_semi_join(semi_join, probe, build)
        }
        KernelOperator::UnionAll(union_all) => lower_union_all(union_all, inputs),
        // TODO: lower the remaining operators (scans and Load), each in its own change.
        _ => Err(DataFusionError::NotImplemented(format!(
            "lowering operator {op} to a DataFusion LogicalPlan"
        ))),
    }
}

/// Lowers a [`Project`](KernelProject) to a single DataFusion projection.
///
/// A kernel Project holds a struct expression and its declared output schema, while DataFusion
/// expects a flat expression list. Each struct field is therefore lowered, cast to its declared
/// type, and aliased with its declared name before the declared output schema is attached to the
/// DataFusion projection.
///
/// # Errors
/// Returns an error if the Project expression is not a `Struct`/`StructPatch`, lowering a field or
/// its nullability guard fails, or DataFusion cannot cast a field to its declared type.
fn lower_project(project: &KernelProject, input: &Arc<DFLogicalPlan>) -> DFResult<DFLogicalPlan> {
    let input_schema: StructType = input.schema().as_arrow().try_into_kernel()?;
    let arrow_schema: ArrowSchema = project.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(DFSchema::try_from(arrow_schema)?);
    let columns = to_df_struct_columns(&project.expr, &input_schema, project.schema.as_ref())
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .into_guarded_columns();
    let exprs: DFResult<Vec<DFExpr>> = columns
        .into_iter()
        .zip(project.schema.fields())
        .map(|((name, expr), field)| {
            let target = field.data_type().try_into_arrow()?;
            let expr = expr.cast_to(&target, input.schema())?.alias(name);
            Ok(expr)
        })
        .collect();
    let projection = DFProjection::try_new_with_schema(exprs?, Arc::clone(input), df_schema)?;
    Ok(DFLogicalPlan::Projection(projection))
}

/// Lowers a [`SemiJoin`](KernelSemiJoin) to a DataFusion left semi or left anti join over its probe
/// and build inputs.
///
/// NULL join keys compare equal, matching `SyncPlanExecutor`. For example, when both inputs contain
/// a NULL key, the corresponding probe row is retained by a left semi join and excluded by a left
/// anti join.
///
/// Join keys with different types follow DataFusion's equality coercion. This can differ from
/// `SyncPlanExecutor`, which compares type-specific Arrow row encodings without coercion.
///
/// # Errors
/// Returns an error if the key counts differ, a key cannot be lowered, or DataFusion rejects the
/// join.
fn lower_semi_join(
    semi_join: &KernelSemiJoin,
    probe: &Arc<DFLogicalPlan>,
    build: &Arc<DFLogicalPlan>,
) -> DFResult<DFLogicalPlan> {
    if semi_join.probe_keys.len() != semi_join.build_keys.len() {
        return Err(DataFusionError::Plan(format!(
            "SemiJoin declares {} probe key(s), but {} build key(s)",
            semi_join.probe_keys.len(),
            semi_join.build_keys.len()
        )));
    }

    let probe_schema = probe.schema().as_ref();
    let build_schema = build.schema().as_ref();
    let join_keys: DFResult<Vec<_>> = semi_join
        .probe_keys
        .iter()
        .zip(&semi_join.build_keys)
        .map(|(probe_key, build_key)| {
            let probe_key = column_to_df_expr(probe_key, probe_schema)?;
            let build_key = column_to_df_expr(build_key, build_schema)?;
            Ok((probe_key, build_key))
        })
        .collect();
    let join_type = if semi_join.inverted {
        JoinType::LeftAnti
    } else {
        JoinType::LeftSemi
    };
    let mut join = DFJoin::try_new(
        Arc::clone(probe),
        Arc::clone(build),
        join_keys?,
        None,
        join_type,
        JoinConstraint::On,
        NullEquality::NullEqualsNull,
        false,
    )?;
    // Left semi and anti joins output only probe columns, so retain the probe schema.
    join.schema = Arc::clone(probe.schema());

    Ok(DFLogicalPlan::Join(join))
}

/// Lowers a [`UnionAll`](KernelUnionAll) to one n-ary DataFusion union that preserves every input
/// row, including duplicates.
///
/// # Errors
/// Returns an error if there are fewer than two inputs, their schemas differ, or DataFusion rejects
/// the union.
fn lower_union_all(
    _union_all: &KernelUnionAll,
    inputs: &[Arc<DFLogicalPlan>],
) -> DFResult<DFLogicalPlan> {
    let [first, _, ..] = inputs else {
        return Err(DataFusionError::Plan(format!(
            "union_all expects at least 2 input(s), but received {}",
            inputs.len()
        )));
    };
    let schemas_match = inputs
        .iter()
        .skip(1)
        .all(|input| input.schema().as_arrow() == first.schema().as_arrow());
    if !schemas_match {
        return Err(DataFusionError::Plan(
            "union_all requires all inputs to have the same schema".to_string(),
        ));
    }

    let union_inputs = inputs.iter().map(Arc::clone).collect();
    let union = DFUnion::try_new(union_inputs)?;

    Ok(DFLogicalPlan::Union(union))
}

/// Lowers an [`Aggregate`](KernelAggregate) to a DataFusion aggregate over its input.
///
/// This lowering does not cast aggregate operands. It passes them using their input-schema types,
/// and DataFusion's type-coercion analyzer inserts any casts required by each aggregate function's
/// signature. The explicit casts here only make group keys and final aggregate results match the
/// aggregate's declared output schema.
fn lower_aggregate(
    aggregate: &KernelAggregate,
    input: &Arc<DFLogicalPlan>,
) -> DFResult<DFLogicalPlan> {
    let input_schema = input.schema().as_ref();
    let output_fields = Vec::from_iter(aggregate.schema.fields());
    if aggregate.group_by.is_empty() && aggregate.aggs.is_empty() {
        let arrow_schema: ArrowSchema = aggregate.schema.as_ref().try_into_arrow()?;
        let empty = EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(arrow_schema.try_into()?),
        };
        return Ok(DFLogicalPlan::EmptyRelation(empty));
    }

    let group_exprs: DFResult<Vec<_>> = aggregate
        .group_by
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let expr = column_to_df_expr(column, input_schema)?;
            cast_aggregate_output(expr, output_fields.get(index).copied(), input_schema)
        })
        .collect();
    let aggregate_exprs: DFResult<Vec<_>> = aggregate
        .aggs
        .iter()
        .enumerate()
        .map(|(index, agg)| {
            let expr = lower_aggregate_function(agg, input_schema)?;
            let field_index = aggregate.group_by.len() + index;
            cast_aggregate_output(expr, output_fields.get(field_index).copied(), input_schema)
        })
        .collect();

    let arrow_schema: ArrowSchema = aggregate.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(DFSchema::try_from(arrow_schema)?);
    let df_aggregate = DFAggregate::try_new_with_schema(
        Arc::clone(input),
        group_exprs?,
        aggregate_exprs?,
        df_schema,
    )?;
    Ok(DFLogicalPlan::Aggregate(df_aggregate))
}

fn cast_aggregate_output(
    expr: DFExpr,
    field: Option<&StructField>,
    input_schema: &DFSchema,
) -> DFResult<DFExpr> {
    let Some(field) = field else {
        // No output field matches this expression; keep it for DataFusion's arity check.
        return Ok(expr);
    };
    let target = field.data_type().try_into_arrow()?;
    let expr = expr.cast_to(&target, input_schema)?;
    Ok(expr.alias(field.name().clone()))
}

fn lower_aggregate_function(agg: &KernelAgg, input_schema: &DFSchema) -> DFResult<DFExpr> {
    match agg {
        KernelAgg::Min(value) => Ok(min(column_to_df_expr(value, input_schema)?)),
        KernelAgg::Max(value) => Ok(max(column_to_df_expr(value, input_schema)?)),
        KernelAgg::Sum(value) => Ok(sum(column_to_df_expr(value, input_schema)?)),
        KernelAgg::Count(value) => Ok(count(column_to_df_expr(value, input_schema)?)),
        // DataFusion produces 1 for every input row, so counting it implements COUNT(*).
        KernelAgg::CountStar => Ok(count(df_lit(1))),
        KernelAgg::MinNonNullBy(operands) => lower_non_null_by(
            &operands.value,
            &operands.null_sentinel,
            &operands.key,
            input_schema,
            true,
        ),
        KernelAgg::MaxNonNullBy(operands) => lower_non_null_by(
            &operands.value,
            &operands.null_sentinel,
            &operands.key,
            input_schema,
            false,
        ),
    }
}

fn lower_non_null_by(
    value: &KernelColumnName,
    null_sentinel: &KernelColumnName,
    key: &KernelColumnName,
    input_schema: &DFSchema,
    ascending: bool,
) -> DFResult<DFExpr> {
    let value = column_to_df_expr(value, input_schema)?;
    let null_sentinel = column_to_df_expr(null_sentinel, input_schema)?;
    let key = column_to_df_expr(key, input_schema)?;
    let filter = null_sentinel.is_not_null().and(key.clone().is_not_null());
    let first_value = first_value_udaf().call(vec![value]);
    first_value
        .order_by(vec![key.sort(ascending, false)])
        .filter(filter)
        .build()
}

/// Lowers a [`Filter`](KernelFilter) node into a DataFusion [`Filter`](DFFilter) logical plan over
/// its input.
fn lower_filter(filter: &KernelFilter, input: &Arc<DFLogicalPlan>) -> DFResult<DFLogicalPlan> {
    let input_schema: StructType = input.schema().as_arrow().try_into_kernel()?;
    let predicate = to_df_predicate_expr(&filter.predicate, &input_schema)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let filter = DFFilter::try_new(predicate, Arc::clone(input))?;
    Ok(DFLogicalPlan::Filter(filter))
}

/// Lowers a [`Values`](KernelValues) node into literal rows carrying `schema`'s field names.
///
/// An empty `rows` is the uninhabited relation over `schema`, which DataFusion spells as an
/// `EmptyRelation` rather than a `Values`.
///
/// DataFusion's [`LogicalPlanBuilder::values_with_schema`] automatically inserts a cast whenever
/// [`can_cast_types`](datafusion::arrow::compute::can_cast_types) accepts a type mismatch. Since
/// this still conforms to Kernel's schema, it is accepted functionality.
fn lower_values(values: &KernelValues) -> DFResult<DFLogicalPlan> {
    let arrow_schema: ArrowSchema = values.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(arrow_schema.try_into()?);

    if values.rows.is_empty() {
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        };
        return Ok(DFLogicalPlan::EmptyRelation(empty));
    }

    let rows: DFResult<Vec<Vec<DFExpr>>> = values.rows.iter().map(|row| lower_row(row)).collect();
    // The builder assigns column1, column2, ...; restore the names declared by the kernel schema.
    let field_aliases = df_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| df_col(format!("column{}", index + 1)).alias(field.name()));
    LogicalPlanBuilder::values_with_schema(rows?, &df_schema)?
        .project(field_aliases)?
        .build()
}

/// Lowers one row of literals into DataFusion expressions, one per column.
///
/// # Errors
/// Returns an error for a literal with no DataFusion equivalent.
fn lower_row(row: &[KernelScalar]) -> DFResult<Vec<DFExpr>> {
    let lower_literal = |scalar| {
        let lowered =
            to_df_scalar(scalar).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(df_lit(lowered))
    };
    row.iter().map(lower_literal).collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue as DFScalarValue;
    use datafusion::logical_expr::{col as df_col, Case};
    use datafusion::prelude::SessionContext;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use delta_kernel::expressions::{
        col, column_name, lit as kernel_lit, null_lit, ArrayData as KernelArrayData,
        BinaryPredicateOp as KernelBinaryPredicateOp, Expression as KernelExpr, ExpressionRef,
        ExpressionStructPatchBuilder, JunctionPredicateOp as KernelJunctionPredicateOp,
        MapData as KernelMapData, Predicate as KernelPredicate, StructData as KernelStructData,
    };
    use delta_kernel::schema::{
        schema, ArrayType, DataType, MapType, SchemaRef, StructField, StructType,
    };
    use delta_kernel::struct_patch::ProjectionStructPatchBuilder;
    use delta_kernel::PlanBuilder;
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    /// A two-field schema: `a` long, `b` string.
    fn test_schema() -> StructType {
        schema! {
            nullable "a": LONG,
            nullable "b": STRING,
        }
    }

    fn lower_values_node(
        schema: StructType,
        rows: Vec<Vec<KernelScalar>>,
    ) -> DFResult<DFLogicalPlan> {
        let values = KernelValues::new(schema, rows);
        lower_operator(&KernelOperator::Values(values), &[])
    }

    async fn execute(plan: DFLogicalPlan) -> DFResult<Vec<RecordBatch>> {
        SessionContext::new()
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await
    }

    /// An empty input relation with `schema`.
    fn input_with_schema(schema: StructType) -> Arc<DFLogicalPlan> {
        Arc::new(lower_values_node(schema, vec![]).unwrap())
    }

    fn output_names(plan: &DFLogicalPlan) -> Vec<&String> {
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect()
    }

    fn output_types(plan: &DFLogicalPlan) -> Vec<ArrowDataType> {
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    }

    fn qualified_input_with_schema(schema: StructType, qualifier: &str) -> Arc<DFLogicalPlan> {
        Arc::new(
            LogicalPlanBuilder::from(input_with_schema(schema))
                .alias(qualifier)
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    // === Values ===

    async fn execute_rows(
        schema: StructType,
        rows: Vec<Vec<KernelScalar>>,
    ) -> DFResult<Vec<RecordBatch>> {
        execute(lower_values_node(schema, rows)?).await
    }

    fn schema_for_row(names: &[&str], row: &[KernelScalar]) -> StructType {
        let fields = names
            .iter()
            .zip(row)
            .map(|(name, scalar)| StructField::nullable(*name, scalar.data_type()));
        schema! { ..(fields) }
    }

    fn nested_scalars() -> Vec<KernelScalar> {
        let struct_fields = schema! {
            not_null "a": INTEGER,
            nullable "b": STRING,
        }
        .into_fields()
        .collect();
        let struct_scalar = KernelScalar::Struct(
            KernelStructData::try_new(
                struct_fields,
                vec![KernelScalar::Integer(1), KernelScalar::String("x".into())],
            )
            .unwrap(),
        );
        let array_scalar = KernelScalar::Array(
            KernelArrayData::try_new(
                ArrayType::new(DataType::INTEGER, true),
                [
                    KernelScalar::Integer(1),
                    KernelScalar::null(DataType::INTEGER),
                ],
            )
            .unwrap(),
        );
        let map_scalar = KernelScalar::Map(
            KernelMapData::try_new(
                MapType::new(DataType::STRING, DataType::INTEGER, true),
                [
                    (KernelScalar::String("x".into()), KernelScalar::Integer(1)),
                    (
                        KernelScalar::String("y".into()),
                        KernelScalar::null(DataType::INTEGER),
                    ),
                ],
            )
            .unwrap(),
        );
        vec![struct_scalar, array_scalar, map_scalar]
    }

    /// Kernel's absent relation builds to an empty [`KernelValues`], so this shape is reachable
    /// from any empty file set and must not be mistaken for a malformed plan.
    #[test]
    fn empty_values_lowers_to_empty_relation() {
        let plan = lower_values_node(test_schema(), vec![]).unwrap();
        let DFLogicalPlan::EmptyRelation(empty) = &plan else {
            panic!("expected EmptyRelation, got {plan:?}");
        };
        assert!(!empty.produce_one_row);
        assert_eq!(
            plan.schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["a", "b"],
            "schema survives an empty relation"
        );
    }

    #[rstest]
    #[case::multiple_rows(
        &["a", "b"],
        vec![vec![1i64.into(), "x".into()], vec![2i64.into(), "y".into()]],
        &[
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | x |",
            "| 2 | y |",
            "+---+---+",
        ]
    )]
    #[case::numeric(
        &["byte", "short", "integer", "long", "float", "double", "decimal"],
        vec![vec![
            KernelScalar::Byte(1),
            KernelScalar::Short(2),
            KernelScalar::Integer(3),
            KernelScalar::Long(4),
            KernelScalar::Float(1.25),
            KernelScalar::Double(2.5),
            KernelScalar::decimal(12345, 7, 2).unwrap(),
        ]],
        &[
            "+------+-------+---------+------+-------+--------+---------+",
            "| byte | short | integer | long | float | double | decimal |",
            "+------+-------+---------+------+-------+--------+---------+",
            "| 1    | 2     | 3       | 4    | 1.25  | 2.5    | 123.45  |",
            "+------+-------+---------+------+-------+--------+---------+",
        ]
    )]
    #[case::string_boolean_binary_and_null(
        &["string", "boolean", "binary", "null"],
        vec![vec![
            KernelScalar::String("hello".into()),
            KernelScalar::Boolean(true),
            KernelScalar::Binary(vec![0x01, 0x02]),
            KernelScalar::null(DataType::INTEGER),
        ]],
        &[
            "+--------+---------+--------+------+",
            "| string | boolean | binary | null |",
            "+--------+---------+--------+------+",
            "| hello  | true    | 0102   |      |",
            "+--------+---------+--------+------+",
        ]
    )]
    #[case::timestamp_timestamp_ntz_and_date(
        &["timestamp", "timestamp_ntz", "date"],
        vec![vec![
            KernelScalar::Timestamp(1_000_000),
            KernelScalar::TimestampNtz(1_000_000),
            KernelScalar::Date(1),
        ]],
        &[
            "+----------------------+---------------------+------------+",
            "| timestamp            | timestamp_ntz       | date       |",
            "+----------------------+---------------------+------------+",
            "| 1970-01-01T00:00:01Z | 1970-01-01T00:00:01 | 1970-01-02 |",
            "+----------------------+---------------------+------------+",
        ]
    )]
    #[case::array_map_and_struct(
        &["struct", "array", "map"],
        vec![nested_scalars()],
        &[
            "+--------------+-------+-------------+",
            "| struct       | array | map         |",
            "+--------------+-------+-------------+",
            "| {a: 1, b: x} | [1, ] | {x: 1, y: } |",
            "+--------------+-------+-------------+",
        ]
    )]
    #[tokio::test]
    async fn values_execute_rows(
        #[case] names: &[&str],
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] expected: &[&str],
    ) {
        let schema = schema_for_row(names, &rows[0]);
        let batches = execute_rows(schema, rows).await.unwrap();
        assert_batches_eq!(expected, &batches);
    }

    #[rstest]
    #[case::too_few(
        vec![vec![1i64.into()]],
        0,
        "got 1 values in row 0 but expected 2"
    )]
    #[case::too_many(
        vec![vec![1i64.into(), "x".into(), true.into()]],
        0,
        "got 3 values in row 0 but expected 2"
    )]
    #[case::unexpected_input(
        vec![],
        1,
        "values expects 0 input(s), but received 1"
    )]
    fn values_reject_wrong_row_width_or_input_count(
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] input_count: usize,
        #[case] expected: &str,
    ) {
        let input = input_with_schema(test_schema());
        let inputs = vec![input; input_count];
        let op = KernelOperator::Values(KernelValues::new(test_schema(), rows));
        let err = lower_operator(&op, &inputs).unwrap_err();
        assert!(err.to_string().contains(expected), "{err}");
    }

    // === Filter ===

    #[test]
    fn filter_wraps_its_input_and_inherits_schema() {
        let input = input_with_schema(test_schema());
        let filter = KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        };
        let lowered = lower_operator(
            &KernelOperator::Filter(filter),
            std::slice::from_ref(&input),
        )
        .unwrap();

        let DFLogicalPlan::Filter(filter) = &lowered else {
            panic!("expected Filter, got {lowered:?}");
        };
        assert!(Arc::ptr_eq(&filter.input, &input));
        assert_eq!(lowered.schema(), input.schema());
    }

    async fn execute_filter(
        rows: Vec<Vec<KernelScalar>>,
        predicate: KernelPredicate,
    ) -> DFResult<Vec<RecordBatch>> {
        let input = Arc::new(lower_values_node(test_schema(), rows)?);
        let filter = KernelFilter {
            predicate: predicate.into(),
        };
        execute(lower_operator(
            &KernelOperator::Filter(filter),
            std::slice::from_ref(&input),
        )?)
        .await
    }

    fn comparison_rows() -> Vec<Vec<KernelScalar>> {
        vec![
            vec![KernelScalar::null(DataType::LONG), "n".into()],
            vec![1i64.into(), "x".into()],
            vec![5i64.into(), "y".into()],
            vec![9i64.into(), "z".into()],
        ]
    }

    const NO_ROWS: &[&str] = &[];
    const NULL_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "|   | n |",
        "+---+---+",
    ];
    const X_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | x |",
        "+---+---+",
    ];
    const Y_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 5 | y |",
        "+---+---+",
    ];
    const Z_ROW: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 9 | z |",
        "+---+---+",
    ];
    const X_AND_Z_ROWS: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | x |",
        "| 9 | z |",
        "+---+---+",
    ];
    const NULL_X_AND_Z_ROWS: &[&str] = &[
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "|   | n |",
        "| 1 | x |",
        "| 9 | z |",
        "+---+---+",
    ];

    #[rstest]
    #[case::is_null(KernelPredicate::is_null(col!("a")), NULL_ROW)]
    #[case::equal(
        KernelPredicate::binary(KernelBinaryPredicateOp::Equal, col!("a"), kernel_lit(5i64)),
        Y_ROW
    )]
    #[case::less_than(
        KernelPredicate::binary(
            KernelBinaryPredicateOp::LessThan,
            col!("a"),
            kernel_lit(5i64),
        ),
        X_ROW
    )]
    #[case::greater_than(
        KernelPredicate::binary(
            KernelBinaryPredicateOp::GreaterThan,
            col!("a"),
            kernel_lit(5i64),
        ),
        Z_ROW
    )]
    #[case::distinct(
        KernelPredicate::binary(
            KernelBinaryPredicateOp::Distinct,
            col!("a"),
            kernel_lit(5i64),
        ),
        NULL_X_AND_Z_ROWS
    )]
    #[case::and(
        KernelPredicate::junction(
            KernelJunctionPredicateOp::And,
            [
                KernelPredicate::gt(col!("a"), kernel_lit(1i64)),
                KernelPredicate::lt(col!("a"), kernel_lit(9i64)),
            ],
        ),
        Y_ROW
    )]
    #[case::or(
        KernelPredicate::junction(
            KernelJunctionPredicateOp::Or,
            [
                KernelPredicate::lt(col!("a"), kernel_lit(5i64)),
                KernelPredicate::gt(col!("a"), kernel_lit(5i64)),
            ],
        ),
        X_AND_Z_ROWS
    )]
    #[case::where_null(KernelPredicate::NULL, NO_ROWS)]
    #[tokio::test]
    async fn filter_executes_predicate(
        #[case] predicate: KernelPredicate,
        #[case] expected: &[&str],
    ) {
        let batches = execute_filter(comparison_rows(), predicate).await.unwrap();
        if expected.is_empty() {
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        } else {
            assert_batches_sorted_eq!(expected, &batches);
        }
    }

    #[rstest]
    #[case::missing(0)]
    #[case::extra(2)]
    fn filter_rejects_wrong_input_count(#[case] actual: usize) {
        let input = input_with_schema(test_schema());
        let inputs = vec![input; actual];
        let op = KernelOperator::Filter(KernelFilter {
            predicate: KernelPredicate::is_null(col!("a")).into(),
        });
        let err = lower_operator(&op, &inputs).unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("filter expects 1 input(s), but received {actual}")),
            "{err}"
        );
    }

    // === Project ===

    fn empty_schema() -> StructType {
        let fields: Vec<StructField> = Vec::new();
        StructType::try_new(fields).unwrap()
    }

    /// Lowers a Project over `parent`.
    fn lower_project_expr(
        expr: impl Into<ExpressionRef>,
        schema: impl Into<SchemaRef>,
        parent: &Arc<DFLogicalPlan>,
    ) -> DFResult<DFLogicalPlan> {
        lower_operator(
            &KernelOperator::Project(KernelProject {
                expr: expr.into(),
                schema: schema.into(),
            }),
            std::slice::from_ref(parent),
        )
    }

    fn project_nested_schema() -> StructType {
        StructType::try_new([StructField::nullable("value", DataType::INTEGER)]).unwrap()
    }

    fn project_input_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("flag", DataType::BOOLEAN),
            StructField::nullable("small", DataType::INTEGER),
            StructField::nullable("nested", project_nested_schema()),
        ])
        .unwrap()
    }

    fn project_nested_value(value: i32) -> KernelScalar {
        let schema = project_nested_schema();
        KernelScalar::Struct(
            KernelStructData::try_new(
                schema.fields().cloned().collect(),
                vec![KernelScalar::Integer(value)],
            )
            .unwrap(),
        )
    }

    fn project_input() -> Arc<DFLogicalPlan> {
        let rows = vec![
            vec![
                10i64.into(),
                2i64.into(),
                true.into(),
                1i32.into(),
                project_nested_value(7),
            ],
            vec![
                20i64.into(),
                KernelScalar::null(DataType::LONG),
                false.into(),
                2i32.into(),
                project_nested_value(8),
            ],
            vec![
                KernelScalar::null(DataType::LONG),
                4i64.into(),
                KernelScalar::null(DataType::BOOLEAN),
                3i32.into(),
                KernelScalar::null(project_nested_schema()),
            ],
        ];
        Arc::new(lower_values_node(project_input_schema(), rows).unwrap())
    }

    fn struct_project(
        fields: impl IntoIterator<Item = (StructField, KernelExpr)>,
    ) -> (SchemaRef, ExpressionRef) {
        let (fields, exprs): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        (
            Arc::new(StructType::try_new(fields).unwrap()),
            KernelExpr::struct_from(exprs).into(),
        )
    }

    fn guarded_struct_project(
        fields: impl IntoIterator<Item = (StructField, KernelExpr)>,
        guard: KernelExpr,
    ) -> (SchemaRef, ExpressionRef) {
        let (fields, exprs): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        (
            Arc::new(StructType::try_new(fields).unwrap()),
            KernelExpr::struct_with_nullability_from(exprs, guard).into(),
        )
    }

    fn struct_patch_project() -> (SchemaRef, ExpressionRef) {
        let input = project_input_schema();
        ProjectionStructPatchBuilder::new(&input)
            .replace_expr("b", KernelExpr::coalesce([col!("b"), kernel_lit(99i64)]))
            .drop("flag")
            .drop("small")
            .drop("nested")
            .append(
                StructField::nullable("sum", DataType::LONG),
                col!("a") + KernelExpr::coalesce([col!("b"), kernel_lit(99i64)]),
            )
            .build()
            .unwrap()
    }

    fn nested_struct_patch_project() -> (SchemaRef, ExpressionRef) {
        let input = project_input_schema();
        ProjectionStructPatchBuilder::new_nested(&input, ["nested"])
            .replace_expr("value", col!("nested.value") + kernel_lit(1i32))
            .build()
            .unwrap()
    }

    #[rstest]
    #[case::missing(0)]
    #[case::extra(2)]
    fn project_rejects_wrong_parent_count(#[case] actual: usize) {
        let parent = input_with_schema(test_schema());
        let parents = vec![parent; actual];
        let op = KernelOperator::Project(KernelProject {
            expr: KernelExpr::struct_from([col!("a")]).into(),
            schema: Arc::new(
                StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap(),
            ),
        });
        let err = lower_operator(&op, &parents).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "project expects 1 input(s), but received {actual}"
            )),
            "{err}"
        );
    }

    #[rstest]
    #[case::flat(
        KernelExpr::struct_from([col!("b"), col!("a")]),
        StructType::try_new([
            StructField::nullable("renamed_b", DataType::STRING),
            StructField::nullable("renamed_a", DataType::LONG),
        ]).unwrap(),
        vec!["renamed_b", "renamed_a"]
    )]
    #[case::nested(
        KernelExpr::struct_from([KernelExpr::struct_from([col!("a")])]),
        StructType::try_new([StructField::nullable(
            "nested",
            StructType::try_new([StructField::nullable("leaf", DataType::LONG)]).unwrap(),
        )]).unwrap(),
        vec!["nested"]
    )]
    fn project_lowers_single_projection_with_declared_schema(
        #[case] expr: KernelExpr,
        #[case] output: StructType,
        #[case] expected_names: Vec<&str>,
    ) {
        let expected_arrow: ArrowSchema = (&output).try_into_arrow().unwrap();
        let expected_types: Vec<ArrowDataType> = expected_arrow
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let parent = input_with_schema(test_schema());
        let lowered = lower_project_expr(expr, output, &parent).unwrap();

        assert_eq!(output_names(&lowered), expected_names);
        assert_eq!(output_types(&lowered), expected_types);
        let DFLogicalPlan::Projection(projection) = &lowered else {
            panic!("expected Projection, got {lowered:?}");
        };
        assert!(Arc::ptr_eq(&projection.input, &parent));
        assert_eq!(projection.expr.len(), expected_names.len());
        assert!(matches!(
            projection.input.as_ref(),
            DFLogicalPlan::EmptyRelation(_)
        ));
    }

    #[test]
    fn project_schema_and_predicate_are_available_to_a_downstream_filter() {
        let parent = input_with_schema(test_schema());
        let output =
            StructType::try_new([StructField::nullable("projected", DataType::LONG)]).unwrap();
        let projected = Arc::new(
            lower_project_expr(KernelExpr::struct_from([col!("a")]), output, &parent).unwrap(),
        );
        let filter = KernelFilter {
            predicate: KernelPredicate::is_null(col!("projected")).into(),
        };
        let filtered = lower_operator(
            &KernelOperator::Filter(filter),
            std::slice::from_ref(&projected),
        )
        .unwrap();
        let DFLogicalPlan::Filter(filter) = &filtered else {
            panic!("expected Filter, got {filtered:?}");
        };
        assert_eq!(filter.predicate, df_col("projected").is_null());
        assert!(Arc::ptr_eq(&filter.input, &projected));
        assert_eq!(filtered.schema(), projected.schema());
    }

    /// A boolean nullability guard masks each output column individually (`CASE WHEN guard THEN
    /// value ELSE NULL`), the per-field equivalent of nulling the whole struct.
    #[test]
    fn project_with_boolean_nullability_guard_masks_each_column() {
        let input = StructType::try_new([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("flag", DataType::BOOLEAN),
        ])
        .unwrap();
        let parent = input_with_schema(input);
        let output = StructType::try_new([StructField::nullable("out", DataType::LONG)]).unwrap();
        let expr = KernelExpr::struct_with_nullability_from([col!("a")], col!("flag"));
        let lowered = lower_project_expr(expr, output, &parent).unwrap();

        assert_eq!(output_names(&lowered), ["out"]);
        assert_eq!(output_types(&lowered), [ArrowDataType::Int64]);
        let DFLogicalPlan::Projection(projection) = &lowered else {
            panic!("expected Projection, got {lowered:?}");
        };
        let expected = DFExpr::Case(Case::new(
            None,
            vec![(Box::new(df_col("flag")), Box::new(df_col("a")))],
            Some(Box::new(df_lit(DFScalarValue::Null))),
        ))
        .alias("out");
        assert_eq!(projection.expr, [expected]);
    }

    #[test]
    fn zero_field_project_lowers_to_empty_projection() {
        let parent = input_with_schema(test_schema());
        let lowered = lower_project_expr(
            KernelExpr::struct_from([] as [KernelExpr; 0]),
            empty_schema(),
            &parent,
        )
        .unwrap();
        assert!(lowered.schema().fields().is_empty());
        let DFLogicalPlan::Projection(project) = &lowered else {
            panic!("expected Projection");
        };
        assert!(project.expr.is_empty());
    }

    #[rstest]
    #[case::unknown(KernelExpr::unknown("engine_expr"), "must be a Struct or StructPatch")]
    #[case::non_struct(KernelExpr::literal(1i64), "must be a Struct or StructPatch")]
    #[case::unresolved_nullability(
        KernelExpr::struct_with_nullability_from(
            [] as [KernelExpr; 0],
            col!("missing")
        ),
        "missing"
    )]
    #[case::malformed_patch(
        KernelExpr::struct_patch(ExpressionStructPatchBuilder::new()).unwrap(),
        "produced more fields"
    )]
    fn zero_field_project_rejects_invalid_expression(
        #[case] expr: KernelExpr,
        #[case] expected_message: &str,
    ) {
        let parent = input_with_schema(test_schema());
        let err = lower_project_expr(expr, empty_schema(), &parent).unwrap_err();
        assert!(err.to_string().contains(expected_message), "{err}");
    }

    #[test]
    fn project_rejects_uncastable_output_type() {
        let parent = input_with_schema(test_schema());
        let output =
            StructType::try_new([StructField::nullable("a", DataType::unshredded_variant())])
                .unwrap();
        let err =
            lower_project_expr(KernelExpr::struct_from([col!("a")]), output, &parent).unwrap_err();
        let message = err.to_string();
        assert!(matches!(&err, DataFusionError::Plan(_)), "{message}");
        assert!(
            message.contains("Cannot automatically convert"),
            "{message}"
        );
    }

    #[rstest]
    #[case::primitive(
        StructType::try_new([
            StructField::nullable("a", DataType::INTEGER),
        ]).unwrap(),
        KernelExpr::struct_from([col!("a")]),
        StructType::try_new([
            StructField::nullable("a", DataType::LONG),
        ]).unwrap()
    )]
    #[case::nested(
        StructType::try_new([
            StructField::nullable(
                "nested",
                StructType::try_new([
                    StructField::nullable("leaf", DataType::INTEGER),
                ]).unwrap(),
            ),
        ]).unwrap(),
        KernelExpr::struct_from([col!("nested")]),
        StructType::try_new([
            StructField::nullable(
                "nested",
                StructType::try_new([
                    StructField::nullable("leaf", DataType::LONG),
                ]).unwrap(),
            ),
        ]).unwrap()
    )]
    fn project_casts_output_to_declared_type(
        #[case] input: StructType,
        #[case] expr: KernelExpr,
        #[case] output: StructType,
    ) {
        let expected: ArrowSchema = (&output).try_into_arrow().unwrap();
        let expected_types: Vec<ArrowDataType> = expected
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let parent = input_with_schema(input);
        let lowered = lower_project_expr(expr, output, &parent).unwrap();
        assert_eq!(output_types(&lowered), expected_types);
    }

    #[rstest]
    #[case::literal_column_and_cast(
        struct_project([
            (StructField::nullable("selected", DataType::LONG), col!("a")),
            (StructField::nullable("literal", DataType::STRING), kernel_lit("constant")),
            (
                StructField::nullable("null_value", DataType::LONG),
                null_lit(DataType::LONG),
            ),
            (StructField::nullable("widened", DataType::LONG), col!("small")),
        ]),
        &[
            "+----------+----------+------------+---------+",
            "| selected | literal  | null_value | widened |",
            "+----------+----------+------------+---------+",
            "| 10       | constant |            | 1       |",
            "| 20       | constant |            | 2       |",
            "|          | constant |            | 3       |",
            "+----------+----------+------------+---------+",
        ]
    )]
    #[case::arithmetic(
        struct_project([
            (StructField::nullable("sum", DataType::LONG), col!("a") + col!("b")),
            (
                StructField::nullable("difference", DataType::LONG),
                col!("a") - col!("b"),
            ),
            (
                StructField::nullable("product", DataType::LONG),
                col!("a") * col!("b"),
            ),
            (
                StructField::nullable("quotient", DataType::LONG),
                col!("a") / col!("b"),
            ),
        ]),
        &[
            "+-----+------------+---------+----------+",
            "| sum | difference | product | quotient |",
            "+-----+------------+---------+----------+",
            "| 12  | 8          | 20      | 5        |",
            "|     |            |         |          |",
            "|     |            |         |          |",
            "+-----+------------+---------+----------+",
        ]
    )]
    #[case::variadic(
        struct_project([
            (
                StructField::nullable("coalesced", DataType::LONG),
                KernelExpr::coalesce([col!("b"), col!("a"), kernel_lit(99i64)]),
            ),
            (
                StructField::nullable(
                    "array",
                    ArrayType::new(DataType::LONG, true),
                ),
                KernelExpr::array([col!("a"), col!("b"), kernel_lit(5i64)]),
            ),
        ]),
        &[
            "+-----------+------------+",
            "| coalesced | array      |",
            "+-----------+------------+",
            "| 2         | [10, 2, 5] |",
            "| 20        | [20, , 5]  |",
            "| 4         | [, 4, 5]   |",
            "+-----------+------------+",
        ]
    )]
    #[case::predicate(
        struct_project([
            (
                StructField::nullable("is_null", DataType::BOOLEAN),
                KernelExpr::from_pred(col!("b").is_null()),
            ),
            (
                StructField::nullable("greater", DataType::BOOLEAN),
                KernelExpr::from_pred(col!("a").gt(kernel_lit(15i64))),
            ),
            (
                StructField::nullable("conjunction", DataType::BOOLEAN),
                KernelExpr::from_pred(KernelPredicate::and(
                    col!("a").is_not_null(),
                    col!("b").lt(kernel_lit(3i64)),
                )),
            ),
            (
                StructField::nullable("distinct", DataType::BOOLEAN),
                KernelExpr::from_pred(col!("a").distinct(col!("b"))),
            ),
        ]),
        &[
            "+---------+---------+-------------+----------+",
            "| is_null | greater | conjunction | distinct |",
            "+---------+---------+-------------+----------+",
            "| false   | false   | true        | true     |",
            "| true    | true    |             | true     |",
            "| false   |         | false       | true     |",
            "+---------+---------+-------------+----------+",
        ]
    )]
    #[case::nested_struct(
        struct_project([(
            StructField::nullable(
                "record",
                StructType::try_new([
                    StructField::nullable("value", DataType::LONG),
                    StructField::nullable("label", DataType::STRING),
                ]).unwrap(),
            ),
            KernelExpr::struct_with_nullability_from(
                [col!("nested.value"), kernel_lit("seen")],
                KernelExpr::from_pred(col!("nested").is_not_null()),
            ),
        )]),
        &[
            "+-------------------------+",
            "| record                  |",
            "+-------------------------+",
            "| {value: 7, label: seen} |",
            "| {value: 8, label: seen} |",
            "|                         |",
            "+-------------------------+",
        ]
    )]
    #[case::array_of_structs(
        struct_project([(
            StructField::nullable(
                "records",
                ArrayType::new(
                    StructType::try_new([StructField::nullable("value", DataType::LONG)]).unwrap(),
                    true,
                ),
            ),
            KernelExpr::array([
                KernelExpr::struct_from([col!("a")]),
                KernelExpr::struct_from([col!("b")]),
            ]),
        )]),
        &[
            "+---------------------------+",
            "| records                   |",
            "+---------------------------+",
            "| [{value: 10}, {value: 2}] |",
            "| [{value: 20}, {value: }]  |",
            "| [{value: }, {value: 4}]   |",
            "+---------------------------+",
        ]
    )]
    #[case::top_level_nullability(
        guarded_struct_project(
            [
                (StructField::nullable("a", DataType::LONG), col!("a")),
                (StructField::nullable("b", DataType::LONG), col!("b")),
            ],
            col!("flag"),
        ),
        &[
            "+----+---+",
            "| a  | b |",
            "+----+---+",
            "| 10 | 2 |",
            "|    |   |",
            "|    |   |",
            "+----+---+",
        ]
    )]
    #[case::struct_patch_replace(
        ProjectionStructPatchBuilder::new(&project_input_schema())
            .replace_expr("a", kernel_lit(7i64))
            .build()
            .unwrap(),
        &[
            "+---+---+-------+-------+------------+",
            "| a | b | flag  | small | nested     |",
            "+---+---+-------+-------+------------+",
            "| 7 | 2 | true  | 1     | {value: 7} |",
            "| 7 |   | false | 2     | {value: 8} |",
            "| 7 | 4 |       | 3     |            |",
            "+---+---+-------+-------+------------+",
        ]
    )]
    #[case::struct_patch_drop(
        ProjectionStructPatchBuilder::new(&project_input_schema())
            .drop("a")
            .build()
            .unwrap(),
        &[
            "+---+-------+-------+------------+",
            "| b | flag  | small | nested     |",
            "+---+-------+-------+------------+",
            "| 2 | true  | 1     | {value: 7} |",
            "|   | false | 2     | {value: 8} |",
            "| 4 |       | 3     |            |",
            "+---+-------+-------+------------+",
        ]
    )]
    #[case::struct_patch_inject(
        ProjectionStructPatchBuilder::new(&project_input_schema())
            .append(
                StructField::nullable("injected", DataType::LONG),
                kernel_lit(7i64),
            )
            .build()
            .unwrap(),
        &[
            "+----+---+-------+-------+------------+----------+",
            "| a  | b | flag  | small | nested     | injected |",
            "+----+---+-------+-------+------------+----------+",
            "| 10 | 2 | true  | 1     | {value: 7} | 7        |",
            "| 20 |   | false | 2     | {value: 8} | 7        |",
            "|    | 4 |       | 3     |            | 7        |",
            "+----+---+-------+-------+------------+----------+",
        ]
    )]
    #[case::struct_patch(
        struct_patch_project(),
        &[
            "+----+----+-----+",
            "| a  | b  | sum |",
            "+----+----+-----+",
            "| 10 | 2  | 12  |",
            "| 20 | 99 | 119 |",
            "|    | 4  |     |",
            "+----+----+-----+",
        ]
    )]
    #[case::nested_struct_patch(
        nested_struct_patch_project(),
        &[
            "+-------+",
            "| value |",
            "+-------+",
            "| 8     |",
            "| 9     |",
            "|       |",
            "+-------+",
        ]
    )]
    #[tokio::test]
    async fn project_executes_expression(
        #[case] project: (SchemaRef, ExpressionRef),
        #[case] expected: &[&str],
    ) {
        let (output, expr) = project;
        let lowered = lower_project_expr(expr, output, &project_input()).unwrap();
        let batches = execute(lowered).await.unwrap();
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn project_rejects_invalid_cast_value_during_execution() {
        let input = StructType::try_new([StructField::nullable("a", DataType::STRING)]).unwrap();
        let parent = Arc::new(
            lower_values_node(input, vec![vec![KernelScalar::String("abc".into())]]).unwrap(),
        );
        let output = StructType::try_new([StructField::nullable("a", DataType::INTEGER)]).unwrap();
        let lowered =
            lower_project_expr(KernelExpr::struct_from([col!("a")]), output, &parent).unwrap();

        let err = execute(lowered).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot cast string 'abc' to value of Int32 type"),
            "{err}"
        );
    }

    #[test]
    fn project_normalizes_kernel_compatible_arrow_representations() {
        let expected = StructType::try_new([
            StructField::nullable("string", DataType::STRING),
            StructField::nullable("array", ArrayType::new(DataType::LONG, true)),
            StructField::nullable(
                "map",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
        ])
        .unwrap();
        let map_entries = ArrowDataType::Struct(
            vec![
                Arc::new(ArrowField::new("key", ArrowDataType::Utf8View, false)),
                Arc::new(ArrowField::new("value", ArrowDataType::Utf8View, true)),
            ]
            .into(),
        );
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("string", ArrowDataType::Utf8View, true),
            ArrowField::new(
                "array",
                ArrowDataType::LargeList(Arc::new(ArrowField::new(
                    "element",
                    ArrowDataType::Int64,
                    true,
                ))),
                true,
            ),
            ArrowField::new(
                "map",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new("entries", map_entries, false)),
                    false,
                ),
                true,
            ),
        ]);
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(arrow_schema).unwrap()),
        };
        let parent = Arc::new(DFLogicalPlan::EmptyRelation(empty));
        let output: ArrowSchema = (&expected).try_into_arrow().unwrap();
        let expected_types: Vec<ArrowDataType> = output
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let expr = KernelExpr::struct_from([col!("string"), col!("array"), col!("map")]);

        let lowered = lower_project_expr(expr, expected, &parent).unwrap();
        assert_eq!(output_types(&lowered), expected_types);
    }

    #[rstest]
    #[case::interval(DataType::INTERVAL_YEAR_MONTH)]
    #[case::variant(DataType::unshredded_variant())]
    fn project_preserves_kernel_physical_type(#[case] data_type: DataType) {
        let schema = StructType::try_new([StructField::nullable("a", data_type)]).unwrap();
        let expected: ArrowSchema = (&schema).try_into_arrow().unwrap();
        let expected_type = expected.field(0).data_type().clone();
        let parent = input_with_schema(schema.clone());
        let lowered =
            lower_project_expr(KernelExpr::struct_from([col!("a")]), schema, &parent).unwrap();
        assert_eq!(output_types(&lowered), [expected_type]);
    }

    /// Errors from converting a Project's child expression propagate to the caller.
    #[test]
    fn project_propagates_expression_conversion_error() {
        let parent = input_with_schema(test_schema());
        let output = StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap();
        let expr = KernelExpr::struct_from([KernelExpr::unknown("engine_expr")]);
        let err = lower_project_expr(expr, output, &parent).unwrap_err();
        let message = err.to_string();
        assert!(matches!(&err, DataFusionError::External(_)), "{message}");
        assert!(
            message.contains(r#"cannot convert Unknown expression "engine_expr""#),
            "{message}"
        );
    }

    // === Aggregate ===

    fn aggregate_input_schema() -> StructType {
        StructType::try_new([
            StructField::nullable("group", DataType::STRING),
            StructField::nullable("value", DataType::STRING),
            StructField::nullable("sentinel", DataType::STRING),
            StructField::nullable("key", DataType::LONG),
        ])
        .unwrap()
    }

    fn lower(builder: PlanBuilder) -> DFLogicalPlan {
        crate::plan::to_df_plan(&builder.build().unwrap()).unwrap()
    }

    fn test_aggregate() -> KernelAggregate {
        KernelAggregate::ungrouped(Arc::new(aggregate_input_schema()))
            .max(column_name!("value"))
            .build()
            .unwrap()
    }

    #[rstest]
    #[case::missing(0)]
    #[case::extra(2)]
    fn aggregate_rejects_wrong_input_count(#[case] actual: usize) {
        let input = input_with_schema(aggregate_input_schema());
        let inputs = vec![input; actual];
        let op = KernelOperator::Aggregate(test_aggregate());
        let err = lower_operator(&op, &inputs).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "aggregate expects 1 input(s), but received {actual}"
            )),
            "{err}"
        );
    }

    #[rstest]
    #[case::min(KernelAgg::min(column_name!("value")), "min", df_col("value"), None)]
    #[case::max(KernelAgg::max(column_name!("value")), "max", df_col("value"), None)]
    #[case::sum(KernelAgg::sum(column_name!("key")), "sum", df_col("key"), None)]
    #[case::count(KernelAgg::count(column_name!("value")), "count", df_col("value"), None)]
    #[case::count_star(KernelAgg::count_star(), "count", df_lit(1), None)]
    #[case::min_non_null_by(
        KernelAgg::min_non_null_by(
            column_name!("value"),
            column_name!("sentinel"),
            column_name!("key")
        ),
        "first_value",
        df_col("value"),
        Some(true)
    )]
    #[case::max_non_null_by(
        KernelAgg::max_non_null_by(
            column_name!("value"),
            column_name!("sentinel"),
            column_name!("key")
        ),
        "first_value",
        df_col("value"),
        Some(false)
    )]
    fn aggregate_lowers_function_with_declared_schema(
        #[case] agg: KernelAgg,
        #[case] expected_function: &str,
        #[case] expected_arg: DFExpr,
        #[case] expected_ascending: Option<bool>,
        #[values(false, true)] grouped: bool,
    ) {
        let parent = PlanBuilder::values(
            Arc::new(aggregate_input_schema()),
            vec![vec![
                "group".into(),
                "value".into(),
                "sentinel".into(),
                1i64.into(),
            ]],
        )
        .unwrap();
        let builder = if grouped {
            parent.aggregate_by([column_name!("group")], |aggregate| {
                aggregate.aggregate_as(agg, "result")
            })
        } else {
            parent.aggregate_ungrouped(|aggregate| aggregate.aggregate_as(agg, "result"))
        };
        let lowered = lower(builder.unwrap());

        let expected_names = if grouped {
            vec!["group", "result"]
        } else {
            vec!["result"]
        };
        assert_eq!(output_names(&lowered), expected_names);
        let DFLogicalPlan::Aggregate(aggregate) = &lowered else {
            panic!("expected Aggregate, got {lowered:?}");
        };
        let expected_group: Vec<_> = grouped
            .then(|| df_col("group").alias("group"))
            .into_iter()
            .collect();
        assert_eq!(aggregate.group_expr, expected_group);

        let [DFExpr::Alias(alias)] = aggregate.aggr_expr.as_slice() else {
            panic!("expected one aliased aggregate expression");
        };
        assert_eq!(alias.name, "result");
        let DFExpr::AggregateFunction(function) = alias.expr.as_ref() else {
            panic!("expected aggregate function, got {:?}", alias.expr);
        };
        assert_eq!(function.func.name(), expected_function);
        assert_eq!(function.params.args, [expected_arg]);

        let Some(ascending) = expected_ascending else {
            assert!(function.params.order_by.is_empty());
            assert!(function.params.filter.is_none());
            return;
        };
        assert_eq!(
            function.params.order_by,
            [df_col("key").sort(ascending, false)]
        );
        let expected_filter = df_col("sentinel")
            .is_not_null()
            .and(df_col("key").is_not_null());
        assert_eq!(function.params.filter.as_deref(), Some(&expected_filter));
    }

    #[test]
    fn aggregate_resolves_columns_without_converting_input_schema_to_kernel() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "value",
            ArrowDataType::Duration(datafusion::arrow::datatypes::TimeUnit::Second),
            true,
        )]);
        let df_schema = Arc::new(DFSchema::try_from(arrow_schema).unwrap());
        let kernel_schema: std::result::Result<StructType, _> =
            df_schema.as_arrow().try_into_kernel();
        assert!(kernel_schema.is_err());
        let parent = Arc::new(DFLogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        }));
        let aggregate = KernelAggregate {
            group_by: vec![],
            aggs: vec![KernelAgg::count(column_name!("value"))],
            schema: Arc::new(
                StructType::try_new([StructField::not_null("count", DataType::LONG)]).unwrap(),
            ),
        };

        let lowered = lower_operator(
            &KernelOperator::Aggregate(aggregate),
            std::slice::from_ref(&parent),
        )
        .unwrap();
        assert_eq!(output_names(&lowered), ["count"]);
        assert_eq!(output_types(&lowered), [ArrowDataType::Int64]);
    }

    #[tokio::test]
    async fn datafusion_rejects_unresolved_aggregate_columns() {
        let parent = Arc::new(
            lower_values_node(
                aggregate_input_schema(),
                vec![vec![
                    "group".into(),
                    "value".into(),
                    "sentinel".into(),
                    1i64.into(),
                ]],
            )
            .unwrap(),
        );
        let aggregate = KernelAggregate {
            group_by: vec![],
            aggs: vec![KernelAgg::min_non_null_by(
                column_name!("value"),
                column_name!("sentinel.missing"),
                column_name!("key"),
            )],
            schema: Arc::new(
                StructType::try_new([StructField::nullable("result", DataType::STRING)]).unwrap(),
            ),
        };

        let lowered = lower_operator(
            &KernelOperator::Aggregate(aggregate),
            std::slice::from_ref(&parent),
        )
        .unwrap();
        let err = execute(lowered).await.unwrap_err();
        assert!(err.to_string().contains("Cannot access field"), "{err}");
    }

    #[tokio::test]
    async fn aggregate_casts_result_not_operand_to_declared_type() {
        let input =
            StructType::try_new([StructField::nullable("value", DataType::INTEGER)]).unwrap();
        let parent =
            Arc::new(lower_values_node(input, vec![vec![KernelScalar::Integer(7)]]).unwrap());
        let aggregate = KernelAggregate {
            group_by: vec![],
            aggs: vec![KernelAgg::max(column_name!("value"))],
            schema: Arc::new(
                StructType::try_new([StructField::nullable("result", DataType::LONG)]).unwrap(),
            ),
        };

        let lowered = lower_operator(
            &KernelOperator::Aggregate(aggregate),
            std::slice::from_ref(&parent),
        )
        .unwrap();
        assert_eq!(output_types(&lowered), [ArrowDataType::Int64]);
        let DFLogicalPlan::Aggregate(aggregate) = &lowered else {
            panic!("expected Aggregate, got {lowered:?}");
        };
        let [DFExpr::Alias(alias)] = aggregate.aggr_expr.as_slice() else {
            panic!("expected one aliased aggregate expression");
        };
        let DFExpr::Cast(cast) = alias.expr.as_ref() else {
            panic!(
                "expected cast around aggregate result, got {:?}",
                alias.expr
            );
        };
        let DFExpr::AggregateFunction(function) = cast.expr.as_ref() else {
            panic!("expected aggregate inside cast, got {:?}", cast.expr);
        };
        assert_eq!(function.params.args, [df_col("value")]);

        let batches = execute(lowered).await.unwrap();
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| 7      |",
                "+--------+"
            ],
            &batches
        );
    }

    #[test]
    fn empty_global_aggregate_lowers_to_one_row_relation() {
        let parent = PlanBuilder::values(Arc::new(test_schema()), vec![]).unwrap();
        let builder = parent.aggregate_ungrouped(|aggregate| aggregate);
        let lowered = lower(builder.unwrap());

        let DFLogicalPlan::EmptyRelation(empty) = &lowered else {
            panic!("expected EmptyRelation, got {lowered:?}");
        };
        assert!(empty.produce_one_row);
        assert!(empty.schema.fields().is_empty());
    }

    #[test]
    fn aggregate_rejects_output_schema_with_wrong_field_count() {
        let parent = input_with_schema(aggregate_input_schema());
        let aggregate = KernelAggregate {
            group_by: vec![column_name!("group")],
            aggs: vec![KernelAgg::max(column_name!("value"))],
            schema: Arc::new(
                StructType::try_new([StructField::nullable("group", DataType::STRING)]).unwrap(),
            ),
        };
        let err = lower_operator(
            &KernelOperator::Aggregate(aggregate),
            std::slice::from_ref(&parent),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Aggregate schema has wrong number of fields. Expected 2 got 1"),
            "{err}"
        );
    }

    /// Exercises ungrouped aggregate NULL handling over mixed, all-NULL, and empty input.
    ///
    /// The mixed case gives the greatest qualifying key a NULL value, so `max_non_null_by` must
    /// retain that NULL rather than skip it.
    ///
    /// ```text
    /// case         | min   | max    | sum  | count | count(*) | min_by | max_by
    /// -------------+-------+--------+------+-------+----------+--------+-------
    /// mixed_values | apple | cherry | 9    | 3     | 4        | apple  | NULL
    /// all_null     | NULL  | NULL   | NULL | 0     | 2        | NULL   | NULL
    /// no_rows      | NULL  | NULL   | NULL | 0     | 0        | NULL   | NULL
    /// ```
    #[rstest]
    #[case::mixed_values(
        vec![
            vec![
                "banana".into(),
                KernelScalar::Long(3),
                "present".into(),
                KernelScalar::Long(2),
            ],
            vec![
                "cherry".into(),
                KernelScalar::Null(DataType::LONG),
                "present".into(),
                KernelScalar::Long(3),
            ],
            vec![
                KernelScalar::Null(DataType::STRING),
                KernelScalar::Long(5),
                "present".into(),
                KernelScalar::Long(4),
            ],
            vec![
                "apple".into(),
                KernelScalar::Long(1),
                "present".into(),
                KernelScalar::Long(1),
            ],
        ],
        vec![
            DFScalarValue::Utf8(Some("apple".into())),
            DFScalarValue::Utf8(Some("cherry".into())),
            DFScalarValue::Int64(Some(9)),
            DFScalarValue::Int64(Some(3)),
            DFScalarValue::Int64(Some(4)),
            DFScalarValue::Utf8(Some("apple".into())),
            DFScalarValue::Utf8(None),
        ]
    )]
    #[case::all_null(
        vec![
            vec![
                KernelScalar::Null(DataType::STRING),
                KernelScalar::Null(DataType::LONG),
                "present".into(),
                KernelScalar::Long(1),
            ],
            vec![
                KernelScalar::Null(DataType::STRING),
                KernelScalar::Null(DataType::LONG),
                "present".into(),
                KernelScalar::Long(2),
            ],
        ],
        vec![
            DFScalarValue::Utf8(None),
            DFScalarValue::Utf8(None),
            DFScalarValue::Int64(None),
            DFScalarValue::Int64(Some(0)),
            DFScalarValue::Int64(Some(2)),
            DFScalarValue::Utf8(None),
            DFScalarValue::Utf8(None),
        ]
    )]
    #[case::no_rows(
        vec![],
        vec![
            DFScalarValue::Utf8(None),
            DFScalarValue::Utf8(None),
            DFScalarValue::Int64(None),
            DFScalarValue::Int64(Some(0)),
            DFScalarValue::Int64(Some(0)),
            DFScalarValue::Utf8(None),
            DFScalarValue::Utf8(None),
        ]
    )]
    #[tokio::test]
    async fn aggregate_executes_ungrouped_functions(
        #[case] rows: Vec<Vec<KernelScalar>>,
        #[case] expected: Vec<DFScalarValue>,
    ) {
        let input_schema = Arc::new(
            StructType::try_new([
                StructField::nullable("value", DataType::STRING),
                StructField::nullable("number", DataType::LONG),
                StructField::nullable("sentinel", DataType::STRING),
                StructField::nullable("key", DataType::LONG),
            ])
            .unwrap(),
        );
        let parent = PlanBuilder::values(input_schema, rows).unwrap();
        let builder = parent.aggregate_ungrouped(|aggregate| {
            aggregate
                .aggregate_as(KernelAgg::min(column_name!("value")), "min_value")
                .aggregate_as(KernelAgg::max(column_name!("value")), "max_value")
                .aggregate_as(KernelAgg::sum(column_name!("number")), "sum_value")
                .aggregate_as(KernelAgg::count(column_name!("number")), "count_value")
                .aggregate_as(KernelAgg::count_star(), "row_count")
                .aggregate_as(
                    KernelAgg::min_non_null_by(
                        column_name!("value"),
                        column_name!("sentinel"),
                        column_name!("key"),
                    ),
                    "min_by_value",
                )
                .aggregate_as(
                    KernelAgg::max_non_null_by(
                        column_name!("value"),
                        column_name!("sentinel"),
                        column_name!("key"),
                    ),
                    "max_by_value",
                )
        });
        let lowered = lower(builder.unwrap());
        assert_eq!(
            output_names(&lowered),
            vec![
                "min_value",
                "max_value",
                "sum_value",
                "count_value",
                "row_count",
                "min_by_value",
                "max_by_value",
            ]
        );
        let batches = execute(lowered).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let actual = batches[0]
            .columns()
            .iter()
            .map(|column| DFScalarValue::try_from_array(column.as_ref(), 0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    /// Input:
    ///
    /// ```text
    /// group      | value        | sentinel | key
    /// -----------+--------------+----------+-----
    /// values     | min          | present  | 1
    /// values     | max          | present  | 3
    /// null-value | ignored-low  | NULL     | 0
    /// null-value | min          | present  | 1
    /// null-value | max          | present  | 3
    /// null-value | NULL         | present  | 4
    /// null-value | ignored-high | NULL     | 5
    /// null-value | no-key       | present  | NULL
    /// invalid    | no-sentinel  | NULL     | 1
    /// invalid    | no-key       | present  | NULL
    /// ```
    ///
    /// ```sql
    /// SELECT group,
    ///        min_non_null_by(value, sentinel, key) AS min_value,
    ///        max_non_null_by(value, sentinel, key) AS max_value
    /// FROM input
    /// GROUP BY group
    /// ```
    ///
    /// ```text
    /// group      | min_value | max_value
    /// -----------+-----------+----------
    /// invalid    | NULL      | NULL
    /// null-value | min       | NULL
    /// values     | min       | max
    /// ```
    #[tokio::test]
    async fn aggregate_non_null_by_filters_on_sentinel_and_key_but_retains_null_value() {
        let rows = vec![
            vec![
                "values".into(),
                "min".into(),
                "present".into(),
                KernelScalar::Long(1),
            ],
            vec![
                "values".into(),
                "max".into(),
                "present".into(),
                KernelScalar::Long(3),
            ],
            vec![
                "null-value".into(),
                "ignored-low".into(),
                KernelScalar::Null(DataType::STRING),
                KernelScalar::Long(0),
            ],
            vec![
                "null-value".into(),
                "min".into(),
                "present".into(),
                KernelScalar::Long(1),
            ],
            vec![
                "null-value".into(),
                "max".into(),
                "present".into(),
                KernelScalar::Long(3),
            ],
            vec![
                "null-value".into(),
                KernelScalar::Null(DataType::STRING),
                "present".into(),
                KernelScalar::Long(4),
            ],
            vec![
                "null-value".into(),
                "ignored-high".into(),
                KernelScalar::Null(DataType::STRING),
                KernelScalar::Long(5),
            ],
            vec![
                "null-value".into(),
                "no-key".into(),
                "present".into(),
                KernelScalar::Null(DataType::LONG),
            ],
            vec![
                "invalid".into(),
                "no-sentinel".into(),
                KernelScalar::Null(DataType::STRING),
                KernelScalar::Long(1),
            ],
            vec![
                "invalid".into(),
                "no-key".into(),
                "present".into(),
                KernelScalar::Null(DataType::LONG),
            ],
        ];
        let parent = PlanBuilder::values(Arc::new(aggregate_input_schema()), rows).unwrap();
        let builder = parent.aggregate_by([column_name!("group")], |aggregate| {
            aggregate
                .aggregate_as(
                    KernelAgg::min_non_null_by(
                        column_name!("value"),
                        column_name!("sentinel"),
                        column_name!("key"),
                    ),
                    "min_value",
                )
                .aggregate_as(
                    KernelAgg::max_non_null_by(
                        column_name!("value"),
                        column_name!("sentinel"),
                        column_name!("key"),
                    ),
                    "max_value",
                )
        });
        let lowered = lower(builder.unwrap());
        let batches = execute(lowered).await.unwrap();
        assert_batches_sorted_eq!(
            &[
                "+------------+-----------+-----------+",
                "| group      | min_value | max_value |",
                "+------------+-----------+-----------+",
                "| invalid    |           |           |",
                "| null-value | min       |           |",
                "| values     | min       | max       |",
                "+------------+-----------+-----------+",
            ],
            &batches
        );
    }

    // === SemiJoin ===

    fn test_semi_join(inverted: bool) -> KernelSemiJoin {
        KernelSemiJoin {
            inverted,
            probe_keys: vec![column_name!("a"), column_name!("b")],
            build_keys: vec![column_name!("a"), column_name!("b")],
        }
    }

    #[rstest]
    #[case::missing(0)]
    #[case::one(1)]
    #[case::extra(3)]
    fn semi_join_rejects_wrong_input_count(#[case] actual: usize) {
        let input = input_with_schema(test_schema());
        let inputs = vec![input; actual];
        let op = KernelOperator::SemiJoin(test_semi_join(false));
        let err = lower_operator(&op, &inputs).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "semi_join expects 2 input(s), but received {actual}"
            )),
            "{err}"
        );
    }

    #[rstest]
    #[case::semi(false, JoinType::LeftSemi)]
    #[case::anti(true, JoinType::LeftAnti)]
    fn semi_join_lowers_with_declared_schema_and_null_safe_semantics(
        #[case] inverted: bool,
        #[case] expected_join_type: JoinType,
    ) {
        let probe = input_with_schema(test_schema());
        let build = input_with_schema(test_schema());
        let lowered = lower_operator(
            &KernelOperator::SemiJoin(test_semi_join(inverted)),
            &[Arc::clone(&probe), Arc::clone(&build)],
        )
        .unwrap();

        assert_eq!(lowered.schema(), probe.schema());
        let DFLogicalPlan::Join(join) = &lowered else {
            panic!("expected Join, got {lowered:?}");
        };
        assert!(Arc::ptr_eq(&join.left, &probe));
        assert!(Arc::ptr_eq(&join.right, &build));
        assert_eq!(join.join_type, expected_join_type);
        assert_eq!(join.join_constraint, JoinConstraint::On);
        assert_eq!(join.null_equality, NullEquality::NullEqualsNull);
        assert!(!join.null_aware);
        assert_eq!(
            join.on,
            [(df_col("a"), df_col("a")), (df_col("b"), df_col("b")),]
        );
        assert_eq!(join.schema.as_ref(), probe.schema().as_ref());
    }

    #[test]
    fn semi_join_rejects_different_key_counts() {
        let probe = input_with_schema(test_schema());
        let build = input_with_schema(test_schema());
        let semi_join = KernelSemiJoin {
            inverted: false,
            probe_keys: vec![column_name!("a")],
            build_keys: vec![],
        };
        let err = lower_operator(
            &KernelOperator::SemiJoin(semi_join),
            &[Arc::clone(&probe), Arc::clone(&build)],
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("declares 1 probe key(s), but 0 build key(s)"),
            "{err}"
        );
    }

    /// Inputs and expected outputs (row order is unspecified):
    ///
    /// ```text
    /// probe          build
    /// p    | v       b
    /// -----+-----    ----
    /// NULL | no-key  NULL
    /// 1    | one     2
    /// 2    | two
    ///
    /// semi output    anti output
    /// p    | v       p | v
    /// -----+-----    --+----
    /// NULL | no-key  1 | one
    /// 2    | two
    /// ```
    #[rstest]
    #[case::semi(
        false,
        &[
            "+---+--------+",
            "| p | v      |",
            "+---+--------+",
            "|   | no-key |",
            "| 2 | two    |",
            "+---+--------+",
        ]
    )]
    #[case::anti(
        true,
        &[
            "+---+-----+",
            "| p | v   |",
            "+---+-----+",
            "| 1 | one |",
            "+---+-----+",
        ]
    )]
    #[tokio::test]
    async fn semi_join_executes_with_null_keys_equal(
        #[case] inverted: bool,
        #[case] expected: &[&str],
    ) {
        let probe_schema = StructType::try_new([
            StructField::nullable("p", DataType::LONG),
            StructField::nullable("v", DataType::STRING),
        ])
        .unwrap();
        let build_schema =
            StructType::try_new([StructField::nullable("b", DataType::LONG)]).unwrap();
        let probe = Arc::new(
            lower_values_node(
                probe_schema,
                vec![
                    vec![KernelScalar::null(DataType::LONG), "no-key".into()],
                    vec![1i64.into(), "one".into()],
                    vec![2i64.into(), "two".into()],
                ],
            )
            .unwrap(),
        );
        let build = Arc::new(
            lower_values_node(
                build_schema,
                vec![vec![KernelScalar::null(DataType::LONG)], vec![2i64.into()]],
            )
            .unwrap(),
        );
        let semi_join = KernelSemiJoin {
            inverted,
            probe_keys: vec![column_name!("p")],
            build_keys: vec![column_name!("b")],
        };

        let lowered = lower_operator(
            &KernelOperator::SemiJoin(semi_join),
            &[Arc::clone(&probe), Arc::clone(&build)],
        )
        .unwrap();
        let batches = execute(lowered).await.unwrap();
        assert_batches_sorted_eq!(expected, &batches);
    }

    /// Inputs and expected outputs (row order is unspecified):
    ///
    /// ```text
    /// probe                    build
    /// p    | q    | v          b    | c
    /// -----+------+---------   -----+-----
    /// NULL | null | null-key   NULL | null
    /// 1    | x    | one        1    | x
    /// 2    | x    | cross      2    | y
    /// 2    | y    | two
    ///
    /// semi output              anti output
    /// p    | q    | v          p | q | v
    /// -----+------+---------   --+---+------
    /// NULL | null | null-key   2 | x | cross
    /// 1    | x    | one
    /// 2    | y    | two
    /// ```
    #[rstest]
    #[case::semi(
        false,
        &[
            "+---+------+----------+",
            "| p | q    | v        |",
            "+---+------+----------+",
            "|   | null | null-key |",
            "| 1 | x    | one      |",
            "| 2 | y    | two      |",
            "+---+------+----------+",
        ]
    )]
    #[case::anti(
        true,
        &[
            "+---+---+-------+",
            "| p | q | v     |",
            "+---+---+-------+",
            "| 2 | x | cross |",
            "+---+---+-------+",
        ]
    )]
    #[tokio::test]
    async fn semi_join_executes_multiple_keys_with_nulls_equal(
        #[case] inverted: bool,
        #[case] expected: &[&str],
    ) {
        let probe_schema = StructType::try_new([
            StructField::nullable("p", DataType::LONG),
            StructField::nullable("q", DataType::STRING),
            StructField::nullable("v", DataType::STRING),
        ])
        .unwrap();
        let build_schema = StructType::try_new([
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("c", DataType::STRING),
        ])
        .unwrap();
        let probe = Arc::new(
            lower_values_node(
                probe_schema,
                vec![
                    vec![
                        KernelScalar::null(DataType::LONG),
                        "null".into(),
                        "null-key".into(),
                    ],
                    vec![1i64.into(), "x".into(), "one".into()],
                    vec![2i64.into(), "x".into(), "cross".into()],
                    vec![2i64.into(), "y".into(), "two".into()],
                ],
            )
            .unwrap(),
        );
        let build = Arc::new(
            lower_values_node(
                build_schema,
                vec![
                    vec![KernelScalar::null(DataType::LONG), "null".into()],
                    vec![1i64.into(), "x".into()],
                    vec![2i64.into(), "y".into()],
                ],
            )
            .unwrap(),
        );
        let semi_join = KernelSemiJoin {
            inverted,
            probe_keys: vec![column_name!("p"), column_name!("q")],
            build_keys: vec![column_name!("b"), column_name!("c")],
        };

        let lowered = lower_operator(
            &KernelOperator::SemiJoin(semi_join),
            &[Arc::clone(&probe), Arc::clone(&build)],
        )
        .unwrap();
        let batches = execute(lowered).await.unwrap();
        assert_batches_sorted_eq!(expected, &batches);
    }

    /// Inputs and expected outputs:
    ///
    /// ```text
    /// probe          build
    /// nested         nested
    /// ----------     ----------
    /// {probe: 1}     {build: 2}
    /// {probe: 2}
    ///
    /// keys: nested.probe = nested.build
    ///
    /// semi output     anti output
    /// nested          nested
    /// ----------      ----------
    /// {probe: 2}      {probe: 1}
    /// ```
    #[rstest]
    #[case::semi(
        false,
        &[
            "+------------+",
            "| nested     |",
            "+------------+",
            "| {probe: 2} |",
            "+------------+",
        ]
    )]
    #[case::anti(
        true,
        &[
            "+------------+",
            "| nested     |",
            "+------------+",
            "| {probe: 1} |",
            "+------------+",
        ]
    )]
    #[tokio::test]
    async fn semi_join_executes_with_nested_key_paths(
        #[case] inverted: bool,
        #[case] expected: &[&str],
    ) {
        let nested_schema = |leaf| {
            StructType::try_new([StructField::nullable(
                "nested",
                StructType::try_new([StructField::nullable(leaf, DataType::LONG)]).unwrap(),
            )])
            .unwrap()
        };
        let nested_scalar = |leaf, value| {
            KernelScalar::Struct(
                KernelStructData::try_new(
                    vec![StructField::nullable(leaf, DataType::LONG)],
                    vec![KernelScalar::Long(value)],
                )
                .unwrap(),
            )
        };
        let probe = Arc::new(
            lower_values_node(
                nested_schema("probe"),
                vec![
                    vec![nested_scalar("probe", 1)],
                    vec![nested_scalar("probe", 2)],
                ],
            )
            .unwrap(),
        );
        let build = Arc::new(
            lower_values_node(
                nested_schema("build"),
                vec![vec![nested_scalar("build", 2)]],
            )
            .unwrap(),
        );
        let semi_join = KernelSemiJoin {
            inverted,
            probe_keys: vec![column_name!("nested.probe")],
            build_keys: vec![column_name!("nested.build")],
        };

        let lowered = lower_operator(
            &KernelOperator::SemiJoin(semi_join),
            &[Arc::clone(&probe), Arc::clone(&build)],
        )
        .unwrap();
        let DFLogicalPlan::Join(join) = &lowered else {
            panic!("expected Join, got {lowered:?}");
        };
        assert_eq!(join.on.len(), 1);
        let batches = execute(lowered).await.unwrap();
        assert_batches_eq!(expected, &batches);
    }

    // === UnionAll ===

    #[rstest]
    #[case::missing(0)]
    #[case::one(1)]
    fn union_all_rejects_too_few_inputs(#[case] actual: usize) {
        let input = input_with_schema(test_schema());
        let inputs = vec![input; actual];
        let err = lower_operator(&KernelOperator::UnionAll(KernelUnionAll), &inputs).unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "union_all expects at least 2 input(s), but received {actual}"
            )),
            "{err}"
        );
    }

    #[test]
    fn union_all_lowers_differently_qualified_inputs_to_unqualified_schema() {
        let first = qualified_input_with_schema(test_schema(), "scan_json");
        let second = qualified_input_with_schema(test_schema(), "scan_parquet");
        let third = qualified_input_with_schema(test_schema(), "another_scan");
        let lowered = lower_operator(
            &KernelOperator::UnionAll(KernelUnionAll),
            &[Arc::clone(&first), Arc::clone(&second), Arc::clone(&third)],
        )
        .unwrap();

        assert_ne!(first.schema(), second.schema());
        assert_eq!(first.schema().as_arrow(), second.schema().as_arrow());
        assert!(
            lowered
                .schema()
                .iter()
                .all(|(qualifier, _)| qualifier.is_none()),
            "union output must be unqualified"
        );
        assert_eq!(lowered.schema().as_arrow(), first.schema().as_arrow());
        let DFLogicalPlan::Union(union) = &lowered else {
            panic!("expected Union, got {lowered:?}");
        };
        assert_eq!(union.inputs.len(), 3);
        assert!(Arc::ptr_eq(&union.inputs[0], &first));
        assert!(Arc::ptr_eq(&union.inputs[1], &second));
        assert!(Arc::ptr_eq(&union.inputs[2], &third));
    }

    /// Inputs and expected output (row order is unspecified):
    ///
    /// ```text
    /// input 0    input 1    input 2
    /// a | b      (empty)    a | b
    /// --+--                 --+--
    /// 1 | x                 2 | y
    /// 2 | y
    ///
    /// output
    /// a | b
    /// --+--
    /// 1 | x
    /// 2 | y
    /// 2 | y
    /// ```
    #[tokio::test]
    async fn union_all_executes_all_inputs_and_preserves_duplicates() {
        let first = Arc::new(
            lower_values_node(
                test_schema(),
                vec![vec![1i64.into(), "x".into()], vec![2i64.into(), "y".into()]],
            )
            .unwrap(),
        );
        let empty = input_with_schema(test_schema());
        let third = Arc::new(
            lower_values_node(test_schema(), vec![vec![2i64.into(), "y".into()]]).unwrap(),
        );
        let lowered = lower_operator(
            &KernelOperator::UnionAll(KernelUnionAll),
            &[Arc::clone(&first), Arc::clone(&empty), Arc::clone(&third)],
        )
        .unwrap();

        let batches = execute(lowered).await.unwrap();
        assert_batches_sorted_eq!(
            &[
                "+---+---+",
                "| a | b |",
                "+---+---+",
                "| 1 | x |",
                "| 2 | y |",
                "| 2 | y |",
                "+---+---+",
            ],
            &batches
        );
    }

    /// Inputs and expected output (row order is unspecified):
    ///
    /// ```text
    /// input 0                 input 1
    /// payload                 payload
    /// --------------------    --------------------
    /// {id: 1, name: one}      {id: 2, name: two}
    ///
    /// output
    /// payload
    /// --------------------
    /// {id: 1, name: one}
    /// {id: 2, name: two}
    /// ```
    #[tokio::test]
    async fn union_all_executes_struct_columns() {
        let payload_type = StructType::try_new([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("name", DataType::STRING),
        ])
        .unwrap();
        let schema =
            StructType::try_new([StructField::nullable("payload", payload_type.clone())]).unwrap();
        let payload = |id, name: &str| {
            KernelScalar::Struct(
                KernelStructData::try_new(
                    payload_type.fields().cloned().collect(),
                    vec![KernelScalar::Long(id), KernelScalar::String(name.into())],
                )
                .unwrap(),
            )
        };
        let first =
            Arc::new(lower_values_node(schema.clone(), vec![vec![payload(1, "one")]]).unwrap());
        let second = Arc::new(lower_values_node(schema, vec![vec![payload(2, "two")]]).unwrap());

        let lowered = lower_operator(
            &KernelOperator::UnionAll(KernelUnionAll),
            &[Arc::clone(&first), Arc::clone(&second)],
        )
        .unwrap();
        let batches = execute(lowered).await.unwrap();
        assert_batches_sorted_eq!(
            &[
                "+--------------------+",
                "| payload            |",
                "+--------------------+",
                "| {id: 1, name: one} |",
                "| {id: 2, name: two} |",
                "+--------------------+",
            ],
            &batches
        );
    }

    #[test]
    fn union_all_rejects_different_input_schemas() {
        let first = input_with_schema(test_schema());
        let different = input_with_schema(
            StructType::try_new([StructField::nullable("different", DataType::LONG)]).unwrap(),
        );
        let err = lower_operator(
            &KernelOperator::UnionAll(KernelUnionAll),
            &[Arc::clone(&first), Arc::clone(&different)],
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("requires all inputs to have the same schema"),
            "{err}"
        );
    }
}
