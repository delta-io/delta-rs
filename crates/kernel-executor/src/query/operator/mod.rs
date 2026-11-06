//! Translates one kernel [`Operator`] into a DataFusion `LogicalPlan`.
//!
//! Each kernel operator has one function here. The function receives the already translated
//! inputs in the order [`PlanNode::inputs`] lists them. Scans need the executor for its object
//! stores; every other operator is a pure function of its inputs and goes through
//! [`lower_operator`].

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::{DFSchema, DataFusionError, NullEquality, Result as DFResult};
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::logical_expr::{
    Aggregate as DFAggregate, EmptyRelation, Expr, ExprFunctionExt, ExprSchemable,
    Filter as DFFilter, Join as DFJoin, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder,
    Projection as DFProjection, Union as DFUnion, col, lit,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::{ColumnName, Scalar};
use delta_kernel::plans::ir::nodes::{
    Agg, Aggregate, DynamicScan, FileType, Filter, Operator, Project, ScanJson, ScanParquet,
    SemiJoin, UnionAll, Values,
};
use delta_kernel::schema::{StructField, StructType};

use super::scan;
use crate::expressions::{
    column_to_df_expr, to_datafusion_predicate_expr, to_datafusion_scalar, to_df_struct_columns,
};
use crate::{DataFusionPlanExecutor, ExecutorError};

/// Dispatches `op` to the function that translates it.
pub fn to_logical_plan(
    executor: &DataFusionPlanExecutor,
    op: &Operator,
    inputs: &[Arc<LogicalPlan>],
) -> crate::Result<LogicalPlan> {
    match op {
        Operator::ScanParquet(op) => scan_parquet(op),
        Operator::ScanJson(op) => scan_json(op),
        Operator::DynamicScan(op) => dynamic_scan(executor, op, inputs),
        _ => Ok(lower_operator(op, inputs)?),
    }
}

/// The operator's name as it appears in error messages: `values`, `semi_join`, ...
pub(crate) fn name(op: &Operator) -> &'static str {
    match op {
        Operator::ScanParquet(_) => "scan_parquet",
        Operator::ScanJson(_) => "scan_json",
        Operator::Values(_) => "values",
        Operator::Project(_) => "project",
        Operator::Filter(_) => "filter",
        Operator::DynamicScan(_) => "dynamic_scan",
        Operator::Aggregate(_) => "aggregate",
        Operator::SemiJoin(_) => "semi_join",
        Operator::UnionAll(_) => "union_all",
    }
}

/// Lowers one operator that needs no I/O over its already-lowered inputs.
///
/// # Errors
/// Returns an error if the operator has the wrong number of inputs, has no DataFusion lowering
/// yet, or if lowering its payload fails.
pub(crate) fn lower_operator(op: &Operator, inputs: &[Arc<LogicalPlan>]) -> DFResult<LogicalPlan> {
    let input_count_error = |expected: usize| {
        DataFusionError::Plan(format!(
            "{} expects {expected} input(s), but received {}",
            name(op),
            inputs.len()
        ))
    };
    match op {
        Operator::Values(values) => {
            let [] = inputs else {
                return Err(input_count_error(0));
            };
            lower_values(values)
        }
        Operator::Project(project) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_project(project, input)
        }
        Operator::Filter(filter) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_filter(filter, input)
        }
        Operator::Aggregate(aggregate) => {
            let [input] = inputs else {
                return Err(input_count_error(1));
            };
            lower_aggregate(aggregate, input)
        }
        Operator::SemiJoin(semi_join) => {
            let [probe, build] = inputs else {
                return Err(input_count_error(2));
            };
            lower_semi_join(semi_join, probe, build)
        }
        Operator::UnionAll(union_all) => lower_union_all(union_all, inputs),
        // The scans go through `to_logical_plan`, which has the executor they need.
        _ => Err(DataFusionError::NotImplemented(format!(
            "lowering operator {} to a DataFusion LogicalPlan",
            name(op)
        ))),
    }
}

/// Source: reads Parquet files from the session's object stores (see [`scan`]).
fn scan_parquet(op: &ScanParquet) -> crate::Result<LogicalPlan> {
    scan::lower_scan(
        FileType::Parquet,
        &op.files,
        &op.file_constant_columns,
        &op.schema,
    )
}

/// Source: reads newline-delimited JSON files from the session's object stores (see [`scan`]).
fn scan_json(op: &ScanJson) -> crate::Result<LogicalPlan> {
    scan::lower_scan(
        FileType::Json,
        &op.files,
        &op.file_constant_columns,
        &op.schema,
    )
}

/// Lowers a [`Values`](Values) node into literal rows carrying `schema`'s field names.
///
/// DataFusion's [`LogicalPlanBuilder::values_with_schema`] automatically inserts a cast whenever
/// [`can_cast_types`](datafusion::arrow::compute::can_cast_types) accepts a type mismatch.
fn lower_values(values: &Values) -> DFResult<LogicalPlan> {
    let arrow_schema: ArrowSchema = values.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(arrow_schema.try_into()?);

    if values.rows.is_empty() {
        let empty = EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        };
        return Ok(LogicalPlan::EmptyRelation(empty));
    }
    let rows: DFResult<Vec<Vec<Expr>>> = values.rows.iter().map(|row| lower_row(row)).collect();

    // The builder assigns column1, column2, ...; restore the names declared by the kernel schema.
    let field_aliases = df_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| col(format!("column{}", index + 1)).alias(field.name()));
    LogicalPlanBuilder::values_with_schema(rows?, &df_schema)?
        .project(field_aliases)?
        .build()
}

/// Lowers one row of literals into DataFusion expressions, one per column.
///
/// # Errors
/// Returns an error for a literal with no DataFusion equivalent.
fn lower_row(row: &[Scalar]) -> DFResult<Vec<Expr>> {
    let lower_literal = |scalar| {
        let lowered =
            to_datafusion_scalar(scalar).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(lit(lowered))
    };
    row.iter().map(lower_literal).collect()
}

/// Lowers a [`Project`](Project) to a single DataFusion projection.
///
/// A kernel Project holds a struct expression and its declared output schema, while DataFusion
/// expects a flat expression list. Each struct field is therefore lowered, cast to its declared
/// type, and aliased with its declared name before the declared output schema is attached to the
/// DataFusion projection.
///
/// # Errors
/// Returns an error if the Project expression is not a `Struct`/`StructPatch`, lowering a field or
/// its nullability guard fails, or DataFusion cannot cast a field to its declared type.
fn lower_project(project: &Project, input: &Arc<LogicalPlan>) -> DFResult<LogicalPlan> {
    let input_schema: StructType = input.schema().as_arrow().try_into_kernel()?;
    let arrow_schema: ArrowSchema = project.schema.as_ref().try_into_arrow()?;
    let df_schema = Arc::new(DFSchema::try_from(arrow_schema)?);

    let columns = to_df_struct_columns(&project.expr, &input_schema, project.schema.as_ref())
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .into_guarded_columns();

    let exprs: DFResult<Vec<Expr>> = columns
        .into_iter()
        .zip(project.schema.fields())
        .map(|((name, expr), field)| {
            let target = field.data_type().try_into_arrow()?;
            let expr = expr.cast_to(&target, input.schema())?.alias(name);
            Ok(expr)
        })
        .collect();

    let projection = DFProjection::try_new_with_schema(exprs?, Arc::clone(input), df_schema)?;
    Ok(LogicalPlan::Projection(projection))
}

/// Lowers a [`Filter`](Filter) node into a DataFusion [`Filter`](DFFilter) logical plan over
/// its input.
fn lower_filter(filter: &Filter, input: &Arc<LogicalPlan>) -> DFResult<LogicalPlan> {
    let input_schema: StructType = input.schema().as_arrow().try_into_kernel()?;
    let predicate = to_datafusion_predicate_expr(&filter.predicate, &input_schema)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let filter = DFFilter::try_new(predicate, Arc::clone(input))?;
    Ok(LogicalPlan::Filter(filter))
}

/// Unary: scans the files named by the input rows.
fn dynamic_scan(
    _executor: &DataFusionPlanExecutor,
    _op: &DynamicScan,
    _inputs: &[Arc<LogicalPlan>],
) -> crate::Result<LogicalPlan> {
    Err(ExecutorError::unsupported("operator dynamic_scan"))
}

/// Lowers an [`Aggregate`](Aggregate) to a DataFusion aggregate over its input.
///
/// This lowering does not cast aggregate operands. It passes them using their input-schema types,
/// and DataFusion's type-coercion analyzer inserts any casts required by each aggregate function's
/// signature. The explicit casts here only make group keys and final aggregate results match the
/// aggregate's declared output schema.
fn lower_aggregate(aggregate: &Aggregate, input: &Arc<LogicalPlan>) -> DFResult<LogicalPlan> {
    let input_schema = input.schema().as_ref();
    let output_fields = Vec::from_iter(aggregate.schema.fields());
    if aggregate.group_by.is_empty() && aggregate.aggs.is_empty() {
        let arrow_schema: ArrowSchema = aggregate.schema.as_ref().try_into_arrow()?;
        let empty = EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(arrow_schema.try_into()?),
        };
        return Ok(LogicalPlan::EmptyRelation(empty));
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
    Ok(LogicalPlan::Aggregate(df_aggregate))
}

fn cast_aggregate_output(
    expr: Expr,
    field: Option<&StructField>,
    input_schema: &DFSchema,
) -> DFResult<Expr> {
    let Some(field) = field else {
        // No output field matches this expression; keep it for DataFusion's arity check.
        return Ok(expr);
    };
    let target = field.data_type().try_into_arrow()?;
    let expr = expr.cast_to(&target, input_schema)?;
    Ok(expr.alias(field.name().clone()))
}

fn lower_aggregate_function(agg: &Agg, input_schema: &DFSchema) -> DFResult<Expr> {
    match agg {
        Agg::Min(value) => Ok(min(column_to_df_expr(value, input_schema)?)),
        Agg::Max(value) => Ok(max(column_to_df_expr(value, input_schema)?)),
        Agg::Sum(value) => Ok(sum(column_to_df_expr(value, input_schema)?)),
        Agg::Count(value) => Ok(count(column_to_df_expr(value, input_schema)?)),
        // DataFusion produces 1 for every input row, so counting it implements COUNT(*).
        Agg::CountStar => Ok(count(lit(1))),
        Agg::MinNonNullBy(operands) => lower_non_null_by(
            &operands.value,
            &operands.null_sentinel,
            &operands.key,
            input_schema,
            true,
        ),
        Agg::MaxNonNullBy(operands) => lower_non_null_by(
            &operands.value,
            &operands.null_sentinel,
            &operands.key,
            input_schema,
            false,
        ),
    }
}

fn lower_non_null_by(
    value: &ColumnName,
    null_sentinel: &ColumnName,
    key: &ColumnName,
    input_schema: &DFSchema,
    ascending: bool,
) -> DFResult<Expr> {
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

/// Lowers a [`SemiJoin`](SemiJoin) to a DataFusion left semi or left anti join over its probe
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
    semi_join: &SemiJoin,
    probe: &Arc<LogicalPlan>,
    build: &Arc<LogicalPlan>,
) -> DFResult<LogicalPlan> {
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

    Ok(LogicalPlan::Join(join))
}

/// Lowers a [`UnionAll`](UnionAll) to one n-ary DataFusion union that preserves every input
/// row, including duplicates.
///
/// # Errors
/// Returns an error if there are fewer than two inputs, their schemas differ, or DataFusion rejects
/// the union.
fn lower_union_all(_union_all: &UnionAll, inputs: &[Arc<LogicalPlan>]) -> DFResult<LogicalPlan> {
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

    Ok(LogicalPlan::Union(union))
}

#[cfg(test)]
mod tests;
