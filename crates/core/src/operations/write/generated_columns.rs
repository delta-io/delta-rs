use arrow_schema::Schema;
use datafusion::catalog::Session;
use datafusion::common::{HashMap, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{ExprSchemable, LogicalPlan, LogicalPlanBuilder, col};
use datafusion::prelude::{DataFrame, lit, when};
use datafusion::scalar::ScalarValue;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use itertools::Itertools as _;
use tracing::debug;

use crate::DeltaTableError;
use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::{DeltaResult, kernel::DataCheck, table::GeneratedColumn};

pub fn with_generated_columns(
    session: &dyn Session,
    plan: LogicalPlan,
    table_schema: &Schema,
    generated_cols: &Vec<GeneratedColumn>,
) -> DeltaResult<LogicalPlan> {
    if generated_cols.is_empty() {
        return Ok(plan);
    }

    let mut gen_lookup: HashMap<_, _> = generated_cols
        .iter()
        .map(|gc| {
            Ok::<_, DeltaTableError>((
                gc.get_name(),
                parse_predicate_expression(plan.schema(), &gc.generation_expr, session)?
                    .alias(gc.get_name()),
            ))
        })
        .try_collect()?;

    let projection: Vec<_> = table_schema
        .fields()
        .iter()
        .map(|f| {
            let expr = if plan.schema().field_with_unqualified_name(f.name()).is_err() {
                if let Some(expr) = gen_lookup.remove(f.name().as_str()) {
                    debug!("Adding missing generated column {}.", f.name());
                    expr.cast_to(f.data_type(), plan.schema())?
                } else {
                    return plan_err!(
                        "Generated column expression for missing column {} not found.",
                        f.name()
                    );
                }
            } else {
                col(f.name())
            };
            Ok(expr)
        })
        .try_collect()?;

    Ok(LogicalPlanBuilder::new(plan).project(projection)?.build()?)
}

/// Add generated column expressions to a dataframe
pub fn add_missing_generated_columns(
    mut df: DataFrame,
    generated_cols: &Vec<GeneratedColumn>,
) -> DeltaResult<(DataFrame, Vec<String>)> {
    let mut missing_cols = vec![];
    for generated_col in generated_cols {
        let col_name = generated_col.get_name();

        if df
            .clone()
            .schema()
            .field_with_unqualified_name(col_name)
            .is_err()
        // implies it doesn't exist
        {
            debug!("Adding missing generated column {col_name} in source as placeholder");
            // If column doesn't exist, we add a null column, later we will generate the values after
            // all the merge is projected.
            // Other generated columns that were provided upon the start we only validate during write
            missing_cols.push(col_name.to_string());
            df = df.clone().with_column(col_name, lit(ScalarValue::Null))?;
        }
    }
    Ok((df, missing_cols))
}

/// Add generated column expressions to a dataframe
pub fn add_generated_columns(
    mut df: DataFrame,
    generated_cols: &Vec<GeneratedColumn>,
    generated_cols_missing_in_source: &[String],
    state: &SessionState,
) -> DeltaResult<DataFrame> {
    debug!("Generating columns in dataframe");
    for generated_col in generated_cols {
        // We only validate columns that were missing from the start. We don't update
        // update generated columns that were provided during runtime
        if !generated_cols_missing_in_source.contains(&generated_col.name) {
            continue;
        }

        let generation_expr = state.create_logical_expr(
            generated_col.get_generation_expression(),
            df.clone().schema(),
        )?;
        let col_name = generated_col.get_name();

        df = df.clone().with_column(
            generated_col.get_name(),
            when(col(col_name).is_null(), generation_expr)
                .otherwise(col(col_name))?
                .cast_to(&((&generated_col.data_type).try_into_arrow()?), df.schema())?,
        )?
    }
    Ok(df)
}
