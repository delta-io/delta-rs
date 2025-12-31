use crate::{delta_datafusion::logical::LogicalPlanBuilderExt, kernel::EagerSnapshot};
use datafusion::execution::SessionState;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, LogicalPlan, LogicalPlanBuilder, col, lit, when};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use tracing::debug;

use crate::{DeltaResult, kernel::DataCheck, table::GeneratedColumn};

/// check if the writer version is able to write generated columns
pub fn able_to_gc(snapshot: &EagerSnapshot) -> DeltaResult<bool> {
    if let Some(features) = &snapshot.protocol().writer_features() {
        if snapshot.protocol().min_writer_version() < 4 {
            return Ok(false);
        }
        if snapshot.protocol().min_writer_version() == 7
            && !features.contains(&delta_kernel::table_features::TableFeature::GeneratedColumns)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Add generated column expressions to a Logical Plan
pub fn add_missing_generated_columns(
    mut logical_plan: LogicalPlan,
    generated_cols: &Vec<GeneratedColumn>,
) -> DeltaResult<(LogicalPlan, Vec<String>)> {
    let mut missing_cols = vec![];
    for generated_col in generated_cols {
        let col_name = generated_col.get_name();

        if logical_plan
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
            logical_plan = LogicalPlanBuilder::from(logical_plan)
                .with_column(col_name, lit(ScalarValue::Null), false)?
                .build()?;
        }
    }
    Ok((logical_plan, missing_cols))
}

/// Add generated column expressions to a dataframe
pub fn add_generated_columns(
    mut logical_plan: LogicalPlan,
    generated_cols: &Vec<GeneratedColumn>,
    generated_cols_missing_in_source: &[String],
    state: &SessionState,
) -> DeltaResult<LogicalPlan> {
    debug!("Generating columns in logical plan");
    for generated_col in generated_cols {
        // We only validate columns that were missing from the start. We don't update
        // update generated columns that were provided during runtime
        if !generated_cols_missing_in_source.contains(&generated_col.name) {
            continue;
        }

        let generation_expr = state.create_logical_expr(
            generated_col.get_generation_expression(),
            logical_plan.schema(),
        )?;
        let col_name = generated_col.get_name();

        logical_plan = LogicalPlanBuilder::from(logical_plan.clone())
            .with_column(
                col_name,
                when(col(col_name).is_null(), generation_expr)
                    .otherwise(col(col_name))?
                    .cast_to(
                        &((&generated_col.data_type).try_into_arrow()?),
                        logical_plan.schema(),
                    )?,
                false,
            )?
            .build()?;
    }
    Ok(logical_plan)
}
