use datafusion::{execution::SessionState, prelude::DataFrame};
use datafusion_common::ScalarValue;
use datafusion_expr::{col, when, Expr, ExprSchemable};
use tracing::debug;

use crate::{kernel::DataCheck, table::GeneratedColumn, DeltaResult};

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
            df = df
                .clone()
                .with_column(col_name, Expr::Literal(ScalarValue::Null))?;
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
                .cast_to(
                    &arrow_schema::DataType::try_from(&generated_col.data_type)?,
                    df.schema(),
                )?,
        )?
    }
    Ok(df)
}
