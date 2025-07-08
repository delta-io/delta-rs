use crate::table::state::DeltaTableState;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{col, when, ExprSchemable};
use datafusion::prelude::lit;
use datafusion::{execution::SessionState, prelude::DataFrame};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use tracing::debug;

use crate::{kernel::DataCheck, table::GeneratedColumn, DeltaResult};

/// check if the writer version is able to write generated columns
pub fn able_to_gc(snapshot: &DeltaTableState) -> DeltaResult<bool> {
    if let Some(features) = &snapshot.protocol().writer_features() {
        if snapshot.protocol().min_writer_version() < 4 {
            return Ok(false);
        }
        if snapshot.protocol().min_writer_version() == 7
            && !features.contains(&delta_kernel::table_features::WriterFeature::GeneratedColumns)
        {
            return Ok(false);
        }
    }
    Ok(true)
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
