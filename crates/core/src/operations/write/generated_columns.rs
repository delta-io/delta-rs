use arrow_schema::Schema;
use datafusion::catalog::Session;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{ExprSchemable, LogicalPlan, LogicalPlanBuilder, col, when};
use datafusion::prelude::{cast, lit};
use datafusion::{execution::SessionState, prelude::DataFrame};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::table_features::TableFeature;
use tracing::debug;

use crate::{
    DeltaResult,
    delta_datafusion::expr::parse_predicate_expression,
    kernel::{DataCheck, EagerSnapshot},
    table::GeneratedColumn,
};

/// check if the writer version is able to write generated columns
#[inline]
pub fn gc_is_enabled(snapshot: &EagerSnapshot) -> bool {
    snapshot
        .table_configuration()
        .is_feature_enabled(&TableFeature::GeneratedColumns)
}

pub fn with_generated_columns(
    session: &dyn Session,
    plan: LogicalPlan,
    table_schema: &Schema,
    generated_cols: &Vec<GeneratedColumn>,
) -> Result<LogicalPlan> {
    if generated_cols.is_empty() {
        return Ok(plan);
    }

    // Preserve the full input projection: missing non-generated columns are handled later by
    // schema evolution logic when `SchemaMode::Merge` is used.
    let mut projection: Vec<_> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| col(f.name()))
        .collect();

    for generated_col in generated_cols {
        let name = generated_col.get_name();
        if plan.schema().field_with_unqualified_name(name).is_ok() {
            continue;
        }

        debug!("Adding missing generated column {}.", name);
        // Try to resolve the generation expression against the current plan schema.
        // When SchemaMode::Merge is used, the input batch may omit nullable columns
        // that the expression references. In that case, parse_predicate_expression
        // will fail because the column doesn't exist yet (schema evolution hasn't
        // run). We fall back to a typed NULL placeholder so the pipeline can
        // continue; schema evolution will later add the missing base columns as NULL,
        // and DataValidationExec will see NULL IS NOT DISTINCT FROM NULL = true.
        let expr = match parse_predicate_expression(
            plan.schema(),
            &generated_col.generation_expr,
            session,
        ) {
            Ok(resolved) => {
                let mut e = resolved.alias(name);
                if let Ok(field) = table_schema.field_with_name(name) {
                    e = e.cast_to(field.data_type(), plan.schema())?;
                }
                e
            }
            Err(_) => {
                debug!(
                    "Could not resolve generation expression for column {}, \
                     inserting NULL placeholder (will be resolved after schema evolution).",
                    name
                );
                // Use the target data type from the table schema if available,
                // otherwise fall back to a bare NULL.
                if let Ok(field) = table_schema.field_with_name(name) {
                    cast(lit(ScalarValue::Null), field.data_type().clone()).alias(name)
                } else {
                    lit(ScalarValue::Null).alias(name)
                }
            }
        };
        projection.push(expr);
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use arrow_array::RecordBatch;
    use datafusion::catalog::MemTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::execution::SessionState;
    use datafusion::prelude::SessionContext;
    use delta_kernel::schema::DataType as KernelDataType;
    use std::sync::Arc;

    fn create_test_session() -> SessionState {
        SessionContext::new().state()
    }

    fn create_test_plan() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let value_array = Int32Array::from(vec![10, 20, 30]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(value_array)]).unwrap();

        let source = provider_as_source(Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap(),
        ));
        LogicalPlanBuilder::scan("test", source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_empty_generated_columns() {
        let session = create_test_session();
        let plan = create_test_plan();

        let table_schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]);

        let generated_cols = vec![];

        let result = with_generated_columns(&session, plan.clone(), &table_schema, &generated_cols);
        assert!(result.is_ok());
        // When no generated columns, the plan should be returned unchanged
        let result_plan = result.unwrap();
        assert_eq!(
            result_plan.schema().fields().len(),
            plan.schema().fields().len()
        );
    }

    #[test]
    fn test_add_missing_generated_column() {
        let session = create_test_session();
        let plan = create_test_plan();

        // Table schema includes a new generated column "computed"
        let table_schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
            ArrowField::new("computed", ArrowDataType::Int32, false),
        ]);

        let generated_cols = vec![GeneratedColumn::new(
            "computed",
            "id + value",
            &KernelDataType::INTEGER,
        )];

        let result = with_generated_columns(&session, plan, &table_schema, &generated_cols);
        assert!(result.is_ok());

        let result_plan = result.unwrap();
        // Should have 3 fields now: id, value, computed
        assert_eq!(result_plan.schema().fields().len(), 3);
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("computed")
                .is_ok()
        );
    }

    #[test]
    fn test_existing_columns_pass_through() {
        let session = create_test_session();
        let plan = create_test_plan();

        let table_schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]);

        let generated_cols = vec![];

        let result = with_generated_columns(&session, plan, &table_schema, &generated_cols);
        assert!(result.is_ok());

        let result_plan = result.unwrap();
        assert_eq!(result_plan.schema().fields().len(), 2);
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("id")
                .is_ok()
        );
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("value")
                .is_ok()
        );
    }

    #[test]
    fn test_multiple_generated_columns() {
        let session = create_test_session();
        let plan = create_test_plan();

        let table_schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
            ArrowField::new("sum", ArrowDataType::Int32, false),
            ArrowField::new("product", ArrowDataType::Int32, false),
        ]);

        let generated_cols = vec![
            GeneratedColumn::new("sum", "id + value", &KernelDataType::INTEGER),
            GeneratedColumn::new("product", "id * value", &KernelDataType::INTEGER),
        ];

        let result = with_generated_columns(&session, plan, &table_schema, &generated_cols);
        assert!(result.is_ok());

        let result_plan = result.unwrap();
        assert_eq!(result_plan.schema().fields().len(), 4);
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("sum")
                .is_ok()
        );
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("product")
                .is_ok()
        );
    }

    #[test]
    fn test_mixed_existing_and_generated_columns() {
        let session = create_test_session();
        let plan = create_test_plan();

        // Mix of existing columns and new generated column
        let table_schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("computed", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]);

        let generated_cols = vec![GeneratedColumn::new(
            "computed",
            "id * 2",
            &KernelDataType::INTEGER,
        )];

        let result = with_generated_columns(&session, plan, &table_schema, &generated_cols);
        assert!(result.is_ok());

        let result_plan = result.unwrap();
        assert_eq!(result_plan.schema().fields().len(), 3);

        // Verify all columns are present
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("id")
                .is_ok()
        );
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("computed")
                .is_ok()
        );
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("value")
                .is_ok()
        );
    }

    #[test]
    fn test_missing_non_generated_nullable_column_does_not_error() {
        let session = create_test_session();
        let plan = create_test_plan();

        let table_schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
            ArrowField::new("computed", ArrowDataType::Int32, false),
            ArrowField::new("user", ArrowDataType::Utf8, true),
        ]);

        let generated_cols = vec![GeneratedColumn::new(
            "computed",
            "id + value",
            &KernelDataType::INTEGER,
        )];

        let result = with_generated_columns(&session, plan, &table_schema, &generated_cols);
        assert!(result.is_ok());

        let result_plan = result.unwrap();
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("computed")
                .is_ok()
        );
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("user")
                .is_err()
        );
    }

    /// Test that a generated column referencing a column not in the input batch
    /// does not fail, but instead produces a NULL placeholder.
    /// This is the core fix for #4169.
    #[test]
    fn test_generated_column_referencing_missing_column_uses_null_placeholder() {
        let session = create_test_session();
        // Plan only has "id" — missing "user" column
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let source = provider_as_source(Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap(),
        ));
        let plan = LogicalPlanBuilder::scan("test", source, None)
            .unwrap()
            .build()
            .unwrap();

        // Table schema has id, user (nullable), and computed = user
        let table_schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("user", ArrowDataType::Utf8, true),
            ArrowField::new("computed", ArrowDataType::Utf8, true),
        ]);

        // "computed" references "user", which is NOT in the input plan
        let generated_cols = vec![GeneratedColumn::new(
            "computed",
            "\"user\"",
            &KernelDataType::STRING,
        )];

        // Previously this would fail with "column user not found"
        let result = with_generated_columns(&session, plan, &table_schema, &generated_cols);
        assert!(
            result.is_ok(),
            "should not fail when generated column references a missing column: {:?}",
            result.err()
        );

        let result_plan = result.unwrap();
        assert_eq!(result_plan.schema().fields().len(), 2); // id + computed
        assert!(
            result_plan
                .schema()
                .field_with_unqualified_name("computed")
                .is_ok()
        );
    }
}
