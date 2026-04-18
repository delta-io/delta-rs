use crate::errors::DeltaResult;
use crate::{kernel::EagerSnapshot, table::IdentityColumnInfo};
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{ExprSchemable, LogicalPlan, LogicalPlanBuilder, col};
use datafusion::prelude::lit;
use delta_kernel::schema::{MetadataValue, StructField, StructType};
use delta_kernel::table_features::TableFeature;

#[inline]
pub fn identity_columns_enabled(snapshot: &EagerSnapshot) -> bool {
    snapshot
        .table_configuration()
        .is_feature_enabled(&TableFeature::IdentityColumns)
}

pub fn with_identity_columns(
    plan: LogicalPlan,
    identity_columns: &[IdentityColumnInfo],
) -> Result<LogicalPlan> {
    if identity_columns.is_empty() {
        return Ok(plan);
    }

    // Reject user-provided values for GENERATED ALWAYS columns
    for c in identity_columns {
        let present_in_plan = plan.schema().field_with_unqualified_name(&c.name).is_ok();
        if !c.allow_explicit_insert && present_in_plan {
            return Err(DataFusionError::Plan(format!(
                "Cannot provide values for identity column '{}': \
                 allowExplicitInsert is false (GENERATED ALWAYS)",
                c.name
            )));
        }
    }

    // Decide which identity columns need values generated:
    //   - allowExplicitInsert=false -> always generate (user values rejected above)
    //   - allowExplicitInsert=true and column is present -> skip (user supplied values)
    //   - allowExplicitInsert=true and column is absent  -> generate
    let to_generate: Vec<&IdentityColumnInfo> = identity_columns
        .iter()
        .filter(|c| {
            let present_in_plan = plan.schema().field_with_unqualified_name(&c.name).is_ok();
            !present_in_plan
        })
        .collect();

    if to_generate.is_empty() {
        return Ok(plan);
    }

    // Step 1: add a __row_num__ column via window function
    // row_number() is 1-based
    let row_num_expr = row_number().alias("__row_num__");

    let plan = LogicalPlanBuilder::new(plan)
        .window(vec![row_num_expr])?
        .build()?;

    // Step 2: project all original columns + computed identity columns
    // drop __row_num__ and any identity columns we are replacing
    let generated_names: std::collections::HashSet<&str> =
        to_generate.iter().map(|c| c.name.as_str()).collect();
    let mut projection: Vec<_> = plan
        .schema()
        .fields()
        .iter()
        .filter(|f| f.name() != "__row_num__" && !generated_names.contains(f.name().as_str()))
        .map(|f| col(f.name()))
        .collect();

    for id_col in &to_generate {
        // base = hwm if present, else start - step
        // so that row 1 → base + 1*step = hwm + step (first new value)
        let base: i64 = match id_col.high_water_mark {
            Some(hwm) => hwm,
            None => id_col.start.checked_sub(id_col.step).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "identity column '{}': start - step overflows i64",
                    id_col.name
                ))
            })?,
        };

        // value = base + row_number * step
        let expr = (lit(base)
            + col("__row_num__").cast_to(&arrow_schema::DataType::Int64, plan.schema())?
                * lit(id_col.step))
        .alias(&id_col.name);

        projection.push(expr);
    }

    LogicalPlanBuilder::new(plan).project(projection)?.build()
}

/// After writing `num_rows`, compute the new HWM for each identity column
/// and return an updated schema with the new values in field metadata.
pub fn update_identity_column_hwm(
    schema: &StructType,
    identity_columns: &[IdentityColumnInfo],
    num_rows: usize,
) -> DeltaResult<StructType> {
    let identity_map: std::collections::HashMap<&str, &IdentityColumnInfo> = identity_columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let updated_fields: Vec<StructField> = schema
        .fields()
        .map(|f| {
            if let Some(id_col) = identity_map.get(f.name.as_str()) {
                let base: i64 = match id_col.high_water_mark {
                    Some(hwm) => hwm,
                    None => id_col.start - id_col.step,
                };
                let new_hwm = base + (num_rows as i64) * id_col.step;
                let mut metadata = f.metadata.clone();
                metadata.insert(
                    "delta.identity.highWaterMark".to_string(),
                    MetadataValue::String(new_hwm.to_string()),
                );
                StructField::new(f.name.clone(), f.data_type.clone(), f.nullable)
                    .with_metadata(metadata)
            } else {
                f.clone()
            }
        })
        .collect();

    Ok(StructType::try_new(updated_fields)?)
}
