use arrow_schema::Schema;
use datafusion::{
    catalog::Session,
    common::{DFSchema, HashMap, HashSet, Result, ToDFSchema, plan_datafusion_err, plan_err},
    functions::core::expr_ext::FieldAccessor,
    logical_expr::{ExprSchemable as _, LogicalPlan},
    prelude::{Expr, col, lit, r#struct, try_cast},
    scalar::ScalarValue,
};
use delta_kernel::{
    engine::arrow_conversion::{TryIntoArrow as _, TryIntoKernel as _},
    expressions::ColumnName,
    schema::StructType,
    table_configuration::TableConfiguration,
    table_features::TableFeature,
};
use futures::TryStreamExt as _;
use indexmap::IndexSet;
use itertools::Itertools as _;

use crate::{
    DeltaTableError, StructTypeExt as _,
    delta_datafusion::{
        expr::parse_predicate_expression,
        logical::{LogicalPlanBuilderExt as _, LogicalPlanExt as _},
        scan_files_where_matches,
    },
    kernel::{Action, EagerSnapshot},
    logstore::LogStore,
    merge_arrow_vec_fields,
    operations::{cdc::CDC_COLUMN_NAME, write::SchemaMode},
};

pub(crate) enum SchemaAction {
    /// Source and table schema are logically equivalent, no action needed.
    NoChange,
    /// Evolve the table schema to include new fields from the source schema.
    Evolve(DFSchema),
    /// Overwrite the table schema with the source schema.
    Overwrite(DFSchema),
}

/// Plans the necessary projection and schema action to align source
/// to be inserted into a table with the tables' schema.
///
/// This includes;
/// - determining the effective table schema after potential schema evolution,
/// - projecting the source data to the effective table schema
///   - imputing NULLs for missing nullable fields
///   - using generated column expressions
///   - casting types as necessary
///
/// Arguments:
/// - `session`: The DataFusion session context.
/// - `plan`: The logical plan representing the source data.
/// - `table_schema`: The schema of the target table.
/// - `table_config`: The configuration of the target table.
/// - `schema_mode`: The schema mode indicating how to handle schema differences.
/// - `safe_cast`: Whether to use safe casting for type conversions.
///   If `true` will emit `NULL` on cast failure.
///
/// Returns:
/// A tuple containing the possibly modified logical plan with necessary projections
/// and the determined schema action.
pub(crate) fn plan_insert(
    session: &dyn Session,
    plan: LogicalPlan,
    table_config: &TableConfiguration,
    schema_mode: Option<SchemaMode>,
    safe_cast: bool,
) -> Result<(LogicalPlan, SchemaAction)> {
    let table_schema: Schema = table_config.schema().as_ref().try_into_arrow()?;
    let table_schema = table_schema.to_dfschema()?;
    let source_schema = plan.schema().as_ref().clone().strip_qualifiers();

    if table_schema.logically_equivalent_names_and_types(&source_schema) {
        return Ok((plan, SchemaAction::NoChange));
    }

    // schemas differ, but we are just overwriting the schema with the source schema
    if schema_mode == Some(SchemaMode::Overwrite) {
        return Ok((plan, SchemaAction::Overwrite(source_schema)));
    }

    let table_norm = table_schema.as_arrow().normalize(".", None)?;
    let table_leaves = table_norm
        .fields()
        .iter()
        .map(|f| ColumnName::new([f.name()]))
        .collect::<HashSet<_>>();
    let source_norm = source_schema.as_arrow().normalize(".", None)?;
    let source_leaves = source_norm
        .fields()
        .iter()
        .map(|f| ColumnName::new([f.name()]))
        .collect::<HashSet<_>>();

    let missing_in_source = table_leaves.difference(&source_leaves).count();
    let missing_in_table = source_leaves.difference(&table_leaves).count();

    let gen_lookup: HashMap<_, _> =
        if table_config.is_feature_enabled(&TableFeature::GeneratedColumns) {
            let generated_cols = table_config
                .schema()
                .get_generated_columns()
                .map_err(|e| plan_datafusion_err!("{e}"))?;
            generated_cols
                .iter()
                .map(|gc| {
                    let col_name = ColumnName::from_naive_str_split(&gc.name);
                    let field_name = col_name.path().last().unwrap().clone();
                    Ok::<_, DeltaTableError>((
                        col_name,
                        parse_predicate_expression(plan.schema(), &gc.generation_expr, session)?
                            .alias(field_name),
                    ))
                })
                .try_collect()?
        } else {
            Default::default()
        };

    let (plan, action) = if missing_in_source > 0 && missing_in_table > 0 {
        // Both source and table schema have fields missing in the other
        // We need to merge the schemas and impute missing fields
        // in the source data. In case we are rescuing data from the table
        // an analogous projection will be applied there as well.
        if !matches!(
            schema_mode,
            Some(SchemaMode::Merge) | Some(SchemaMode::Overwrite)
        ) {
            return plan_err!(
                "Schema evolution required but schema mode not set. \
                Please set schema mode to enable schema evolution."
            );
        }
        if schema_mode == Some(SchemaMode::Overwrite) {
            validate_schema_evolution(source_schema.as_arrow(), table_config, &gen_lookup)?;
            (plan, SchemaAction::Overwrite(source_schema))
        } else {
            let merged_schema = Schema::new(merge_arrow_vec_fields(
                table_schema.as_arrow().fields(),
                source_schema.as_arrow().fields(),
                true,
            )?)
            .to_dfschema()?;
            validate_schema_evolution(merged_schema.as_arrow(), table_config, &gen_lookup)?;
            let projection =
                project_for_insert(&merged_schema, &source_schema, gen_lookup, safe_cast)?;
            let plan = plan.into_builder().project(projection)?.build()?;
            (plan, SchemaAction::Evolve(merged_schema))
        }
    } else if missing_in_source > 0 {
        // The source schema is missing fields present in the table schema
        // We need to impute those missing fields
        let projection = project_for_insert(&table_schema, &source_schema, gen_lookup, safe_cast)?;
        let plan = plan.into_builder().project(projection)?.build()?;
        (plan, SchemaAction::NoChange)
    } else if missing_in_table > 0 {
        // The source schema has new fields not present in the table schema
        // We need to evolve the table schema to include those new fields
        // The source data already has all the necessary fields
        validate_schema_evolution(plan.schema().as_arrow(), table_config, &gen_lookup)?;
        (plan, SchemaAction::Evolve(source_schema))
    } else {
        // Schemas differ only in nullability or physical types
        // We just need to cast fields as necessary
        let projection =
            project_for_insert(&table_schema, &source_schema, Default::default(), safe_cast)?;
        let plan = plan.into_builder().project(projection)?.build()?;
        (plan, SchemaAction::NoChange)
    };

    if !matches!(action, SchemaAction::NoChange) {
        if schema_mode.is_none() {
            return plan_err!(
                "Schema evolution required but schema mode not specified. \
                Please set schema mode to Merge or Overwrite."
            );
        }
    }

    Ok((plan, action))
}

/// Validate the schema evolution can br performed on the table.
///
/// Schema evolution cannot be performed if er are evolving
/// to a state that invalidates the current data in the table.
/// Specifically, validations should hold true for existing data.
/// We could probably analyze existing predicates prove this, but
/// for now we just check that no new validations are being added.
// TODO: here we should also validate type widening rules as per delta spec.
fn validate_schema_evolution(
    source_schema: &Schema,
    table_config: &TableConfiguration,
    lookup: &HashMap<ColumnName, Expr>,
) -> Result<()> {
    let source_kernel: StructType = source_schema.try_into_kernel()?;
    let source_cols = source_kernel
        .get_generated_columns()
        .map_err(|e| plan_datafusion_err!("{e}"))?;
    if !table_config.is_feature_enabled(&TableFeature::GeneratedColumns) {
        if source_cols.is_empty() {
            return Ok(());
        }
        return plan_err!(
            "Schema evolved fields cannot have generated expressions. \
            Recreate the table to achieve this."
        );
    }
    for gc in source_cols {
        let col_name = ColumnName::from_naive_str_split(&gc.name);
        if !lookup.contains_key(&col_name) {
            return plan_err!(
                "Schema evolved fields cannot have generated expressions. \
                    Recreate the table to achieve this."
            );
        }
    }
    Ok(())
}

/// Generates a projection expression to align source schema to target schema for insert.
///
/// This handles:
/// - Picking existing fields from source.
/// - Imputing NULLs for missing nullable fields.
/// - Using generated column expressions.
/// - Casting types as necessary.
///
/// # Arguments
/// - `target_schema`: The schema of the target table.
/// - `source_schema`: The schema of the source data.
/// - `generated_columns`: A map of generated column expressions to use.
/// - `safe_cast`: Whether to use safe casting for type conversions.
///
/// # Returns
/// A vector of expressions representing the projection to apply to the source data.
fn project_for_insert(
    target_schema: &DFSchema,
    source_schema: &DFSchema,
    mut generated_columns: HashMap<ColumnName, Expr>,
    safe_cast: bool,
) -> Result<Vec<Expr>> {
    // WE first normalize both schemas to get their leaf fields.
    //
    // Then for each leaf field in the target schema, we determine
    // how to obtain it from the source schema:
    // - If present in source, we pick it (with casting if needed).
    // - If missing but generated, we use the generated expression.
    // - If missing and nullable, we impute NULL.
    // - If missing and non-nullable, we error.
    //
    // We then re-assemble the leaf fields back into their nested structure,
    let target_norm = target_schema.as_arrow().normalize(".", None)?;
    let target_leaves = target_norm
        .fields()
        .iter()
        // field names were concatednated with '.' during normalization
        // so we need to split them back into paths
        .map(|f| ColumnName::from_naive_str_split(f.name()))
        .collect::<IndexSet<_>>();
    let source_norm = source_schema.as_arrow().normalize(".", None)?;
    let source_leaves = source_norm
        .fields()
        .iter()
        .map(|f| ColumnName::from_naive_str_split(f.name()))
        .collect::<IndexSet<_>>();

    // collect transforms we need to apply for each leaf field in the table schema
    let mut leaf_transforms: Vec<(ColumnName, Expr, bool)> =
        Vec::with_capacity(target_leaves.len());
    for (leaf, field) in target_leaves.into_iter().zip(target_norm.fields().iter()) {
        if source_leaves.contains(&leaf) {
            let mut path_iter = leaf.path().iter();
            // safety: we know there is at least one element from above check
            let mut expr = col(path_iter.next().unwrap());
            for part in path_iter {
                expr = expr.field(part.as_str());
            }
            if &expr.get_type(source_schema)? != field.data_type() {
                if safe_cast {
                    expr = try_cast(expr, field.data_type().clone());
                } else {
                    expr = expr.cast_to(field.data_type(), source_schema)?;
                }
            }
            expr = expr.alias(field.name());
            leaf_transforms.push((leaf, expr, true));
        } else if let Some(gen_expr) = generated_columns.remove(&leaf) {
            leaf_transforms.push((leaf, gen_expr, false));
        } else {
            if field.is_nullable() {
                leaf_transforms.push((
                    leaf,
                    lit(ScalarValue::Null).cast_to(field.data_type(), source_schema)?,
                    false,
                ));
            } else {
                return plan_err!(
                    "Cannot impute missing non-nullable field {} in source schema.",
                    leaf
                );
            }
        }
    }

    // Group by root field name (preserves order from table schema)
    let mut root_groups: Vec<(String, Vec<(ColumnName, Expr, bool)>)> = Vec::new();
    for (leaf, expr, is_pick) in leaf_transforms {
        let root_name = leaf.path()[0].to_string();

        // Add to existing group or create new one
        if let Some((last_root, leaves)) = root_groups.last_mut() {
            if last_root == &root_name {
                leaves.push((leaf, expr, is_pick));
                continue;
            }
        }
        root_groups.push((root_name, vec![(leaf, expr, is_pick)]));
    }

    // Build expression for each root field
    let expressions: Vec<Expr> = root_groups
        .into_iter()
        .map(|(root_name, leaves)| {
            build_field_expr(vec![root_name.clone()], leaves).map(|e| e.alias(&root_name))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(expressions)
}

/// Recursively build an expression for a field (possibly nested struct).
///
/// ## Arguments
/// - `prefix`: Path to the current field being built (e.g., ["person", "address"])
/// - `leaves`: All leaf fields under this prefix with their expressions
///
/// ## Returns
/// An expression that either:
/// - Picks the field directly if all children are picks (optimized)
/// - Builds a struct expression if any child is generated/null
fn build_field_expr(prefix: Vec<String>, leaves: Vec<(ColumnName, Expr, bool)>) -> Result<Expr> {
    // Base case: single leaf at this exact path
    if leaves.len() == 1 && leaves[0].0.path() == prefix.as_slice() {
        let (_, expr, _) = leaves.into_iter().next().unwrap();
        return Ok(expr);
    }

    // Group leaves by the next path segment after prefix
    let depth = prefix.len();
    let mut child_groups: Vec<(String, Vec<(ColumnName, Expr, bool)>)> = Vec::new();

    for (leaf, expr, is_pick) in leaves {
        // Get the child segment at current depth
        if leaf.path().len() <= depth {
            return plan_err!("Leaf path shorter than expected prefix depth");
        }
        let child_segment = leaf.path()[depth].to_string();

        // Add to existing group or create new one
        if let Some((last_child, child_leaves)) = child_groups.last_mut() {
            if last_child == &child_segment {
                child_leaves.push((leaf, expr, is_pick));
                continue;
            }
        }
        child_groups.push((child_segment, vec![(leaf, expr, is_pick)]));
    }

    // Check if all children are picks - enables simplification
    let all_pick = child_groups
        .iter()
        .all(|(_, leaves)| leaves.iter().all(|(_, _, is_pick)| *is_pick));

    if all_pick {
        // Optimization: all children are picks, so pick parent directly
        let mut expr = col(prefix[0].as_str());
        for segment in &prefix[1..] {
            expr = expr.field(segment.as_str());
        }
        Ok(expr)
    } else {
        // Build struct expression with recursive field building
        let fields: Vec<_> = child_groups
            .into_iter()
            .map(|(child_name, child_leaves)| {
                let mut child_prefix = prefix.clone();
                child_prefix.push(child_name.clone());
                let child_expr = build_field_expr(child_prefix, child_leaves)?;
                Ok(child_expr.alias(&child_name))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(r#struct(fields))
    }
}

pub(super) async fn plan_overwrite(
    session: &dyn Session,
    log_store: &dyn LogStore,
    snapshot: &EagerSnapshot,
    plan: LogicalPlan,
    where_clause: Option<Expr>,
) -> Result<(Vec<Action>, LogicalPlan, bool)> {
    match where_clause {
        Some(pred) => plan_replace_where(session, log_store, snapshot, plan, pred).await,
        _ => {
            // Full overwrite, so we remove all existing data files
            let removes = snapshot
                .file_views(log_store, None)
                .map_ok(|p| p.remove_action(true).into());
            Ok((removes.try_collect().await?, plan, false))
        }
    }
}

/// Plans a replace operation with a WHERE clause.
///
/// This involves identifying the files that match the WHERE clause,
/// removing them, and rewriting the data that does not match the WHERE clause
/// back to the table, along with the new data from the original plan.
///
/// The incoming data should already be projected to comply with the table schema.
async fn plan_replace_where(
    session: &dyn Session,
    log_store: &dyn LogStore,
    snapshot: &EagerSnapshot,
    plan: LogicalPlan,
    where_clause: Expr,
) -> Result<(Vec<Action>, LogicalPlan, bool)> {
    let maybe_files_scan = scan_files_where_matches(session, snapshot, where_clause).await?;
    let Some(files_scan) = maybe_files_scan else {
        // No files match the predicate, so we can just return the original plan
        return Ok((vec![], plan, false));
    };

    let removes = files_scan.remove_actions(log_store, snapshot).await?;

    if files_scan.partition_only {
        // No data files need to be rewritten
        return Ok((removes, plan, false));
    }

    // All data in the files which do NOT match the predicate
    // needs to be rescued and rewritten back to the table
    let mut rescued_data = files_scan
        .scan()
        .clone()
        .into_builder()
        .filter(files_scan.predicate.clone().is_not_true())?
        .build()?;

    if rescued_data.schema() != plan.schema() {
        // the inserted data may have evolved the schema, so we need to
        // ensure the rescued data matches the schema of the inserted data.
        // So we just pretned the rescued data is data we we are inserting
        // into a table with the schema of the inserted data.
        let projection = project_for_insert(
            plan.schema(),
            rescued_data.schema(),
            Default::default(),
            false,
        )?;
        rescued_data = rescued_data.into_builder().project(projection)?.build()?;
    }

    // The inserted data needs to compliy with the predicate.
    // TODO(roeap): in databricks this can be configured to skip validation.
    //   We should consider adding a similar config. The default is to validate.
    let insert_data = plan
        .into_builder()
        .validate(Some(files_scan.predicate.clone()))?
        .build()?;

    // If we do not need to create a CDF change feed, we can just union
    // the rescued data and the inserted data and be done with it.
    if !snapshot
        .table_configuration()
        .is_feature_enabled(&TableFeature::ChangeDataFeed)
    {
        let write_plan = insert_data.into_builder().union(rescued_data)?.build()?;
        return Ok((removes, write_plan, false));
    }

    let cdf_delete = files_scan
        .scan()
        .clone()
        .into_builder()
        .filter(files_scan.predicate.clone())?
        .with_column(CDC_COLUMN_NAME, lit("delete"))?
        .build()?;
    let cdf_insert = insert_data
        .into_builder()
        .with_column(CDC_COLUMN_NAME, lit("insert"))?
        .build()?;
    let cdf_plan = rescued_data
        .into_builder()
        // we assign a dummy value here, so the data gets
        // send to the data files but ignored in the CDF files.
        .with_column(CDC_COLUMN_NAME, lit(""))?
        .union(cdf_delete)?
        .union(cdf_insert)?
        .build()?;

    Ok((removes, cdf_plan, true))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use std::ops::Add as _;
    use std::sync::Arc;

    use super::*;

    // Tests for source_projection_for_impute

    fn create_simple_source_schema() -> DFSchema {
        let schema = Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]);
        DFSchema::try_from(schema).unwrap()
    }

    #[test]
    fn test_projection_all_fields_present() {
        // Test case: all table fields are present in source - should just pick them
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]))
        .to_dfschema()
        .unwrap();
        let source_schema = create_simple_source_schema();

        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        assert_eq!(exprs.len(), 2);

        // Both fields should be simple column picks
        let expr_strings: Vec<_> = exprs.iter().map(|e| format!("{}", e)).collect();
        assert!(expr_strings[0].contains("id"));
        assert!(expr_strings[1].contains("name"));
    }

    #[test]
    fn test_projection_with_nullable_missing_field() {
        // Test case: table has a nullable field missing from source - should impute NULL
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("optional", ArrowDataType::Int32, true), // Missing in source
        ]))
        .to_dfschema()
        .unwrap();

        let source_schema = create_simple_source_schema();
        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        assert_eq!(exprs.len(), 3);
    }

    #[test]
    fn test_projection_with_non_nullable_missing_field() {
        // Test case: table has a non-nullable field missing from source - should error
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("required", ArrowDataType::Int32, false), // Non-nullable, missing in source
        ]))
        .to_dfschema()
        .unwrap();

        let source_schema = create_simple_source_schema();

        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-nullable"));
    }

    #[test]
    fn test_projection_with_nested_struct_all_pick() {
        // Test case: nested struct where all fields are present in source
        // Should simplify to picking the parent struct
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "address",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("street", ArrowDataType::Utf8, true),
                        ArrowField::new("city", ArrowDataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]))
        .to_dfschema()
        .unwrap();

        // Source has same schema
        let source_schema = table_schema.clone();

        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        // Should have 2 root fields: id and address
        assert_eq!(exprs.len(), 2);
    }

    #[test]
    fn test_projection_with_nested_struct_partial_pick() {
        // Test case: nested struct where some fields are missing
        // Should create struct expression with mixed pick/null
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "address",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("street", ArrowDataType::Utf8, true),
                        ArrowField::new("city", ArrowDataType::Utf8, true),
                        ArrowField::new("zipcode", ArrowDataType::Utf8, true), // Missing in source
                    ]
                    .into(),
                ),
                true,
            ),
        ]))
        .to_dfschema()
        .unwrap();

        // Source only has street and city
        let source_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "address",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("street", ArrowDataType::Utf8, true),
                        ArrowField::new("city", ArrowDataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));
        let source_schema = DFSchema::try_from(source_schema.as_ref().clone()).unwrap();

        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        // Should have 2 root fields: id and address
        assert_eq!(exprs.len(), 2);

        // The address field should be a struct expression (not a simple pick)
        let address_expr = &exprs[1];
        let expr_string = format!("{}", address_expr);
        // Should contain struct keyword since we're building a struct expression
        assert!(expr_string.contains("struct") || expr_string.contains("Struct"));
    }

    #[test]
    fn test_projection_with_generated_column() {
        // Test case: table has a generated column that needs to be computed
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
            ArrowField::new("computed", ArrowDataType::Int32, false), // Generated column
        ]))
        .to_dfschema()
        .unwrap();

        // Source only has id and value
        let source_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        let source_df_schema = DFSchema::try_from(source_schema.as_ref().clone()).unwrap();

        // Create a generated expression for 'computed'
        let mut generated = HashMap::new();
        let computed_col = ColumnName::new(["computed"]);
        let gen_expr = col("id").add(col("value"));
        generated.insert(computed_col, gen_expr);

        let result = project_for_insert(&table_schema, &source_df_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        assert_eq!(exprs.len(), 3);
    }

    #[test]
    fn test_projection_multi_level_nesting_all_pick() {
        // Test case: multi-level nested struct where all fields are present
        // Should simplify across multiple levels
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "person",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("name", ArrowDataType::Utf8, true),
                        ArrowField::new(
                            "address",
                            ArrowDataType::Struct(
                                vec![
                                    ArrowField::new("street", ArrowDataType::Utf8, true),
                                    ArrowField::new("city", ArrowDataType::Utf8, true),
                                ]
                                .into(),
                            ),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]))
        .to_dfschema()
        .unwrap();

        // Source has same schema
        let source_schema = table_schema.clone();

        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        // Should have 2 root fields: id and person
        assert_eq!(exprs.len(), 2);

        // Since all fields are present, person should be a simple pick
        let person_expr = &exprs[1];
        let expr_string = format!("{}", person_expr);
        // Should be a simple column reference, not a complex struct expression
        assert!(expr_string.contains("person"));
    }

    #[test]
    fn test_projection_multiple_structs_same_depth() {
        // Test case: multiple independent structs at same depth - the key issue
        // Schema: id, person.name, person.age, contact.email, contact.phone
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "person",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("name", ArrowDataType::Utf8, true),
                        ArrowField::new("age", ArrowDataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            ),
            ArrowField::new(
                "contact",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("email", ArrowDataType::Utf8, true),
                        ArrowField::new("phone", ArrowDataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]))
        .to_dfschema()
        .unwrap();

        // Source has same schema
        let source_schema = table_schema.clone();

        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        // Should have 3 root fields: id, person, contact
        assert_eq!(exprs.len(), 3);

        // All should be simple picks since all fields are present
        let expr_strings: Vec<_> = exprs.iter().map(|e| format!("{}", e)).collect();
        assert!(expr_strings[0].contains("id"));
        assert!(expr_strings[1].contains("person"));
        assert!(expr_strings[2].contains("contact"));
    }

    #[test]
    fn test_projection_multiple_structs_mixed_actions() {
        // Test case: multiple structs at same depth with different actions
        // person struct: all fields present (pick)
        // contact struct: missing phone (null imputation) - should create struct expression
        let table_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "person",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("name", ArrowDataType::Utf8, true),
                        ArrowField::new("age", ArrowDataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            ),
            ArrowField::new(
                "contact",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("email", ArrowDataType::Utf8, true),
                        ArrowField::new("phone", ArrowDataType::Utf8, true), // Missing in source
                    ]
                    .into(),
                ),
                true,
            ),
        ]))
        .to_dfschema()
        .unwrap();

        // Source is missing contact.phone
        let source_schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "person",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("name", ArrowDataType::Utf8, true),
                        ArrowField::new("age", ArrowDataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            ),
            ArrowField::new(
                "contact",
                ArrowDataType::Struct(
                    vec![ArrowField::new("email", ArrowDataType::Utf8, true)].into(),
                ),
                true,
            ),
        ]));
        let source_schema = DFSchema::try_from(source_schema.as_ref().clone()).unwrap();

        let generated = HashMap::new();

        let result = project_for_insert(&table_schema, &source_schema, generated, false);
        assert!(result.is_ok());

        let exprs = result.unwrap();
        // Should have 3 root fields: id, person, contact
        assert_eq!(exprs.len(), 3);

        // person should be simple pick, contact should be struct expression
        let person_expr = &exprs[1];
        let contact_expr = &exprs[2];

        let person_str = format!("{}", person_expr);
        let contact_str = format!("{}", contact_expr);

        // person should be simple
        assert!(person_str.contains("person"));

        // contact should be struct (has null imputation)
        assert!(contact_str.contains("struct") || contact_str.contains("Struct"));
    }
}
