use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::compute::can_cast_types;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_physical_expr_adapter::schema_rewriter::{
    PhysicalExprAdapter, PhysicalExprAdapterFactory,
};

/// Factory for creating physical expression adapters for Delta table scans.
///
/// This factory implements the Sail-style `PhysicalExprAdapterFactory` pattern
/// to handle schema evolution and type mismatches between logical table schemas
/// and physical file schemas during query execution.
///
/// # Schema Evolution Behaviors
///
/// - **Missing nullable column**: Returns `NULL` of the logical column type
/// - **Missing non-nullable column**: Returns zero-value (e.g., `0` for Int32), or `NULL` as fallback
/// - **Type mismatch**: Wraps column in a `CAST` expression if `can_cast_types` allows
/// - **Missing nested struct field**: Returns `NULL` when accessing a field via `GetFieldFunc`
/// - **Parquet read schema**: This adapter does not extend the file read schema; nested struct
///   evolution in the scan schema will still surface a parquet schema mismatch
/// - **Partition columns**: Partition values are injected by the scan path; the adapter does
///   not synthesize partition literals on its own
///
/// # Usage
///
/// This factory is wired into the Delta scan paths via `FileScanConfigBuilder::with_expr_adapter()`.
/// DataFusion calls `create()` once per physical file during scan planning.
#[derive(Debug, Default)]
pub struct DeltaPhysicalExprAdapterFactory;

impl DeltaPhysicalExprAdapterFactory {
    pub fn new() -> Self {
        Self
    }

    fn create_column_mapping(
        logical_schema: &Schema,
        physical_schema: &Schema,
    ) -> (Vec<Option<usize>>, Vec<Option<ScalarValue>>) {
        logical_schema
            .fields()
            .iter()
            .map(
                |logical_field| match physical_schema.index_of(logical_field.name()) {
                    Ok(physical_index) => (Some(physical_index), None),
                    Err(_) => {
                        let default_value = if logical_field.is_nullable() {
                            ScalarValue::try_from(logical_field.data_type())
                                .unwrap_or(ScalarValue::Null)
                        } else {
                            ScalarValue::new_zero(logical_field.data_type())
                                .unwrap_or(ScalarValue::Null)
                        };
                        (None, Some(default_value))
                    }
                },
            )
            .unzip()
    }
}

impl PhysicalExprAdapterFactory for DeltaPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        let (column_mapping, default_values) =
            Self::create_column_mapping(&logical_file_schema, &physical_file_schema);

        Arc::new(DeltaPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
            column_mapping,
            default_values,
        })
    }
}

#[derive(Debug)]
pub(crate) struct DeltaPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    column_mapping: Vec<Option<usize>>,
    default_values: Vec<Option<ScalarValue>>,
}

impl PhysicalExprAdapter for DeltaPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = DeltaPhysicalExprRewriter {
            logical_file_schema: &self.logical_file_schema,
            physical_file_schema: &self.physical_file_schema,
            column_mapping: &self.column_mapping,
            default_values: &self.default_values,
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }
}

impl Clone for DeltaPhysicalExprAdapter {
    fn clone(&self) -> Self {
        Self {
            logical_file_schema: Arc::clone(&self.logical_file_schema),
            physical_file_schema: Arc::clone(&self.physical_file_schema),
            column_mapping: self.column_mapping.clone(),
            default_values: self.default_values.clone(),
        }
    }
}

struct DeltaPhysicalExprRewriter<'a> {
    logical_file_schema: &'a Schema,
    physical_file_schema: &'a Schema,
    column_mapping: &'a [Option<usize>],
    default_values: &'a [Option<ScalarValue>],
}

impl<'a> DeltaPhysicalExprRewriter<'a> {
    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(transformed) = self.try_rewrite_struct_field_access(&expr)? {
            return Ok(Transformed::yes(transformed));
        }
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            return self.rewrite_column(Arc::clone(&expr), column);
        }

        Ok(Transformed::no(expr))
    }

    fn try_rewrite_struct_field_access(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let get_field_expr =
            match ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref()) {
                Some(expr) => expr,
                None => return Ok(None),
            };

        let source_expr = match get_field_expr.args().first() {
            Some(expr) => expr,
            None => return Ok(None),
        };
        let field_name_expr = match get_field_expr.args().get(1) {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let lit = match field_name_expr.as_any().downcast_ref::<Literal>() {
            Some(lit) => lit,
            None => return Ok(None),
        };
        let field_name = match lit.value().try_as_str().flatten() {
            Some(name) => name,
            None => return Ok(None),
        };

        let column = match source_expr.as_any().downcast_ref::<Column>() {
            Some(column) => column,
            None => return Ok(None),
        };

        let physical_field = match self.physical_file_schema.field_with_name(column.name()) {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };
        let physical_struct_fields = match physical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };
        if physical_struct_fields
            .iter()
            .any(|f| f.name() == field_name)
        {
            return Ok(None);
        }

        let logical_field = match self.logical_file_schema.field_with_name(column.name()) {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };
        let logical_struct_fields = match logical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };
        let logical_struct_field = match logical_struct_fields
            .iter()
            .find(|f| f.name() == field_name)
        {
            Some(field) => field,
            None => return Ok(None),
        };
        let null_value = ScalarValue::Null.cast_to(logical_struct_field.data_type())?;
        Ok(Some(Arc::new(Literal::new(null_value))))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        let logical_field_index = match self.logical_file_schema.index_of(column.name()) {
            Ok(index) => index,
            Err(_) => {
                if self
                    .physical_file_schema
                    .field_with_name(column.name())
                    .is_ok()
                {
                    return Ok(Transformed::no(expr));
                }
                return exec_err!(
                    "Column '{}' not found in either logical or physical schema",
                    column.name()
                );
            }
        };

        let logical_field = self.logical_file_schema.field(logical_field_index);

        match self.column_mapping.get(logical_field_index) {
            Some(Some(physical_index)) => {
                let physical_field = self.physical_file_schema.field(*physical_index);
                self.handle_existing_column(
                    expr,
                    column,
                    logical_field,
                    physical_field,
                    *physical_index,
                )
            }
            Some(None) => {
                if let Some(Some(default_value)) = self.default_values.get(logical_field_index) {
                    Ok(Transformed::yes(Arc::new(Literal::new(
                        default_value.clone(),
                    ))))
                } else if logical_field.is_nullable() {
                    let null_value = ScalarValue::Null.cast_to(logical_field.data_type())?;
                    Ok(Transformed::yes(Arc::new(Literal::new(null_value))))
                } else {
                    exec_err!(
                        "Non-nullable column '{}' is missing from physical schema and no default value provided",
                        column.name()
                    )
                }
            }
            None => exec_err!(
                "Column mapping not found for logical field index {}",
                logical_field_index
            ),
        }
    }

    fn handle_existing_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
        logical_field: &Field,
        physical_field: &Field,
        physical_index: usize,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        let needs_index_update = column.index() != physical_index;
        let needs_type_cast = logical_field.data_type() != physical_field.data_type();

        match (needs_index_update, needs_type_cast) {
            (false, false) => Ok(Transformed::no(expr)),
            (true, false) => {
                let new_column =
                    Column::new_with_schema(logical_field.name(), self.physical_file_schema)?;
                Ok(Transformed::yes(Arc::new(new_column)))
            }
            (false, true) => self.apply_type_cast(expr, logical_field, physical_field),
            (true, true) => {
                let new_column =
                    Column::new_with_schema(logical_field.name(), self.physical_file_schema)?;
                self.apply_type_cast(Arc::new(new_column), logical_field, physical_field)
            }
        }
    }

    fn apply_type_cast(
        &self,
        column_expr: Arc<dyn PhysicalExpr>,
        logical_field: &Field,
        physical_field: &Field,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // For struct types with schema evolution (physical has subset of logical fields),
        // pass through without casting. GetFieldFunc handler will manage missing field access.
        if let (DataType::Struct(physical_fields), DataType::Struct(logical_fields)) =
            (physical_field.data_type(), logical_field.data_type())
        {
            // Check if physical fields are a subset of logical fields (schema evolution)
            let physical_names: std::collections::HashSet<_> =
                physical_fields.iter().map(|f| f.name()).collect();
            let logical_names: std::collections::HashSet<_> =
                logical_fields.iter().map(|f| f.name()).collect();
            if physical_names.is_subset(&logical_names) {
                // Schema evolution case - pass column through unchanged
                return Ok(Transformed::no(column_expr));
            }
        }

        if !can_cast_types(physical_field.data_type(), logical_field.data_type()) {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical) to '{}' (logical)",
                logical_field.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        let cast_expr = self.create_delta_cast(column_expr, logical_field.data_type());
        Ok(Transformed::yes(cast_expr))
    }

    fn create_delta_cast(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        target_type: &DataType,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(CastExpr::new(expr, target_type.clone(), None))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion::functions::core::expr_fn::get_field;
    use datafusion::physical_expr::expressions::{CastExpr, Literal};
    use datafusion::physical_expr::planner::logical2physical;
    use datafusion::prelude::col;
    use datafusion::scalar::ScalarValue;
    use datafusion_physical_expr_adapter::replace_columns_with_literals;
    use datafusion_physical_expr_adapter::schema_rewriter::PhysicalExprAdapterFactory;

    use super::DeltaPhysicalExprAdapterFactory;

    fn make_adapter(
        logical: Arc<Schema>,
        physical: Arc<Schema>,
    ) -> Arc<dyn super::PhysicalExprAdapter> {
        let factory = DeltaPhysicalExprAdapterFactory::new();
        factory.create(logical, physical)
    }

    fn schema_from_fields(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(Fields::from(fields)))
    }

    #[test]
    fn missing_nullable_column_returns_null() {
        let logical = schema_from_fields(vec![Field::new("a", DataType::Utf8, true)]);
        let physical = schema_from_fields(vec![]);
        let adapter = make_adapter(logical.clone(), physical);

        let expr = logical2physical(&col("a"), logical.as_ref());
        let rewritten = adapter.rewrite(expr).unwrap();
        let literal = rewritten.as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(literal.value(), &ScalarValue::Utf8(None));
    }

    #[test]
    fn missing_non_nullable_column_returns_zero() {
        let logical = schema_from_fields(vec![Field::new("a", DataType::Int32, false)]);
        let physical = schema_from_fields(vec![]);
        let adapter = make_adapter(logical.clone(), physical);

        let expr = logical2physical(&col("a"), logical.as_ref());
        let rewritten = adapter.rewrite(expr).unwrap();
        let literal = rewritten.as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(literal.value(), &ScalarValue::Int32(Some(0)));
    }

    #[test]
    fn type_mismatch_casts_when_possible() {
        let logical = schema_from_fields(vec![Field::new("a", DataType::Int64, false)]);
        let physical = schema_from_fields(vec![Field::new("a", DataType::Int32, false)]);
        let adapter = make_adapter(logical.clone(), physical);

        let expr = logical2physical(&col("a"), logical.as_ref());
        let rewritten = adapter.rewrite(expr).unwrap();
        let cast = rewritten.as_any().downcast_ref::<CastExpr>().unwrap();
        assert_eq!(cast.cast_type(), &DataType::Int64);
    }

    #[test]
    fn incompatible_type_mismatch_returns_error() {
        // Struct to Int32 is not a valid cast - can_cast_types returns false
        let logical = schema_from_fields(vec![Field::new("a", DataType::Int32, false)]);
        let physical = schema_from_fields(vec![Field::new(
            "a",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Utf8, true)])),
            false,
        )]);
        let adapter = make_adapter(logical.clone(), physical);

        let expr = logical2physical(&col("a"), logical.as_ref());
        let result = adapter.rewrite(expr);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Cannot cast column"),
            "Expected error message about incompatible cast, got: {}",
            err_msg
        );
    }

    #[test]
    fn partition_values_substitute_literals() {
        let logical = schema_from_fields(vec![Field::new("p", DataType::Utf8, true)]);
        let physical = logical.clone();
        let adapter = make_adapter(logical.clone(), physical);

        let expr = logical2physical(&col("p"), logical.as_ref());
        let mut literals = HashMap::new();
        literals.insert("p".to_string(), ScalarValue::Utf8(Some("part".to_string())));
        let expr = replace_columns_with_literals(expr, &literals).unwrap();
        let rewritten = adapter.rewrite(expr).unwrap();
        let literal = rewritten.as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(
            literal.value(),
            &ScalarValue::Utf8(Some("part".to_string()))
        );
    }

    #[test]
    fn missing_nested_struct_field_returns_null() {
        let logical = schema_from_fields(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            true,
        )]);
        let physical = schema_from_fields(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
            true,
        )]);
        let adapter = make_adapter(logical.clone(), physical);

        let expr = get_field(col("s"), "b");
        let physical_expr = logical2physical(&expr, logical.as_ref());
        let rewritten = adapter.rewrite(physical_expr).unwrap();
        let literal = rewritten.as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(literal.value(), &ScalarValue::Utf8(None));
    }

    #[test]
    fn no_rewrite_when_struct_field_exists() {
        let logical = schema_from_fields(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
            true,
        )]);
        let physical = logical.clone();
        let adapter = make_adapter(logical.clone(), physical);

        let expr = get_field(col("s"), "a");
        let physical_expr = logical2physical(&expr, logical.as_ref());
        let rewritten = adapter.rewrite(physical_expr.clone()).unwrap();
        let literal = rewritten.as_any().downcast_ref::<Literal>();
        assert!(literal.is_none());
    }

    #[test]
    fn missing_non_nullable_column_falls_back_to_null() {
        let logical = schema_from_fields(vec![Field::new(
            "a",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        )]);
        let physical = schema_from_fields(vec![]);
        let adapter = make_adapter(logical.clone(), physical);

        let expr = logical2physical(&col("a"), logical.as_ref());
        let rewritten = adapter.rewrite(expr).unwrap();
        let literal = rewritten.as_any().downcast_ref::<Literal>().unwrap();
        assert_eq!(literal.value(), &ScalarValue::Null);
    }
}
