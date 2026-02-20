use std::any::Any;
use std::fmt;
use std::fmt::Display;
use std::hash::Hash;
use arrow_cast::{can_cast_types, CastOptions};
use arrow_schema::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::common::{arrow_datafusion_err, cast_column, Result};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{ScalarValue, exec_err};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr, expressions};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::DataType::Struct;
use datafusion::common::deep::can_cast_datatype_deep;
use datafusion::common::error::_plan_err;
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::physical_plan::ColumnarValue;
use crate::cast_field;

// by default, this is enabled.
// set DELTA_USE_EXPR_ADAPTER=0 to disable it
pub fn build_expr_adapter_factory() -> Option<Arc<dyn PhysicalExprAdapterFactory>> {
    let flag = if let Ok(v) = std::env::var("DELTA_USE_EXPR_ADAPTER")
        && let Ok(opt) = v.parse::<usize>()
    {
        opt
    } else {
        1
    };

    match flag {
        0 => Some(Arc::new(DefaultPhysicalExprAdapterFactory) as Arc<dyn PhysicalExprAdapterFactory>),
        _ => Some(Arc::new(DeltaPhysicalExprAdapterFactory) as Arc<dyn PhysicalExprAdapterFactory>),
    }
}

#[derive(Debug, Clone)]
pub struct DeltaPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for DeltaPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(DeltaPhysicalExprAdapter::new(
            logical_file_schema,
            physical_file_schema,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct DeltaPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl DeltaPhysicalExprAdapter {
    /// Create a new instance of the default physical expression adapter.
    ///
    /// This adapter rewrites expressions to match the physical schema of the file being scanned,
    /// handling type mismatches and missing columns by filling them with default values.
    pub fn new(logical_file_schema: SchemaRef, physical_file_schema: SchemaRef) -> Self {
        Self {
            logical_file_schema,
            physical_file_schema,
        }
    }
}

impl PhysicalExprAdapter for DeltaPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = DefaultPhysicalExprAdapterRewriter {
            logical_file_schema: &self.logical_file_schema,
            physical_file_schema: &self.physical_file_schema,
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }
}

// Copied from datafusion - private for now
struct DefaultPhysicalExprAdapterRewriter<'a> {
    logical_file_schema: &'a Schema,
    physical_file_schema: &'a Schema,
}

impl<'a> DefaultPhysicalExprAdapterRewriter<'a> {
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

    /// Attempt to rewrite struct field access expressions to return null if the field does not exist in the physical schema.
    /// Note that this does *not* handle nested struct fields, only top-level struct field access.
    /// See <https://github.com/apache/datafusion/issues/17114> for more details.
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

        let lit = match field_name_expr
            .as_any()
            .downcast_ref::<expressions::Literal>()
        {
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
        Ok(Some(expressions::lit(null_value)))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Get the logical field for this column if it exists in the logical schema
        let logical_field = match self.logical_file_schema.field_with_name(column.name()) {
            Ok(field) => field,
            Err(e) => {
                // This can be hit if a custom rewrite injected a reference to a column that doesn't exist in the logical schema.
                // For example, a pre-computed column that is kept only in the physical schema.
                // If the column exists in the physical schema, we can still use it.
                if let Ok(physical_field) = self.physical_file_schema.field_with_name(column.name())
                {
                    // If the column exists in the physical schema, we can use it in place of the logical column.
                    // This is nice to users because if they do a rewrite that results in something like `physical_int32_col = 123u64`
                    // we'll at least handle the casts for them.
                    physical_field
                } else {
                    // A completely unknown column that doesn't exist in either schema!
                    // This should probably never be hit unless something upstream broke, but nonetheless it's better
                    // for us to return a handleable error than to panic / do something unexpected.
                    return Err(e.into());
                }
            }
        };

        // Check if the column exists in the physical schema
        let physical_column_index = match self.physical_file_schema.index_of(column.name()) {
            Ok(index) => index,
            Err(_) => {
                if !logical_field.is_nullable() {
                    return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        column.name()
                    );
                }
                // If the column is missing from the physical schema fill it in with nulls.
                // For a different behavior, provide a custom `PhysicalExprAdapter` implementation.
                let null_value = ScalarValue::Null.cast_to(logical_field.data_type())?;
                return Ok(Transformed::yes(expressions::lit(null_value)));
            }
        };
        let physical_field = self.physical_file_schema.field(physical_column_index);

        let column = match (
            column.index() == physical_column_index,
            logical_field.data_type() == physical_field.data_type(),
        ) {
            // If the column index matches and the data types match, we can use the column as is
            (true, true) => return Ok(Transformed::no(expr)),
            // If the indexes or data types do not match, we need to create a new column expression
            (true, _) => column.clone(),
            (false, _) => Column::new_with_schema(logical_field.name(), self.physical_file_schema)?,
        };

        if logical_field.data_type() == physical_field.data_type() {
            // If the data types match, we can use the column as is
            return Ok(Transformed::yes(Arc::new(column)));
        }

        // We need to cast the column to the logical data type
        // TODO: add optimization to move the cast from the column to literal expressions in the case of `col = 123`
        // since that's much cheaper to evalaute.
        // See https://github.com/apache/datafusion/issues/15780#issuecomment-2824716928
        //
        // For struct types, use validate_struct_compatibility which handles:
        // - Missing fields in source (filled with nulls)
        // - Extra fields in source (ignored)
        // - Recursive validation of nested structs
        // For non-struct types, use Arrow's can_cast_types
        if !can_cast_datatype_deep(physical_field.data_type(), logical_field.data_type(), true) {
            return exec_err!(
                        "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
                        column.name(),
                        physical_field.data_type(),
                        logical_field.data_type()
                    );
        }
        // match (physical_field.data_type(), logical_field.data_type()) {
        //     (DataType::Struct(physical_fields), DataType::Struct(logical_fields)) => {
        //         validate_struct_compatibility(physical_fields, logical_fields)?;
        //     }
        //     _ => {
        //         let is_compatible =
        //             can_cast_types(physical_field.data_type(), logical_field.data_type());
        //         let is_compatible_deep = can_cast_datatype_deep(physical_field.data_type(), logical_field.data_type(), true);
        //         if !(is_compatible || is_compatible_deep) {
        //             return exec_err!(
        //                 "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
        //                 column.name(),
        //                 physical_field.data_type(),
        //                 logical_field.data_type()
        //             );
        //         }
        //     }
        // }

        let cast_expr = Arc::new(CastDeltaColumnExpr::new_with_deep_cast(
            Arc::new(column),
            Arc::new(physical_field.clone()),
            Arc::new(logical_field.clone()),
            Some(CastOptions {
                safe: true,
                format_options: Default::default(),
            }),
            true
        ));

        Ok(Transformed::yes(cast_expr))
    }
}

pub fn validate_struct_compatibility(
    source_fields: &[FieldRef],
    target_fields: &[FieldRef],
) -> Result<()> {
    // Check compatibility for each target field
    for target_field in target_fields {
        // Look for matching field in source by name
        if let Some(source_field) = source_fields
            .iter()
            .find(|f| f.name() == target_field.name())
        {
            // Ensure nullability is compatible. It is invalid to cast a nullable
            // source field to a non-nullable target field as this may discard
            // null values.
            // @HStack @DeltaIntegration
            // ATM, we have delta tables where we have fields that are marked nullable in the parquet files, but NON-nullable in the Delta schema
            // this makes this test fail.
            // So, we ignore it and we leave the casting. IF there are actual null rows in the data, we'll get an error `Found unmasked nulls for non-nullable`
            // if source_field.is_nullable() && !target_field.is_nullable() {
            //     return _plan_err!(
            //         "Cannot cast nullable struct field '{}' to non-nullable field",
            //         target_field.name()
            //     );
            // }
            // Check if the matching field types are compatible
            match (source_field.data_type(), target_field.data_type()) {
                // Recursively validate nested structs
                (Struct(source_nested), Struct(target_nested)) => {
                    validate_struct_compatibility(source_nested, target_nested)?;
                }
                // For non-struct types, use the existing castability check
                _ => {
                    if !can_cast_datatype_deep(
                        source_field.data_type(),
                        target_field.data_type(),
                        true
                    ) {
                        return _plan_err!(
                            "Cannot cast struct field '{}' from type {} to type {}",
                            target_field.name(),
                            source_field.data_type(),
                            target_field.data_type()
                        );
                    }
                }
            }
        }
        // Missing fields in source are OK - they'll be filled with nulls
    }

    // Extra fields in source are OK - they'll be ignored
    Ok(())
}

#[derive(Debug, Clone, Eq)]
pub struct CastDeltaColumnExpr {
    /// The physical expression producing the value to cast.
    expr: Arc<dyn PhysicalExpr>,
    /// The logical field of the input column.
    input_field: FieldRef,
    /// The field metadata describing the desired output column.
    target_field: FieldRef,
    /// Options forwarded to [`cast_column`].
    cast_options: CastOptions<'static>,
    /// Use deep cast options
    use_deep_cast: bool
}

// Manually derive `PartialEq`/`Hash` as `Arc<dyn PhysicalExpr>` does not
// implement these traits by default for the trait object.
impl PartialEq for CastDeltaColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_field.eq(&other.input_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
            && self.use_deep_cast.eq(&other.use_deep_cast)
    }
}

impl Hash for CastDeltaColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
        self.use_deep_cast.hash(state);
    }
}

impl CastDeltaColumnExpr {
    /// Create a new [`CastColumnExpr`].
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        input_field: FieldRef,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            input_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
            use_deep_cast: false,
        }
    }

    pub fn new_with_deep_cast(
        expr: Arc<dyn PhysicalExpr>,
        input_field: FieldRef,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
        use_deep_cast: bool,
    ) -> Self {
        Self {
            expr,
            input_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
            use_deep_cast,
        }
    }

    /// The expression that produces the value to be cast.
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Field metadata describing the resolved input column.
    pub fn input_field(&self) -> &FieldRef {
        &self.input_field
    }

    /// Field metadata describing the output column after casting.
    pub fn target_field(&self) -> &FieldRef {
        &self.target_field
    }
}

impl Display for CastDeltaColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CAST_COLUMN({} AS {:?})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for CastDeltaColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => {
                if self.use_deep_cast {
                    let casted =
                        cast_field(&array, self.target_field(), &self.cast_options, true)
                            .map_err(|e|arrow_datafusion_err!(e))?;
                    return Ok(ColumnarValue::Array(casted));
                }
                let casted =
                    cast_column(&array, self.target_field.as_ref(), &self.cast_options)?;
                Ok(ColumnarValue::Array(casted))
            }
            ColumnarValue::Scalar(scalar) => {
                let as_array = scalar.to_array_of_size(1)?;
                let casted = cast_column(
                    &as_array,
                    self.target_field.as_ref(),
                    &self.cast_options,
                )?;
                let result = ScalarValue::try_from_array(casted.as_ref(), 0)?;
                Ok(ColumnarValue::Scalar(result))
            }
        }
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().expect("CastDeltaColumnExpr child");
        Ok(Arc::new(Self::new_with_deep_cast(
            child,
            Arc::clone(&self.input_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
            self.use_deep_cast,
        )))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}