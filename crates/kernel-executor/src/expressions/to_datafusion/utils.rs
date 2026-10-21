//! Shared helpers for lowering kernel plans to DataFusion.

use datafusion::common::{Column as DFColumn, DFSchema, DataFusionError, Result as DFResult};
use datafusion::functions::core::expr_fn::get_field_path;
use datafusion::logical_expr::{lit, Expr as DFExpr};
use delta_kernel::expressions::ColumnName as KernelColumnName;
use delta_kernel::schema::StructType;
use delta_kernel::{KernelError, KernelResult};

/// A schema that can resolve the root of a kernel column path to a DataFusion column.
pub(crate) trait ColumnResolver {
    /// Error returned when the column cannot be resolved.
    type Error;

    /// Validates and returns the root DataFusion column.
    fn resolve_column(&self, name: &KernelColumnName)
        -> std::result::Result<DFColumn, Self::Error>;
}

/// Lowers a column reference to a nested field access, e.g. `a.b.c` becomes a single
/// `get_field(col("a"), "b", "c")` call.
///
/// # Errors
/// Returns an error when the path is empty or the schema's resolver rejects it.
pub(crate) fn column_to_df_expr<E>(
    name: &KernelColumnName,
    input_schema: &impl ColumnResolver<Error = E>,
) -> std::result::Result<DFExpr, E> {
    let root = DFExpr::Column(input_schema.resolve_column(name)?);
    let field_names = Vec::from_iter(name.iter().skip(1).map(lit));
    // A bare column stays a bare column; only nested access wraps it in a `get_field` call.
    if field_names.is_empty() {
        Ok(root)
    } else {
        Ok(get_field_path(root, field_names))
    }
}

impl ColumnResolver for StructType {
    type Error = KernelError;

    fn resolve_column(&self, name: &KernelColumnName) -> KernelResult<DFColumn> {
        let Some(root) = name.first() else {
            return Err(KernelError::generic(
                "cannot convert an empty column reference",
            ));
        };
        let _ = self.field_at(name)?;
        Ok(DFColumn::new_unqualified(root.as_str()))
    }
}

impl ColumnResolver for DFSchema {
    type Error = DataFusionError;

    fn resolve_column(&self, name: &KernelColumnName) -> DFResult<DFColumn> {
        let Some(root) = name.first() else {
            return Err(DataFusionError::Plan(
                "cannot convert an empty column reference".to_string(),
            ));
        };
        let _ = self.field_with_unqualified_name(root)?;
        Ok(DFColumn::new_unqualified(root.as_str()))
    }
}
