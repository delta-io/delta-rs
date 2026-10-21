//! Resolves a kernel column path against an input schema.

use datafusion::common::{Column, DFSchema, DataFusionError, Result as DFResult};
use datafusion::functions::core::expr_fn::get_field_path;
use datafusion::logical_expr::{Expr, lit};
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error as KernelError};

/// A schema that can resolve the root of a kernel column path to a DataFusion column.
pub(crate) trait ColumnResolver {
    /// Error returned when the column cannot be resolved.
    type Error;

    /// Validates and returns the root DataFusion column.
    fn resolve_column(&self, name: &ColumnName) -> std::result::Result<Column, Self::Error>;
}

/// Lowers a column reference to a nested field access, e.g. `a.b.c` becomes a single
/// `get_field(col("a"), "b", "c")` call.
///
/// Kernel column paths are already exact segments, so the root becomes an unqualified
/// `Column` as is: no SQL identifier normalization (e.g. lowercasing), and a `.` inside a
/// segment is a name character rather than a qualifier separator. This is required for
/// correctness with camelCase and other non-normalized column names (see #4082).
///
/// # Errors
/// Returns an error when the path is empty or the schema's resolver rejects it.
pub(crate) fn column_to_df_expr<E>(
    name: &ColumnName,
    input_schema: &impl ColumnResolver<Error = E>,
) -> std::result::Result<Expr, E> {
    let root = Expr::Column(input_schema.resolve_column(name)?);
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

    fn resolve_column(&self, name: &ColumnName) -> DeltaResult<Column> {
        let Some(root) = name.first() else {
            return Err(KernelError::generic(
                "cannot convert an empty column reference",
            ));
        };
        let _ = self.field_at(name)?;
        Ok(Column::new_unqualified(root.as_str()))
    }
}

impl ColumnResolver for DFSchema {
    type Error = DataFusionError;

    fn resolve_column(&self, name: &ColumnName) -> DFResult<Column> {
        let Some(root) = name.first() else {
            return Err(DataFusionError::Plan(
                "cannot convert an empty column reference".to_string(),
            ));
        };
        let _ = self.field_with_unqualified_name(root)?;
        Ok(Column::new_unqualified(root.as_str()))
    }
}
