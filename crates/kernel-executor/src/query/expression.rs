//! Converts kernel expressions and predicates into DataFusion [`Expr`]s.
//!
//! Kernel operators carry [`Expression`]s (projections, aggregate operands) and [`Predicate`]s
//! (filters, join conditions). DataFusion has one `Expr` type for both. The type and null
//! semantics the kernel documents on each variant must hold after conversion.

use datafusion::logical_expr::Expr;
use delta_kernel::expressions::{Expression, Predicate};

use crate::Error;

/// Converts a kernel expression into a DataFusion expression.
pub fn to_df_expr(expr: &Expression) -> crate::Result<Expr> {
    Err(Error::unsupported(format!("expression {expr}")))
}

/// Converts a kernel predicate into a boolean DataFusion expression.
pub fn to_df_predicate(pred: &Predicate) -> crate::Result<Expr> {
    Err(Error::unsupported(format!("predicate {pred}")))
}
