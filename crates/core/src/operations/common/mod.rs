//! Code that more than one operation uses

pub(crate) mod assignment;

use arrow_schema::DataType;
use datafusion::logical_expr::{Expr, cast, try_cast};

/// Casts `expr` to `data_type`. With `safe_cast`, a value that the cast cannot convert becomes
/// NULL. Without it, such a value is an error.
pub(crate) fn cast_expr(expr: Expr, data_type: DataType, safe_cast: bool) -> Expr {
    if safe_cast {
        try_cast(expr, data_type)
    } else {
        cast(expr, data_type)
    }
}
