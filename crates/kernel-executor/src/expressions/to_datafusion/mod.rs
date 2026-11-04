//! Translates kernel expressions, predicates and scalars to DataFusion [`Expr`]s and
//! [`ScalarValue`]s, split by kernel type:
//!
//! - [`expression`]: `to_datafusion_expr` for an [`Expression`], with an optional output type
//!   for the expressions whose result type is not derivable from their inputs (`Struct`,
//!   `Coalesce`, `Array`, `MapToStruct`). `to_df_struct_columns` takes a struct expression apart
//!   into one `Expr` per output column, which the `Project` operator lowering uses.
//! - [`predicate`]: `to_datafusion_predicate_expr` for a [`Predicate`]; the result is a boolean
//!   `Expr`.
//! - [`scalar`]: `to_datafusion_scalar` for a [`Scalar`], including typed nulls and nested
//!   array/struct/map values.
//! - [`utils`]: `column_to_df_expr` resolves a kernel column path against an input schema.
//!
//! [`Expr`]: datafusion::logical_expr::Expr
//! [`ScalarValue`]: datafusion::common::ScalarValue
//! [`Expression`]: delta_kernel::expressions::Expression
//! [`Predicate`]: delta_kernel::Predicate
//! [`Scalar`]: delta_kernel::expressions::Scalar

mod expression;
mod predicate;
mod scalar;
mod utils;

pub use expression::to_datafusion_expr;
pub(crate) use expression::to_df_struct_columns;
pub use predicate::to_datafusion_predicate_expr;
pub use scalar::to_datafusion_scalar;
pub(crate) use utils::column_to_df_expr;
