//! Delta Kernel module
//!
//! The Kernel module contains all the logic for reading and processing the Delta Lake transaction log.
use delta_kernel::engine::arrow_expression::ArrowExpressionHandler;

pub mod arrow;
pub mod error;
pub mod models;
pub mod scalars;
mod snapshot;

pub use error::*;
pub use models::*;
pub use snapshot::*;

/// A trait for all kernel types that are used as part of data checking
pub trait DataCheck {
    /// The name of the specific check
    fn get_name(&self) -> &str;
    /// The SQL expression to use for the check
    fn get_expression(&self) -> &str;
}

lazy_static::lazy_static! {
    static ref ARROW_HANDLER: ArrowExpressionHandler = ArrowExpressionHandler {};
}
