use std::any::Any;

pub mod cast;
pub mod partitions;
mod schema;

pub use cast::*;
pub use schema::*;

/// A trait for all kernel types that are used as part of data checking
pub trait DataCheck {
    /// The name of the specific check
    fn get_name(&self) -> &str;
    /// The SQL expression to use for the check
    fn get_expression(&self) -> &str;

    fn as_any(&self) -> &dyn Any;
}
