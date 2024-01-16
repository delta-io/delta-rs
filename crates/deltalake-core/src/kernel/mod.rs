//! Delta Kernel module
//!
//! The Kernel module contains all the logic for reading and processing the Delta Lake transaction log.

pub mod arrow;
pub mod error;
pub mod expressions;
pub mod models;
mod snapshot;

pub use error::*;
pub use expressions::*;
pub use models::*;
pub use snapshot::*;

/// A trait for all kernel types that are used as part of data checking
pub trait DataCheck {
    /// The name of the specific check
    fn get_name(&self) -> &str;
    /// The SQL expression to use for the check
    fn get_expression(&self) -> &str;
}
