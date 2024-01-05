//! Kernel module

pub mod actions;
#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod arrow;
#[cfg(feature = "arrow")]
pub mod client;
pub mod error;
pub mod expressions;
pub mod schema;
mod snapshot;

pub use actions::*;
pub use error::*;
pub use expressions::*;
pub use schema::*;
pub use snapshot::*;

/// A trait for all kernel types that are used as part of data checking
pub trait DataCheck {
    /// The name of the specific check
    fn get_name(&self) -> &str;
    /// The SQL expression to use for the check
    fn get_expression(&self) -> &str;
}
