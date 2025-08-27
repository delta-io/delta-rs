//! Delta Kernel module
//!
//! The Kernel module contains all the logic for reading and processing the Delta Lake transaction log.

use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use std::sync::LazyLock;

pub mod arrow;
pub mod error;
pub mod models;
pub mod scalars;
pub mod schema;
mod snapshot;
pub mod transaction;

pub use arrow::engine_ext::StructDataExt;
pub use delta_kernel::engine;
pub use error::*;
pub use models::*;
pub use schema::*;
pub use snapshot::*;

pub(crate) static ARROW_HANDLER: LazyLock<ArrowEvaluationHandler> =
    LazyLock::new(|| ArrowEvaluationHandler {});
