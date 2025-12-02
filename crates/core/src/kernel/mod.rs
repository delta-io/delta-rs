//! Delta Kernel module
//!
//! The Kernel module contains all the logic for reading and processing the Delta Lake transaction log.

use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use std::sync::{Arc, LazyLock};
use tokio::task::JoinHandle;
use tracing::Span;
use tracing::dispatcher;

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

pub(crate) static ARROW_HANDLER: LazyLock<Arc<ArrowEvaluationHandler>> =
    LazyLock::new(|| Arc::new(ArrowEvaluationHandler {}));

pub(crate) fn spawn_blocking_with_span<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // Capture the current dispatcher and span
    let dispatch = dispatcher::get_default(|d| d.clone());
    let span = Span::current();

    tokio::task::spawn_blocking(move || {
        dispatcher::with_default(&dispatch, || {
            let _enter = span.enter();
            f()
        })
    })
}
