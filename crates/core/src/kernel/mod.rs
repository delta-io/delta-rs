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
/// Core Delta log action models (Add, Remove, Metadata, Protocol, ...) and related types.
pub mod models;
pub mod scalars;
/// Delta and Arrow schema types, conversions, casting and partition handling.
pub mod schema;
pub(crate) mod snapshot;
pub mod transaction;

pub use arrow::engine_ext::StructDataExt;
pub use delta_kernel::Version;
pub use delta_kernel::engine;
pub use error::*;
pub use models::*;
pub use schema::*;
pub use snapshot::*;

pub(crate) static ARROW_HANDLER: LazyLock<Arc<ArrowEvaluationHandler>> =
    LazyLock::new(|| Arc::new(ArrowEvaluationHandler {}));

/// Runs a blocking closure on Tokio's blocking pool while keeping the caller's
/// `tracing` context.
///
/// `tokio::task::spawn_blocking` runs the closure on a different OS thread. That
/// thread does not inherit the caller's scoped [`tracing::Dispatch`] or current
/// [`tracing::Span`], so events emitted inside the closure are routed to the global
/// dispatcher and spans lose their parent. This helper captures both at the call
/// site and re-installs them on the blocking thread for the duration of `f`.
///
/// Use it for blocking work called from an async path that should appear under the
/// caller's span, such as log segment listing or snapshot construction (see the
/// history of #3894, which introduced it and moved those call sites over). Use
/// `tokio::task::spawn_blocking` directly when the work emits no tracing or should not
/// be attributed to the caller.
///
/// This is not for futures; use `tokio::spawn` for async work. It changes only the
/// tracing context: scheduling, cancellation, and panic propagation match
/// `spawn_blocking`, and the returned [`JoinHandle`] behaves the same way.
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
