//! Bridges from the kernel's synchronous calls to the executor's tokio runtime.
//!
//! The kernel calls `execute_op` and pulls result iterators synchronously, often from a thread
//! that is itself a tokio worker. `Handle::block_on` panics there, so this module follows the
//! default engine's `TokioMultiThreadExecutor`: spawn the async work on the runtime, hand the
//! result back over a channel, and wait for it with [`tokio::task::block_in_place`] when the
//! caller is a multi-thread runtime worker.
//!
//! The runtime behind the [`Handle`] must be driven by other threads while the caller waits: a
//! multi-thread runtime, or a current-thread runtime running on its own thread.

use std::future::Future;
use std::sync::mpsc;

use tokio::runtime::{Handle, RuntimeFlavor};

/// Runs `wait`, a call that blocks the current thread until a spawned task delivers a result.
///
/// On a multi-thread runtime worker this uses [`tokio::task::block_in_place`] so the worker's
/// queued tasks can be stolen by other workers. Elsewhere it calls `wait` directly.
pub(crate) fn block_in_place<R>(wait: impl FnOnce() -> R) -> R {
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
    }
}

/// Spawns `future` on `handle` and blocks the current thread until it completes.
pub(crate) fn block_on<F>(handle: &Handle, future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (sender, receiver) = mpsc::channel();
    handle.spawn(async move {
        // A std sender never waits for capacity, so sending from the async task cannot block.
        sender.send(future.await).ok();
    });
    block_in_place(|| {
        receiver
            .recv()
            .expect("runtime dropped the task before it completed")
    })
}
