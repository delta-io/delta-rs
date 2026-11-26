//! Bridges from kernel's synchronous calls to the executor's tokio runtime.
//!
//! Kernel calls `execute_op` and pulls result iterators synchronously, often from a thread
//! that is itself a tokio worker. `Handle::block_on` panics there, so this module follows the
//! default engine's `TokioMultiThreadExecutor`: spawn the async work on the runtime, hand the
//! result back over a channel, and wait for it with [`tokio::task::block_in_place`] when the
//! caller is a multi-thread runtime worker.
//!
//! The runtime behind the [`Handle`] must be driven by other threads while the caller waits.
//! A current-thread runtime runs its tasks only on the thread that drives it, and that can be
//! the caller's thread, so [`query_handle`] moves query execution to a background runtime, as
//! the default engine's `TokioBackgroundExecutor` does.

use std::future::Future;
use std::sync::{Mutex, PoisonError, mpsc};

use tokio::runtime::{Handle, Runtime, RuntimeFlavor};

use crate::{ExecutorError, Result};

/// The handle to run queries on for an executor built with `handle`.
///
/// A multi-thread runtime keeps running tasks while one worker waits, so it is used as is. For
/// a current-thread runtime this is a background runtime shared by every executor, which is
/// never dropped, so no executor drops a runtime from inside an async context.
///
/// # Errors
///
/// [`ExecutorError::BackgroundRuntime`] if the background runtime cannot be built. A later call
/// tries again.
pub(crate) fn query_handle(handle: &Handle) -> Result<Handle> {
    // A mutex, not a `OnceLock`: the build can fail, and two first calls must not both build a
    // runtime, as dropping the extra one would block inside an async context.
    static BACKGROUND: Mutex<Option<Runtime>> = Mutex::new(None);
    if handle.runtime_flavor() != RuntimeFlavor::CurrentThread {
        return Ok(handle.clone());
    }
    let mut background = BACKGROUND.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(runtime) = background.as_ref() {
        return Ok(runtime.handle().clone());
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("deltalake-kernel-executor")
        .enable_all()
        .build()
        .map_err(ExecutorError::BackgroundRuntime)?;
    let query_handle = runtime.handle().clone();
    *background = Some(runtime);
    Ok(query_handle)
}

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
