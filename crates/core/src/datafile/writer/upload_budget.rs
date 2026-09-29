//! [`UploadBudget`] limits the memory of closed data files that are still uploading.

use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};

use sysinfo::{MemoryRefreshKind, System};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::*;

use crate::table::config::DEFAULT_TARGET_FILE_SIZE;

/// Budget used when the process memory limit cannot be read.
const DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES: usize = 512 * 1024 * 1024;
const UPLOAD_BUDGET_MEMORY_DIVISOR: u64 = 4;
const UPLOAD_BUDGET_MIN_FILES: u64 = 4;
const UPLOAD_BUDGET_MAX_FILES: u64 = 32;

/// An explicit `DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES`: a positive byte value, or `-1`
/// for unbounded.
fn explicit_upload_budget_bytes(raw: Option<&str>) -> Option<usize> {
    let value = raw?.trim();
    if value == "-1" {
        return Some(usize::MAX);
    }
    value.parse::<usize>().ok().filter(|bytes| *bytes > 0)
}

/// Memory this process should size itself against (read once): the cgroup limit when one is
/// set, otherwise total system memory. `None` when neither can be read.
fn process_memory_limit() -> Option<NonZeroU64> {
    static LIMIT: OnceLock<Option<NonZeroU64>> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        let mut system = System::new();
        system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        let limit = system
            .cgroup_limits()
            .map_or_else(|| system.total_memory(), |limits| limits.total_memory);
        NonZeroU64::new(limit)
    })
}

/// Default budget: a share of the process memory limit, clamped to
/// [`UPLOAD_BUDGET_MIN_FILES`]..[`UPLOAD_BUDGET_MAX_FILES`] times `target_file_size`.
/// Below the floor only one upload fits at a time, so encode stops overlapping
/// upload. Above the ceiling nothing goes faster: how many files keep a store busy
/// depends on its request concurrency, and a four-request pool saturates at 8x while
/// a deeply parallel one saturates at 32x.
///
/// Warns when the floor raises the share, which means `target_file_size` is large
/// for the memory available.
fn default_upload_budget_bytes(
    memory_limit: Option<NonZeroU64>,
    target_file_size: Option<NonZeroU64>,
) -> usize {
    let target = target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE).get();
    let floor = target.saturating_mul(UPLOAD_BUDGET_MIN_FILES);
    let ceiling = target.saturating_mul(UPLOAD_BUDGET_MAX_FILES);
    let share = memory_limit.map_or(DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES as u64, |limit| {
        limit.get() / UPLOAD_BUDGET_MEMORY_DIVISOR
    });
    let bytes = usize::try_from(share.clamp(floor, ceiling)).unwrap_or(usize::MAX);
    if share < floor {
        warn!(
            "target file size is large for the memory available; raising the upload \
             budget to {bytes} bytes so more than one upload can be in flight"
        );
    }
    bytes
}

/// Byte budget for data files whose upload is still in flight.
///
/// When a [`PartitionWriter`] reaches its target file size it closes that file, spawns
/// a background task to upload it, and starts the next file at once. Each upload task
/// holds its file's bytes until the store accepts them. Unbounded, a slow store
/// therefore grows memory by one file every time the writer starts a new one.
///
/// The writer reserves the bytes before it spawns the upload task, and releases them
/// when that task ends. Once the budget is spent, `write` waits rather than start
/// another file, and that wait reaches the data source through the bounded batch
/// channels.
///
/// A budget belongs to one write. [`WriterConfig::new`] creates one; every clone of
/// that config, and every partition writer built from it, shares it.
///
/// [`PartitionWriter`]: super::PartitionWriter
/// [`WriterConfig::new`]: super::WriterConfig::new
#[derive(Debug, Clone)]
pub(crate) struct UploadBudget {
    pub(super) bytes: usize,
    pub(super) semaphore: Arc<Semaphore>,
}

impl UploadBudget {
    /// A budget of `bytes`, clamped to `1..=Semaphore::MAX_PERMITS`.
    pub(crate) fn new(bytes: usize) -> Self {
        let bytes = bytes.clamp(1, Semaphore::MAX_PERMITS);
        Self {
            bytes,
            semaphore: Arc::new(Semaphore::new(bytes)),
        }
    }

    /// The budget for one write.
    ///
    /// `DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES` wins when set. Unlike the other writer
    /// knobs it is not cached, so every caller picks up the current value. That is
    /// once per `write_deltalake` call, which builds one config and clones it, but once
    /// per flush window for `RecordBatchWriter` and `JsonWriter`, which rebuild their
    /// sink after each flush.
    ///
    /// Otherwise see [`default_upload_budget_bytes`].
    pub(crate) fn for_write(target_file_size: Option<NonZeroU64>) -> Self {
        let raw = std::env::var("DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES").ok();
        let bytes = explicit_upload_budget_bytes(raw.as_deref()).unwrap_or_else(|| {
            default_upload_budget_bytes(process_memory_limit(), target_file_size)
        });
        Self::new(bytes)
    }

    /// Bytes not currently reserved by an in-flight upload.
    pub(crate) fn available_bytes(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Reserve `bytes` for one upload, waiting until that much budget is free.
    /// Dropping the returned permit releases it.
    ///
    /// A file larger than the whole budget takes all of it and uploads alone. The
    /// semaphore caps one reservation at `u32::MAX` bytes, so a file beyond that can
    /// exceed the budget.
    pub(super) async fn reserve(&self, bytes: usize) -> OwnedSemaphorePermit {
        let permits = bytes.min(self.bytes.min(u32::MAX as usize)) as u32;
        let free = self.available_bytes();
        if free < permits as usize {
            debug!(
                "waiting for {permits} bytes of upload budget ({free} of {} free)",
                self.bytes
            );
        }
        Arc::clone(&self.semaphore)
            .acquire_many_owned(permits)
            // The semaphore is private to the budget and never closed.
            .await
            .expect("upload budget semaphore is never closed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::unbounded(Some("-1"), Some(Semaphore::MAX_PERMITS))]
    #[case::unbounded_padded(Some(" -1 "), Some(Semaphore::MAX_PERMITS))]
    #[case::explicit(Some("65536"), Some(65536))]
    // Unusable values fall through to the memory-derived default.
    #[case::zero(Some("0"), None)]
    #[case::other_negative(Some("-2"), None)]
    #[case::unparsable(Some("plenty"), None)]
    #[case::missing(None, None)]
    fn upload_budget_size_from_env_value(
        #[case] raw: Option<&str>,
        #[case] expected: Option<usize>,
    ) {
        let bytes = explicit_upload_budget_bytes(raw);
        assert_eq!(bytes.map(|b| UploadBudget::new(b).bytes), expected);
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    /// The default `target_file_size`, which the cases below multiply.
    const TARGET: usize = DEFAULT_TARGET_FILE_SIZE.get() as usize;

    #[rstest]
    // A quarter of the limit, when that sits between the floor and the ceiling.
    #[case::small_container(NonZeroU64::new(2 * GIB), None, 512 * 1024 * 1024)]
    #[case::roomy(NonZeroU64::new(8 * GIB), None, 2 * GIB as usize)]
    // The ceiling caps a big machine at 32 files; more does not go faster.
    #[case::large_machine(NonZeroU64::new(128 * GIB), None, 32 * TARGET)]
    // The floor takes over when a quarter would admit fewer than four files.
    #[case::large_files(NonZeroU64::new(4 * GIB), NonZeroU64::new(GIB), 4 * GIB as usize)]
    #[case::tiny_container(NonZeroU64::new(256 * 1024 * 1024), None, 4 * TARGET)]
    // No readable limit falls back to the fixed default.
    #[case::no_limit(None, None, DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES)]
    fn default_upload_budget_is_a_clamped_share_of_memory(
        #[case] limit: Option<NonZeroU64>,
        #[case] target_file_size: Option<NonZeroU64>,
        #[case] expected_bytes: usize,
    ) {
        assert_eq!(
            default_upload_budget_bytes(limit, target_file_size),
            expected_bytes
        );
    }

    #[test]
    fn upload_budget_clamps_its_size() {
        assert_eq!(UploadBudget::new(0).bytes, 1);
    }
}
