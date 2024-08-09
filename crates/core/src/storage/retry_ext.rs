//! Retry extension for [`ObjectStore`]

use object_store::{path::Path, Error, ObjectStore, PutPayload, PutResult, Result};
use tracing::log::*;

/// Retry extension for [`ObjectStore`]
///
/// Read-only operations are retried by [`ObjectStore`] internally. However, PUT/DELETE operations
/// are not retried even thought they are technically idempotent. [`ObjectStore`] does not retry
/// those operations because having preconditions may produce different results for the same
/// request. PUT/DELETE operations without preconditions are idempotent and can be retried.
/// Unfortunately, [`ObjectStore`]'s retry mechanism only works on HTTP request level, thus there
/// is no way to distinguish whether a request has preconditions or not.
///
/// This trait provides additional methods for working with [`ObjectStore`] that automatically retry
/// unconditional operations when they fail.
///
/// See also:
/// - https://github.com/apache/arrow-rs/pull/5278
#[async_trait::async_trait]
pub trait ObjectStoreRetryExt: ObjectStore {
    /// Save the provided bytes to the specified location
    ///
    /// The operation is guaranteed to be atomic, it will either successfully write the entirety of
    /// bytes to location, or fail. No clients should be able to observe a partially written object
    ///
    /// Note that `put_with_opts` may have precondition semantics, and thus may not be retriable.
    async fn put_with_retries(
        &self,
        location: &Path,
        bytes: PutPayload,
        max_retries: usize,
    ) -> Result<PutResult> {
        let mut attempt_number = 1;
        while attempt_number <= max_retries {
            match self.put(location, bytes.clone()).await {
                Ok(result) => return Ok(result),
                Err(err) if attempt_number == max_retries => {
                    return Err(err);
                }
                Err(Error::Generic { store, source }) => {
                    debug!(
                        "put_with_retries attempt {} failed: {} {}",
                        attempt_number, store, source
                    );
                    attempt_number += 1;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        unreachable!("loop yields Ok or Err in body when attempt_number = max_retries")
    }

    /// Delete the object at the specified location
    async fn delete_with_retries(&self, location: &Path, max_retries: usize) -> Result<()> {
        let mut attempt_number = 1;
        while attempt_number <= max_retries {
            match self.delete(location).await {
                Ok(()) | Err(Error::NotFound { .. }) => return Ok(()),
                Err(err) if attempt_number == max_retries => {
                    return Err(err);
                }
                Err(Error::Generic { store, source }) => {
                    debug!(
                        "delete_with_retries attempt {} failed: {} {}",
                        attempt_number, store, source
                    );
                    attempt_number += 1;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        unreachable!("loop yields Ok or Err in body when attempt_number = max_retries")
    }
}

impl<T: ObjectStore + ?Sized> ObjectStoreRetryExt for T {}
