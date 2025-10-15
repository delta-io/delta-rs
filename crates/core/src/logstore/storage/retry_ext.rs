//! Retry extension for [`ObjectStore`]

use ::object_store::path::Path;
use ::object_store::{Error, ObjectStore, PutPayload, PutResult, Result};
use tracing::*;

#[cfg(feature = "cloud")]
use crate::logstore::config;

impl<T: ObjectStore + ?Sized> ObjectStoreRetryExt for T {}

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
    #[instrument(skip(self, bytes), fields(path = %location, size = bytes.content_length()))]
    async fn put_with_retries(
        &self,
        location: &Path,
        bytes: PutPayload,
        max_retries: usize,
    ) -> Result<PutResult> {
        let mut attempt_number = 1;
        while attempt_number <= max_retries {
            match self.put(location, bytes.clone()).await {
                Ok(result) => {
                    debug!(attempt = attempt_number, "put operation succeeded");
                    return Ok(result);
                }
                Err(err) if attempt_number == max_retries => {
                    warn!(attempt = attempt_number, error = %err, "put operation failed after max retries");
                    return Err(err);
                }
                Err(Error::Generic { store, source }) => {
                    debug!("put_with_retries attempt {attempt_number} failed: {store} {source}");
                    attempt_number += 1;
                }
                Err(err) => {
                    error!(attempt = attempt_number, error = %err, "put operation failed with non-retryable error");
                    return Err(err);
                }
            }
        }
        unreachable!("loop yields Ok or Err in body when attempt_number = max_retries")
    }

    /// Delete the object at the specified location
    #[instrument(skip(self), fields(path = %location))]
    async fn delete_with_retries(&self, location: &Path, max_retries: usize) -> Result<()> {
        let mut attempt_number = 1;
        while attempt_number <= max_retries {
            match self.delete(location).await {
                Ok(()) | Err(Error::NotFound { .. }) => {
                    debug!(attempt = attempt_number, "delete operation succeeded");
                    return Ok(());
                }
                Err(err) if attempt_number == max_retries => {
                    warn!(attempt = attempt_number, error = %err, "delete operation failed after max retries");
                    return Err(err);
                }
                Err(Error::Generic { store, source }) => {
                    debug!("delete_with_retries attempt {attempt_number} failed: {store} {source}");
                    attempt_number += 1;
                }
                Err(err) => {
                    error!(attempt = attempt_number, error = %err, "delete operation failed with non-retryable error");
                    return Err(err);
                }
            }
        }
        unreachable!("loop yields Ok or Err in body when attempt_number = max_retries")
    }
}

#[cfg(feature = "cloud")]
impl config::TryUpdateKey for object_store::RetryConfig {
    fn try_update_key(&mut self, key: &str, v: &str) -> crate::DeltaResult<Option<()>> {
        match key {
            "max_retries" => self.max_retries = config::parse_usize(v)?,
            "retry_timeout" => self.retry_timeout = config::parse_duration(v)?,
            "init_backoff" | "backoff_config.init_backoff" | "backoff.init_backoff" => {
                self.backoff.init_backoff = config::parse_duration(v)?
            }
            "max_backoff" | "backoff_config.max_backoff" | "backoff.max_backoff" => {
                self.backoff.max_backoff = config::parse_duration(v)?;
            }
            "base" | "backoff_config.base" | "backoff.base" => {
                self.backoff.base = config::parse_f64(v)?;
            }
            _ => return Ok(None),
        }
        Ok(Some(()))
    }

    fn load_from_environment(&mut self) -> crate::DeltaResult<()> {
        Ok(())
    }
}
