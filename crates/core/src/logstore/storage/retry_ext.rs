//! Retry extension for [`ObjectStore`]

use ::object_store::path::Path;
use ::object_store::{Error, ObjectStore, PutPayload, PutResult, Result};
use humantime::parse_duration;
use tracing::log::*;

use super::StorageOptions;
use crate::{DeltaResult, DeltaTableError};

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
                    debug!("put_with_retries attempt {attempt_number} failed: {store} {source}");
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
                    debug!("delete_with_retries attempt {attempt_number} failed: {store} {source}");
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

#[cfg(feature = "cloud")]
pub trait RetryConfigParse {
    fn parse_retry_config(
        &self,
        options: &StorageOptions,
    ) -> DeltaResult<::object_store::RetryConfig> {
        let mut retry_config = ::object_store::RetryConfig::default();
        if let Some(max_retries) = options.0.get("max_retries") {
            retry_config.max_retries = max_retries
                .parse::<usize>()
                .map_err(|e| DeltaTableError::generic(e.to_string()))?;
        }

        if let Some(retry_timeout) = options.0.get("retry_timeout") {
            retry_config.retry_timeout = parse_duration(retry_timeout).map_err(|_| {
                DeltaTableError::generic(format!("failed to parse \"{retry_timeout}\" as Duration"))
            })?;
        }

        if let Some(bc_init_backoff) = options.0.get("backoff_config.init_backoff") {
            retry_config.backoff.init_backoff = parse_duration(bc_init_backoff).map_err(|_| {
                DeltaTableError::generic(format!(
                    "failed to parse \"{bc_init_backoff}\" as Duration"
                ))
            })?;
        }

        if let Some(bc_max_backoff) = options.0.get("backoff_config.max_backoff") {
            retry_config.backoff.max_backoff = parse_duration(bc_max_backoff).map_err(|_| {
                DeltaTableError::generic(format!(
                    "failed to parse \"{bc_max_backoff}\" as Duration"
                ))
            })?;
        }

        if let Some(bc_base) = options.0.get("backoff_config.base") {
            retry_config.backoff.base = bc_base
                .parse::<f64>()
                .map_err(|e| DeltaTableError::generic(e.to_string()))?;
        }

        Ok(retry_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_retry_config_from_options() {
        struct TestFactory {}
        impl RetryConfigParse for TestFactory {}

        let options = maplit::hashmap! {
            "max_retries".to_string() => "100".to_string() ,
            "retry_timeout".to_string()  => "300s".to_string() ,
            "backoff_config.init_backoff".to_string()  => "20s".to_string() ,
            "backoff_config.max_backoff".to_string()  => "1h".to_string() ,
            "backoff_config.base".to_string()  =>  "50.0".to_string() ,
        };
        let retry_config = TestFactory {}
            .parse_retry_config(&StorageOptions(options))
            .unwrap();

        assert_eq!(retry_config.max_retries, 100);
        assert_eq!(retry_config.retry_timeout, Duration::from_secs(300));
        assert_eq!(retry_config.backoff.init_backoff, Duration::from_secs(20));
        assert_eq!(retry_config.backoff.max_backoff, Duration::from_secs(3600));
        assert_eq!(retry_config.backoff.base, 50_f64);
    }
}
