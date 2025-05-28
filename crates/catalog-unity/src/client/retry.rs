//! A shared HTTP client implementation incorporating retries

use super::backoff::BackoffConfig;
use deltalake_core::DataCatalogError;
use reqwest::StatusCode;
use reqwest_retry::policies::ExponentialBackoff;
use std::time::Duration;

/// Retry request error
#[derive(Debug)]
pub struct RetryError {
    retries: usize,
    message: String,
    source: Option<reqwest::Error>,
}

impl std::fmt::Display for RetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "response error \"{}\", after {} retries",
            self.message, self.retries
        )?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RetryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e as _)
    }
}

impl RetryError {
    /// Returns the status code associated with this error if any
    pub fn status(&self) -> Option<StatusCode> {
        self.source.as_ref().and_then(|e| e.status())
    }
}

impl From<RetryError> for std::io::Error {
    fn from(err: RetryError) -> Self {
        use std::io::ErrorKind;
        match (&err.source, err.status()) {
            (Some(source), _) if source.is_builder() || source.is_request() => {
                Self::new(ErrorKind::InvalidInput, err)
            }
            (_, Some(StatusCode::NOT_FOUND)) => Self::new(ErrorKind::NotFound, err),
            (_, Some(StatusCode::BAD_REQUEST)) => Self::new(ErrorKind::InvalidInput, err),
            (Some(source), None) if source.is_timeout() => Self::new(ErrorKind::TimedOut, err),
            (Some(source), None) if source.is_connect() => Self::new(ErrorKind::NotConnected, err),
            _ => Self::other(err),
        }
    }
}

impl From<RetryError> for DataCatalogError {
    fn from(value: RetryError) -> Self {
        DataCatalogError::Generic {
            catalog: "",
            source: Box::new(value),
        }
    }
}

/// Error retrying http requests
pub type Result<T, E = RetryError> = std::result::Result<T, E>;

/// Contains the configuration for how to respond to server errors
///
/// By default, they will be retried up to some limit, using exponential
/// backoff with jitter. See [`BackoffConfig`] for more information
///
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// The backoff configuration
    pub backoff: BackoffConfig,

    /// The maximum number of times to retry a request
    ///
    /// Set to 0 to disable retries
    pub max_retries: usize,

    /// The maximum length of time from the initial request
    /// after which no further retries will be attempted
    ///
    /// This not only bounds the length of time before a server
    /// error will be surfaced to the application, but also bounds
    /// the length of time a request's credentials must remain valid.
    ///
    /// As requests are retried without renewing credentials or
    /// regenerating request payloads, this number should be kept
    /// below 5 minutes to avoid errors due to expired credentials
    /// and/or request payloads
    pub retry_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            backoff: Default::default(),
            max_retries: 10,
            retry_timeout: Duration::from_secs(3 * 60),
        }
    }
}

impl From<&RetryConfig> for ExponentialBackoff {
    fn from(val: &RetryConfig) -> ExponentialBackoff {
        ExponentialBackoff::builder()
            .retry_bounds(val.backoff.init_backoff, val.backoff.max_backoff)
            .base(val.backoff.base as u32)
            .build_with_max_retries(val.max_retries as u32)
    }
}
