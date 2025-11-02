//! Generic utilities reqwest based Catalog implementations

pub mod backoff;
#[allow(unused)]
pub mod pagination;
pub mod retry;
pub mod token;

use crate::client::retry::RetryConfig;
use crate::UnityCatalogError;
use deltalake_core::data_catalog::DataCatalogResult;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{ClientBuilder, Proxy};
use reqwest_middleware::ClientWithMiddleware;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::time::Duration;
use typed_builder::TypedBuilder;

fn map_client_error(e: reqwest::Error) -> super::DataCatalogError {
    super::DataCatalogError::Generic {
        catalog: "HTTP client",
        source: Box::new(e),
    }
}

static DEFAULT_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// HTTP client configuration for remote catalogs
#[derive(Debug, Clone, Default, TypedBuilder)]
#[builder(doc)]
pub struct ClientOptions {
    /// User-Agent header to use for requests
    #[builder(default, setter(strip_option))]
    user_agent: Option<HeaderValue>,
    /// Default headers for every request
    #[builder(default, setter(strip_option))]
    default_headers: Option<HeaderMap>,
    /// HTTP proxy URL
    #[builder(default, setter(strip_option, into))]
    proxy_url: Option<String>,
    /// Allow HTTP connections (default: false)
    #[builder(default)]
    pub(crate) allow_http: bool,
    /// Allow invalid SSL certificates (default: false)
    #[builder(default)]
    allow_insecure: bool,
    /// Request timeout
    #[builder(default, setter(strip_option))]
    timeout: Option<Duration>,
    /// Connect timeout
    #[builder(default, setter(strip_option))]
    connect_timeout: Option<Duration>,
    /// Pool idle timeout
    #[builder(default, setter(strip_option))]
    pool_idle_timeout: Option<Duration>,
    /// Maximum number of idle connections per host
    #[builder(default, setter(strip_option))]
    pool_max_idle_per_host: Option<usize>,
    /// HTTP2 keep alive interval
    #[builder(default, setter(strip_option))]
    http2_keep_alive_interval: Option<Duration>,
    /// HTTP2 keep alive timeout
    #[builder(default, setter(strip_option))]
    http2_keep_alive_timeout: Option<Duration>,
    /// Enable HTTP2 keep alive while idle
    #[builder(default)]
    http2_keep_alive_while_idle: bool,
    /// Only use HTTP1
    #[builder(default)]
    http1_only: bool,
    /// Only use HTTP2
    #[builder(default)]
    http2_only: bool,
    /// Retry configuration
    #[builder(default, setter(strip_option))]
    retry_config: Option<RetryConfig>,
}

impl ClientOptions {
    pub(crate) fn client(&self) -> DataCatalogResult<ClientWithMiddleware> {
        let mut builder = ClientBuilder::new();

        match &self.user_agent {
            Some(user_agent) => builder = builder.user_agent(user_agent),
            None => builder = builder.user_agent(DEFAULT_USER_AGENT),
        }

        if let Some(headers) = &self.default_headers {
            builder = builder.default_headers(headers.clone())
        }

        if let Some(proxy) = &self.proxy_url {
            let proxy = Proxy::all(proxy).map_err(map_client_error)?;
            builder = builder.proxy(proxy);
        }

        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout)
        }

        if let Some(timeout) = self.connect_timeout {
            builder = builder.connect_timeout(timeout)
        }

        if let Some(timeout) = self.pool_idle_timeout {
            builder = builder.pool_idle_timeout(timeout)
        }

        if let Some(max) = self.pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(max)
        }

        if let Some(interval) = self.http2_keep_alive_interval {
            builder = builder.http2_keep_alive_interval(interval)
        }

        if let Some(interval) = self.http2_keep_alive_timeout {
            builder = builder.http2_keep_alive_timeout(interval)
        }

        if self.http2_keep_alive_while_idle {
            builder = builder.http2_keep_alive_while_idle(true)
        }

        if self.http1_only {
            builder = builder.http1_only()
        }

        if self.http2_only {
            builder = builder.http2_prior_knowledge()
        }

        if self.allow_insecure {
            builder = builder.danger_accept_invalid_certs(self.allow_insecure)
        }

        let inner_client = builder
            .https_only(!self.allow_http)
            .build()
            .map_err(UnityCatalogError::from)?;
        let retry_policy = self
            .retry_config
            .as_ref()
            .map(|retry| retry.into())
            .unwrap_or(ExponentialBackoff::builder().build_with_max_retries(3));

        let middleware = RetryTransientMiddleware::new_with_policy(retry_policy);
        Ok(reqwest_middleware::ClientBuilder::new(inner_client)
            .with(middleware)
            .build())
    }
}
