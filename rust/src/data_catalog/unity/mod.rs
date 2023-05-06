//! Databricks Unity Catalog.
//!
//! This module is gated behind the "unity-experimental" feature.
use super::{DataCatalog, DataCatalogError};
use reqwest::header;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Deserialize;

/// Databricks Unity Catalog - implementation of the `DataCatalog` trait
pub struct UnityCatalog {
    client_with_retry: ClientWithMiddleware,
    workspace_url: String,
}

impl UnityCatalog {
    /// Creates a new UnityCatalog
    pub fn new() -> Result<Self, DataCatalogError> {
        let token_var = "DATABRICKS_ACCESS_TOKEN";
        let access_token =
            std::env::var(token_var).map_err(|_| DataCatalogError::MissingEnvVar {
                var_name: token_var.into(),
            })?;

        let auth_header_val = header::HeaderValue::from_str(&format!("Bearer {}", &access_token))
            .map_err(|_| DataCatalogError::InvalidAccessToken)?;

        let headers = header::HeaderMap::from_iter([(header::AUTHORIZATION, auth_header_val)]);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(10);

        let client_with_retry = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let workspace_var = "DATABRICKS_WORKSPACE_URL";
        let workspace_url =
            std::env::var(workspace_var).map_err(|_| DataCatalogError::MissingEnvVar {
                var_name: workspace_var.into(),
            })?;

        Ok(Self {
            client_with_retry,
            workspace_url,
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TableResponse {
    Success { storage_location: String },
    Error { error_code: String, message: String },
}

/// Possible errors from the unity-catalog/tables API call
#[derive(thiserror::Error, Debug)]
pub enum GetTableError {
    #[error("GET request error: {source}")]
    /// Error from reqwest library
    RequestError {
        /// The underlying reqwest_middleware::Error
        #[from]
        source: reqwest_middleware::Error,
    },

    /// Request returned error response
    #[error("Invalid table error: {error_code}: {message}")]
    InvalidTable {
        /// Error code
        error_code: String,
        /// Error description
        message: String,
    },
}

impl From<reqwest_middleware::Error> for DataCatalogError {
    fn from(value: reqwest_middleware::Error) -> Self {
        value.into()
    }
}

impl From<reqwest::Error> for DataCatalogError {
    fn from(value: reqwest::Error) -> Self {
        reqwest_middleware::Error::Reqwest(value).into()
    }
}

#[async_trait::async_trait]
impl DataCatalog for UnityCatalog {
    /// Get the table storage location from the UnityCatalog
    async fn get_table_storage_location(
        &self,
        catalog_id: Option<String>,
        database_name: &str,
        table_name: &str,
    ) -> Result<String, DataCatalogError> {
        let resp = self
            .client_with_retry
            .get(format!(
                "{}/api/2.1/unity-catalog/tables/{}.{}.{}",
                &self.workspace_url,
                catalog_id.as_deref().unwrap_or("main"),
                &database_name,
                &table_name
            ))
            .send()
            .await?;

        let parsed_resp: TableResponse = resp.json().await?;
        match parsed_resp {
            TableResponse::Success { storage_location } => Ok(storage_location),
            TableResponse::Error {
                error_code,
                message,
            } => Err(GetTableError::InvalidTable {
                error_code,
                message,
            }
            .into()),
        }
    }
}

impl std::fmt::Debug for UnityCatalog {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "UnityCatalog")
    }
}
