//! Catalog abstraction for Delta Table

use std::fmt::Debug;

#[cfg(feature = "unity-experimental")]
pub use unity::*;

#[cfg(feature = "unity-experimental")]
pub mod client;
#[cfg(feature = "datafusion")]
pub mod storage;
#[cfg(feature = "unity-experimental")]
pub mod unity;

/// A result type for data catalog implementations
pub type DataCatalogResult<T> = Result<T, DataCatalogError>;

/// Error enum that represents a CatalogError.
#[derive(thiserror::Error, Debug)]
pub enum DataCatalogError {
    /// A generic error qualified in the message
    #[error("Error in {catalog} catalog: {source}")]
    Generic {
        /// Name of the catalog
        catalog: &'static str,
        /// Error message
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// A generic error qualified in the message
    #[cfg(feature = "unity-experimental")]
    #[error("{source}")]
    Retry {
        /// Error message
        #[from]
        source: client::retry::RetryError,
    },

    #[error("Request error: {source}")]
    #[cfg(feature = "unity-experimental")]
    /// Error from reqwest library
    RequestError {
        /// The underlying reqwest_middleware::Error
        #[from]
        source: reqwest::Error,
    },

    /// Error caused by missing environment variable for Unity Catalog.
    #[cfg(feature = "unity-experimental")]
    #[error("Missing Unity Catalog environment variable: {var_name}")]
    MissingEnvVar {
        /// Variable name
        var_name: String,
    },

    /// Error caused by invalid access token value
    #[cfg(feature = "unity-experimental")]
    #[error("Invalid Databricks personal access token")]
    InvalidAccessToken,

    /// Error representing an invalid Data Catalog.
    #[error("This data catalog doesn't exist: {data_catalog}")]
    InvalidDataCatalog {
        /// The catalog input
        data_catalog: String,
    },

    /// Unknown configuration key
    #[error("Unknown configuration key '{catalog}' in '{key}' catalog.")]
    UnknownConfigKey {
        /// Name of the catalog
        catalog: &'static str,

        /// configuration key
        key: String,
    },
}

/// Abstractions for data catalog for the Delta table. To add support for new cloud, simply implement this trait.
#[async_trait::async_trait]
pub trait DataCatalog: Send + Sync + Debug {
    /// Get the table storage location from the Data Catalog
    async fn get_table_storage_location(
        &self,
        catalog_id: Option<String>,
        database_name: &str,
        table_name: &str,
    ) -> Result<String, DataCatalogError>;
}
