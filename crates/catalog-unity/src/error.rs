#[derive(thiserror::Error, Debug)]
pub enum UnityCatalogError {
    /// A generic error qualified in the message
    #[error("Error in {catalog} catalog: {source}")]
    Generic {
        /// Name of the catalog
        catalog: &'static str,
        /// Error message
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// A generic error qualified in the message
    #[error("{source}")]
    Retry {
        /// Error message
        #[from]
        source: crate::client::retry::RetryError,
    },

    #[error("Request error: {source}")]
    /// Error from reqwest library
    RequestError {
        /// The underlying reqwest_middleware::Error
        #[from]
        source: reqwest::Error,
    },

    /// Error caused by missing environment variable for Unity Catalog.
    #[error("Missing Unity Catalog environment variable: {var_name}")]
    MissingEnvVar {
        /// Variable name
        var_name: String,
    },

    /// Error caused by invalid access token value
    #[error("Invalid Databricks personal access token")]
    InvalidAccessToken,
}
