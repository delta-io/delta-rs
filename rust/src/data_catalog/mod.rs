//! Catalog abstraction for Delta Table

use std::fmt::Debug;

#[cfg(feature = "glue")]
pub mod glue;

/// Error enum that represents a CatalogError.
#[derive(thiserror::Error, Debug)]
pub enum DataCatalogError {
    /// Missing metadata in the catalog
    #[cfg(feature = "glue")]
    #[error("Missing Metadata {metadata} in the Data Catalog ")]
    MissingMetadata {
        /// The missing metadata property
        metadata: String,
    },

    /// Glue Glue Data Catalog Error
    #[cfg(feature = "glue")]
    #[error("Catalog glue error: {source}")]
    GlueError {
        /// The underlying Glue Data Catalog Error
        #[from]
        source: rusoto_core::RusotoError<rusoto_glue::GetTableError>,
    },

    /// Error caused by the http request dispatcher not being able to be created.
    #[cfg(feature = "glue")]
    #[error("Failed to create request dispatcher: {source}")]
    AWSHttpClient {
        /// The underlying Rusoto TlsError
        #[from]
        source: rusoto_core::request::TlsError,
    },

    /// Error representing a failure to retrieve AWS credentials.
    #[cfg(feature = "glue")]
    #[error("Failed to retrieve AWS credentials: {source}")]
    AWSCredentials {
        /// The underlying Rusoto CredentialsError
        #[from]
        source: rusoto_credential::CredentialsError,
    },

    /// Error representing an invalid Data Catalog.
    #[error("This data catalog doesn't exist: {data_catalog}")]
    InvalidDataCatalog {
        /// The catalog input
        data_catalog: String,
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

/// Get the Data Catalog
pub fn get_data_catalog(data_catalog: &str) -> Result<Box<dyn DataCatalog>, DataCatalogError> {
    match data_catalog {
        #[cfg(feature = "gcp")]
        "gcp" => unimplemented!("GCP Data Catalog is not implemented"),
        #[cfg(feature = "azure")]
        "azure" => unimplemented!("Azure Data Catalog is not implemented"),
        #[cfg(feature = "glue")]
        "glue" => Ok(Box::new(glue::GlueDataCatalog::new()?)),
        _ => Err(DataCatalogError::InvalidDataCatalog {
            data_catalog: data_catalog.to_string(),
        }),
    }
}
