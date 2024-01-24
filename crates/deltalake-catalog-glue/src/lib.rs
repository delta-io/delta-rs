//! Glue Data Catalog.
//!
use aws_config::{BehaviorVersion, SdkConfig};
use deltalake_core::data_catalog::{DataCatalog, DataCatalogError};

#[derive(thiserror::Error, Debug)]
pub enum GlueError {
    /// Missing metadata in the catalog
    #[error("Missing Metadata {metadata} in the Data Catalog ")]
    MissingMetadata {
        /// The missing metadata property
        metadata: String,
    },

    /// Error calling the AWS SDK
    #[error("Failed in an AWS SDK call")]
    AWSError {
        #[from]
        source: aws_sdk_glue::Error,
    },
}

impl From<GlueError> for DataCatalogError {
    fn from(val: GlueError) -> Self {
        DataCatalogError::Generic {
            catalog: "glue",
            source: Box::new(val),
        }
    }
}

/// A Glue Data Catalog implement of the `Catalog` trait
pub struct GlueDataCatalog {
    client: aws_sdk_glue::Client,
}

impl GlueDataCatalog {
    /// Creates a new GlueDataCatalog with environmental configuration
    pub async fn from_env() -> Result<Self, GlueError> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = aws_sdk_glue::Client::new(&config);
        Ok(Self { client })
    }

    /// Create a new [GlueDataCatalog] with the given [aws_config::SdkConfig]
    pub fn with_config(config: &SdkConfig) -> Self {
        let client = aws_sdk_glue::Client::new(config);
        Self { client }
    }
}

impl std::fmt::Debug for GlueDataCatalog {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "GlueDataCatalog")
    }
}

// Placeholder suffix created by Spark in the Glue Data Catalog Location
const PLACEHOLDER_SUFFIX: &str = "-__PLACEHOLDER__";

#[async_trait::async_trait]
impl DataCatalog for GlueDataCatalog {
    /// Get the table storage location from the Glue Data Catalog
    async fn get_table_storage_location(
        &self,
        catalog_id: Option<String>,
        database_name: &str,
        table_name: &str,
    ) -> Result<String, DataCatalogError> {
        let mut builder = self
            .client
            .get_table()
            .database_name(database_name)
            .name(table_name);

        if let Some(catalog) = catalog_id {
            builder = builder.catalog_id(catalog);
        }

        let response = builder
            .send()
            .await
            .map_err(|e| GlueError::AWSError { source: e.into() })
            .map_err(<GlueError as Into<DataCatalogError>>::into)?;

        let location = response
            .table
            .ok_or(GlueError::MissingMetadata {
                metadata: "Table".to_string(),
            })
            .map_err(<GlueError as Into<DataCatalogError>>::into)?
            .storage_descriptor
            .ok_or(GlueError::MissingMetadata {
                metadata: "Storage Descriptor".to_string(),
            })
            .map_err(<GlueError as Into<DataCatalogError>>::into)?
            .location
            .map(|l| l.replace("s3a", "s3"))
            .ok_or(GlueError::MissingMetadata {
                metadata: "Location".to_string(),
            });

        match location {
            Ok(location) => {
                if location.ends_with(PLACEHOLDER_SUFFIX) {
                    Ok(location[..location.len() - PLACEHOLDER_SUFFIX.len()].to_string())
                } else {
                    Ok(location)
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}
