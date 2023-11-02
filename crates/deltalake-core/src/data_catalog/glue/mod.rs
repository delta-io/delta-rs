//! Glue Data Catalog.
//!
//! This module is gated behind the "glue" feature.
use super::{DataCatalog, DataCatalogError};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::AutoRefreshingProvider;
use rusoto_sts::WebIdentityProvider;

use rusoto_glue::{GetTableRequest, Glue, GlueClient};

/// A Glue Data Catalog implement of the `Catalog` trait
pub struct GlueDataCatalog {
    client: GlueClient,
}

impl GlueDataCatalog {
    /// Creates a new GlueDataCatalog.
    pub fn new() -> Result<Self, DataCatalogError> {
        let region = if let Ok(url) = std::env::var("AWS_ENDPOINT_URL") {
            Region::Custom {
                name: std::env::var("AWS_REGION").unwrap_or_else(|_| "custom".to_string()),
                endpoint: url,
            }
        } else {
            Region::default()
        };

        let client = create_glue_client(region)?;

        Ok(Self { client })
    }
}

impl From<GlueClient> for GlueDataCatalog {
    fn from(client: GlueClient) -> Self {
        Self { client }
    }
}

fn get_web_identity_provider(
) -> Result<AutoRefreshingProvider<WebIdentityProvider>, DataCatalogError> {
    let provider = WebIdentityProvider::from_k8s_env();
    Ok(AutoRefreshingProvider::new(provider)?)
}

fn create_glue_client(region: Region) -> Result<GlueClient, DataCatalogError> {
    match std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE") {
        Ok(_) => Ok(GlueClient::new_with(
            HttpClient::new()?,
            get_web_identity_provider()?,
            region,
        )),
        Err(_) => Ok(GlueClient::new(region)),
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
        let response = self
            .client
            .get_table(GetTableRequest {
                catalog_id,
                database_name: database_name.to_string(),
                name: table_name.to_string(),
            })
            .await?;

        let location = response
            .table
            .ok_or(DataCatalogError::MissingMetadata {
                metadata: "Table".to_string(),
            })?
            .storage_descriptor
            .ok_or(DataCatalogError::MissingMetadata {
                metadata: "Storage Descriptor".to_string(),
            })?
            .location
            .map(|l| l.replace("s3a", "s3"))
            .ok_or(DataCatalogError::MissingMetadata {
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
            Err(err) => Err(err),
        }
    }
}
