//! Datafusion integration for UnityCatalog

use std::any::Any;

use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use tracing::error;

use crate::data_catalog::models::ListSchemasResponse;

use super::UnityCatalog;

/// A datafusion [`CatalogProvider`] backed by Databricks UnityCatalog
pub struct UnityCatalogProvider {
    /// UnityCatalog Api client
    client: UnityCatalog,

    /// Parent catalog for schemas of interest.
    catalog_name: String,
}

impl CatalogProvider for UnityCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let maybe_schemas =
            rt.block_on(async move { self.client.list_schemas(&self.catalog_name).await });

        let schemas = if let Ok(response) = maybe_schemas {
            match response {
                ListSchemasResponse::Success { schemas } => schemas,
                ListSchemasResponse::Error(err) => {
                    error!(
                        "failed to fetch schemas from unity catalog: {}",
                        err.message
                    );
                    Vec::new()
                }
            }
        } else {
            error!("failed to fetch schemas from unity catalog");
            Vec::new()
        };

        schemas.into_iter().map(|s| s.name).collect()
    }

    fn schema(&self, _name: &str) -> Option<std::sync::Arc<dyn SchemaProvider>> {
        let _rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        todo!()
    }
}
