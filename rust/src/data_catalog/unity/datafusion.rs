//! Datafusion integration for UnityCatalog

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use tracing::error;

use super::models::{GetSchemaResponse, GetTableResponse, ListTableSummariesResponse};
use super::UnityCatalog;
use crate::data_catalog::models::ListSchemasResponse;
use crate::DeltaTableBuilder;

/// A datafusion [`CatalogProvider`] backed by Databricks UnityCatalog
pub struct UnityCatalogProvider {
    /// UnityCatalog Api client
    client: Arc<UnityCatalog>,

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

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let maybe_schema =
            rt.block_on(async move { self.client.get_schema(&self.catalog_name, name).await });

        if let Ok(response) = maybe_schema {
            match response {
                GetSchemaResponse::Success(schema) => Some(Arc::new(UnitySchemaProvider {
                    client: self.client.clone(),
                    catalog_name: self.catalog_name.clone(),
                    schema_name: schema.name,
                    // TODO pass down options
                    storage_options: Default::default(),
                })),
                GetSchemaResponse::Error(err) => {
                    error!(
                        "failed to fetch schemas from unity catalog: {}",
                        err.message
                    );
                    None
                }
            }
        } else {
            error!("failed to fetch schemas from unity catalog");
            None
        }
    }
}

/// A datafusion [`SchemaProvider`] backed by Databricks UnityCatalog
pub struct UnitySchemaProvider {
    /// UnityCatalog Api client
    client: Arc<UnityCatalog>,

    /// Parent catalog for schemas of interest.
    catalog_name: String,

    /// Parent catalog for schemas of interest.
    schema_name: String,

    storage_options: HashMap<String, String>,
}

#[async_trait::async_trait]
impl SchemaProvider for UnitySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let maybe_tables = rt.block_on(async move {
            self.client
                .list_table_summaries(&self.catalog_name, &self.schema_name)
                .await
        });

        let tables = if let Ok(response) = maybe_tables {
            match response {
                ListTableSummariesResponse::Success { tables, .. } => tables,
                ListTableSummariesResponse::Error(err) => {
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

        tables
            .into_iter()
            .filter_map(|t| t.full_name.split('.').last().map(|n| n.to_string()))
            .collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let maybe_table = self
            .client
            .get_table(&self.catalog_name, &self.schema_name, name)
            .await
            .ok()?;

        match maybe_table {
            GetTableResponse::Success(table) => {
                let table = DeltaTableBuilder::from_uri(table.storage_location)
                    .with_storage_options(self.storage_options.clone())
                    .load()
                    .await
                    .ok()?;
                Some(Arc::new(table))
            }
            GetTableResponse::Error(err) => {
                error!("failed to fetch table from unity catalog: {}", err.message);
                None
            }
        }
    }

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}
