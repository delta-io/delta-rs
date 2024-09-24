//! Datafusion integration for UnityCatalog

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::datasource::TableProvider;
use datafusion_common::DataFusionError;
use tracing::error;

use super::models::{GetTableResponse, ListCatalogsResponse, ListTableSummariesResponse};
use super::{DataCatalogResult, UnityCatalog};
use crate::data_catalog::models::ListSchemasResponse;
use crate::DeltaTableBuilder;

/// In-memory list of catalogs populated by unity catalog
pub struct UnityCatalogList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl UnityCatalogList {
    /// Create a new instance of [`UnityCatalogList`]
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        storage_options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)> + Clone,
    ) -> DataCatalogResult<Self> {
        let catalogs = match client.list_catalogs().await? {
            ListCatalogsResponse::Success { catalogs } => {
                let mut providers = Vec::new();
                for catalog in catalogs {
                    let provider = UnityCatalogProvider::try_new(
                        client.clone(),
                        &catalog.name,
                        storage_options.clone(),
                    )
                    .await?;
                    providers.push((catalog.name, Arc::new(provider) as Arc<dyn CatalogProvider>));
                }
                providers
            }
            _ => vec![],
        };
        Ok(Self {
            catalogs: catalogs.into_iter().collect(),
        })
    }
}

impl CatalogProviderList for UnityCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().clone()).collect()
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|c| c.value().clone())
    }
}

/// A datafusion [`CatalogProvider`] backed by Databricks UnityCatalog
pub struct UnityCatalogProvider {
    /// Parent catalog for schemas of interest.
    pub schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl UnityCatalogProvider {
    /// Create a new instance of [`UnityCatalogProvider`]
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_name: impl Into<String>,
        storage_options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)> + Clone,
    ) -> DataCatalogResult<Self> {
        let catalog_name = catalog_name.into();
        let schemas = match client.list_schemas(&catalog_name).await? {
            ListSchemasResponse::Success { schemas } => {
                let mut providers = Vec::new();
                for schema in schemas {
                    let provider = UnitySchemaProvider::try_new(
                        client.clone(),
                        &catalog_name,
                        &schema.name,
                        storage_options.clone(),
                    )
                    .await?;
                    providers.push((schema.name, Arc::new(provider) as Arc<dyn SchemaProvider>));
                }
                providers
            }
            _ => vec![],
        };
        Ok(Self {
            schemas: schemas.into_iter().collect(),
        })
    }
}

impl CatalogProvider for UnityCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|c| c.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|c| c.value().clone())
    }
}

/// A datafusion [`SchemaProvider`] backed by Databricks UnityCatalog
pub struct UnitySchemaProvider {
    /// UnityCatalog Api client
    client: Arc<UnityCatalog>,

    catalog_name: String,

    schema_name: String,

    /// Parent catalog for schemas of interest.
    table_names: Vec<String>,

    storage_options: HashMap<String, String>,
}

impl UnitySchemaProvider {
    /// Create a new instance of [`UnitySchemaProvider`]
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        storage_options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> DataCatalogResult<Self> {
        let catalog_name = catalog_name.into();
        let schema_name = schema_name.into();
        let table_names = match client
            .list_table_summaries(&catalog_name, &schema_name)
            .await?
        {
            ListTableSummariesResponse::Success { tables, .. } => tables
                .into_iter()
                .filter_map(|t| t.full_name.split('.').last().map(|n| n.into()))
                .collect(),
            ListTableSummariesResponse::Error(_) => vec![],
        };
        Ok(Self {
            client,
            table_names,
            catalog_name,
            schema_name,
            storage_options: storage_options
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl SchemaProvider for UnitySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(&self, name: &str) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        let maybe_table = self
            .client
            .get_table(&self.catalog_name, &self.schema_name, name)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        match maybe_table {
            GetTableResponse::Success(table) => {
                let table = DeltaTableBuilder::from_uri(table.storage_location)
                    .with_storage_options(self.storage_options.clone())
                    .load()
                    .await?;
                Ok(Some(Arc::new(table)))
            }
            GetTableResponse::Error(err) => {
                error!("failed to fetch table from unity catalog: {}", err.message);
                Err(DataFusionError::External(Box::new(err)))
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.contains(&String::from(name))
    }
}
