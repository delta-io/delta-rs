//! Datafusion integration for UnityCatalog

use chrono::prelude::*;
use dashmap::DashMap;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use moka::future::Cache;
use moka::Expiry;
use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::error;

use super::models::{
    GetTableResponse, ListCatalogsResponse, ListSchemasResponse, ListTableSummariesResponse,
    TableTempCredentialsResponse, TemporaryTableCredentials,
};
use super::{DataCatalogResult, UnityCatalog, UnityCatalogError};
use deltalake_core::{ensure_table_uri, DeltaTableBuilder};

/// In-memory list of catalogs populated by unity catalog
#[derive(Debug)]
pub struct UnityCatalogList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl UnityCatalogList {
    /// Create a new instance of [`UnityCatalogList`]
    pub async fn try_new(client: Arc<UnityCatalog>) -> DataCatalogResult<Self> {
        let catalogs = match client.list_catalogs().await? {
            ListCatalogsResponse::Success { catalogs, .. } => {
                let mut providers = Vec::new();
                for catalog in catalogs {
                    let provider =
                        UnityCatalogProvider::try_new(client.clone(), &catalog.name).await?;
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

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|c| c.value().clone())
    }
}

/// A datafusion [`CatalogProvider`] backed by Databricks UnityCatalog
#[derive(Debug)]
pub struct UnityCatalogProvider {
    /// Parent catalog for schemas of interest.
    pub schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl UnityCatalogProvider {
    /// Create a new instance of [`UnityCatalogProvider`]
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_name: impl Into<String>,
    ) -> DataCatalogResult<Self> {
        let catalog_name = catalog_name.into();
        let schemas = match client.list_schemas(&catalog_name).await? {
            ListSchemasResponse::Success { schemas } => {
                let mut providers = Vec::new();
                for schema in schemas {
                    let provider =
                        UnitySchemaProvider::try_new(client.clone(), &catalog_name, &schema.name)
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

struct TokenExpiry;

impl Expiry<String, TemporaryTableCredentials> for TokenExpiry {
    fn expire_after_read(
        &self,
        _key: &String,
        value: &TemporaryTableCredentials,
        _read_at: Instant,
        _duration_until_expiry: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        let time_to_expire = value.expiration_time - Utc::now();
        tracing::info!("Token {_key} expires in {time_to_expire}");
        time_to_expire.to_std().ok()
    }
}

/// A datafusion [`SchemaProvider`] backed by Databricks UnityCatalog
#[derive(Debug)]
pub struct UnitySchemaProvider {
    client: Arc<UnityCatalog>,
    catalog_name: String,
    schema_name: String,

    /// Parent catalog for schemas of interest.
    table_names: Vec<String>,
    token_cache: Cache<String, TemporaryTableCredentials>,
}

impl UnitySchemaProvider {
    /// Create a new instance of [`UnitySchemaProvider`]
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
    ) -> DataCatalogResult<Self> {
        let catalog_name = catalog_name.into();
        let schema_name = schema_name.into();
        let table_names = match client
            .list_table_summaries(&catalog_name, &schema_name)
            .await?
        {
            ListTableSummariesResponse::Success { tables, .. } => tables
                .into_iter()
                .filter_map(|t| t.full_name.split('.').next_back().map(|n| n.into()))
                .collect(),
            ListTableSummariesResponse::Error(_) => vec![],
        };
        let token_cache = Cache::builder().expire_after(TokenExpiry).build();
        Ok(Self {
            client,
            table_names,
            catalog_name,
            schema_name,
            token_cache,
        })
    }

    async fn get_creds(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<TemporaryTableCredentials, UnityCatalogError> {
        tracing::debug!("Fetching new credential for: {catalog}.{schema}.{table}",);
        match self
            .client
            .get_temp_table_credentials_with_permission(catalog, schema, table, "READ_WRITE")
            .await
        {
            Ok(TableTempCredentialsResponse::Success(temp_creds)) => Ok(temp_creds),
            Ok(TableTempCredentialsResponse::Error(_err)) => match self
                .client
                .get_temp_table_credentials(catalog, schema, table)
                .await?
            {
                TableTempCredentialsResponse::Success(temp_creds) => Ok(temp_creds),
                _ => Err(UnityCatalogError::TemporaryCredentialsFetchFailure),
            },
            Err(err) => Err(err),
        }
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

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let maybe_table = self
            .client
            .get_table(&self.catalog_name, &self.schema_name, name)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        match maybe_table {
            GetTableResponse::Success(table) => {
                let temp_creds = self
                    .token_cache
                    .try_get_with(
                        table.table_id,
                        self.get_creds(&self.catalog_name, &self.schema_name, name),
                    )
                    .await
                    .map_err(|err| DataFusionError::External(err.into()))?;

                let new_storage_opts = temp_creds.get_credentials().ok_or_else(|| {
                    DataFusionError::External(UnityCatalogError::MissingCredential.into())
                })?;
                let table_url = ensure_table_uri(&table.storage_location)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let table = DeltaTableBuilder::from_uri(table_url)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .with_storage_options(new_storage_opts)
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
