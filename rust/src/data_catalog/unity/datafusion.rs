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
    // rt: Arc<Runtime>,
}

impl UnityCatalogProvider {
    /// Create a new instance of [`UnityCatalogProvider`]
    pub fn new(
        // rt: Arc<Runtime>,
        client: Arc<UnityCatalog>,
        catalog_name: impl Into<String>,
    ) -> Self {
        Self {
            // rt,
            client,
            catalog_name: catalog_name.into(),
        }
    }
}

impl CatalogProvider for UnityCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let handle = tokio::runtime::Runtime::new().unwrap();

        let maybe_schemas =
            handle.block_on(async move { self.client.list_schemas(&self.catalog_name).await });

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
        // let rt = tokio::runtime::Builder::new_current_thread()
        //     .build()
        //     .unwrap();
        let handle = tokio::runtime::Handle::try_current().unwrap();

        let maybe_schema =
            handle.block_on(async move { self.client.get_schema(&self.catalog_name, name).await });

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

#[cfg(test)]
mod tests {
    use hyper::{Body, Response};
    use reqwest::Method;

    use super::*;
    use crate::data_catalog::client::mock_server::MockServer;
    use crate::data_catalog::client::ClientOptions;
    use crate::data_catalog::unity::UnityCatalogBuilder;

    const LIST_SCHEMAS_RESPONSE: &str = r#"
    {
        "schemas": [
            {
                "name": "test_tables",
                "full_name": "delta_rs.test_tables",
                "catalog_type": "catalog_type",
                "catalog_name": "delta_rs",
                "storage_root": "string",
                "storage_location": "string",
                "created_at": 0,
                "owner": "owners",
                "updated_at": 0,
                "metastore_id": "store-id"
            }
        ]
    }"#;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_catalog_provider() {
        let server = MockServer::new();
        let options = ClientOptions::default().with_allow_http(true);
        let client = UnityCatalogBuilder::new()
            .with_workspace_url(server.url())
            .with_bearer_token("bearer_token")
            .with_client_options(options)
            .build()
            .unwrap();

        server.push_fn(move |req| {
            assert_eq!(req.uri().path(), "/api/2.1/unity-catalog/schemas");
            assert_eq!(req.method(), &Method::GET);
            Response::new(Body::from(LIST_SCHEMAS_RESPONSE))
        });

        let provider = UnityCatalogProvider::new(Arc::new(client), "delta_rs");
        let schemas = provider.schema_names();
        assert_eq!(schemas[0], "test_tables")
    }
}
