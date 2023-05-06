//! Databricks Unity Catalog.
//!
//! This module is gated behind the "unity-experimental" feature.
use std::str::FromStr;

use reqwest::header::{HeaderValue, AUTHORIZATION};

use self::credential::{ClientSecretOAuthProvider, CredentialProvider};
use self::models::{
    GetSchemaResponse, GetTableResponse, ListSchemasResponse, ListTableSummariesResponse,
};
use super::client::retry::RetryExt;
use super::{client::retry::RetryConfig, CatalogResult, DataCatalog, DataCatalogError};

pub mod credential;
#[cfg(feature = "datafusion")]
pub mod datafusion;
pub mod models;

/// Possible errors from the unity-catalog/tables API call
#[derive(thiserror::Error, Debug)]
enum UnityCatalogError {
    #[error("GET request error: {source}")]
    /// Error from reqwest library
    RequestError {
        /// The underlying reqwest_middleware::Error
        #[from]
        source: reqwest::Error,
    },

    /// Request returned error response
    #[error("Invalid table error: {error_code}: {message}")]
    InvalidTable {
        /// Error code
        error_code: String,
        /// Error description
        message: String,
    },

    /// Unknown configuration key
    #[error("Unknown configuration key: {0}")]
    UnknownConfigKey(String),

    /// Unknown configuration key
    #[error("Missing configuration key: {0}")]
    MissingConfiguration(String),

    /// Unknown configuration key
    #[error("Failed to get a credential from UnityCatalog client configuration.")]
    MissingCredential,
}

impl From<UnityCatalogError> for DataCatalogError {
    fn from(value: UnityCatalogError) -> Self {
        match value {
            UnityCatalogError::UnknownConfigKey(key) => DataCatalogError::UnknownConfigKey {
                catalog: "Unity",
                key,
            },
            _ => DataCatalogError::Generic {
                catalog: "Unity",
                source: Box::new(value),
            },
        }
    }
}

/// Configuration options for unity catalog client
pub enum UnityCatalogConfigKey {
    /// Url of a Databricks workspace
    ///
    /// Supported keys:
    /// - `unity_workspace_url`
    /// - `databricks_workspace_url`
    /// - `workspace_url`
    WorkspaceUrl,

    /// Access token to authorize API requests
    ///
    /// Supported keys:
    /// - `unity_access_token`
    /// - `databricks_access_token`
    /// - `access_token`
    AccessToken,

    /// Service principal client id for authorizing requests
    ///
    /// Supported keys:
    /// - `azure_client_id`
    /// - `unity_client_id`
    /// - `client_id`
    ClientId,

    /// Service principal client secret for authorizing requests
    ///
    /// Supported keys:
    /// - `azure_client_secret`
    /// - `unity_client_secret`
    /// - `client_secret`
    ClientSecret,

    /// Tenant id used in oauth flows
    ///
    /// Supported keys:
    /// - `azure_tenant_id`
    /// - `unity_tenant_id`
    /// - `tenant_id`
    TenantId,
}

impl FromStr for UnityCatalogConfigKey {
    type Err = DataCatalogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "workspace_url" | "unity_workspace_url" | "databricks_workspace_url" => {
                Ok(UnityCatalogConfigKey::WorkspaceUrl)
            }
            "access_token" | "unity_access_token" | "databricks_access_token" => {
                Ok(UnityCatalogConfigKey::AccessToken)
            }
            _ => Err(UnityCatalogError::UnknownConfigKey(s.into()).into()),
        }
    }
}

impl AsRef<str> for UnityCatalogConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            UnityCatalogConfigKey::WorkspaceUrl => "unity_workspace_url",
            UnityCatalogConfigKey::AccessToken => "unity_access_token",
            UnityCatalogConfigKey::TenantId => "unity_tenant_id",
            UnityCatalogConfigKey::ClientId => "unity_client_id",
            UnityCatalogConfigKey::ClientSecret => "unity_client_secret",
        }
    }
}

/// Builder for crateing a UnityCatalogClient
#[derive(Default)]
pub struct UnityCatalogBuilder {
    /// Url of a Databricks workspace
    workspace_url: Option<String>,

    /// Bearer token
    bearer_token: Option<String>,

    /// Client id
    client_id: Option<String>,

    /// Client secret
    client_secret: Option<String>,

    /// Tenant id
    tenant_id: Option<String>,

    /// Options for the underlying http client
    client_options: super::client::ClientOptions,
}

impl UnityCatalogBuilder {
    /// Create a new [`UnityCatalogBuilder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Set an option on the builder via a key - value pair.
    pub fn try_with_option(
        mut self,
        key: impl AsRef<str>,
        value: impl Into<String>,
    ) -> CatalogResult<Self> {
        match UnityCatalogConfigKey::from_str(key.as_ref())? {
            UnityCatalogConfigKey::WorkspaceUrl => self.workspace_url = Some(value.into()),
            UnityCatalogConfigKey::AccessToken => self.bearer_token = Some(value.into()),
            UnityCatalogConfigKey::ClientId => self.client_id = Some(value.into()),
            UnityCatalogConfigKey::ClientSecret => self.client_secret = Some(value.into()),
            UnityCatalogConfigKey::TenantId => self.tenant_id = Some(value.into()),
        };
        Ok(self)
    }

    /// Hydrate builder from key value pairs
    pub fn try_with_options<I: IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>>(
        mut self,
        options: I,
    ) -> CatalogResult<Self> {
        for (key, value) in options {
            self = self.try_with_option(key, value)?;
        }
        Ok(self)
    }

    /// Parse configuration from the environment.
    ///
    /// Environment keys prefixed with "UNITY_" or "DATABRICKS_" will be considered
    pub fn from_env() -> Self {
        let mut builder = Self::default();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("UNITY_") || key.starts_with("DATABRICKS_") {
                    if let Ok(config_key) =
                        UnityCatalogConfigKey::from_str(&key.to_ascii_lowercase())
                    {
                        builder = builder.try_with_option(config_key, value).unwrap();
                    }
                }
            }
        }

        builder
    }

    /// Set the URL of a Databricks workspace.
    pub fn with_workspace_url(mut self, url: impl Into<String>) -> Self {
        self.workspace_url = Some(url.into());
        self
    }

    /// Sets the client id for use in client secret or k8s federated credential flow
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets the client secret for use in client secret flow
    pub fn with_client_secret(mut self, client_secret: impl Into<String>) -> Self {
        self.client_secret = Some(client_secret.into());
        self
    }

    /// Sets the tenant id for use in client secret or k8s federated credential flow
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set a static bearer token to be used for authorizing requests
    pub fn with_bearer_token(mut self, bearer_token: impl Into<String>) -> Self {
        self.bearer_token = Some(bearer_token.into());
        self
    }

    /// Set a personal access token (PAT) to be used for authorizing requests
    pub fn with_access_token(self, access_token: impl Into<String>) -> Self {
        self.with_bearer_token(access_token)
    }

    /// Sets the client options, overriding any already set
    pub fn with_client_options(mut self, options: super::client::ClientOptions) -> Self {
        self.client_options = options;
        self
    }

    /// Build an instance of [`UnityCatalog`]
    pub fn build(self) -> CatalogResult<UnityCatalog> {
        let workspace_url = self
            .workspace_url
            .ok_or(UnityCatalogError::MissingConfiguration(
                "workspace_url".into(),
            ))?
            .trim_end_matches('/')
            .to_string();

        let credential = if let Some(token) = self.bearer_token {
            Ok(CredentialProvider::BearerToken(token))
        } else if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
            (self.client_id, self.client_secret, self.tenant_id)
        {
            Ok(CredentialProvider::TokenCredential(
                Default::default(),
                Box::new(ClientSecretOAuthProvider::new(
                    client_id,
                    client_secret,
                    tenant_id,
                    None,
                )),
            ))
        } else {
            Err(UnityCatalogError::MissingCredential)
        }?;

        let client = self.client_options.client()?;

        Ok(UnityCatalog {
            client,
            workspace_url,
            credential,
            retry_config: Default::default(),
        })
    }
}

/// Databricks Unity Catalog
pub struct UnityCatalog {
    client: reqwest::Client,
    credential: CredentialProvider,
    workspace_url: String,
    retry_config: RetryConfig,
}

impl UnityCatalog {
    async fn get_credential(&self) -> CatalogResult<HeaderValue> {
        match &self.credential {
            CredentialProvider::BearerToken(token) => {
                // we do the conversion to a HeaderValue here, since it is fallible
                // and we want to use it in an infallible function
                HeaderValue::from_str(&format!("Bearer {token}")).map_err(|err| {
                    super::DataCatalogError::Generic {
                        catalog: "Unity",
                        source: Box::new(err),
                    }
                })
            }
            CredentialProvider::TokenCredential(cache, cred) => {
                let token = cache
                    .get_or_insert_with(|| cred.fetch_token(&self.client, &self.retry_config))
                    .await?;

                // we do the conversion to a HeaderValue here, since it is fallible
                // and we want to use it in an infallible function
                HeaderValue::from_str(&format!("Bearer {token}")).map_err(|err| {
                    super::DataCatalogError::Generic {
                        catalog: "Unity",
                        source: Box::new(err),
                    }
                })
            }
        }
    }

    fn catalog_url(&self) -> String {
        format!("{}/api/2.1/unity-catalog", &self.workspace_url)
    }

    /// List all schemas for a catalog in the metastore.
    ///
    /// If the caller is the metastore admin or the owner of the parent catalog, all schemas
    /// for the catalog will be retrieved. Otherwise, only schemas owned by the caller
    /// (or for which the caller has the USE_SCHEMA privilege) will be retrieved.
    /// There is no guarantee of a specific ordering of the elements in the array.
    ///
    /// # Parameters
    /// - catalog_name: Parent catalog for schemas of interest.
    pub async fn list_schemas(
        &self,
        catalog_name: impl AsRef<str>,
    ) -> CatalogResult<ListSchemasResponse> {
        let token = self.get_credential().await?;
        // https://docs.databricks.com/api-explorer/workspace/schemas/list
        let resp = self
            .client
            .get(format!("{}/schemas", self.catalog_url()))
            .header(AUTHORIZATION, token)
            .query(&[("catalog_name", catalog_name.as_ref())])
            .send_retry(&self.retry_config)
            .await?;
        Ok(resp.json().await?)
    }

    /// Gets the specified schema within the metastore.#
    ///
    /// The caller must be a metastore admin, the owner of the schema,
    /// or a user that has the USE_SCHEMA privilege on the schema.
    pub async fn get_schema(
        &self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
    ) -> CatalogResult<GetSchemaResponse> {
        let token = self.get_credential().await?;
        // https://docs.databricks.com/api-explorer/workspace/schemas/get
        let resp = self
            .client
            .get(format!(
                "{}/schemas/{}.{}",
                self.catalog_url(),
                catalog_name.as_ref(),
                schema_name.as_ref()
            ))
            .header(AUTHORIZATION, token)
            .send_retry(&self.retry_config)
            .await?;
        Ok(resp.json().await?)
    }

    /// Gets an array of summaries for tables for a schema and catalog within the metastore.
    ///
    /// The table summaries returned are either:
    /// - summaries for all tables (within the current metastore and parent catalog and schema),
    ///   when the user is a metastore admin, or:
    /// - summaries for all tables and schemas (within the current metastore and parent catalog)
    ///   for which the user has ownership or the SELECT privilege on the table and ownership or
    ///   USE_SCHEMA privilege on the schema, provided that the user also has ownership or the
    ///   USE_CATALOG privilege on the parent catalog.
    ///
    /// There is no guarantee of a specific ordering of the elements in the array.
    pub async fn list_table_summaries(
        &self,
        catalog_name: impl AsRef<str>,
        schema_name_pattern: impl AsRef<str>,
    ) -> CatalogResult<ListTableSummariesResponse> {
        let token = self.get_credential().await?;
        // https://docs.databricks.com/api-explorer/workspace/tables/listsummaries
        let resp = self
            .client
            .get(format!("{}/table-summaries", self.catalog_url()))
            .query(&[
                ("catalog_name", catalog_name.as_ref()),
                ("schema_name_pattern", schema_name_pattern.as_ref()),
            ])
            .header(AUTHORIZATION, token)
            .send_retry(&self.retry_config)
            .await?;

        Ok(resp.json().await?)
    }

    /// Gets a table from the metastore for a specific catalog and schema.
    ///
    /// The caller must be a metastore admin, be the owner of the table and have the
    /// USE_CATALOG privilege on the parent catalog and the USE_SCHEMA privilege on
    /// the parent schema, or be the owner of the table and have the SELECT privilege on it as well.
    ///
    /// # Parameters
    pub async fn get_table(
        &self,
        catalog_id: Option<impl AsRef<str>>,
        database_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
    ) -> CatalogResult<GetTableResponse> {
        let token = self.get_credential().await?;
        // https://docs.databricks.com/api-explorer/workspace/tables/get
        let resp = self
            .client
            .get(format!(
                "{}/tables/{}.{}.{}",
                self.catalog_url(),
                catalog_id
                    .map(|c| c.as_ref().to_string())
                    .unwrap_or("main".into()),
                database_name.as_ref(),
                table_name.as_ref()
            ))
            .header(AUTHORIZATION, token)
            .send_retry(&self.retry_config)
            .await?;

        Ok(resp.json().await?)
    }
}

#[async_trait::async_trait]
impl DataCatalog for UnityCatalog {
    /// Get the table storage location from the UnityCatalog
    async fn get_table_storage_location(
        &self,
        catalog_id: Option<String>,
        database_name: &str,
        table_name: &str,
    ) -> Result<String, DataCatalogError> {
        match self
            .get_table(catalog_id, database_name, table_name)
            .await?
        {
            GetTableResponse::Success(table) => Ok(table.storage_location),
            GetTableResponse::Error(err) => Err(UnityCatalogError::InvalidTable {
                error_code: err.error_code,
                message: err.message,
            }
            .into()),
        }
    }
}

impl std::fmt::Debug for UnityCatalog {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "UnityCatalog")
    }
}

#[cfg(test)]
mod tests {
    use crate::data_catalog::client::ClientOptions;

    use super::super::client::mock_server::MockServer;
    use super::models::tests::{GET_SCHEMA_RESPONSE, GET_TABLE_RESPONSE, LIST_SCHEMAS_RESPONSE};
    use super::*;
    use hyper::{Body, Response};
    use reqwest::Method;

    #[tokio::test]
    async fn test_unity_client() {
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

        let list_schemas_response = client.list_schemas("catalog_name").await.unwrap();
        assert!(matches!(
            list_schemas_response,
            ListSchemasResponse::Success { .. }
        ));

        server.push_fn(move |req| {
            assert_eq!(
                req.uri().path(),
                "/api/2.1/unity-catalog/schemas/catalog_name.schema_name"
            );
            assert_eq!(req.method(), &Method::GET);
            Response::new(Body::from(GET_SCHEMA_RESPONSE))
        });

        let get_schema_response = client
            .get_schema("catalog_name", "schema_name")
            .await
            .unwrap();
        assert!(matches!(get_schema_response, GetSchemaResponse::Success(_)));

        server.push_fn(move |req| {
            assert_eq!(
                req.uri().path(),
                "/api/2.1/unity-catalog/tables/catalog_name.schema_name.table_name"
            );
            assert_eq!(req.method(), &Method::GET);
            Response::new(Body::from(GET_TABLE_RESPONSE))
        });

        let get_table_response = client
            .get_table(Some("catalog_name"), "schema_name", "table_name")
            .await
            .unwrap();
        assert!(matches!(get_table_response, GetTableResponse::Success(_)));
    }
}
