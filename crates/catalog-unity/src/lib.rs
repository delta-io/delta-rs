#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
//! Databricks Unity Catalog.
#[cfg(not(any(feature = "aws", feature = "azure", feature = "gcp", feature = "r2")))]
compile_error!(
    "At least one of the following crate features `aws`, `azure`, `gcp`, or `r2` must be enabled \
    for this crate to function properly."
);

use deltalake_core::logstore::{
    default_logstore, logstore_factories, object_store::RetryConfig, LogStore, LogStoreFactory,
    StorageConfig,
};
use reqwest::header::{HeaderValue, InvalidHeaderValue, AUTHORIZATION};
use reqwest::Url;
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Handle;

use crate::credential::{
    AzureCliCredential, ClientSecretOAuthProvider, CredentialProvider, WorkspaceOAuthProvider,
};
use crate::models::{
    ErrorResponse, GetSchemaResponse, GetTableResponse, ListCatalogsResponse, ListSchemasResponse,
    ListTableSummariesResponse, TableTempCredentialsResponse, TemporaryTableCredentialsRequest,
};

use deltalake_core::data_catalog::DataCatalogResult;
use deltalake_core::{
    DataCatalog, DataCatalogError, DeltaResult, DeltaTableBuilder, DeltaTableError,
    ObjectStoreError, Path,
};

use crate::client::retry::*;
use deltalake_core::logstore::{
    config::str_is_truthy, object_store_factories, IORuntime, ObjectStoreFactory, ObjectStoreRef,
};
pub mod client;
pub mod credential;

const STORE_NAME: &str = "UnityCatalogObjectStore";
#[cfg(feature = "datafusion")]
pub mod datafusion;
pub mod models;
pub mod prelude;

/// Possible errors from the unity-catalog/tables API call
#[derive(thiserror::Error, Debug)]
pub enum UnityCatalogError {
    #[error("GET request error: {source}")]
    /// Error from reqwest library
    RequestError {
        /// The underlying reqwest_middleware::Error
        #[from]
        source: reqwest::Error,
    },

    #[error("Error in middleware: {source}")]
    RequestMiddlewareError {
        /// The underlying reqwest_middleware::Error
        #[from]
        source: reqwest_middleware::Error,
    },

    /// Request returned error response
    #[error("Invalid table error: {error_code}: {message}")]
    InvalidTable {
        /// Error code
        error_code: String,
        /// Error description
        message: String,
    },

    #[error("Invalid token for auth header: {header_error}")]
    InvalidHeader {
        #[from]
        header_error: InvalidHeaderValue,
    },

    /// Invalid Table URI
    #[error("Invalid Unity Catalog Table URI: {table_uri}")]
    InvalidTableURI {
        /// Table URI
        table_uri: String,
    },

    /// Unknown configuration key
    #[error("Missing configuration key: {0}")]
    MissingConfiguration(String),

    /// Unknown configuration key
    #[error("Failed to get a credential from UnityCatalog client configuration.")]
    MissingCredential,

    /// Temporary Credentials Fetch Failure
    #[error("Unable to get temporary credentials from Unity Catalog.")]
    TemporaryCredentialsFetchFailure,

    #[error("Azure CLI error: {message}")]
    AzureCli {
        /// Error description
        message: String,
    },

    #[error("Missing or corrupted federated token file for WorkloadIdentity.")]
    FederatedTokenFile,

    #[cfg(feature = "datafusion")]
    #[error("Datafusion error: {0}")]
    DatafusionError(#[from] ::datafusion::common::DataFusionError),

    /// Cannot initialize DynamoDbConfiguration due to some sort of threading issue
    #[error("Unable to initialize Unity Catalog, potentially a threading issue")]
    InitializationError,

    /// A generic error from a source
    #[error("An error occurred in catalog: {source}")]
    Generic {
        /// Error message
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl From<ErrorResponse> for UnityCatalogError {
    fn from(value: ErrorResponse) -> Self {
        UnityCatalogError::InvalidTable {
            error_code: value.error_code,
            message: value.message,
        }
    }
}

impl From<DataCatalogError> for UnityCatalogError {
    fn from(value: DataCatalogError) -> Self {
        UnityCatalogError::Generic {
            source: Box::new(value),
        }
    }
}

impl From<UnityCatalogError> for DataCatalogError {
    fn from(value: UnityCatalogError) -> Self {
        DataCatalogError::Generic {
            catalog: "Unity",
            source: Box::new(value),
        }
    }
}

impl From<UnityCatalogError> for DeltaTableError {
    fn from(value: UnityCatalogError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(value),
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
    #[deprecated(since = "0.17.0", note = "Please use the DATABRICKS_HOST env variable")]
    WorkspaceUrl,

    /// Host of the Databricks workspace
    Host,

    /// Access token to authorize API requests
    ///
    /// Supported keys:
    /// - `unity_access_token`
    /// - `databricks_access_token`
    /// - `access_token`
    #[deprecated(
        since = "0.17.0",
        note = "Please use the DATABRICKS_TOKEN env variable"
    )]
    AccessToken,

    /// Token to use for Databricks Unity
    Token,

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

    /// Authority (tenant) id used in oauth flows
    ///
    /// Supported keys:
    /// - `azure_tenant_id`
    /// - `unity_tenant_id`
    /// - `tenant_id`
    AuthorityId,

    /// Authority host used in oauth flows
    ///
    /// Supported keys:
    /// - `azure_authority_host`
    /// - `unity_authority_host`
    /// - `authority_host`
    AuthorityHost,

    /// Endpoint to request a imds managed identity token
    ///
    /// Supported keys:
    /// - `azure_msi_endpoint`
    /// - `azure_identity_endpoint`
    /// - `identity_endpoint`
    /// - `msi_endpoint`
    MsiEndpoint,

    /// Object id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_object_id`
    /// - `object_id`
    ObjectId,

    /// Msi resource id for use with managed identity authentication
    ///
    /// Supported keys:
    /// - `azure_msi_resource_id`
    /// - `msi_resource_id`
    MsiResourceId,

    /// File containing token for Azure AD workload identity federation
    ///
    /// Supported keys:
    /// - `azure_federated_token_file`
    /// - `federated_token_file`
    FederatedTokenFile,

    /// Use azure cli for acquiring access token
    ///
    /// Supported keys:
    /// - `azure_use_azure_cli`
    /// - `use_azure_cli`
    UseAzureCli,

    /// Allow http url (e.g. http://localhost:8080/api/2.1/...)
    /// Supported keys:
    /// - `unity_allow_http_url`
    AllowHttpUrl,
}

impl FromStr for UnityCatalogConfigKey {
    type Err = DataCatalogError;

    #[allow(deprecated)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "access_token"
            | "unity_access_token"
            | "databricks_access_token"
            | "databricks_token" => Ok(UnityCatalogConfigKey::AccessToken),
            "authority_host" | "unity_authority_host" | "databricks_authority_host" => {
                Ok(UnityCatalogConfigKey::AuthorityHost)
            }
            "authority_id" | "unity_authority_id" | "databricks_authority_id" => {
                Ok(UnityCatalogConfigKey::AuthorityId)
            }
            "client_id" | "unity_client_id" | "databricks_client_id" => {
                Ok(UnityCatalogConfigKey::ClientId)
            }
            "client_secret" | "unity_client_secret" | "databricks_client_secret" => {
                Ok(UnityCatalogConfigKey::ClientSecret)
            }
            "federated_token_file"
            | "unity_federated_token_file"
            | "databricks_federated_token_file" => Ok(UnityCatalogConfigKey::FederatedTokenFile),
            "host" => Ok(UnityCatalogConfigKey::Host),
            "msi_endpoint" | "unity_msi_endpoint" | "databricks_msi_endpoint" => {
                Ok(UnityCatalogConfigKey::MsiEndpoint)
            }
            "msi_resource_id" | "unity_msi_resource_id" | "databricks_msi_resource_id" => {
                Ok(UnityCatalogConfigKey::MsiResourceId)
            }
            "object_id" | "unity_object_id" | "databricks_object_id" => {
                Ok(UnityCatalogConfigKey::ObjectId)
            }
            "token" => Ok(UnityCatalogConfigKey::Token),
            "use_azure_cli" | "unity_use_azure_cli" | "databricks_use_azure_cli" => {
                Ok(UnityCatalogConfigKey::UseAzureCli)
            }
            "workspace_url"
            | "unity_workspace_url"
            | "databricks_workspace_url"
            | "databricks_host" => Ok(UnityCatalogConfigKey::WorkspaceUrl),
            "allow_http_url" | "unity_allow_http_url" => Ok(UnityCatalogConfigKey::AllowHttpUrl),
            _ => Err(DataCatalogError::UnknownConfigKey {
                catalog: "unity",
                key: s.to_string(),
            }),
        }
    }
}

#[allow(deprecated)]
impl AsRef<str> for UnityCatalogConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            UnityCatalogConfigKey::AccessToken => "unity_access_token",
            UnityCatalogConfigKey::AllowHttpUrl => "unity_allow_http_url",
            UnityCatalogConfigKey::AuthorityHost => "unity_authority_host",
            UnityCatalogConfigKey::AuthorityId => "unity_authority_id",
            UnityCatalogConfigKey::ClientId => "unity_client_id",
            UnityCatalogConfigKey::ClientSecret => "unity_client_secret",
            UnityCatalogConfigKey::FederatedTokenFile => "unity_federated_token_file",
            UnityCatalogConfigKey::Host => "databricks_host",
            UnityCatalogConfigKey::MsiEndpoint => "unity_msi_endpoint",
            UnityCatalogConfigKey::MsiResourceId => "unity_msi_resource_id",
            UnityCatalogConfigKey::ObjectId => "unity_object_id",
            UnityCatalogConfigKey::UseAzureCli => "unity_use_azure_cli",
            UnityCatalogConfigKey::Token => "databricks_token",
            UnityCatalogConfigKey::WorkspaceUrl => "unity_workspace_url",
        }
    }
}

/// Builder for creating a UnityCatalogClient
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
    authority_id: Option<String>,

    /// Authority host
    authority_host: Option<String>,

    /// Msi endpoint for acquiring managed identity token
    msi_endpoint: Option<String>,

    /// Object id for use with managed identity authentication
    object_id: Option<String>,

    /// Msi resource id for use with managed identity authentication
    msi_resource_id: Option<String>,

    /// File containing token for Azure AD workload identity federation
    federated_token_file: Option<String>,

    /// When set to true, azure cli has to be used for acquiring access token
    use_azure_cli: bool,

    /// When set to true, http will be allowed in the catalog url
    allow_http_url: bool,

    /// Retry config
    retry_config: RetryConfig,

    /// Options for the underlying http client
    client_options: client::ClientOptions,
}

#[allow(deprecated)]
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
    ) -> DataCatalogResult<Self> {
        match UnityCatalogConfigKey::from_str(key.as_ref())? {
            UnityCatalogConfigKey::AccessToken => self.bearer_token = Some(value.into()),
            UnityCatalogConfigKey::AllowHttpUrl => {
                self.allow_http_url = str_is_truthy(&value.into())
            }
            UnityCatalogConfigKey::ClientId => self.client_id = Some(value.into()),
            UnityCatalogConfigKey::ClientSecret => self.client_secret = Some(value.into()),
            UnityCatalogConfigKey::AuthorityId => self.authority_id = Some(value.into()),
            UnityCatalogConfigKey::AuthorityHost => self.authority_host = Some(value.into()),
            UnityCatalogConfigKey::Host => self.workspace_url = Some(value.into()),
            UnityCatalogConfigKey::MsiEndpoint => self.msi_endpoint = Some(value.into()),
            UnityCatalogConfigKey::ObjectId => self.object_id = Some(value.into()),
            UnityCatalogConfigKey::MsiResourceId => self.msi_resource_id = Some(value.into()),
            UnityCatalogConfigKey::FederatedTokenFile => {
                self.federated_token_file = Some(value.into())
            }
            UnityCatalogConfigKey::Token => self.bearer_token = Some(value.into()),
            UnityCatalogConfigKey::UseAzureCli => self.use_azure_cli = str_is_truthy(&value.into()),
            UnityCatalogConfigKey::WorkspaceUrl => self.workspace_url = Some(value.into()),
        };
        Ok(self)
    }

    /// Hydrate builder from key value pairs
    pub fn try_with_options<I: IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>>(
        mut self,
        options: I,
    ) -> DataCatalogResult<Self> {
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
                    tracing::debug!("Found relevant env: {key}");
                    if let Ok(config_key) =
                        UnityCatalogConfigKey::from_str(&key.to_ascii_lowercase())
                    {
                        tracing::debug!("Trying: {key} with {value}");
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

    /// Sets the authority id for use service principal credential based authentication
    pub fn with_authority_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.authority_id = Some(tenant_id.into());
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
    pub fn with_client_options(mut self, options: client::ClientOptions) -> Self {
        self.client_options = options;
        self
    }

    /// Sets the retry config, overriding any already set
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    fn execute_uc_future<F, T>(future: F) -> DeltaResult<T>
    where
        T: Send,
        F: Future<Output = T> + Send,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    Ok(tokio::task::block_in_place(move || handle.block_on(future)))
                }
                _ => {
                    let mut cfg: Option<T> = None;
                    std::thread::scope(|scope| {
                        scope.spawn(|| {
                            cfg = Some(handle.block_on(future));
                        });
                    });
                    cfg.ok_or(DeltaTableError::ObjectStore {
                        source: ObjectStoreError::Generic {
                            store: STORE_NAME,
                            source: Box::new(UnityCatalogError::InitializationError),
                        },
                    })
                }
            },
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a tokio runtime is required by the Unity Catalog Builder");
                Ok(runtime.block_on(future))
            }
        }
    }

    /// Returns the storage location and temporary token to be used with the
    /// Unity Catalog table.
    pub async fn get_uc_location_and_token(
        table_uri: &str,
    ) -> Result<(String, HashMap<String, String>), UnityCatalogError> {
        let uri_parts: Vec<&str> = table_uri[5..].split('.').collect();
        if uri_parts.len() != 3 {
            return Err(UnityCatalogError::InvalidTableURI {
                table_uri: table_uri.to_string(),
            });
        }

        let catalog_id = uri_parts[0];
        let database_name = uri_parts[1];
        let table_name = uri_parts[2];

        let unity_catalog = UnityCatalogBuilder::from_env().build()?;
        let storage_location = unity_catalog
            .get_table_storage_location(Some(catalog_id.to_string()), database_name, table_name)
            .await?;
        let temp_creds_res = unity_catalog
            .get_temp_table_credentials(catalog_id, database_name, table_name)
            .await?;
        let credentials = match temp_creds_res {
            TableTempCredentialsResponse::Success(temp_creds) => temp_creds
                .get_credentials()
                .ok_or_else(|| UnityCatalogError::MissingCredential)?,
            TableTempCredentialsResponse::Error(_error) => {
                return Err(UnityCatalogError::TemporaryCredentialsFetchFailure)
            }
        };
        Ok((storage_location, credentials))
    }

    fn get_credential_provider(&self) -> Option<CredentialProvider> {
        if let Some(token) = self.bearer_token.as_ref() {
            return Some(CredentialProvider::BearerToken(token.clone()));
        }

        if let (Some(client_id), Some(client_secret), Some(workspace_host)) =
            (&self.client_id, &self.client_secret, &self.workspace_url)
        {
            return Some(CredentialProvider::TokenCredential(
                Default::default(),
                Box::new(WorkspaceOAuthProvider::new(
                    client_id,
                    client_secret,
                    workspace_host,
                )),
            ));
        }

        if let (Some(client_id), Some(client_secret), Some(authority_id)) = (
            self.client_id.as_ref(),
            self.client_secret.as_ref(),
            self.authority_id.as_ref(),
        ) {
            return Some(CredentialProvider::TokenCredential(
                Default::default(),
                Box::new(ClientSecretOAuthProvider::new(
                    client_id,
                    client_secret,
                    authority_id,
                    self.authority_host.as_ref(),
                )),
            ));
        }
        if self.use_azure_cli {
            return Some(CredentialProvider::TokenCredential(
                Default::default(),
                Box::new(AzureCliCredential::new()),
            ));
        }

        None
    }

    /// Build an instance of [`UnityCatalog`]
    pub fn build(self) -> DataCatalogResult<UnityCatalog> {
        let credential = self
            .get_credential_provider()
            .ok_or(UnityCatalogError::MissingCredential)?;

        let workspace_url = self
            .workspace_url
            .ok_or(UnityCatalogError::MissingConfiguration(
                "workspace_url".into(),
            ))?
            .trim_end_matches('/')
            .to_string();

        let client_options = if self.allow_http_url {
            self.client_options.with_allow_http(true)
        } else {
            self.client_options
        };
        let client = client_options.client()?;

        Ok(UnityCatalog {
            client,
            workspace_url,
            credential,
        })
    }
}

/// Databricks Unity Catalog
pub struct UnityCatalog {
    client: reqwest_middleware::ClientWithMiddleware,
    credential: CredentialProvider,
    workspace_url: String,
}

impl UnityCatalog {
    async fn get_credential(&self) -> Result<HeaderValue, UnityCatalogError> {
        match &self.credential {
            CredentialProvider::BearerToken(token) => {
                // we do the conversion to a HeaderValue here, since it is fallible,
                // and we want to use it in an infallible function
                Ok(HeaderValue::from_str(&format!("Bearer {token}"))?)
            }
            CredentialProvider::TokenCredential(cache, cred) => {
                let token = cache
                    .get_or_insert_with(|| cred.fetch_token(&self.client))
                    .await?;

                // we do the conversion to a HeaderValue here, since it is fallible,
                // and we want to use it in an infallible function
                Ok(HeaderValue::from_str(&format!("Bearer {token}"))?)
            }
        }
    }

    fn catalog_url(&self) -> String {
        format!("{}/api/2.1/unity-catalog", self.workspace_url)
    }

    /// Gets an array of catalogs in the metastore. If the caller is the metastore admin,
    /// all catalogs will be retrieved. Otherwise, only catalogs owned by the caller
    /// (or for which the caller has the USE_CATALOG privilege) will be retrieved.
    /// There is no guarantee of a specific ordering of the elements in the array.
    pub async fn list_catalogs(&self) -> Result<ListCatalogsResponse, UnityCatalogError> {
        let token = self.get_credential().await?;
        // https://docs.databricks.com/api-explorer/workspace/schemas/list
        let resp = self
            .client
            .get(format!("{}/catalogs", self.catalog_url()))
            .header(AUTHORIZATION, token)
            .send()
            .await?;
        Ok(resp.json().await?)
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
    ) -> Result<ListSchemasResponse, UnityCatalogError> {
        let token = self.get_credential().await?;
        // https://docs.databricks.com/api-explorer/workspace/schemas/list
        let resp = self
            .client
            .get(format!("{}/schemas", self.catalog_url()))
            .header(AUTHORIZATION, token)
            .query(&[("catalog_name", catalog_name.as_ref())])
            .send()
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
    ) -> Result<GetSchemaResponse, UnityCatalogError> {
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
            .send()
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
    ) -> Result<ListTableSummariesResponse, UnityCatalogError> {
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
            .send()
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
        catalog_id: impl AsRef<str>,
        database_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
    ) -> Result<GetTableResponse, UnityCatalogError> {
        let token = self.get_credential().await?;
        // https://docs.databricks.com/api-explorer/workspace/tables/get
        let resp = self
            .client
            .get(format!(
                "{}/tables/{}.{}.{}",
                self.catalog_url(),
                catalog_id.as_ref(),
                database_name.as_ref(),
                table_name.as_ref(),
            ))
            .header(AUTHORIZATION, token)
            .send()
            .await?;

        Ok(resp.json().await?)
    }

    pub async fn get_temp_table_credentials(
        &self,
        catalog_id: impl AsRef<str>,
        database_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
    ) -> Result<TableTempCredentialsResponse, UnityCatalogError> {
        let token = self.get_credential().await?;
        let table_info = self
            .get_table(catalog_id, database_name, table_name)
            .await?;
        let response = match table_info {
            GetTableResponse::Success(table) => {
                let request = TemporaryTableCredentialsRequest::new(&table.table_id, "READ");
                Ok(self
                    .client
                    .post(format!(
                        "{}/temporary-table-credentials",
                        self.catalog_url()
                    ))
                    .header(AUTHORIZATION, token)
                    .json(&request)
                    .send()
                    .await?)
            }
            GetTableResponse::Error(err) => Err(UnityCatalogError::InvalidTable {
                error_code: err.error_code,
                message: err.message,
            }),
        }?;

        Ok(response.json().await?)
    }
}

#[derive(Clone, Default, Debug)]
pub struct UnityCatalogFactory {}

impl ObjectStoreFactory for UnityCatalogFactory {
    fn parse_url_opts(
        &self,
        table_uri: &Url,
        options: &HashMap<String, String>,
        _retry: &RetryConfig,
        handle: Option<Handle>,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let (table_path, temp_creds) = UnityCatalogBuilder::execute_uc_future(
            UnityCatalogBuilder::get_uc_location_and_token(table_uri.as_str()),
        )??;

        let mut storage_options = options.clone();
        storage_options.extend(temp_creds);

        // TODO(roeap): we should not have to go through the table here.
        // ideally we just create the right storage ...
        let mut builder = DeltaTableBuilder::from_uri(&table_path);

        if let Some(handle) = handle {
            builder = builder.with_io_runtime(IORuntime::RT(handle));
        }
        if !storage_options.is_empty() {
            builder = builder.with_storage_options(storage_options.clone());
        }
        let prefix = Path::parse(table_uri.path())?;
        let store = builder.build_storage()?.object_store(None);

        Ok((store, prefix))
    }
}

impl LogStoreFactory for UnityCatalogFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register an [ObjectStoreFactory] for common UnityCatalogFactory [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(UnityCatalogFactory::default());
    let url = Url::parse("uc://").unwrap();
    object_store_factories().insert(url.clone(), factory.clone());
    logstore_factories().insert(url.clone(), factory.clone());
}

#[async_trait::async_trait]
impl DataCatalog for UnityCatalog {
    type Error = UnityCatalogError;
    /// Get the table storage location from the UnityCatalog
    async fn get_table_storage_location(
        &self,
        catalog_id: Option<String>,
        database_name: &str,
        table_name: &str,
    ) -> Result<String, UnityCatalogError> {
        match self
            .get_table(
                catalog_id.unwrap_or("main".into()),
                database_name,
                table_name,
            )
            .await?
        {
            GetTableResponse::Success(table) => Ok(table.storage_location),
            GetTableResponse::Error(err) => Err(UnityCatalogError::InvalidTable {
                error_code: err.error_code,
                message: err.message,
            }),
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
    use crate::client::ClientOptions;
    use crate::models::tests::{GET_SCHEMA_RESPONSE, GET_TABLE_RESPONSE, LIST_SCHEMAS_RESPONSE};
    use crate::models::*;
    use crate::UnityCatalogBuilder;
    use deltalake_core::DataCatalog;
    use httpmock::prelude::*;

    #[tokio::test]
    async fn test_unity_client() {
        let server = MockServer::start_async().await;

        let options = ClientOptions::default().with_allow_http(true);

        let client = UnityCatalogBuilder::new()
            .with_workspace_url(server.url(""))
            .with_bearer_token("bearer_token")
            .with_client_options(options)
            .build()
            .unwrap();

        server
            .mock_async(|when, then| {
                when.path("/api/2.1/unity-catalog/schemas").method("GET");
                then.body(LIST_SCHEMAS_RESPONSE);
            })
            .await;

        server
            .mock_async(|when, then| {
                when.path("/api/2.1/unity-catalog/schemas/catalog_name.schema_name")
                    .method("GET");
                then.body(GET_SCHEMA_RESPONSE);
            })
            .await;

        server
            .mock_async(|when, then| {
                when.path("/api/2.1/unity-catalog/tables/catalog_name.schema_name.table_name")
                    .method("GET");
                then.body(GET_TABLE_RESPONSE);
            })
            .await;

        let list_schemas_response = client.list_schemas("catalog_name").await.unwrap();
        assert!(matches!(
            list_schemas_response,
            ListSchemasResponse::Success { .. }
        ));

        let get_schema_response = client
            .get_schema("catalog_name", "schema_name")
            .await
            .unwrap();
        assert!(matches!(get_schema_response, GetSchemaResponse::Success(_)));

        let get_table_response = client
            .get_table("catalog_name", "schema_name", "table_name")
            .await;
        assert!(matches!(
            get_table_response.unwrap(),
            GetTableResponse::Success(_)
        ));

        let storage_location = client
            .get_table_storage_location(
                Some("catalog_name".to_string()),
                "schema_name",
                "table_name",
            )
            .await
            .unwrap();
        assert!(storage_location.eq_ignore_ascii_case("string"));
    }
}
