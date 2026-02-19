#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
//! Databricks Unity Catalog.
#[cfg(not(any(feature = "aws", feature = "azure", feature = "gcp", feature = "r2")))]
compile_error!(
    "At least one of the following crate features `aws`, `azure`, `gcp`, or `r2` must be enabled \
    for this crate to function properly."
);

use deltalake_core::logstore::{
    LogStore, LogStoreFactory, StorageConfig, default_logstore, logstore_factories,
    object_store::RetryConfig,
};
use reqwest::Url;
use reqwest::header::{AUTHORIZATION, HeaderValue, InvalidHeaderValue};
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use typed_builder::TypedBuilder;

use crate::credential::{
    AzureCliCredential, ClientSecretOAuthProvider, CredentialProvider, WorkspaceOAuthProvider,
};
use crate::models::{
    ErrorResponse, GetSchemaResponse, GetTableResponse, ListCatalogsResponse, ListSchemasResponse,
    ListTableSummariesResponse, TableTempCredentialsResponse, TemporaryTableCredentialsRequest,
    TokenErrorResponse,
};

use deltalake_core::data_catalog::DataCatalogResult;
use deltalake_core::{
    DataCatalog, DataCatalogError, DeltaResult, DeltaTableBuilder, DeltaTableError,
    ObjectStoreError, Path, ensure_table_uri,
};

use crate::client::retry::*;
use deltalake_core::logstore::{
    ObjectStoreFactory, ObjectStoreRef, config::str_is_truthy, object_store_factories,
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
    #[error("Unable to get temporary credentials from Unity Catalog: {error_code}: {message}")]
    TemporaryCredentialsFetchFailure {
        /// Error code from Unity Catalog
        error_code: String,
        /// Error message from Unity Catalog
        message: String,
    },

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

    #[error("Non-200 returned on token acquisition: {0}")]
    InvalidCredentials(TokenErrorResponse),
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
#[derive(TypedBuilder)]
#[builder(doc)]
pub struct UnityCatalogBuilder {
    /// Url of a Databricks workspace
    #[builder(default, setter(strip_option, into))]
    workspace_url: Option<String>,

    /// Bearer token
    #[builder(default, setter(strip_option, into))]
    bearer_token: Option<String>,

    /// Client id
    #[builder(default, setter(strip_option, into))]
    client_id: Option<String>,

    /// Client secret
    #[builder(default, setter(strip_option, into))]
    client_secret: Option<String>,

    /// Tenant id
    #[builder(default, setter(strip_option, into))]
    authority_id: Option<String>,

    /// Authority host
    #[builder(default, setter(strip_option, into))]
    authority_host: Option<String>,

    /// Msi endpoint for acquiring managed identity token
    #[builder(default, setter(strip_option, into))]
    msi_endpoint: Option<String>,

    /// Object id for use with managed identity authentication
    #[builder(default, setter(strip_option, into))]
    object_id: Option<String>,

    /// Msi resource id for use with managed identity authentication
    #[builder(default, setter(strip_option, into))]
    msi_resource_id: Option<String>,

    /// File containing token for Azure AD workload identity federation
    #[builder(default, setter(strip_option, into))]
    federated_token_file: Option<String>,

    /// When set to true, azure cli has to be used for acquiring access token
    #[builder(default)]
    use_azure_cli: bool,

    /// When set to true, http will be allowed in the catalog url
    #[builder(default)]
    allow_http_url: bool,

    /// Retry config
    #[builder(default)]
    #[allow(dead_code)]
    retry_config: RetryConfig,

    /// Options for the underlying http client
    #[builder(default)]
    client_options: client::ClientOptions,
}

#[allow(deprecated)]
impl UnityCatalogBuilder {
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
        let mut builder = Self::builder().build();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str())
                && (key.starts_with("UNITY_") || key.starts_with("DATABRICKS_"))
            {
                tracing::debug!("Found relevant env: {key}");
                if let Ok(config_key) = UnityCatalogConfigKey::from_str(&key.to_ascii_lowercase()) {
                    tracing::debug!("Trying: {key} with {value}");
                    builder = builder.try_with_option(config_key, value).unwrap();
                }
            }
        }

        builder
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

    /// Returns the storage location and temporary token for the Unity Catalog table.
    ///
    /// If storage options are provided, they override environment variables for authentication.
    pub async fn get_uc_location_and_token(
        table_uri: &str,
        storage_options: Option<&HashMap<String, String>>,
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

        let unity_catalog = if let Some(options) = storage_options {
            let mut builder = UnityCatalogBuilder::from_env();
            builder =
                builder.try_with_options(options.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;
            builder.build()?
        } else {
            UnityCatalogBuilder::from_env().build()?
        };

        let storage_location = unity_catalog
            .get_table_storage_location(Some(catalog_id.to_string()), database_name, table_name)
            .await?;
        // Attempt to get read/write permissions to begin with.
        let temp_creds_res = unity_catalog
            .get_temp_table_credentials_with_permission(
                catalog_id,
                database_name,
                table_name,
                "READ_WRITE",
            )
            .await?;
        let credentials = match temp_creds_res {
            TableTempCredentialsResponse::Success(temp_creds) => temp_creds
                .get_credentials()
                .ok_or_else(|| UnityCatalogError::MissingCredential)?,
            TableTempCredentialsResponse::Error(rw_error) => {
                // If that fails attempt to get just read permissions.
                match unity_catalog
                    .get_temp_table_credentials(catalog_id, database_name, table_name)
                    .await?
                {
                    TableTempCredentialsResponse::Success(temp_creds) => temp_creds
                        .get_credentials()
                        .ok_or_else(|| UnityCatalogError::MissingCredential)?,
                    TableTempCredentialsResponse::Error(read_error) => {
                        return Err(UnityCatalogError::TemporaryCredentialsFetchFailure {
                            error_code: read_error.error_code,
                            message: format!(
                                "READ_WRITE failed: {}. READ failed: {}",
                                rw_error.message, read_error.message
                            ),
                        });
                    }
                }
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

        let mut client_options = self.client_options;
        if self.allow_http_url {
            client_options.allow_http = true;
        }
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
        let status = resp.status();
        let body = resp.text().await?;
        serde_json::from_str(&body).map_err(|e| {
            tracing::error!(
                "Failed to parse list_schemas response (status {}): {}",
                status,
                body
            );
            UnityCatalogError::Generic {
                source: Box::new(e),
            }
        })
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
        self.get_temp_table_credentials_with_permission(
            catalog_id,
            database_name,
            table_name,
            "READ",
        )
        .await
    }

    pub async fn get_temp_table_credentials_with_permission(
        &self,
        catalog_id: impl AsRef<str>,
        database_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        operation: impl AsRef<str>,
    ) -> Result<TableTempCredentialsResponse, UnityCatalogError> {
        let token = self.get_credential().await?;
        let table_info = self
            .get_table(catalog_id, database_name, table_name)
            .await?;
        let response = match table_info {
            GetTableResponse::Success(table) => {
                let request =
                    TemporaryTableCredentialsRequest::new(&table.table_id, operation.as_ref());
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
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let (table_path, temp_creds) = UnityCatalogBuilder::execute_uc_future(
            UnityCatalogBuilder::get_uc_location_and_token(table_uri.as_str(), Some(&config.raw)),
        )??;

        let mut storage_options = config.raw.clone();
        storage_options.extend(temp_creds);

        // TODO(roeap): we should not have to go through the table here.
        // ideally we just create the right storage ...
        let table_url = ensure_table_uri(&table_path)?;
        let mut builder = DeltaTableBuilder::from_url(table_url)?;

        if let Some(runtime) = &config.runtime {
            builder = builder.with_io_runtime(runtime.clone());
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
    use crate::UnityCatalogBuilder;
    use crate::client::ClientOptions;
    use crate::models::tests::{GET_SCHEMA_RESPONSE, GET_TABLE_RESPONSE, LIST_SCHEMAS_RESPONSE};
    use crate::models::*;
    use deltalake_core::DataCatalog;
    use httpmock::prelude::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_unity_client() {
        let server = MockServer::start_async().await;

        let options = ClientOptions::builder().allow_http(true).build();

        let client = UnityCatalogBuilder::builder()
            .workspace_url(server.url(""))
            .bearer_token("bearer_token")
            .client_options(options)
            .build()
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

    #[test]
    fn test_unitycatalogbuilder_with_storage_options() {
        let mut storage_options = HashMap::new();
        storage_options.insert(
            "databricks_host".to_string(),
            "https://test.databricks.com".to_string(),
        );
        storage_options.insert("databricks_token".to_string(), "test_token".to_string());

        let builder = UnityCatalogBuilder::builder()
            .build()
            .try_with_options(&storage_options)
            .unwrap();

        assert_eq!(
            builder.workspace_url,
            Some("https://test.databricks.com".to_string())
        );
        assert_eq!(builder.bearer_token, Some("test_token".to_string()));
    }

    #[test]
    fn test_unitycatalogbuilder_client_credentials() {
        let mut storage_options = HashMap::new();
        storage_options.insert(
            "databricks_host".to_string(),
            "https://test.databricks.com".to_string(),
        );
        storage_options.insert("unity_client_id".to_string(), "test_client_id".to_string());
        storage_options.insert("unity_client_secret".to_string(), "test_secret".to_string());
        storage_options.insert("unity_authority_id".to_string(), "test_tenant".to_string());

        let builder = UnityCatalogBuilder::builder()
            .build()
            .try_with_options(&storage_options)
            .unwrap();

        assert_eq!(
            builder.workspace_url,
            Some("https://test.databricks.com".to_string())
        );
        assert_eq!(builder.client_id, Some("test_client_id".to_string()));
        assert_eq!(builder.client_secret, Some("test_secret".to_string()));
        assert_eq!(builder.authority_id, Some("test_tenant".to_string()));
    }

    #[test]
    fn test_env_with_storage_options_override() {
        unsafe {
            std::env::set_var("DATABRICKS_HOST", "https://env.databricks.com");
            std::env::set_var("DATABRICKS_TOKEN", "env_token");
        }

        let mut storage_options = HashMap::new();
        storage_options.insert(
            "databricks_host".to_string(),
            "https://override.databricks.com".to_string(),
        );

        let builder = UnityCatalogBuilder::from_env()
            .try_with_options(&storage_options)
            .unwrap();

        assert_eq!(
            builder.workspace_url,
            Some("https://override.databricks.com".to_string())
        );
        assert_eq!(builder.bearer_token, Some("env_token".to_string()));

        unsafe {
            std::env::remove_var("DATABRICKS_HOST");
            std::env::remove_var("DATABRICKS_TOKEN");
        }
    }

    #[test]
    fn test_storage_options_key_variations() {
        let test_cases = vec![
            ("databricks_host", "workspace_url"),
            ("unity_workspace_url", "workspace_url"),
            ("databricks_workspace_url", "workspace_url"),
            ("databricks_token", "bearer_token"),
            ("token", "bearer_token"),
            ("unity_client_id", "client_id"),
            ("databricks_client_id", "client_id"),
            ("client_id", "client_id"),
        ];

        for (key, field) in test_cases {
            let mut storage_options = HashMap::new();
            let test_value = format!("test_value_for_{}", key);
            storage_options.insert(key.to_string(), test_value.clone());

            let result = UnityCatalogBuilder::builder()
                .build()
                .try_with_options(&storage_options);
            assert!(result.is_ok(), "Failed to parse key: {}", key);

            let builder = result.unwrap();
            match field {
                "workspace_url" => assert_eq!(builder.workspace_url, Some(test_value)),
                "bearer_token" => assert_eq!(builder.bearer_token, Some(test_value)),
                "client_id" => assert_eq!(builder.client_id, Some(test_value)),
                _ => {}
            }
        }
    }

    #[test]
    fn test_invalid_config_key() {
        let mut storage_options = HashMap::new();
        storage_options.insert("invalid_key".to_string(), "test_value".to_string());

        let result = UnityCatalogBuilder::builder()
            .build()
            .try_with_options(&storage_options);
        assert!(result.is_err());
    }

    #[test]
    fn test_boolean_options() {
        let test_cases = vec![
            ("true", true),
            ("false", false),
            ("1", true),
            ("0", false),
            ("yes", true),
            ("no", false),
        ];

        for (value, expected) in test_cases {
            let mut storage_options = HashMap::new();
            storage_options.insert("unity_allow_http_url".to_string(), value.to_string());
            storage_options.insert("unity_use_azure_cli".to_string(), value.to_string());

            let builder = UnityCatalogBuilder::builder()
                .build()
                .try_with_options(&storage_options)
                .unwrap();

            assert_eq!(
                builder.allow_http_url, expected,
                "Failed for value: {}",
                value
            );
            assert_eq!(
                builder.use_azure_cli, expected,
                "Failed for value: {}",
                value
            );
        }
    }

    #[tokio::test]
    async fn test_invalid_table_uri() {
        let test_cases = vec![
            "uc://invalid",
            "uc://",
            "uc://catalog",
            "uc://catalog.schema",
            "uc://catalog.schema.table.extra",
            "invalid://catalog.schema.table",
        ];

        for uri in test_cases {
            let result = UnityCatalogBuilder::get_uc_location_and_token(uri, None).await;
            assert!(result.is_err(), "Expected error for URI: {}", uri);

            if let Err(e) = result
                && uri.starts_with("uc://")
                && uri.len() > 5
            {
                assert!(matches!(
                    e,
                    crate::UnityCatalogError::InvalidTableURI { .. }
                ));
            }
        }
    }
}
