//! Authorization credentials

use std::process::Command;
use std::time::{Duration, Instant};

use reqwest::header::{HeaderValue, ACCEPT};
use reqwest::{Client, Method};
use serde::Deserialize;

use super::UnityCatalogError;
use crate::data_catalog::client::retry::{RetryConfig, RetryExt};
use crate::data_catalog::client::token::{TemporaryToken, TokenCache};
use crate::data_catalog::DataCatalogResult;

// https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication

const DATABRICKS_RESOURCE_SCOPE: &str = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";
const CONTENT_TYPE_JSON: &str = "application/json";
const MSI_SECRET_ENV_KEY: &str = "IDENTITY_HEADER";
const MSI_API_VERSION: &str = "2019-08-01";

/// A list of known Azure authority hosts
pub mod authority_hosts {
    /// China-based Azure Authority Host
    pub const AZURE_CHINA: &str = "https://login.chinacloudapi.cn";
    /// Germany-based Azure Authority Host
    pub const AZURE_GERMANY: &str = "https://login.microsoftonline.de";
    /// US Government Azure Authority Host
    pub const AZURE_GOVERNMENT: &str = "https://login.microsoftonline.us";
    /// Public Cloud Azure Authority Host
    pub const AZURE_PUBLIC_CLOUD: &str = "https://login.microsoftonline.com";
}

/// Trait for providing authorization tokens for catalog requests
#[async_trait::async_trait]
pub trait TokenCredential: std::fmt::Debug + Send + Sync + 'static {
    /// get the token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> DataCatalogResult<TemporaryToken<String>>;
}

/// Provides credentials for use when signing requests
#[derive(Debug)]
pub enum CredentialProvider {
    /// static bearer token
    BearerToken(String),

    /// a credential to fetch expiring auth tokens
    TokenCredential(TokenCache<String>, Box<dyn TokenCredential>),
}

#[derive(Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Encapsulates the logic to perform an OAuth token challenge
#[derive(Debug, Clone)]
pub struct ClientSecretOAuthProvider {
    token_url: String,
    client_id: String,
    client_secret: String,
}

impl ClientSecretOAuthProvider {
    /// Create a new [`ClientSecretOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        authority_id: impl AsRef<str>,
        authority_host: Option<impl Into<String>>,
    ) -> Self {
        let authority_host = authority_host
            .map(|h| h.into())
            .unwrap_or_else(|| authority_hosts::AZURE_PUBLIC_CLOUD.to_owned());

        Self {
            token_url: format!(
                "{}/{}/oauth2/v2.0/token",
                authority_host,
                authority_id.as_ref()
            ),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for ClientSecretOAuthProvider {
    /// Fetch a token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> DataCatalogResult<TemporaryToken<String>> {
        let response: TokenResponse = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", &format!("{}/.default", DATABRICKS_RESOURCE_SCOPE)),
                ("grant_type", "client_credentials"),
            ])
            .send_retry(retry)
            .await?
            .json()
            .await?;

        Ok(TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

mod az_cli_date_format {
    use chrono::{DateTime, TimeZone};
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<chrono::Local>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // expiresOn from azure cli uses the local timezone
        let date = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S.%6f")
            .map_err(serde::de::Error::custom)?;
        chrono::Local
            .from_local_datetime(&date)
            .single()
            .ok_or(serde::de::Error::custom(
                "azure cli returned ambiguous expiry date",
            ))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AzureCliTokenResponse {
    pub access_token: String,
    #[serde(with = "az_cli_date_format")]
    pub expires_on: chrono::DateTime<chrono::Local>,
    pub token_type: String,
}

/// Credential for acquiring access tokens via the Azure CLI
#[derive(Default, Debug)]
pub struct AzureCliCredential {
    _private: (),
}

impl AzureCliCredential {
    /// Create a new instance of [`AzureCliCredential`]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl TokenCredential for AzureCliCredential {
    /// Fetch a token
    async fn fetch_token(
        &self,
        _client: &Client,
        _retry: &RetryConfig,
    ) -> DataCatalogResult<TemporaryToken<String>> {
        // on window az is a cmd and it should be called like this
        // see https://doc.rust-lang.org/nightly/std/process/struct.Command.html
        let program = if cfg!(target_os = "windows") {
            "cmd"
        } else {
            "az"
        };
        let mut args = Vec::new();
        if cfg!(target_os = "windows") {
            args.push("/C");
            args.push("az");
        }
        args.push("account");
        args.push("get-access-token");
        args.push("--output");
        args.push("json");
        args.push("--resource");
        args.push(DATABRICKS_RESOURCE_SCOPE);

        match Command::new(program).args(args).output() {
            Ok(az_output) if az_output.status.success() => {
                let output = std::str::from_utf8(&az_output.stdout).map_err(|_| {
                    UnityCatalogError::AzureCli {
                        message: "az response is not a valid utf-8 string".to_string(),
                    }
                })?;

                let token_response = serde_json::from_str::<AzureCliTokenResponse>(output)
                    .map_err(|err| UnityCatalogError::AzureCli {
                        message: format!("failed seserializing token response: {:?}", err),
                    })?;
                if !token_response.token_type.eq_ignore_ascii_case("bearer") {
                    return Err(UnityCatalogError::AzureCli {
                        message: format!(
                            "got unexpected token type from azure cli: {0}",
                            token_response.token_type
                        ),
                    }
                    .into());
                }
                let duration =
                    token_response.expires_on.naive_local() - chrono::Local::now().naive_local();
                Ok(TemporaryToken {
                    token: token_response.access_token,
                    expiry: Some(
                        Instant::now()
                            + duration.to_std().map_err(|_| UnityCatalogError::AzureCli {
                                message: "az returned invalid lifetime".to_string(),
                            })?,
                    ),
                })
            }
            Ok(az_output) => {
                let message = String::from_utf8_lossy(&az_output.stderr);
                Err(UnityCatalogError::AzureCli {
                    message: message.into(),
                }
                .into())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => Err(UnityCatalogError::AzureCli {
                    message: "Azure Cli not installed".into(),
                }
                .into()),
                error_kind => Err(UnityCatalogError::AzureCli {
                    message: format!("io error: {error_kind:?}"),
                }
                .into()),
            },
        }
    }
}

/// Credential for using workload identity dfederation
///
/// <https://learn.microsoft.com/en-us/azure/active-directory/develop/workload-identity-federation>
#[derive(Debug)]
pub struct WorkloadIdentityOAuthProvider {
    token_url: String,
    client_id: String,
    federated_token_file: String,
}

impl WorkloadIdentityOAuthProvider {
    /// Create a new [`WorkloadIdentityOAuthProvider`]
    pub fn new(
        client_id: impl Into<String>,
        federated_token_file: impl Into<String>,
        tenant_id: impl AsRef<str>,
        authority_host: Option<String>,
    ) -> Self {
        let authority_host =
            authority_host.unwrap_or_else(|| authority_hosts::AZURE_PUBLIC_CLOUD.to_owned());

        Self {
            token_url: format!(
                "{}/{}/oauth2/v2.0/token",
                authority_host,
                tenant_id.as_ref()
            ),
            client_id: client_id.into(),
            federated_token_file: federated_token_file.into(),
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for WorkloadIdentityOAuthProvider {
    /// Fetch a token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> DataCatalogResult<TemporaryToken<String>> {
        let token_str = std::fs::read_to_string(&self.federated_token_file)
            .map_err(|_| UnityCatalogError::FederatedTokenFile)?;

        // https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#third-case-access-token-request-with-a-federated-credential
        let response: TokenResponse = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", token_str.as_str()),
                ("scope", &format!("{}/.default", DATABRICKS_RESOURCE_SCOPE)),
                ("grant_type", "client_credentials"),
            ])
            .send_retry(retry)
            .await?
            .json()
            .await?;

        Ok(TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

fn expires_in_string<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let v = String::deserialize(deserializer)?;
    v.parse::<u64>().map_err(serde::de::Error::custom)
}

// NOTE: expires_on is a String version of unix epoch time, not an integer.
// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
#[derive(Debug, Clone, Deserialize)]
struct MsiTokenResponse {
    pub access_token: String,
    #[serde(deserialize_with = "expires_in_string")]
    pub expires_in: u64,
}

/// Attempts authentication using a managed identity that has been assigned to the deployment environment.
///
/// This authentication type works in Azure VMs, App Service and Azure Functions applications, as well as the Azure Cloud Shell
/// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
#[derive(Debug)]
pub struct ImdsManagedIdentityOAuthProvider {
    msi_endpoint: String,
    client_id: Option<String>,
    object_id: Option<String>,
    msi_res_id: Option<String>,
    client: Client,
}

impl ImdsManagedIdentityOAuthProvider {
    /// Create a new [`ImdsManagedIdentityOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: Option<String>,
        object_id: Option<String>,
        msi_res_id: Option<String>,
        msi_endpoint: Option<String>,
        client: Client,
    ) -> Self {
        let msi_endpoint = msi_endpoint
            .unwrap_or_else(|| "http://169.254.169.254/metadata/identity/oauth2/token".to_owned());

        Self {
            msi_endpoint,
            client_id,
            object_id,
            msi_res_id,
            client,
        }
    }
}

#[async_trait::async_trait]
impl TokenCredential for ImdsManagedIdentityOAuthProvider {
    /// Fetch a token
    async fn fetch_token(
        &self,
        _client: &Client,
        retry: &RetryConfig,
    ) -> DataCatalogResult<TemporaryToken<String>> {
        let resource_scope = format!("{}/.default", DATABRICKS_RESOURCE_SCOPE);
        let mut query_items = vec![
            ("api-version", MSI_API_VERSION),
            ("resource", &resource_scope),
        ];

        let mut identity = None;
        if let Some(client_id) = &self.client_id {
            identity = Some(("client_id", client_id));
        }
        if let Some(object_id) = &self.object_id {
            identity = Some(("object_id", object_id));
        }
        if let Some(msi_res_id) = &self.msi_res_id {
            identity = Some(("msi_res_id", msi_res_id));
        }
        if let Some((key, value)) = identity {
            query_items.push((key, value));
        }

        let mut builder = self
            .client
            .request(Method::GET, &self.msi_endpoint)
            .header("metadata", "true")
            .query(&query_items);

        if let Ok(val) = std::env::var(MSI_SECRET_ENV_KEY) {
            builder = builder.header("x-identity-header", val);
        };

        let response: MsiTokenResponse = builder.send_retry(retry).await?.json().await?;

        Ok(TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_catalog::client::mock_server::MockServer;
    use futures::executor::block_on;
    use hyper::body::to_bytes;
    use hyper::{Body, Response};
    use reqwest::{Client, Method};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_managed_identity() {
        let server = MockServer::new();

        std::env::set_var(MSI_SECRET_ENV_KEY, "env-secret");

        let endpoint = server.url();
        let client = Client::new();
        let retry_config = RetryConfig::default();

        // Test IMDS
        server.push_fn(|req| {
            assert_eq!(req.uri().path(), "/metadata/identity/oauth2/token");
            assert!(req.uri().query().unwrap().contains("client_id=client_id"));
            assert_eq!(req.method(), &Method::GET);
            let t = req
                .headers()
                .get("x-identity-header")
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(t, "env-secret");
            let t = req.headers().get("metadata").unwrap().to_str().unwrap();
            assert_eq!(t, "true");
            Response::new(Body::from(
                r#"
            {
                "access_token": "TOKEN",
                "refresh_token": "",
                "expires_in": "3599",
                "expires_on": "1506484173",
                "not_before": "1506480273",
                "resource": "https://management.azure.com/",
                "token_type": "Bearer"
              }
            "#,
            ))
        });

        let credential = ImdsManagedIdentityOAuthProvider::new(
            Some("client_id".into()),
            None,
            None,
            Some(format!("{endpoint}/metadata/identity/oauth2/token")),
            client.clone(),
        );

        let token = credential
            .fetch_token(&client, &retry_config)
            .await
            .unwrap();

        assert_eq!(&token.token, "TOKEN");
    }

    #[tokio::test]
    async fn test_workload_identity() {
        let server = MockServer::new();
        let tokenfile = NamedTempFile::new().unwrap();
        let tenant = "tenant";
        std::fs::write(tokenfile.path(), "federated-token").unwrap();

        let endpoint = server.url();
        let client = Client::new();
        let retry_config = RetryConfig::default();

        // Test IMDS
        server.push_fn(move |req| {
            assert_eq!(req.uri().path(), format!("/{tenant}/oauth2/v2.0/token"));
            assert_eq!(req.method(), &Method::POST);
            let body = block_on(to_bytes(req.into_body())).unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains("federated-token"));
            Response::new(Body::from(
                r#"
            {
                "access_token": "TOKEN",
                "refresh_token": "",
                "expires_in": 3599,
                "expires_on": "1506484173",
                "not_before": "1506480273",
                "resource": "https://management.azure.com/",
                "token_type": "Bearer"
              }
            "#,
            ))
        });

        let credential = WorkloadIdentityOAuthProvider::new(
            "client_id",
            tokenfile.path().to_str().unwrap(),
            tenant,
            Some(endpoint.to_string()),
        );

        let token = credential
            .fetch_token(&client, &retry_config)
            .await
            .unwrap();

        assert_eq!(&token.token, "TOKEN");
    }
}
