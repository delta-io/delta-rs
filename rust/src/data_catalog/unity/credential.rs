//! Authorization credentials

use std::time::{Duration, Instant};

use reqwest::header::{HeaderValue, ACCEPT};
use reqwest::{Client, Method};
use serde::Deserialize;

use crate::data_catalog::client::retry::{RetryConfig, RetryExt};
use crate::data_catalog::client::token::{TemporaryToken, TokenCache};
use crate::data_catalog::CatalogResult;

// https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication

const DATABRICKS_RESOURCE_SCOPE: &str = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default";
const CONTENT_TYPE_JSON: &str = "application/json";

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

/// Trait for providing authrization tokens for catalog requests
#[async_trait::async_trait]
pub trait TokenCredential: std::fmt::Debug + Send + Sync + 'static {
    /// get the token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> CatalogResult<TemporaryToken<String>>;
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
        client_id: String,
        client_secret: String,
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
            client_id,
            client_secret,
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
    ) -> CatalogResult<TemporaryToken<String>> {
        let response: TokenResponse = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", DATABRICKS_RESOURCE_SCOPE),
                ("grant_type", "client_credentials"),
            ])
            .send_retry(retry)
            .await?
            .json()
            .await?;

        let token = TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        };

        Ok(token)
    }
}

fn _expires_in_string<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let v = String::deserialize(deserializer)?;
    v.parse::<u64>().map_err(serde::de::Error::custom)
}
