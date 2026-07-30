//! Object store credentials backed by Unity Catalog's temporary tokens.
//!
//! Unity Catalog vends credentials that expire (~1 hour). Handing them to an
//! object store as static options freezes them at table open, so a store that
//! outlives the token starts failing. Instead we give the store a credential
//! provider it can re-ask, which re-vends from Unity Catalog shortly before the
//! cached token expires.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use deltalake_core::logstore::CloudCredentialProvider;
use deltalake_core::logstore::object_store::aws::AwsCredential;
use deltalake_core::logstore::object_store::azure::AzureCredential;
use deltalake_core::logstore::object_store::gcp::GcpCredential;
use deltalake_core::logstore::object_store::{
    CredentialProvider, Error as ObjectStoreError, Result as ObjectStoreResult,
};
use reqwest::Url;
use tokio::sync::Mutex;

use crate::models::{TableTempCredentialsResponse, TemporaryTableCredentials};
use crate::{UnityCatalog, UnityCatalogError};

/// Re-vend this long before the token expires, so we never hand out a credential
/// that lapses mid-request.
const EXPIRY_SKEW: Duration = Duration::seconds(60);

/// Floor on how often we re-vend. An `expiration_time` that is already past
/// (client clock skew, or an unusually short token) would otherwise send one
/// request to the rate-limited Unity Catalog API per object store operation.
const MIN_REVEND_INTERVAL: Duration = Duration::seconds(5);

/// Build a credential provider for the cloud hosting `table_url`.
///
/// Returns `None` for schemes Unity Catalog does not vend credentials for, so the
/// caller keeps whatever credentials it already resolved.
pub(crate) fn provider_for(
    table_url: &Url,
    table_uri: &str,
    catalog: UnityCatalog,
) -> Result<Option<CloudCredentialProvider>, UnityCatalogError> {
    let scheme = table_url.scheme();
    if !matches!(
        scheme,
        "s3" | "s3a" | "gs" | "az" | "adl" | "azure" | "abfs" | "abfss"
    ) {
        return Ok(None);
    }

    let source = Arc::new(TempCredentials::new(table_uri, catalog)?);
    Ok(Some(match scheme {
        "s3" | "s3a" => CloudCredentialProvider::Aws(Arc::new(Aws(source))),
        "gs" => CloudCredentialProvider::Gcp(Arc::new(Gcp(source))),
        _ => CloudCredentialProvider::Azure(Arc::new(Azure(source))),
    }))
}

/// Vends Unity Catalog's temporary credentials for one table, caching the token
/// until it nears expiry.
struct TempCredentials {
    catalog: UnityCatalog,
    catalog_id: String,
    database_name: String,
    table_name: String,
    cache: Mutex<Option<Cached>>,
}

struct Cached {
    credentials: Arc<TemporaryTableCredentials>,
    /// Reuse the token until this instant, then re-vend.
    refresh_after: DateTime<Utc>,
}

impl std::fmt::Debug for TempCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the cached token.
        f.debug_struct("TempCredentials")
            .field("catalog_id", &self.catalog_id)
            .field("database_name", &self.database_name)
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

/// When to re-vend a token handed out at `vended_at`: shortly before it expires,
/// but never more often than [`MIN_REVEND_INTERVAL`].
fn refresh_after(vended_at: DateTime<Utc>, expires_at: DateTime<Utc>) -> DateTime<Utc> {
    (expires_at - EXPIRY_SKEW).max(vended_at + MIN_REVEND_INTERVAL)
}

impl TempCredentials {
    fn new(table_uri: &str, catalog: UnityCatalog) -> Result<Self, UnityCatalogError> {
        let parts: Vec<&str> = table_uri
            .strip_prefix("uc://")
            .unwrap_or(table_uri)
            .split('.')
            .collect();
        let [catalog_id, database_name, table_name] = parts[..] else {
            return Err(UnityCatalogError::InvalidTableURI {
                table_uri: table_uri.to_string(),
            });
        };
        Ok(Self {
            catalog,
            catalog_id: catalog_id.to_string(),
            database_name: database_name.to_string(),
            table_name: table_name.to_string(),
            cache: Mutex::new(None),
        })
    }

    /// The cached token, re-vending first if it is due for refresh.
    async fn current(&self) -> ObjectStoreResult<Arc<TemporaryTableCredentials>> {
        // Held across the vend on purpose: concurrent requests wait for one
        // refresh rather than each issuing their own.
        let mut guard = self.cache.lock().await;

        if let Some(cached) = guard.as_ref()
            && Utc::now() < cached.refresh_after
        {
            return Ok(Arc::clone(&cached.credentials));
        }

        let vended = self.vend().await.map_err(|e| ObjectStoreError::Generic {
            store: "UnityCatalog",
            source: Box::new(e),
        })?;
        let refresh_after = refresh_after(Utc::now(), vended.expiration_time);
        let credentials = Arc::new(vended);
        *guard = Some(Cached {
            credentials: Arc::clone(&credentials),
            refresh_after,
        });
        Ok(credentials)
    }

    /// Ask Unity Catalog for a fresh token, preferring read/write and falling
    /// back to read-only.
    async fn vend(&self) -> Result<TemporaryTableCredentials, UnityCatalogError> {
        let rw = self
            .catalog
            .get_temp_table_credentials_with_permission(
                &self.catalog_id,
                &self.database_name,
                &self.table_name,
                "READ_WRITE",
            )
            .await?;
        let rw_error = match rw {
            TableTempCredentialsResponse::Success(credentials) => return Ok(credentials),
            TableTempCredentialsResponse::Error(err) => err,
        };

        match self
            .catalog
            .get_temp_table_credentials(&self.catalog_id, &self.database_name, &self.table_name)
            .await?
        {
            TableTempCredentialsResponse::Success(credentials) => Ok(credentials),
            TableTempCredentialsResponse::Error(read_error) => {
                Err(UnityCatalogError::TemporaryCredentialsFetchFailure {
                    error_code: read_error.error_code,
                    message: format!(
                        "READ_WRITE failed: {}. READ failed: {}",
                        rw_error.message, read_error.message
                    ),
                })
            }
        }
    }
}

fn missing(cloud: &'static str) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "UnityCatalog",
        source: Box::new(UnityCatalogError::Generic {
            source: format!("Unity Catalog vended no {cloud} credentials for this table").into(),
        }),
    }
}

// One `CredentialProvider` per cloud, since the trait fixes a single credential
// type. All three read the same cached token.
#[derive(Debug)]
struct Aws(Arc<TempCredentials>);
#[derive(Debug)]
struct Azure(Arc<TempCredentials>);
#[derive(Debug)]
struct Gcp(Arc<TempCredentials>);

#[async_trait]
impl CredentialProvider for Aws {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> ObjectStoreResult<Arc<AwsCredential>> {
        let vended = self.0.current().await?;

        if let Some(aws) = vended.aws_temp_credentials.clone() {
            return Ok(Arc::new(AwsCredential {
                key_id: aws.access_key_id,
                secret_key: aws.secret_access_key,
                token: aws.session_token,
            }));
        }

        // R2 is S3-compatible and reports an s3:// location, but vends under its
        // own key. Mirrors the fallback order in `get_credentials`.
        #[cfg(feature = "r2")]
        if let Some(r2) = vended.r2_temp_credentials.clone() {
            return Ok(Arc::new(AwsCredential {
                key_id: r2.access_key_id,
                secret_key: r2.secret_access_key,
                token: Some(r2.session_token),
            }));
        }

        Err(missing("AWS"))
    }
}

#[async_trait]
impl CredentialProvider for Azure {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> ObjectStoreResult<Arc<AzureCredential>> {
        let sas = self
            .0
            .current()
            .await?
            .azure_user_delegation_sas
            .clone()
            .ok_or_else(|| missing("Azure"))?;
        Ok(Arc::new(AzureCredential::SASToken(split_sas(
            &sas.sas_token,
        ))))
    }
}

#[async_trait]
impl CredentialProvider for Gcp {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> ObjectStoreResult<Arc<GcpCredential>> {
        let token = self
            .0
            .current()
            .await?
            .gcp_oauth_token
            .clone()
            .ok_or_else(|| missing("GCP"))?;
        Ok(Arc::new(GcpCredential {
            bearer: token.oauth_token,
        }))
    }
}

/// Split a SAS token into the decoded query pairs `object_store` expects.
fn split_sas(sas: &str) -> Vec<(String, String)> {
    let mut url = Url::parse("http://sas/").expect("static url");
    url.set_query(Some(sas.trim_start_matches('?')));
    url.query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnityCatalogBuilder;
    use crate::client::ClientOptions;
    use crate::models::tests::GET_TABLE_RESPONSE;
    use httpmock::Mock;
    use httpmock::prelude::*;

    const TABLE_PATH: &str = "/api/2.1/unity-catalog/tables/catalog_name.schema_name.table_name";
    const CREDS_PATH: &str = "/api/2.1/unity-catalog/temporary-table-credentials";

    /// Mock the table lookup and the credential vend, returning the vend mock so
    /// tests can count how often Unity Catalog was asked.
    async fn mock_catalog(server: &MockServer, expires_at: DateTime<Utc>) -> Mock<'_> {
        server
            .mock_async(|when, then| {
                when.path(TABLE_PATH).method(GET);
                then.body(GET_TABLE_RESPONSE);
            })
            .await;
        server
            .mock_async(|when, then| {
                when.path(CREDS_PATH).method(POST);
                then.json_body_obj(&serde_json::json!({
                    "aws_temp_credentials": {
                        "access_key_id": "AKIATEST",
                        "secret_access_key": "secret",
                        "session_token": "session-token",
                    },
                    "azure_user_delegation_sas": { "sas_token": "sv=2021-08-06&sig=a%2Bb" },
                    "gcp_oauth_token": { "oauth_token": "ya29.token" },
                    "expiration_time": expires_at.timestamp_millis(),
                    "url": "s3://bucket/table",
                }));
            })
            .await
    }

    fn source_for(server: &MockServer) -> Arc<TempCredentials> {
        let catalog = UnityCatalogBuilder::builder()
            .workspace_url(server.url(""))
            .bearer_token("bearer_token")
            .client_options(ClientOptions::builder().allow_http(true).build())
            .build()
            .build()
            .unwrap();
        Arc::new(TempCredentials::new("uc://catalog_name.schema_name.table_name", catalog).unwrap())
    }

    /// Force the next lookup to re-vend, standing in for time passing.
    async fn expire_cache(source: &TempCredentials) {
        source.cache.lock().await.as_mut().unwrap().refresh_after = Utc::now();
    }

    #[test]
    fn refresh_leads_expiry_but_is_floored() {
        let now = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let hour = Duration::hours(1);

        assert_eq!(refresh_after(now, now + hour), now + hour - EXPIRY_SKEW);
        // An already-expired token must not be re-vended once per request.
        assert_eq!(refresh_after(now, now - hour), now + MIN_REVEND_INTERVAL);
    }

    #[tokio::test]
    async fn caches_token_then_revends_once_stale() {
        let server = MockServer::start_async().await;
        let vends = mock_catalog(&server, Utc::now() + Duration::hours(1)).await;
        let provider = Aws(source_for(&server));

        let first = provider.get_credential().await.unwrap();
        provider.get_credential().await.unwrap();
        // A valid token is reused: two reads, one vend.
        assert_eq!(vends.calls_async().await, 1);

        expire_cache(&provider.0).await;
        provider.get_credential().await.unwrap();
        // A provider that cached forever (the bug) would still show 1.
        assert_eq!(vends.calls_async().await, 2);

        assert_eq!(first.key_id, "AKIATEST");
        assert_eq!(first.token.as_deref(), Some("session-token"));
    }

    /// R2 tables report an s3:// location but vend under `r2_temp_credentials`,
    /// so the AWS provider has to fall back to them the way `get_credentials` does.
    #[cfg(feature = "r2")]
    #[tokio::test]
    async fn falls_back_to_r2_credentials() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.path(TABLE_PATH).method(GET);
                then.body(GET_TABLE_RESPONSE);
            })
            .await;
        server
            .mock_async(|when, then| {
                when.path(CREDS_PATH).method(POST);
                then.json_body_obj(&serde_json::json!({
                    "r2_temp_credentials": {
                        "access_key_id": "R2KEY",
                        "secret_access_key": "r2-secret",
                        "session_token": "r2-token",
                    },
                    "expiration_time": (Utc::now() + Duration::hours(1)).timestamp_millis(),
                    "url": "s3://bucket/table",
                }));
            })
            .await;

        let credential = Aws(source_for(&server)).get_credential().await.unwrap();
        assert_eq!(credential.key_id, "R2KEY");
        assert_eq!(credential.token.as_deref(), Some("r2-token"));
    }

    #[tokio::test]
    async fn maps_azure_and_gcp_credentials() {
        let server = MockServer::start_async().await;
        mock_catalog(&server, Utc::now() + Duration::hours(1)).await;
        let source = source_for(&server);

        // The SAS arrives as one string and has to reach object_store as decoded
        // query pairs.
        let azure = Azure(Arc::clone(&source)).get_credential().await.unwrap();
        let expected = vec![
            ("sv".to_string(), "2021-08-06".to_string()),
            ("sig".to_string(), "a+b".to_string()),
        ];
        assert!(matches!(
            azure.as_ref(),
            AzureCredential::SASToken(pairs) if *pairs == expected
        ));

        let gcp = Gcp(source).get_credential().await.unwrap();
        assert_eq!(gcp.bearer, "ya29.token");
    }

    /// The scheme decides which cloud's provider we install, so it has to track
    /// the schemes each object store factory registers.
    #[test]
    fn provider_matches_the_location_scheme() {
        let cases = [
            ("s3://bucket/t", Some("Aws")),
            ("s3a://bucket/t", Some("Aws")),
            ("gs://bucket/t", Some("Gcp")),
            ("abfss://c@a.dfs.core.windows.net/t", Some("Azure")),
            ("az://c/t", Some("Azure")),
            ("file:///tmp/t", None),
        ];

        let catalog = || {
            UnityCatalogBuilder::builder()
                .workspace_url("https://example.databricks.com")
                .bearer_token("token")
                .build()
                .build()
                .unwrap()
        };

        for (location, expected) in cases {
            let got = provider_for(
                &Url::parse(location).unwrap(),
                "uc://catalog_name.schema_name.table_name",
                catalog(),
            )
            .unwrap()
            .map(|p| match p {
                CloudCredentialProvider::Aws(_) => "Aws",
                CloudCredentialProvider::Azure(_) => "Azure",
                CloudCredentialProvider::Gcp(_) => "Gcp",
            });
            assert_eq!(got, expected, "for {location}");
        }
    }
}
