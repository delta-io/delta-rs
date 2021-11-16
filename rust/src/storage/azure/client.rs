use super::StorageError;
use azure_core::{prelude::*, TokenCredential};
use azure_identity::token_credentials::DefaultAzureCredential;
use azure_storage::blob::prelude::*;
use azure_storage::core::clients::{AsStorageClient, StorageAccountClient};
use chrono::{DateTime, Duration, Utc};
use std::clone::Clone;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct ClientContainer {
    pub client: Arc<ContainerClient>,
    expires_on: Option<DateTime<Utc>>,
}

impl ClientContainer {
    fn is_expired(&self) -> bool {
        match self.expires_on {
            // The 20 second offset is somewhat arbitrary to account for a delta between
            // getting a new client instance and submitting a request using the client.
            Some(ref e) => *e < Utc::now() + Duration::seconds(20),
            None => false,
        }
    }
}

#[async_trait::async_trait]
pub trait ProvideClientContainer {
    async fn get_client_container(&self) -> Result<ClientContainer, StorageError>;
}

#[async_trait::async_trait]
impl<P: ProvideClientContainer + Send + Sync> ProvideClientContainer for Arc<P> {
    async fn get_client_container(&self) -> Result<ClientContainer, StorageError> {
        P::get_client_container(self).await
    }
}

/// Provides a container client that does not expire - at least not for the lifetime of the storage backend.
/// Allows using a connection string, SAS, or master key for authorization.
#[derive(Clone, Debug)]
pub struct StaticClientProvider {
    inner: ClientContainer,
}

impl StaticClientProvider {
    pub fn new_with_sas(
        shared_access_signature: String,
        account: String,
        container: String,
    ) -> Result<Self, StorageError> {
        let http_client = new_http_client();
        let client = StorageAccountClient::new_sas_token(
            http_client.clone(),
            &account,
            &shared_access_signature,
        )
        .map_err(|op| StorageError::AzureConfig(op.to_string()))?
        .as_storage_client()
        .as_container_client(container);
        Ok(Self {
            inner: ClientContainer {
                client,
                expires_on: None,
            },
        })
    }

    pub fn new_with_access_key(
        access_key: String,
        account: String,
        container: String,
    ) -> Result<Self, StorageError> {
        let http_client = new_http_client();
        let client =
            StorageAccountClient::new_access_key(http_client.clone(), &account, &access_key)
                .as_storage_client()
                .as_container_client(container);
        Ok(Self {
            inner: ClientContainer {
                client,
                expires_on: None,
            },
        })
    }

    pub fn new_with_connection_string(
        connection_string: String,
        container: String,
    ) -> Result<Self, StorageError> {
        let http_client = new_http_client();
        let client =
            StorageAccountClient::new_connection_string(http_client.clone(), &connection_string)
                .map_err(|op| StorageError::AzureConfig(op.to_string()))?
                .as_storage_client()
                .as_container_client(container);
        Ok(Self {
            inner: ClientContainer {
                client,
                expires_on: None,
            },
        })
    }
}

#[async_trait::async_trait]
impl ProvideClientContainer for StaticClientProvider {
    async fn get_client_container(&self) -> Result<ClientContainer, StorageError> {
        Ok(self.inner.clone())
    }
}

#[derive(Debug, Clone)]
pub struct AutoRefreshingProvider<P: ProvideClientContainer + 'static> {
    client_provider: P,
    current_client: Arc<Mutex<Option<Result<ClientContainer, StorageError>>>>,
}

impl<P: ProvideClientContainer + 'static> AutoRefreshingProvider<P> {
    /// Create a new `AutoRefreshingProvider` around the provided base provider.
    pub fn new(provider: P) -> Result<AutoRefreshingProvider<P>, StorageError> {
        Ok(AutoRefreshingProvider {
            client_provider: provider,
            current_client: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait::async_trait]
impl<P: ProvideClientContainer + Send + Sync + 'static> ProvideClientContainer
    for AutoRefreshingProvider<P>
{
    async fn get_client_container(&self) -> Result<ClientContainer, StorageError> {
        loop {
            let mut guard = self.current_client.lock().await;
            match guard.as_ref() {
                // no result from the future yet, let's keep using it
                None => {
                    let res = self.client_provider.get_client_container().await;
                    *guard = Some(res);
                }
                Some(Err(_)) => {
                    // had to return a generic error here
                    return Err(StorageError::AzureConfig(
                        "Error getting credentials".to_string(),
                    ));
                }
                Some(Ok(container)) => {
                    if container.is_expired() {
                        *guard = None;
                    } else {
                        return Ok(container.clone());
                    };
                }
            }
        }
    }
}

struct DefaultAzureProvider {
    account: String,
    container: String,
    credential: DefaultAzureCredential,
}

impl DefaultAzureProvider {
    pub fn new(account: String, container: String) -> Self {
        Self {
            account,
            container,
            credential: DefaultAzureCredential::default(),
        }
    }
}

impl Clone for DefaultAzureProvider {
    fn clone(&self) -> Self {
        Self {
            account: self.account.clone(),
            container: self.container.clone(),
            credential: DefaultAzureCredential::default(),
        }
    }
}

impl fmt::Debug for DefaultAzureProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DefaultAzureProvider")
            .field("credential", &"DefaultAzureCredential")
            .finish()
    }
}

#[async_trait::async_trait]
impl ProvideClientContainer for DefaultAzureProvider {
    async fn get_client_container(&self) -> Result<ClientContainer, StorageError> {
        let http_client = new_http_client();
        let token = self
            .credential
            .get_token("https://storage.azure.com/")
            .await?;
        let client = StorageAccountClient::new_bearer_token(
            http_client,
            &self.account,
            token.token.secret(),
        )
        .as_storage_client()
        .as_container_client(&self.container);
        Ok(ClientContainer {
            client,
            expires_on: Some(token.expires_on),
        })
    }
}

/// Wraps the `DefaultAzureCredential` from the azure_identity crate in an AutoRefreshingProvider to
/// enable authorization using azure identities.
#[derive(Clone, Debug)]
pub struct DefaultClientProvider(AutoRefreshingProvider<DefaultAzureProvider>);

impl DefaultClientProvider {
    /// Creates a new thread-safe `DefaultClientProvider`.
    pub fn new(account: String, container: String) -> Result<DefaultClientProvider, StorageError> {
        let inner = AutoRefreshingProvider::new(DefaultAzureProvider::new(account, container))?;
        Ok(DefaultClientProvider(inner))
    }
}

#[async_trait::async_trait]
impl ProvideClientContainer for DefaultClientProvider {
    async fn get_client_container(&self) -> Result<ClientContainer, StorageError> {
        self.0.get_client_container().await
    }
}
