//! The Azure Data Lake Storage Gen2 storage backend. It currently only supports read operations.
//!
//! This module is gated behind the "azure" feature.
//!
//! There are several authentication options available. Either via the environment:
//! a) `AZURE_STORAGE_CONNECTION_STRING`
//! b) `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_SAS`
//! c) `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`
//!
//! Alternatively, the default credential from the azure_identity crate is used,
//! which iterates through several authentication alternatives.
//! - EnvironmentCredential: uses `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` and `AZURE_TENANT_ID`
//!   from the environment to authenticate via an azure client application
//! - ManagedIdentityCredential: This authentication type works in Azure VMs,
//!   App Service and Azure Functions applications, as well as the Azure Cloud Shell
//! - AzureCliCredential: Enables authentication to Azure Active Directory
//!   using Azure CLI to obtain an access token.
//!
//! all alternatives but using a connection string require `AZURE_STORAGE_ACCOUNT` to be set
//! and will panic if this is not set. This also implies that the backend is
//! only valid for a single Storage Account.

use azure_core::HttpError as AzureError;
use azure_storage::blob::prelude::*;
use azure_storage::ConnectionString;
use client::{DefaultClientProvider, ProvideClientContainer, StaticClientProvider};
use futures::stream::Stream;
use log::debug;
use std::error::Error;
use std::fmt::Debug;
use std::{fmt, pin::Pin};
mod client;
use super::{parse_uri, ObjectMeta, StorageBackend, StorageError, UriError};
use std::env;
use std::sync::Arc;

/// An object on an Azure Data Lake Storage Gen2 account.
#[derive(Debug, PartialEq)]
pub struct AdlsGen2Object<'a> {
    /// The storage account name.
    pub account_name: &'a str,
    /// The container, or filesystem, of the object.
    pub file_system: &'a str,
    /// The path of the object on the filesystem.
    pub path: &'a str,
}

impl<'a> fmt::Display for AdlsGen2Object<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // This URI syntax is documented at
        // https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
        write!(
            f,
            "abfss://{}@{}.dfs.core.windows.net/{}",
            self.file_system, self.account_name, self.path
        )
    }
}

struct AzureContainerClient {
    pub account: String,
    pub container: String,
    inner: Arc<dyn ProvideClientContainer + Send + Sync>,
}

impl std::fmt::Debug for AzureContainerClient {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "AzureContainerClient")
    }
}

impl AzureContainerClient {
    pub fn new(container: &str) -> Result<Self, StorageError> {
        if let Ok(connection_string) = env::var("AZURE_STORAGE_CONNECTION_STRING") {
            let inner = StaticClientProvider::new_with_connection_string(
                connection_string.clone(),
                container.to_string(),
            )?;
            let parsed = ConnectionString::new(connection_string.as_str()).unwrap();
            return Ok(Self {
                account: parsed.account_name.unwrap().to_string(),
                container: container.to_string(),
                inner: Arc::new(inner),
            });
        }

        let account = env::var("AZURE_STORAGE_ACCOUNT").map_err(|_| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT must be set".to_string())
        })?;

        if let Ok(sas) = env::var("AZURE_STORAGE_SAS") {
            let inner =
                StaticClientProvider::new_with_sas(sas, account.clone(), container.to_string())?;
            return Ok(Self {
                account,
                container: container.to_string(),
                inner: Arc::new(inner),
            });
        }

        if let Ok(key) = env::var("AZURE_STORAGE_KEY") {
            let inner = StaticClientProvider::new_with_access_key(
                key,
                account.clone(),
                container.to_string(),
            )?;
            return Ok(Self {
                account,
                container: container.to_string(),
                inner: Arc::new(inner),
            });
        }

        // Other authorization options will panic if applicable. however for the default credential
        // we do not know if we can get a token until we actually try, an thus fail only when we try to use it.
        // using it here is not an option (i think) since fetching a token is an async operation.
        let inner = DefaultClientProvider::new(account.clone(), container.to_string())?;
        Ok(Self {
            account,
            container: container.to_string(),
            inner: Arc::new(inner),
        })
    }

    pub async fn as_container_client(&self) -> Arc<ContainerClient> {
        self.inner.get_client_container().await.unwrap().client
    }
}

/// A storage backend backed by an Azure Data Lake Storage Gen2 account.
///
/// This uses the `dfs.core.windows.net` endpoint.
#[derive(Debug)]
pub struct AdlsGen2Backend {
    container: String,
    client: AzureContainerClient,
}

impl AdlsGen2Backend {
    /// Create a new [`AdlsGen2Backend`].
    ///
    /// There are several authentication options available. Either via the environment:
    /// a) `AZURE_STORAGE_CONNECTION_STRING`
    /// b) `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_SAS`
    /// c) `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`
    ///
    /// Alternatively, the default credential from the azure_identity crate is used,
    /// which iterates through several authentication alternatives.
    /// - EnvironmentCredential: uses `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` and `AZURE_TENANT_ID`
    ///   from the environment to authenticate via an azure client application
    /// - ManagedIdentityCredential: This authentication type works in Azure VMs,
    ///   App Service and Azure Functions applications, as well as the Azure Cloud Shell
    /// - AzureCliCredential: Enables authentication to Azure Active Directory
    ///   using Azure CLI to obtain an access token.
    ///
    /// all alternatives but using a connection string require `AZURE_STORAGE_ACCOUNT` to be set
    /// and will panic if this is not set. This also implies that the backend is
    /// only valid for a single Storage Account.
    pub fn new(container: &str) -> Result<Self, StorageError> {
        Ok(Self {
            container: container.to_string(),
            client: AzureContainerClient::new(container)?,
        })
    }

    fn validate_container<'a>(&self, obj: &AdlsGen2Object<'a>) -> Result<(), StorageError> {
        if obj.file_system != self.client.container {
            Err(StorageError::Uri {
                source: UriError::ContainerMismatch {
                    expected: self.container.clone(),
                    got: obj.file_system.to_string(),
                },
            })
        } else {
            Ok(())
        }
    }
}

fn to_storage_err(err: Box<dyn Error + Sync + std::marker::Send>) -> StorageError {
    match err.downcast_ref::<AzureError>() {
        Some(AzureError::ErrorStatusCode { status, body: _ }) if status.as_u16() == 404 => {
            StorageError::NotFound
        }
        _ => StorageError::AzureGeneric { source: err },
    }
}

#[async_trait::async_trait]
impl StorageBackend for AdlsGen2Backend {
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        debug!("Getting properties for {}", path);
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        let properties = self
            .client
            .as_container_client()
            .await
            .as_blob_client(obj.path)
            .get_properties()
            .execute()
            .await
            .map_err(to_storage_err)?;
        let modified = properties.blob.properties.last_modified;
        Ok(ObjectMeta {
            path: path.to_string(),
            modified,
        })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("Loading {}", path);
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        Ok(self
            .client
            .as_container_client()
            .await
            .as_blob_client(obj.path)
            .get()
            .execute()
            .await
            .map_err(to_storage_err)?
            .data
            .to_vec())
    }

    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    > {
        debug!("Listing objects under {}", path);
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        let objs = self
            .client
            .as_container_client()
            .await
            .list_blobs()
            .prefix(obj.path)
            .execute()
            .await
            .unwrap()
            .blobs
            .blobs
            .into_iter()
            .map(|blob| {
                let object = AdlsGen2Object {
                    account_name: &self.client.account,
                    file_system: &self.client.container,
                    path: &blob.name,
                };
                Ok(ObjectMeta {
                    path: object.to_string(),
                    modified: blob.properties.last_modified,
                })
            })
            .collect::<Vec<Result<ObjectMeta, StorageError>>>();

        // Likely due to https://github.com/Azure/azure-sdk-for-rust/issues/377
        // we have to collect the stream and then forward it again, instead of
        // just passing it down ...
        let output = futures::stream::iter(objs);
        Ok(Box::pin(output))
    }

    async fn put_obj(&self, _path: &str, _obj_bytes: &[u8]) -> Result<(), StorageError> {
        unimplemented!("put_obj not implemented for azure");
    }

    async fn rename_obj_noreplace(&self, _src: &str, _dst: &str) -> Result<(), StorageError> {
        unimplemented!("rename_obj_noreplace not implemented for azure");
    }

    async fn delete_obj(&self, _path: &str) -> Result<(), StorageError> {
        unimplemented!("delete_obj not implemented for azure");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_azure_object_uri() {
        let uri = parse_uri("abfss://fs@sa.dfs.core.windows.net/foo").unwrap();
        assert_eq!(uri.path(), "foo");
        assert_eq!(
            uri.into_adlsgen2_object().unwrap(),
            AdlsGen2Object {
                account_name: "sa",
                file_system: "fs",
                path: "foo",
            }
        );
    }
}
