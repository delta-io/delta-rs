//! The Azure Data Lake Storage Gen2 storage backend. It currently only supports read operations.
//!
//! This module is gated behind the "azure" feature. Its usage also requires
//! the `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` environment variables
//! to be set to the name and key of the Azure Storage Account, respectively.

use std::error::Error;
use std::sync::Arc;
use std::{env, fmt, pin::Pin};

use azure_core::errors::AzureError;
use azure_core::prelude::*;
use azure_storage::clients::{
    AsBlobClient, AsContainerClient, AsStorageClient, ContainerClient, StorageAccountClient,
};
use futures::stream::{Stream, TryStreamExt};
use log::debug;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError, UriError};

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

/// A storage backend backed by an Azure Data Lake Storage Gen2 account.
///
/// This uses the `dfs.core.windows.net` endpoint.
#[derive(Debug)]
pub struct AdlsGen2Backend {
    account: String,
    container_client: Arc<ContainerClient>,
}

impl AdlsGen2Backend {
    /// Create a new [`AdlsGen2Backend`].
    ///
    /// This currently requires the `AZURE_STORAGE_ACCOUNT` and one of the
    /// following environment variables to be set:
    ///
    /// - `AZURE_STORAGE_SAS`
    /// - `AZURE_STORAGE_KEY`
    ///
    /// and will panic if both are unset. This also implies that the backend is
    /// only valid for a single Storage Account.
    pub fn new(container: &str) -> Result<Self, StorageError> {
        let http_client: Arc<Box<dyn HttpClient>> = Arc::new(Box::new(reqwest::Client::new()));

        let account_name = env::var("AZURE_STORAGE_ACCOUNT").map_err(|_| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT must be set".to_string())
        })?;

        let storage_account_client = if let Ok(sas) = env::var("AZURE_STORAGE_SAS") {
            debug!("Authenticating to Azure using SAS token");
            StorageAccountClient::new_sas_token(http_client.clone(), &account_name, &sas)
        } else if let Ok(key) = env::var("AZURE_STORAGE_KEY") {
            debug!("Authenticating to Azure using access key");
            StorageAccountClient::new_access_key(http_client.clone(), &account_name, &key)
        } else {
            return Err(StorageError::AzureConfig(
                "Either AZURE_STORAGE_SAS or AZURE_STORAGE_KEY must be set".to_string(),
            ));
        };

        Ok(Self {
            account: account_name,
            container_client: storage_account_client
                .as_storage_client()
                .as_container_client(container),
        })
    }

    fn validate_container<'a>(&self, obj: &AdlsGen2Object<'a>) -> Result<(), StorageError> {
        if obj.file_system != self.container_client.container_name() {
            Err(StorageError::Uri {
                source: UriError::ContainerMismatch {
                    expected: self.container_client.container_name().to_string(),
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
        Some(AzureError::UnexpectedHTTPResult(e)) if e.status_code().as_u16() == 404 => {
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
            .container_client
            .as_blob_client(obj.path)
            .get_properties()
            .execute()
            .await
            .map_err(to_storage_err)?;
        let modified = properties
            .blob
            .last_modified
            .expect("Last-Modified should never be None for committed blobs");
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
            .container_client
            .as_blob_client(obj.path)
            .get()
            .execute()
            .await
            .map_err(to_storage_err)?
            .data)
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

        let stream = self
            .container_client
            .list_blobs()
            .prefix(obj.path)
            .stream()
            .map_ok(move |response| {
                futures::stream::iter(response.incomplete_vector.vector.into_iter().map(
                    move |blob| {
                        let object = AdlsGen2Object {
                            account_name: &self.account,
                            file_system: &blob.container_name,
                            path: &blob.name,
                        };
                        Ok(ObjectMeta {
                            path: object.to_string(),
                            modified: blob
                                .last_modified
                                .expect("Last-Modified should never be None for committed blobs"),
                        })
                    },
                ))
            })
            .map_err(to_storage_err)
            .try_flatten();

        Ok(Box::pin(stream))
    }

    async fn put_obj(&self, _path: &str, _obj_bytes: &[u8]) -> Result<(), StorageError> {
        unimplemented!("put_obj not implemented for azure");
    }

    async fn rename_obj(&self, _src: &str, _dst: &str) -> Result<(), StorageError> {
        unimplemented!("rename_obj not implemented for azure");
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
