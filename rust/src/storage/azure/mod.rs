//! The Azure Data Lake Storage Gen2 storage backend.
//!
//! This module is gated behind the "azure" feature.
//!
/// Shared key authentication is used (temporarily).
///
/// `AZURE_STORAGE_ACCOUNT_NAME` is required to be set in the environment.
/// `AZURE_STORAGE_ACCOUNT_KEY` is required to be set in the environment.
use super::{parse_uri, ObjectMeta, StorageBackend, StorageError, UriError};
use azure_core::prelude::*;
use azure_core::new_http_client;
use azure_storage::core::clients::{AsStorageClient, StorageAccountClient};
use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_blobs::prelude::*;
use azure_storage_datalake::prelude::*;
use futures::stream::Stream;
use log::debug;
use std::env;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::{fmt, pin::Pin};

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

/// A storage backend for use with an Azure Data Lake Storage Gen2 account (HNS=enabled).
///
/// This uses the `dfs.core.windows.net` endpoint.
#[derive(Debug)]
pub struct AdlsGen2Backend {
    storage_account_name: String,
    file_system_name: String,
    file_system_client: FileSystemClient, // TODO: use Arc?
    container_client: Arc<ContainerClient>,
}

impl AdlsGen2Backend {
    /// Create a new [`AdlsGen2Backend`].
    ///
    /// Shared key authentication is used (temporarily).
    ///
    /// `AZURE_STORAGE_ACCOUNT_NAME` is required to be set in the environment.
    /// `AZURE_STORAGE_ACCOUNT_KEY` is required to be set in the environment.
    pub fn new(file_system_name: &str) -> Result<Self, StorageError> {
        let storage_account_name = env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|_| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT_NAME must be set".to_string())
        })?;

        let storage_account_key = env::var("AZURE_STORAGE_ACCOUNT_KEY").map_err(|_| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT_KEY must be set".to_string())
        })?;

        let data_lake_client = DataLakeClient::new(
            StorageSharedKeyCredential::new(storage_account_name.to_owned(), storage_account_key.to_owned()),
            None,
        );

        let file_system_client =
            data_lake_client.into_file_system_client(file_system_name.to_owned());

        // TODO: The container_client should go away in favor of using DirectoryClient and FileClient
        // See: https://github.com/Azure/azure-sdk-for-rust/issues/496
        // See: https://github.com/Azure/azure-sdk-for-rust/pull/610
        // Missing: get_file_properties, read_file, list_directory
        let http_client = new_http_client();
        let storage_account_client = StorageAccountClient::new_access_key(
            http_client.clone(),
            storage_account_name.to_owned(),
            storage_account_key.to_owned(),
        );
        let storage_client = storage_account_client.as_storage_client();
        let container_client = storage_client.as_container_client(file_system_name.to_owned());

        Ok(Self {
            storage_account_name,
            file_system_name: file_system_name.to_owned(),
            file_system_client,
            container_client,
        })
    }

    fn validate_container<'a>(&self, obj: &AdlsGen2Object<'a>) -> Result<(), StorageError> {
        if obj.file_system != self.file_system_name {
            Err(StorageError::Uri {
                source: UriError::ContainerMismatch {
                    expected: self.file_system_name.clone(),
                    got: obj.file_system.to_string(),
                },
            })
        } else {
            Ok(())
        }
    }
}

fn to_storage_err(err: Box<dyn Error + Sync + std::marker::Send>) -> StorageError {
    match err.downcast_ref::<azure_core::HttpError>() {
        Some(azure_core::HttpError::StatusCode { status, body: _ })
            if status.as_u16() == 404 =>
        {
            StorageError::NotFound
        }
        _ => StorageError::AzureGeneric { source: err },
    }
}

fn to_storage_err2(err: azure_storage::core::Error) -> StorageError {
    StorageError::AzureStorage { source: err }
}

#[async_trait::async_trait]
impl StorageBackend for AdlsGen2Backend {
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        debug!("Getting properties for {}", path);
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        // TODO: Use file_system_client once it can get file properties
        let properties = self
            .container_client
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

        // TODO: Use file_system_client once it can read files
        Ok(self
            .container_client
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

        // TODO: Use file_system_client once it can list files
        let objs = self
            .container_client
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
                    account_name: &self.storage_account_name,
                    file_system: &self.file_system_name,
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

    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        // TODO: remove
        println!("put_obj path = '{}'\n", path);

        let data = bytes::Bytes::from(obj_bytes.to_owned()); // TODO: Review obj_bytes.to_owned()
        let length = data.len() as i64;

        self.file_system_client
            .create_file(Context::default(), path, FileCreateOptions::default())
            .await
            .map_err(to_storage_err2)?;

        self.file_system_client
            .append_to_file(
                Context::default(),
                path,
                data,
                0,
                FileAppendOptions::default(),
            )
            .await
            .map_err(to_storage_err2)?;

        self.file_system_client
            .flush_file(
                Context::default(),
                path,
                length,
                true,
                FileFlushOptions::default(),
            )
            .await
            .map_err(to_storage_err2)?;

        Ok(())
    }

    async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        self.file_system_client
            .rename_file_if_not_exists(Context::default(), src, dst)
            .await
            .map_err(to_storage_err2)?;

        Ok(())
    }

    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        self.file_system_client
            .delete_file(Context::default(), path, FileDeleteOptions::default())
            .await
            .map_err(to_storage_err2)?;

        Ok(())
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
