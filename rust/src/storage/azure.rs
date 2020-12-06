//! The Azure Data Lake Storage Gen2 storage backend.
//!
//! This module is gated behind the "azure" feature. Its usage also requires
//! the `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` environment variables
//! to be set to the name and key of the Azure Storage Account, respectively.
use std::{env, fmt, pin::Pin};

use azure_core::prelude::*;
use azure_storage::{client, key_client::KeyClient, Blob, ClientEndpoint, Container};
use futures::stream::{Stream, TryStreamExt};
use log::debug;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};

/// An object on an Azure Data Lake Storage Gen2 account.
#[derive(Debug, PartialEq)]
pub struct ADLSGen2Object<'a> {
    /// The storage account name.
    pub account_name: &'a str,
    /// The container, or filesystem, of the object.
    pub file_system: &'a str,
    /// The path of the object on the filesystem.
    pub path: &'a str,
}

impl<'a> fmt::Display for ADLSGen2Object<'a> {
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
pub struct ADLSGen2Backend {
    client: KeyClient,
}

impl ADLSGen2Backend {
    /// Create a new [`ADLSGen2Backend`].
    ///
    /// This currently requires the `AZURE_STORAGE_ACCOUNT` and one of the
    /// following environment variables to be set:
    ///
    /// - `AZURE_STORAGE_SAS`
    /// - `AZURE_STORAGE_KEY`
    ///
    /// and will panic if both are unset. This also implies that the backend is
    /// only valid for a single Storage Account.
    pub fn new() -> Self {
        let account_name =
            env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT must be set");
        let client = match env::var("AZURE_STORAGE_SAS") {
            Ok(sas) => {
                debug!("Authenticating to Azure using SAS token");
                client::with_azure_sas(&account_name, &sas)
            }
            Err(_) => match env::var("AZURE_STORAGE_KEY") {
                Ok(key) => {
                    debug!("Authenticating to Azure using access key");
                    client::with_access_key(&account_name, &key)
                }
                _ => panic!("Either AZURE_STORAGE_SAS or AZURE_STORAGE_KEY must be set"),
            },
        };
        Self { client }
    }
}

impl Default for ADLSGen2Backend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StorageBackend for ADLSGen2Backend {
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        let path = path.to_string();
        debug!("Getting properties for {}", path);
        let properties = self
            .client
            .get_blob_properties()
            .with_container_name(obj.file_system)
            .with_blob_name(obj.path)
            .finalize()
            .await?;
        let modified = properties
            .blob
            .last_modified
            .expect("Last-Modified should never be None for committed blobs");
        Ok(ObjectMeta { path, modified })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        debug!("Loading {}", path);
        Ok(self
            .client
            .get_blob()
            .with_container_name(obj.file_system)
            .with_blob_name(obj.path)
            .finalize()
            .await?
            .data)
    }

    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + 'a>>, StorageError>
    {
        debug!("Listing objects under {}", path);
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        let account_name = self.client.account();
        let stream = self
            .client
            .list_blobs()
            .with_container_name(obj.file_system)
            .with_prefix(obj.path)
            .stream()
            .map_ok(move |response| {
                futures::stream::iter(response.incomplete_vector.vector.into_iter().map(
                    move |blob| {
                        let object = ADLSGen2Object {
                            account_name,
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
            .try_flatten();

        Ok(Box::pin(stream))
    }
}
