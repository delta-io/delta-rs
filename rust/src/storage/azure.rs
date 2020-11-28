//! The Azure Data Lake Storage Gen2 storage backend.
//!
//! This module is gated behind the "azure" feature. Its usage also requires
//! the `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` environment variables
//! to be set to the name and key of the Azure Storage Account, respectively.
use std::{env, fmt};

use azure_core::{errors::AzureError, prelude::*};
use azure_storage::{client, key_client::KeyClient, Blob, ClientEndpoint, Container};
use futures::stream::StreamExt;
use log::debug;
use tokio::runtime;

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

    fn gen_tokio_rt() -> runtime::Runtime {
        runtime::Builder::new()
            .enable_time()
            .enable_io()
            .basic_scheduler()
            .build()
            .unwrap()
    }
}

impl Default for ADLSGen2Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for ADLSGen2Backend {
    fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        debug!("Getting properties for {}", path);
        let properties = Self::gen_tokio_rt().block_on(
            self.client
                .get_blob_properties()
                .with_container_name(obj.file_system)
                .with_blob_name(obj.path)
                .finalize(),
        )?;
        Ok(ObjectMeta {
            path: path.to_string(),
            modified: properties
                .blob
                .last_modified
                .expect("Last-Modified should never be None for committed blobs"),
        })
    }

    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        debug!("Loading {}", path);
        let blob = Self::gen_tokio_rt().block_on(
            self.client
                .get_blob()
                .with_container_name(obj.file_system)
                .with_blob_name(obj.path)
                .finalize(),
        )?;

        Ok(blob.data)
    }

    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = ObjectMeta>>, StorageError> {
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        debug!("Listing objects under {}", path);
        let vec: Result<Vec<ObjectMeta>, AzureError> = Self::gen_tokio_rt().block_on(async {
            let mut objects = vec![];
            let mut stream = Box::pin(
                self.client
                    .list_blobs()
                    .with_container_name(obj.file_system)
                    .with_prefix(obj.path)
                    .stream(),
            );
            while let Some(res) = stream.next().await {
                let response = res?;
                response
                    .incomplete_vector
                    .vector
                    .into_iter()
                    .for_each(|blob| {
                        let object = ADLSGen2Object {
                            account_name: &self.client.account(),
                            file_system: &blob.container_name,
                            path: &blob.name,
                        };
                        objects.push(ObjectMeta {
                            path: object.to_string(),
                            modified: blob
                                .last_modified
                                .expect("Last-Modified should never be None for committed blobs"),
                        })
                    });
            }
            Ok(objects)
        });

        Ok(Box::new(vec?.into_iter()))
    }
}
