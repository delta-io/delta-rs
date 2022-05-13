//! The Azure Data Lake Storage Gen2 storage backend.
//!
//! This module is gated behind the "azure" feature.
//!
/// Shared key authentication is used (temporarily).
///
/// `AZURE_STORAGE_ACCOUNT_NAME` is required to be set in the environment.
/// `AZURE_STORAGE_ACCOUNT_KEY` is required to be set in the environment.
use super::{parse_uri, ObjectMeta, StorageBackend, StorageError, UriError};
use azure_core::auth::TokenCredential;
use azure_core::ClientOptions;
use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_datalake::prelude::*;
//use azure_storage_datalake::file_system::Path;
use futures::stream::Stream;
use futures::StreamExt;
use log::debug;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::{fmt, pin::Pin};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::LocalPoolHandle;

///The ADLS Gen2 Access Key
pub const AZURE_STORAGE_ACCOUNT_KEY: &str = "AZURE_STORAGE_ACCOUNT_KEY";
///The name of storage account
pub const AZURE_STORAGE_ACCOUNT_NAME: &str = "AZURE_STORAGE_ACCOUNT_NAME";

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
        // This URI syntax is an invention of delta-rs.
        // ABFS URIs should not be used since delta-rs doesn't use the Hadoop ABFS driver.
        write!(
            f,
            "adls2://{}/{}/{}",
            self.account_name, self.file_system, self.path
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
    file_system_client: FileSystemClient,
    local_pool_handle: LocalPoolHandle,
}

impl AdlsGen2Backend {
    /// Create a new [`AdlsGen2Backend`].
    ///
    /// Shared key authentication is used (temporarily).
    ///
    /// `AZURE_STORAGE_ACCOUNT_NAME` is required to be set in the environment.
    /// `AZURE_STORAGE_ACCOUNT_KEY` is required to be set in the environment.
    pub fn new(file_system_name: &str) -> Result<Self, StorageError> {
        let mut map: HashMap<String, String> = HashMap::new();

        let storage_account_name = env::var(AZURE_STORAGE_ACCOUNT_NAME).map_err(|_| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT_NAME must be set".to_string())
        })?;

        let storage_account_key = env::var(AZURE_STORAGE_ACCOUNT_KEY).map_err(|_| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT_KEY must be set".to_string())
        })?;

        map.insert(AZURE_STORAGE_ACCOUNT_NAME.to_string(), storage_account_name);
        map.insert(AZURE_STORAGE_ACCOUNT_KEY.to_string(), storage_account_key);

        Self::from_map(file_system_name, map)
    }

    ///TODO
    pub fn new_with_token_credential(
        storage_account_name: &str,
        file_system_name: &str,
        token_credential: Arc<dyn TokenCredential>,
    ) -> Result<Self, StorageError> {
        let data_lake_client = DataLakeClient::new_with_token_credential(
            token_credential.clone(),
            storage_account_name,
            None,
            ClientOptions::default(),
        );

        let file_system_client =
            data_lake_client.into_file_system_client(file_system_name.to_owned());

        Ok(AdlsGen2Backend {
            storage_account_name: storage_account_name.to_string(),
            file_system_name: file_system_name.to_string(),
            file_system_client,
            local_pool_handle: LocalPoolHandle::new(1),
        })
    }

    ///TODO
    pub fn new_with_shared_key(
        storage_account_name: &str,
        file_system_name: &str,
        storage_account_key: &str,
    ) -> Result<Self, StorageError> {
        let key = StorageSharedKeyCredential::new(
            storage_account_name.to_owned(),
            storage_account_key.to_owned(),
        );

        let data_lake_client =
            DataLakeClient::new_with_shared_key(key, None, ClientOptions::default());

        let file_system_client =
            data_lake_client.into_file_system_client(file_system_name.to_owned());

        Ok(AdlsGen2Backend {
            storage_account_name: storage_account_name.to_string(),
            file_system_name: file_system_name.to_string(),
            file_system_client,
            local_pool_handle: LocalPoolHandle::new(1),
        })
    }

    /// Create a new [`AdlsGen2Backend`].
    ///
    /// Shared key authentication is used (temporarily).
    ///
    /// `AZURE_STORAGE_ACCOUNT_NAME` is required to be set in the map.
    /// `AZURE_STORAGE_ACCOUNT_KEY` is required to be set in the map.
    pub fn from_map(
        file_system_name: &str,
        map: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let storage_account_name = map.get(AZURE_STORAGE_ACCOUNT_NAME).ok_or_else(|| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT_NAME must be set".to_string())
        })?;

        let storage_account_key = map.get(AZURE_STORAGE_ACCOUNT_KEY).ok_or_else(|| {
            StorageError::AzureConfig("AZURE_STORAGE_ACCOUNT_KEY must be set".to_string())
        })?;

        Self::new_with_shared_key(storage_account_name, file_system_name, storage_account_key)
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
        Some(azure_core::HttpError::StatusCode { status, body: _ }) if status.as_u16() == 404 => {
            StorageError::NotFound
        }
        _ => StorageError::AzureGeneric { source: err },
    }
}

fn to_storage_err2(err: azure_storage::core::Error) -> StorageError {
    if let azure_storage::core::Error::CoreError(azure_core::Error::Other(ref other_err)) = err {
        match other_err.downcast_ref::<azure_core::error::Error>() {
            Some(other_err) => match other_err.downcast_ref::<azure_core::error::HttpError>() {
                Some(e) => {
                    if e.status() == 404 {
                        return StorageError::NotFound;
                    }
                }
                None => {}
            },
            None => {}
        }
    }
    StorageError::AzureStorage { source: err }
}

async fn list_obj_future(
    client: FileSystemClient,
    path: String,
    storage_account_name: String,
    file_system_name: String,
    tx: Sender<Result<ObjectMeta, StorageError>>,
) {
    let mut stream = client.list_paths().directory(path).into_stream();

    while let Some(path_response_res) = stream.next().await {
        match path_response_res {
            Ok(path_response) => {
                for path in path_response.paths {
                    let object = AdlsGen2Object {
                        account_name: &storage_account_name,
                        file_system: &file_system_name,
                        path: &path.name,
                    };
                    let object_meta = Ok(ObjectMeta {
                        path: object.to_string(),
                        modified: path.last_modified,
                    });
                    let res = tx.send(object_meta).await;

                    if let Err(_e) = res {
                        return;
                    }
                }
            }
            Err(err) => {
                let _res = tx.send(Err(to_storage_err(Box::new(err)))).await;
                return;
            }
        }
    }
}

#[async_trait::async_trait]
impl StorageBackend for AdlsGen2Backend {
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        debug!("Getting properties for {}", path);
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        let properties = self
            .file_system_client
            .get_file_client(obj.path)
            .get_properties()
            .into_future()
            .await
            .map_err(to_storage_err2)?;

        let modified = properties.last_modified;
        Ok(ObjectMeta {
            path: path.to_string(),
            modified,
        })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("Loading {}", path);
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        let data = self
            .file_system_client
            .get_file_client(obj.path)
            .read()
            .into_future()
            .await
            .map_err(to_storage_err2)?
            .data
            .to_vec();
        Ok(data)
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

        let client = self.file_system_client.clone();
        let storage_account_name = self.storage_account_name.to_owned();
        let file_system_name = self.file_system_name.to_owned();
        let prefix_path = path.to_owned();
        let (tx, rx) = mpsc::channel(1024);

        let handle = self.local_pool_handle.spawn_pinned(|| {
            list_obj_future(
                client,
                prefix_path,
                storage_account_name,
                file_system_name,
                tx,
            )
        });

        tokio::spawn(handle);
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        let data = bytes::Bytes::from(obj_bytes.to_owned()); // TODO: Review obj_bytes.to_owned()
        let length = data.len() as i64;

        // TODO: Consider using Blob API again since it's just 1 REST call instead of 3
        let file_client = self.file_system_client.get_file_client(obj.path);
        file_client
            .create()
            .into_future()
            .await
            .map_err(to_storage_err2)?;
        file_client
            .append(0, data)
            .into_future()
            .await
            .map_err(to_storage_err2)?;
        file_client
            .flush(length)
            .close(true)
            .into_future()
            .await
            .map_err(to_storage_err2)?;

        Ok(())
    }

    async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        let src_obj = parse_uri(src)?.into_adlsgen2_object()?;
        self.validate_container(&src_obj)?;

        let dst_obj = parse_uri(dst)?.into_adlsgen2_object()?;
        self.validate_container(&dst_obj)?;

        let file_client = self.file_system_client.get_file_client(src_obj.path);
        let result = file_client
            .rename_if_not_exists(dst_obj.path)
            .into_future()
            .await;

        if let Err(err) = result {
            if let azure_storage::core::Error::CoreError(azure_core::Error::Other(ref other_err)) =
                err
            {
                match other_err.downcast_ref::<azure_core::error::Error>() {
                    Some(other_err) => {
                        match other_err.downcast_ref::<azure_core::error::HttpError>() {
                            Some(e) => {
                                if e.status() == 409 {
                                    return Err(StorageError::AlreadyExists(dst.to_string()));
                                }
                            }
                            None => {}
                        }
                    }
                    None => {}
                }
            }
            return Err(StorageError::AzureStorage { source: err });
        }
        Ok(())
    }

    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        let obj = parse_uri(path)?.into_adlsgen2_object()?;
        self.validate_container(&obj)?;

        let file_client = self.file_system_client.get_file_client(obj.path);
        file_client
            .delete()
            .into_future()
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
        let uri = parse_uri("adls2://my_account_name/my_file_system_name/my_path").unwrap();
        assert_eq!(uri.path(), "my_path");
        assert_eq!(
            uri.into_adlsgen2_object().unwrap(),
            AdlsGen2Object {
                account_name: "my_account_name",
                file_system: "my_file_system_name",
                path: "my_path",
            }
        );
    }
}
