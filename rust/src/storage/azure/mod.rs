//! The Azure Data Lake Storage Gen2 storage backend.
//!
//! This module is gated behind the "azure" feature.
//!
use super::{parse_uri, str_option, ObjectMeta, StorageBackend, StorageError, UriError};
use azure_core::auth::TokenCredential;
use azure_core::ClientOptions;
use azure_identity::token_credentials::{
    AutoRefreshingTokenCredential, ClientSecretCredential, TokenCredentialOptions,
};
use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_datalake::prelude::*;
use futures::stream::Stream;
use futures::StreamExt;
use log::debug;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::{fmt, pin::Pin};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::LocalPoolHandle;

/// Options for configuring Azure storage backend
pub mod azure_storage_options {
    ///The ADLS Gen2 Access Key
    pub const AZURE_STORAGE_ACCOUNT_KEY: &str = "AZURE_STORAGE_ACCOUNT_KEY";
    ///The name of storage account
    pub const AZURE_STORAGE_ACCOUNT_NAME: &str = "AZURE_STORAGE_ACCOUNT_NAME";
    /// Connection string for connecting to azure storage account
    pub const AZURE_STORAGE_CONNECTION_STRING: &str = "AZURE_STORAGE_CONNECTION_STRING";
    /// Service principal id
    pub const AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
    /// Service principal secret
    pub const AZURE_CLIENT_SECRET: &str = "AZURE_CLIENT_SECRET";
    /// ID for Azure (AAD) tenant where service principal is registered.
    pub const AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
}

/// Options used to configure the AdlsGen2Backend.
///
/// Available options are described in [azure_storage_options].
#[derive(Clone, Debug, PartialEq)]
pub struct AzureStorageOptions {
    account_key: Option<String>,
    account_name: Option<String>,
    // connection_string: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    tenant_id: Option<String>,
}

impl AzureStorageOptions {
    /// Creates an empty instance of AzureStorageOptions
    pub fn new() -> Self {
        Self {
            account_key: None,
            account_name: None,
            client_id: None,
            client_secret: None,
            tenant_id: None,
        }
    }

    /// Creates an instance of AzureStorageOptions from the given HashMap.
    pub fn from_map(options: HashMap<String, String>) -> Self {
        Self {
            account_key: str_option(&options, azure_storage_options::AZURE_STORAGE_ACCOUNT_KEY),
            account_name: str_option(&options, azure_storage_options::AZURE_STORAGE_ACCOUNT_NAME),
            // connection_string: str_option(
            //     &options,
            //     azure_storage_options::AZURE_STORAGE_CONNECTION_STRING,
            // ),
            client_id: str_option(&options, azure_storage_options::AZURE_CLIENT_ID),
            client_secret: str_option(&options, azure_storage_options::AZURE_CLIENT_SECRET),
            tenant_id: str_option(&options, azure_storage_options::AZURE_TENANT_ID),
        }
    }

    /// set account name
    pub fn with_account_name(&mut self, account_name: impl Into<String>) -> &mut Self {
        self.account_name = Some(account_name.into());
        self
    }

    /// set account key
    pub fn with_account_key(&mut self, account_key: impl Into<String>) -> &mut Self {
        self.account_key = Some(account_key.into());
        self
    }

    /// set client id
    pub fn with_client_id(&mut self, client_id: impl Into<String>) -> &mut Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// set client secret
    pub fn with_client_secret(&mut self, client_secret: impl Into<String>) -> &mut Self {
        self.client_secret = Some(client_secret.into());
        self
    }

    /// set tenant id
    pub fn with_tenant_id(&mut self, tenant_id: impl Into<String>) -> &mut Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }
}

impl Default for AzureStorageOptions {
    /// Creates an instance of AzureStorageOptions from environment variables.
    fn default() -> AzureStorageOptions {
        Self::from_map(HashMap::new())
    }
}

impl TryInto<DataLakeClient> for AzureStorageOptions {
    type Error = StorageError;

    fn try_into(self) -> Result<DataLakeClient, Self::Error> {
        let account_name = self.account_name.ok_or_else(|| {
            StorageError::AzureConfig("account name must be provided".to_string())
        })?;

        if let Some(account_key) = self.account_key {
            let key = StorageSharedKeyCredential::new(account_name, account_key);
            return Ok(DataLakeClient::new_with_shared_key(
                key,
                None,
                ClientOptions::default(),
            ));
        }

        let client_id = self.client_id.ok_or_else(|| {
            StorageError::AzureConfig("account key or client config must be provided".to_string())
        })?;
        let client_secret = self.client_secret.ok_or_else(|| {
            StorageError::AzureConfig("account key or client config must be provided".to_string())
        })?;
        let tenant_id = self.tenant_id.ok_or_else(|| {
            StorageError::AzureConfig("account key or client config must be provided".to_string())
        })?;

        let client_credential = Arc::new(ClientSecretCredential::new(
            tenant_id,
            client_id,
            client_secret,
            TokenCredentialOptions::default(),
        ));

        Ok(DataLakeClient::new_with_token_credential(
            Arc::new(AutoRefreshingTokenCredential::new(client_credential)),
            account_name,
            None,
            ClientOptions::default(),
        ))
    }
}

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
    /// Shared key authentication is the default and requires the following environment variables
    ///
    /// `AZURE_STORAGE_ACCOUNT_NAME`
    /// `AZURE_STORAGE_ACCOUNT_KEY`
    ///
    /// See `new_with_token_credential` for alternative authentication methods.
    ///
    pub fn new(file_system_name: impl Into<String> + Clone) -> Result<Self, StorageError> {
        Self::new_from_options(file_system_name, AzureStorageOptions::default())
    }

    /// Create a new [`AdlsGen2Backend`] using a [`TokenCredential`]
    /// See [`azure_identity::token_credentials`] for various implementations
    pub fn new_with_token_credential(
        storage_account_name: impl Into<String>,
        file_system_name: impl Into<String> + Clone,
        token_credential: Arc<dyn TokenCredential>,
    ) -> Result<Self, StorageError> {
        let storage_account_name: String = storage_account_name.into();
        let data_lake_client = DataLakeClient::new_with_token_credential(
            token_credential,
            storage_account_name.clone(),
            None,
            ClientOptions::default(),
        );

        let file_system_client = data_lake_client.into_file_system_client(file_system_name.clone());

        Ok(AdlsGen2Backend {
            storage_account_name,
            file_system_name: file_system_name.into(),
            file_system_client,
            local_pool_handle: LocalPoolHandle::new(1),
        })
    }

    /// Create a new [`AdlsGen2Backend`] using shared key authentication
    pub fn new_with_shared_key(
        storage_account_name: impl Into<String>,
        file_system_name: impl Into<String> + Clone,
        storage_account_key: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let mut options = AzureStorageOptions::new();
        let options = options
            .with_account_name(storage_account_name)
            .with_account_key(storage_account_key);

        Self::new_from_options(file_system_name, options.clone())
    }

    /// Create a new [`AdlsGen2Backend`] using shared key authentication
    pub fn new_with_client(
        storage_account_name: impl Into<String>,
        file_system_name: impl Into<String> + Clone,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let mut options = AzureStorageOptions::new();
        let options = options
            .with_account_name(storage_account_name)
            .with_client_id(client_id)
            .with_client_secret(client_secret)
            .with_tenant_id(tenant_id);

        Self::new_from_options(file_system_name, options.clone())
    }

    /// Create a new [`AdlsGen2Backend`] from AzureStorageOptions
    ///
    /// Currently only shared shared authentication works with this method.
    /// For each authentication method, the following keys are required
    ///
    /// ## Shared Key Authentication
    /// `AZURE_STORAGE_ACCOUNT_NAME`
    /// `AZURE_STORAGE_ACCOUNT_KEY`
    ///
    pub fn new_from_options(
        file_system_name: impl Into<String> + Clone,
        options: AzureStorageOptions,
    ) -> Result<Self, StorageError> {
        let storage_account_name = options.account_name.clone().ok_or_else(|| {
            StorageError::AzureConfig("account name must be provided".to_string())
        })?;

        let data_lake_client: DataLakeClient = options.try_into()?;
        let file_system_client = data_lake_client.into_file_system_client(file_system_name.clone());

        Ok(AdlsGen2Backend {
            storage_account_name,
            file_system_name: file_system_name.into(),
            file_system_client,
            local_pool_handle: LocalPoolHandle::new(1),
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

// TODO: This implementation exists since Azure's Pageable is !Send.
// When this is resolved in the Azure crates, fix this up too
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
