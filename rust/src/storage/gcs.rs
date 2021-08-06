//! Google Cloud Storage backend. It currently only supports read operations.
//!
//! This module is gated behind the "gcs" feature. Its usage also requires
//! the `SERVICE_ACCOUNT` environment variables to be set to the path of
//! credentials with permission to read from the bucket.

use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::{fmt, pin::Pin};

use futures::{Stream};
use log::debug;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};

/// Struct describing an object stored in GCS.
#[derive(Debug, PartialEq)]
pub struct GCSObject<'a> {
    /// The bucket where the object is stored.
    pub bucket: &'a str,
    /// The key of the object within the bucket.
    pub key: &'a str,
}

impl<'a> fmt::Display for GCSObject<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "gs://{}/{}", self.bucket, self.key)
    }
}

/// A storage backend backed by Google Cloud Storage
#[derive(Debug)]
pub struct GCSStorageBackend {
    client: cloud_storage::Client,
}

impl GCSStorageBackend {
    /// Creates a new S3StorageBackend.
    pub fn new() -> Result<Self, StorageError> {
        let _= std::env::var("SERVICE_ACCOUNT").map_err(|_| {
            StorageError::GCSConfig("SERVICE_ACCOUNT environment variable must be set".to_string())
        })?;

        Ok(Self {
            client: Default::default()
        })
    }
}

impl Default for GCSStorageBackend {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

fn to_storage_err(error: cloud_storage::Error) -> StorageError {
    match error {
        // The cloud_storage api just gives us a not found message in a catch all variant,
        // as a result we need to parse the string to determine if the object exists in the storage
        // backend or some other error occurred.
        cloud_storage::Error::Other(err_str) if err_str.starts_with("No such object:") => StorageError::NotFound,
        _ => StorageError::GCSError { source: error },
    }
}

impl TryFrom<cloud_storage::Object> for ObjectMeta {
    type Error = StorageError;

    fn try_from(object: cloud_storage::Object) -> Result<Self, Self::Error> {
        Ok(ObjectMeta {
            path: object.name,
            modified: object.updated,
        })
    }
}

#[async_trait::async_trait]
impl StorageBackend for GCSStorageBackend {
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        debug!("Getting properties for {}", path);
        let uri = parse_uri(path)?.into_gcs_object()?;
        let object_meta = self.client
            .object()
            .read(&uri.bucket, &uri.key)
            .await?;

        Ok(ObjectMeta {
            path: path.to_string(),
            modified: object_meta.updated,
        })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("fetching gcs object: {}...", path);
        let uri = parse_uri(path)?.into_gcs_object()?;

        self.client
            .object()
            .download(&uri.bucket, &uri.key)
            .await
            .map_err(to_storage_err)
    }

    async fn list_objs<'a>(&'a self, path: &'a str)
    -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    >
    {
        let uri = parse_uri(path)?.into_gcs_object()?;

        // The cloud storage library will issue multiple requests to GCP, if the returned
        // ObjectList contains a next_page_token, which is utilized in subsequent requests.
        let list_request = cloud_storage::object::ListRequest {
            prefix: Some(uri.key.to_string()),
            ..Default::default()
        };

        let object_lists =
            self.client
                .object()
                .list(uri.bucket, list_request)
                .await?;

        let obj_meta_stream = async_stream::stream! {
            for await object_list in object_lists {
                for item in object_list?.items {
                    yield item.try_into();
                }
            }
        };

        Ok(Box::pin(obj_meta_stream))
    }

    async fn put_obj(&self, _path: &str, _obj_bytes: &[u8]) -> Result<(), StorageError> {
        unimplemented!("put_obj not implemented for gcs");
    }

    async fn rename_obj(&self, _src: &str, _dst: &str) -> Result<(), StorageError> {
        unimplemented!("rename_obj not implemented for gcs");
    }

    async fn delete_obj(&self, _path: &str) -> Result<(), StorageError> {
        unimplemented!("delete_obj not implemented for gcs");
    }
}
