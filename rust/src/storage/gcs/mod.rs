//! Google Cloud Storage backend.
//!
//! This module is gated behind the "gcs" feature. Its usage also requires
//! the `SERVICE_ACCOUNT` environment variables to be set to the path of
//! credentials with permission to read from the bucket.

mod client;
mod error;
mod object;
mod util;

// Exports
pub(crate) use client::GCSStorageBackend;
pub(crate) use error::GCSClientError;
pub(crate) use object::GCSObject;

use futures::Stream;
use std::convert::TryInto;
use std::pin::Pin;

use log::debug;

use super::{parse_uri, ObjectMeta, StorageBackend, StorageError};

impl GCSStorageBackend {
    pub(crate) fn new() -> Result<Self, StorageError> {
        let cred_path = std::env::var("SERVICE_ACCOUNT")
            .map(std::path::PathBuf::from)
            .map_err(|_err| {
                StorageError::GCSConfig(
                    "SERVICE_ACCOUNT environment variable must be set".to_string(),
                )
            })?;

        Ok(cred_path.try_into()?)
    }
}

impl From<tame_gcs::objects::Metadata> for ObjectMeta {
    fn from(metadata: tame_gcs::objects::Metadata) -> ObjectMeta {
        ObjectMeta {
            path: metadata.name.unwrap(),
            modified: metadata.updated.unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl StorageBackend for GCSStorageBackend {
    /// Fetch object metadata without reading the actual content
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        debug!("getting meta for: {}", path);
        let obj_uri = parse_uri(path)?.into_gcs_object()?;
        let metadata = self.metadata(obj_uri).await?;
        Ok(metadata.into())
    }

    /// Fetch object content
    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("getting object at: {}", path);
        let obj_uri = parse_uri(path)?.into_gcs_object()?;
        match self.download(obj_uri).await {
            Err(GCSClientError::NotFound) => return Err(StorageError::NotFound),
            res => Ok(res?.to_vec()),
        }
    }

    /// Return a list of objects by `path` prefix in an async stream.
    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    > {
        let prefix = parse_uri(path)?.into_gcs_object()?;
        let obj_meta_stream = async_stream::stream! {
            for await meta in self.list(prefix) {
                let obj_meta = meta?;
               yield Ok(obj_meta.into());
            }
        };

        Ok(Box::pin(obj_meta_stream))
    }

    /// Create new object with `obj_bytes` as content.
    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        let dst = parse_uri(path)?.into_gcs_object()?;
        Ok(self.insert(dst, obj_bytes.to_vec()).await?)
    }

    /// Moves object from `src` to `dst`.
    ///
    /// Implementation note:
    ///
    /// For a multi-writer safe backend, `rename_obj` needs to implement `rename if not exists` semantic.
    /// In other words, if the destination path already exists, rename should return a
    /// [StorageError::AlreadyExists] error.
    async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        let src_uri = parse_uri(src)?.into_gcs_object()?;
        let dst_uri = parse_uri(dst)?.into_gcs_object()?;
        match self.rename_noreplace(src_uri, dst_uri).await {
            Err(GCSClientError::PreconditionFailed) => {
                return Err(StorageError::AlreadyExists(dst.to_string()))
            }
            res => Ok(res?),
        }
    }

    /// Deletes object by `path`.
    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        let uri = parse_uri(path)?.into_gcs_object()?;
        Ok(self.delete(uri).await?)
    }
}
