//! Google Cloud Storage backend. It currently only supports read operations.
//!
//! This module is gated behind the "gcs" feature. Its usage also requires
//! the `SERVICE_ACCOUNT` environment variables to be set to the path of
//! credentials with permission to read from the bucket.

use std::fmt::Debug;
use std::{fmt, pin::Pin};

use futures::Stream;
use log::debug;

use super::{ObjectMeta, StorageBackend, StorageError};

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
}

impl GCSStorageBackend {
    /// Creates a new S3StorageBackend.
    pub fn new() -> Result<Self, StorageError> {
        Ok(Self {})
    }
}

impl Default for GCSStorageBackend {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

/*
impl std::fmt::Debug for S3StorageBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "S3StorageBackend")
    }
}
*/

#[async_trait::async_trait]
impl StorageBackend for GCSStorageBackend {
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        debug!("Getting properties for {}", path);
        unimplemented!("head_obj not implemented for gcs");
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        debug!("Loading {}", path);
        unimplemented!("get_obj not implemented for gcs");
    }

    async fn list_objs<'a>(&'a self, _path: &'a str,)
    -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    > 
    {
        unimplemented!("list_objs not implemented for gcs");
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
