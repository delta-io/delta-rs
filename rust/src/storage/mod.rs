//! Object storage backend abstraction layer for Delta Table transaction logs and data

pub mod file;
pub mod utils;

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub mod s3;

use crate::builder::StorageLocation;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
pub use object_store::{
    path::{Path, DELIMITER},
    DynObjectStore, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result as ObjectStoreResult,
};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

use crate::get_storage_backend;
#[cfg(feature = "datafusion")]
use datafusion::datasource::object_store::ObjectStoreUrl;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Sharable reference to [`DeltaObjectStore`]
pub type ObjectStoreRef = Arc<DeltaObjectStore>;

/// Object Store implementation for DeltaTable.
///
/// The [DeltaObjectStore] implements the [object_store::ObjectStore] trait to facilitate
/// interoperability with the larger rust / arrow ecosystem. Specifically it can directly
/// be registered as store within datafusion.
///
/// The table root is treated as the root of the object store.
/// All [Path] are reported relative to the table root.
#[derive(Debug, Clone)]
pub struct DeltaObjectStore {
    storage: Arc<DynObjectStore>,
    location: StorageLocation,
}

impl Serialize for DeltaObjectStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.location.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DeltaObjectStore {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config = StorageLocation::deserialize(deserializer)?;
        let (storage, storage_url) = get_storage_backend(
            config.clone(),
            None,       // TODO: config options
            Some(true), // TODO: this isn't preserved after builder stage
        )
        .map_err(|_| D::Error::missing_field("storage"))?;
        let storage = Arc::new(DeltaObjectStore::new(storage_url, storage));
        Ok(DeltaObjectStore {
            storage,
            location: config,
        })
    }
}

impl std::fmt::Display for DeltaObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectStore({})", self.location.as_ref())
    }
}

impl DeltaObjectStore {
    /// Create new DeltaObjectStore
    pub fn new(storage_url: StorageLocation, storage: Arc<DynObjectStore>) -> Self {
        Self {
            storage,
            location: storage_url,
        }
    }

    /// Get a reference to the underlying storage backend
    pub fn storage_backend(&self) -> Arc<DynObjectStore> {
        self.storage.clone()
    }

    /// Get fully qualified uri for table root
    pub fn root_uri(&self) -> String {
        self.location.to_uri(&Path::from(""))
    }

    #[cfg(feature = "datafusion")]
    /// generate a unique enough url to identify the store in datafusion.
    pub(crate) fn object_store_url(&self) -> ObjectStoreUrl {
        // we are certain, that the URL can be parsed, since
        // we make sure when we are parsing the table uri
        ObjectStoreUrl::parse(format!(
            "delta-rs://{}",
            // NOTE We need to also replace colons, but its fine, since it just needs
            // to be a unique-ish identifier for the object store in datafusion
            self.location
                .prefix
                .as_ref()
                .replace(DELIMITER, "-")
                .replace(':', "-")
        ))
        .expect("Invalid object store url.")
    }

    /// [Path] to Delta log
    pub fn log_path(&self) -> &Path {
        &DELTA_LOG_PATH
    }

    /// [Path] to Delta log
    pub fn to_uri(&self, location: &Path) -> String {
        self.location.to_uri(location)
    }

    /// Deletes object by `paths`.
    pub async fn delete_batch(&self, paths: &[Path]) -> ObjectStoreResult<()> {
        for path in paths {
            match self.delete(path).await {
                Ok(_) => continue,
                Err(ObjectStoreError::NotFound { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Check if the location is a delta table location
    pub async fn is_delta_table_location(&self) -> ObjectStoreResult<bool> {
        // TODO We should really be using HEAD here, but this fails in windows tests
        let mut stream = self.list(Some(self.log_path())).await?;
        if let Some(res) = stream.next().await {
            match res {
                Ok(_) => Ok(true),
                Err(ObjectStoreError::NotFound { .. }) => Ok(false),
                Err(err) => Err(err),
            }
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeltaObjectStore {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        let full_path = self.location.full_path(location);
        self.storage.put(&full_path, bytes).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let full_path = self.location.full_path(location);
        self.storage.get(&full_path).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let full_path = self.location.full_path(location);
        object_store::ObjectStore::get_range(self.storage.as_ref(), &full_path, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let full_path = self.location.full_path(location);
        self.storage.head(&full_path).await.map(|meta| ObjectMeta {
            last_modified: meta.last_modified,
            size: meta.size,
            location: self
                .location
                .strip_prefix(&meta.location)
                .unwrap_or(meta.location),
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let full_path = self.location.full_path(location);
        self.storage.delete(&full_path).await
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        let prefix = prefix.map(|p| self.location.full_path(p));
        Ok(self
            .storage
            .list(Some(
                &prefix.unwrap_or_else(|| self.location.prefix.clone()),
            ))
            .await?
            .map_ok(|meta| ObjectMeta {
                last_modified: meta.last_modified,
                size: meta.size,
                location: self
                    .location
                    .strip_prefix(&meta.location)
                    .unwrap_or(meta.location),
            })
            .boxed())
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix = prefix.map(|p| self.location.full_path(p));
        self.storage
            .list_with_delimiter(Some(
                &prefix.unwrap_or_else(|| self.location.prefix.clone()),
            ))
            .await
            .map(|lst| ListResult {
                common_prefixes: lst
                    .common_prefixes
                    .iter()
                    .map(|p| self.location.strip_prefix(p).unwrap_or_else(|| p.clone()))
                    .collect(),
                objects: lst
                    .objects
                    .iter()
                    .map(|meta| ObjectMeta {
                        last_modified: meta.last_modified,
                        size: meta.size,
                        location: self
                            .location
                            .strip_prefix(&meta.location)
                            .unwrap_or_else(|| meta.location.clone()),
                    })
                    .collect(),
            })
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.location.full_path(from);
        let full_to = self.location.full_path(to);
        self.storage.copy(&full_from, &full_to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.location.full_path(from);
        let full_to = self.location.full_path(to);
        self.storage.copy_if_not_exists(&full_from, &full_to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.location.full_path(from);
        let full_to = self.location.full_path(to);
        self.storage
            .rename_if_not_exists(&full_from, &full_to)
            .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let full_path = self.location.full_path(location);
        self.storage.put_multipart(&full_path).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        let full_path = self.location.full_path(location);
        self.storage.abort_multipart(&full_path, multipart_id).await
    }
}
