//! Object storage backend abstraction layer for Delta Table transaction logs and data

pub mod file;
pub mod utils;

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub mod s3;

use crate::builder::StorageUrl;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use object_store::{
    path::{Path, DELIMITER},
    DynObjectStore, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result as ObjectStoreResult,
};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

#[cfg(feature = "datafusion-ext")]
use datafusion::datasource::object_store::ObjectStoreUrl;

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Configuration for a DeltaObjectStore
#[derive(Debug, Clone)]
struct DeltaObjectStoreConfig {
    storage_url: StorageUrl,
}

impl DeltaObjectStoreConfig {
    /// Create a new [DeltaObjectStoreConfig]
    pub fn new(storage_url: StorageUrl) -> Self {
        Self { storage_url }
    }

    /// Prefix a path with the table root path
    fn full_path(&self, location: &Path) -> ObjectStoreResult<Path> {
        let path: &str = location.as_ref();
        let stripped = match self.storage_url.prefix.as_ref() {
            "" => path.to_string(),
            p => format!("{}/{}", p, path),
        };
        Ok(Path::parse(stripped.trim_end_matches(DELIMITER))?)
    }

    fn strip_prefix(&self, path: &Path) -> Option<Path> {
        let path: &str = path.as_ref();
        let stripped = match self.storage_url.prefix.as_ref() {
            "" => path,
            p => path.strip_prefix(p)?.strip_prefix(DELIMITER)?,
        };
        Path::parse(stripped).ok()
    }

    /// convert a table [Path] to a fully qualified uri
    pub fn to_uri(&self, location: &Path) -> String {
        let uri = match self.storage_url.scheme() {
            "file" | "" => {
                // On windows the drive (e.g. 'c:') is part of root and must not be prefixed.
                #[cfg(windows)]
                let os_uri = format!("{}/{}", self.storage_url.prefix, location.as_ref());
                #[cfg(unix)]
                let os_uri = format!("/{}/{}", self.storage_url.prefix, location.as_ref());
                os_uri
            }
            _ => format!("{}/{}", self.storage_url.as_str(), location.as_ref()),
        };
        uri.trim_end_matches('/').into()
    }
}

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
    config: DeltaObjectStoreConfig,
}

impl std::fmt::Display for DeltaObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectStore({})", self.config.storage_url.as_str())
    }
}

impl DeltaObjectStore {
    /// Create new DeltaObjectStore
    pub fn new(storage_url: StorageUrl, storage: Arc<DynObjectStore>) -> Self {
        let config = DeltaObjectStoreConfig::new(storage_url);
        Self { storage, config }
    }

    /// Get a reference to the underlying storage backend
    pub fn storage_backend(&self) -> Arc<DynObjectStore> {
        self.storage.clone()
    }

    /// Get fully qualified uri for table root
    pub fn root_uri(&self) -> String {
        self.config.to_uri(&Path::from(""))
    }

    #[cfg(feature = "datafusion-ext")]
    /// generate a unique enough url to identify the store in datafusion.
    pub(crate) fn object_store_url(&self) -> ObjectStoreUrl {
        // we are certain, that the URL can be parsed, since
        // we make sure when we are parsing the table uri
        ObjectStoreUrl::parse(format!(
            "delta-rs://{}",
            // NOTE We need to also replace colons, but its fine, since it just needs
            // to be a unique-ish identifier for the object store in datafusion
            self.config
                .storage_url
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
        self.config.to_uri(location)
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
}

#[async_trait::async_trait]
impl ObjectStore for DeltaObjectStore {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        let full_path = self.config.full_path(location)?;
        self.storage.put(&full_path, bytes).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let full_path = self.config.full_path(location)?;
        self.storage.get(&full_path).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let full_path = self.config.full_path(location)?;
        object_store::ObjectStore::get_range(self.storage.as_ref(), &full_path, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let full_path = self.config.full_path(location)?;
        self.storage.head(&full_path).await.map(|meta| ObjectMeta {
            last_modified: meta.last_modified,
            size: meta.size,
            location: self
                .config
                .strip_prefix(&meta.location)
                .unwrap_or(meta.location),
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let full_path = self.config.full_path(location)?;
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
        let prefix = prefix.and_then(|p| self.config.full_path(p).ok());
        Ok(self
            .storage
            .list(Some(
                &prefix.unwrap_or_else(|| self.config.storage_url.prefix.clone()),
            ))
            .await?
            .map_ok(|meta| ObjectMeta {
                last_modified: meta.last_modified,
                size: meta.size,
                location: self
                    .config
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
        let prefix = prefix.and_then(|p| self.config.full_path(p).ok());
        self.storage
            .list_with_delimiter(Some(
                &prefix.unwrap_or_else(|| self.config.storage_url.prefix.clone()),
            ))
            .await
            .map(|lst| ListResult {
                common_prefixes: lst
                    .common_prefixes
                    .iter()
                    .map(|p| self.config.strip_prefix(p).unwrap_or_else(|| p.clone()))
                    .collect(),
                objects: lst
                    .objects
                    .iter()
                    .map(|meta| ObjectMeta {
                        last_modified: meta.last_modified,
                        size: meta.size,
                        location: self
                            .config
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
        let full_from = self.config.full_path(from)?;
        let full_to = self.config.full_path(to)?;
        self.storage.copy(&full_from, &full_to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from)?;
        let full_to = self.config.full_path(to)?;
        self.storage.copy_if_not_exists(&full_from, &full_to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from)?;
        let full_to = self.config.full_path(to)?;
        self.storage
            .rename_if_not_exists(&full_from, &full_to)
            .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let full_path = self.config.full_path(location)?;
        self.storage.put_multipart(&full_path).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        let full_path = self.config.full_path(location)?;
        self.storage.abort_multipart(&full_path, multipart_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_handling() {
        let storage_url = StorageUrl::parse("s3://bucket").unwrap();
        let file_with_delimiter = Path::from_iter(["a", "b/c", "foo.file"]);
        let config = DeltaObjectStoreConfig::new(storage_url);
        let added = config.full_path(&file_with_delimiter).unwrap();
        assert_eq!(file_with_delimiter, added)
    }
}
