//! Object Store implementation for DeltaTable.
//!
//! The object store abstracts all interactions with the underlying storage system.
//! Currently local filesystem, S3, Azure, and GCS are supported.
use crate::storage::StorageError;
use bytes::Bytes;
#[cfg(feature = "datafusion-ext")]
use datafusion::datasource::object_store::ObjectStoreUrl;
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
use url::{ParseError, Url};

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

impl From<StorageError> for ObjectStoreError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => ObjectStoreError::NotFound {
                path: "".to_string(),
                source: Box::new(error),
            },
            StorageError::AlreadyExists(ref path) => ObjectStoreError::AlreadyExists {
                path: path.clone(),
                source: Box::new(error),
            },
            other => ObjectStoreError::Generic {
                store: "DeltaObjectStore",
                source: Box::new(other),
            },
        }
    }
}

/// Configuration for a DeltaObjectStore
#[derive(Debug, Clone)]
pub struct DeltaObjectStoreConfig {
    table_root: Path,
}

impl DeltaObjectStoreConfig {
    /// Create a new [DeltaObjectStoreConfig]
    pub fn new(table_root: impl Into<Path>) -> Self {
        Self {
            table_root: table_root.into(),
        }
    }

    /// Prefix a path with the table root path
    fn full_path(&self, location: &Path) -> Path {
        Path::from_iter(self.table_root.parts().chain(location.parts()))
    }

    fn strip_prefix(&self, path: &Path) -> Option<Path> {
        let path: &str = path.as_ref();
        let stripped = match self.table_root.as_ref() {
            "" => path,
            p => path.strip_prefix(p)?.strip_prefix(DELIMITER)?,
        };
        Some(Path::from_iter(stripped.split(DELIMITER)))
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
    scheme: String,
    root: Path,
    storage: Arc<DynObjectStore>,
    config: DeltaObjectStoreConfig,
}

impl std::fmt::Display for DeltaObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectStore({}://{})", self.scheme, self.root)
    }
}

impl DeltaObjectStore {
    /// Create new DeltaObjectStore
    pub fn new(table_root: &Path, storage: Arc<DynObjectStore>) -> Self {
        let config = DeltaObjectStoreConfig::new(table_root.clone());
        Self {
            scheme: String::from("file"),
            root: table_root.clone(),
            storage,
            config,
        }
    }

    /// Try creating a new instance of DeltaObjectStore with specified storage
    pub fn try_new(
        table_uri: impl AsRef<str>,
        storage: Arc<DynObjectStore>,
    ) -> ObjectStoreResult<Self> {
        let (scheme, root) = match Url::parse(table_uri.as_ref()) {
            Ok(result) => {
                match result.scheme() {
                    "file" | "gs" | "s3" | "adls2" | "" => {
                        let raw_path =
                            format!("{}{}", result.domain().unwrap_or_default(), result.path());
                        let root = Path::parse(raw_path)?;
                        Ok((result.scheme().to_string(), root))
                    }
                    _ => {
                        // Since we did find some base / scheme, but don't recognize it, it
                        // may be a local path (i.e. c:/.. on windows). We need to pipe it through path though
                        // to get consistent path separators.
                        let local_path = std::path::Path::new(table_uri.as_ref());
                        let root = Path::from_filesystem_path(local_path)?;
                        Ok(("file".to_string(), root))
                    }
                }
            }
            Err(ParseError::RelativeUrlWithoutBase) => {
                let local_path = std::path::Path::new(table_uri.as_ref());
                let root = Path::from_filesystem_path(local_path)?;
                Ok(("file".to_string(), root))
            }
            Err(err) => Err(ObjectStoreError::Generic {
                store: "DeltaObjectStore",
                source: Box::new(err),
            }),
        }?;
        let config = DeltaObjectStoreConfig::new(root.clone());
        Ok(Self {
            scheme,
            root,
            storage,
            config,
        })
    }

    /// Get a reference to the underlying storage backend
    pub fn storage_backend(&self) -> Arc<DynObjectStore> {
        self.storage.clone()
    }

    /// Get fully qualified uri for table root
    pub fn root_uri(&self) -> String {
        self.to_uri(&Path::from(""))
    }

    /// convert a table [Path] to a fully qualified uri
    pub fn to_uri(&self, location: &Path) -> String {
        let uri = match self.scheme.as_ref() {
            "file" | "" => {
                // On windows the drive (e.g. 'c:') is part of root and must not be prefixed.
                #[cfg(windows)]
                let os_uri = format!("{}/{}", self.root, location.as_ref());
                #[cfg(unix)]
                let os_uri = format!("/{}/{}", self.root, location.as_ref());
                os_uri
            }
            _ => format!("{}://{}/{}", self.scheme, self.root, location.as_ref()),
        };
        uri.trim_end_matches('/').to_string()
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
            self.root.as_ref().replace(DELIMITER, "-").replace(':', "-")
        ))
        .expect("Invalid object store url.")
    }

    /// [Path] to Delta log
    pub fn log_path(&self) -> &Path {
        &DELTA_LOG_PATH
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
        let full_path = self.config.full_path(location);
        self.storage.put(&full_path, bytes).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let full_path = self.config.full_path(location);
        self.storage.get(&full_path).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let full_path = self.config.full_path(location);
        object_store::ObjectStore::get_range(self.storage.as_ref(), &full_path, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let full_path = self.config.full_path(location);
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
        let full_path = self.config.full_path(location);
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
        let prefix = prefix.map(|p| self.config.full_path(p));
        Ok(self
            .storage
            .list(Some(&prefix.unwrap_or(self.root.clone())))
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
        let prefix = prefix.map(|p| self.config.full_path(p));
        self.storage
            .list_with_delimiter(Some(&prefix.unwrap_or(self.root.clone())))
            .await
            .map(|lst| ListResult {
                common_prefixes: lst
                    .common_prefixes
                    .iter()
                    .map(|p| self.config.strip_prefix(p).unwrap_or(p.clone()))
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
                            .unwrap_or(meta.location.clone()),
                    })
                    .collect(),
            })
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from);
        let full_to = self.config.full_path(to);
        self.storage.copy(&full_from, &full_to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from);
        let full_to = self.config.full_path(to);
        self.storage.copy_if_not_exists(&full_from, &full_to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from);
        let full_to = self.config.full_path(to);
        self.storage
            .rename_if_not_exists(&full_from, &full_to)
            .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let full_path = self.config.full_path(location);
        self.storage.put_multipart(&full_path).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        let full_path = self.config.full_path(location);
        self.storage.abort_multipart(&full_path, multipart_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use tokio::fs;

    fn create_local_test_store() -> (Arc<DeltaObjectStore>, tempdir::TempDir) {
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let store =
            crate::builder::DeltaTableBuilder::try_from_uri(tmp_dir.path().to_str().unwrap())
                .unwrap()
                .build_storage()
                .unwrap();
        (store, tmp_dir)
    }

    #[tokio::test]
    async fn test_put() {
        let (object_store, tmp_dir) = create_local_test_store();

        // put object
        let tmp_file_path1 = tmp_dir.path().join("tmp_file1");
        let path1 = Path::from("tmp_file1");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();
        assert!(fs::metadata(tmp_file_path1).await.is_ok());

        let tmp_file_path2 = tmp_dir.path().join("tmp_dir1").join("file");
        let path2 = Path::from("tmp_dir1/file");
        object_store.put(&path2, bytes::Bytes::new()).await.unwrap();
        assert!(fs::metadata(tmp_file_path2).await.is_ok())
    }

    #[tokio::test]
    async fn test_head() {
        let (object_store, _tmp_dir) = create_local_test_store();

        // existing file
        let path1 = Path::from("tmp_file1");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();
        let meta = object_store.head(&path1).await;
        assert!(meta.is_ok());

        // nonexistent file
        let path2 = Path::from("nonexistent");
        let meta = object_store.head(&path2).await;
        assert!(meta.is_err());
    }

    #[tokio::test]
    async fn test_get() {
        let (object_store, _tmp_dir) = create_local_test_store();

        // existing file
        let path1 = Path::from("tmp_file1");
        let data = bytes::Bytes::from("random data");
        object_store.put(&path1, data.clone()).await.unwrap();
        let data_get = object_store
            .get(&path1)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, data_get);
    }

    #[tokio::test]
    async fn test_delete() {
        let (object_store, tmp_dir) = create_local_test_store();

        let tmp_file_path1 = tmp_dir.path().join("tmp_file1");

        // put object
        let path1 = Path::from("tmp_file1");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();
        assert!(fs::metadata(tmp_file_path1.clone()).await.is_ok());

        // delete object
        object_store.delete(&path1).await.unwrap();
        assert!(fs::metadata(tmp_file_path1).await.is_err());
    }

    #[tokio::test]
    async fn test_delete_batch() {
        let (object_store, tmp_dir) = create_local_test_store();

        let tmp_file_path1 = tmp_dir.path().join("tmp_file1");
        let tmp_file_path2 = tmp_dir.path().join("tmp_file2");

        // put object
        let path1 = Path::from("tmp_file1");
        let path2 = Path::from("tmp_file2");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();
        object_store.put(&path2, bytes::Bytes::new()).await.unwrap();
        assert!(fs::metadata(tmp_file_path1.clone()).await.is_ok());
        assert!(fs::metadata(tmp_file_path2.clone()).await.is_ok());

        // delete objects
        object_store.delete_batch(&[path1, path2]).await.unwrap();
        assert!(fs::metadata(tmp_file_path1).await.is_err());
        assert!(fs::metadata(tmp_file_path2).await.is_err())
    }

    #[tokio::test]
    async fn test_list() {
        let (object_store, _tmp_dir) = create_local_test_store();

        let path1 = Path::from("tmp_file1");
        let path2 = Path::from("tmp_file2");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();
        object_store.put(&path2, bytes::Bytes::new()).await.unwrap();

        let objs = object_store
            .list(None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objs.len(), 2);

        let path1 = Path::from("prefix/tmp_file1");
        let path2 = Path::from("prefix/tmp_file2");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();
        object_store.put(&path2, bytes::Bytes::new()).await.unwrap();

        let objs = object_store
            .list(None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objs.len(), 4);

        let objs = object_store
            .list(Some(&Path::from("prefix")))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objs.len(), 2)
    }

    #[tokio::test]
    async fn test_list_prefix() {
        let (object_store, _tmp_dir) = create_local_test_store();

        let path1 = Path::from("_delta_log/tmp_file1");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();

        let objs = object_store
            .list(None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objs[0].location, path1)
    }

    #[tokio::test]
    async fn test_rename_if_not_exists() {
        let (object_store, tmp_dir) = create_local_test_store();

        let tmp_file_path1 = tmp_dir.path().join("tmp_file1");
        let tmp_file_path2 = tmp_dir.path().join("tmp_file2");

        let path1 = Path::from("tmp_file1");
        let path2 = Path::from("tmp_file2");
        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();

        // delete objects
        let result = object_store.rename_if_not_exists(&path1, &path2).await;
        assert!(result.is_ok());
        assert!(fs::metadata(tmp_file_path1.clone()).await.is_err());
        assert!(fs::metadata(tmp_file_path2.clone()).await.is_ok());

        object_store.put(&path1, bytes::Bytes::new()).await.unwrap();
        let result = object_store.rename_if_not_exists(&path1, &path2).await;
        assert!(result.is_err());
        assert!(fs::metadata(tmp_file_path1).await.is_ok());
        assert!(fs::metadata(tmp_file_path2).await.is_ok());
    }
}
