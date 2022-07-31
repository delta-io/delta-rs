//! Object Store implementation for DeltaTable.
//!
//! The object store abstracts all interactions with the underlying storage system.
//! Currently local filesystem, S3, Azure, and GCS are supported.
use crate::{
    get_backend_for_uri_with_options,
    storage::{ObjectMeta as StorageObjectMeta, StorageBackend, StorageError},
};
use bytes::Bytes;
#[cfg(feature = "datafusion-ext")]
use datafusion::datasource::object_store::ObjectStoreUrl;
use futures::stream::BoxStream;
use futures::StreamExt;
use lazy_static::lazy_static;
use object_store::{
    path::{Path, DELIMITER},
    Error as ObjectStoreError, GetResult, ListResult, ObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
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
    storage: Arc<dyn StorageBackend>,
}

impl std::fmt::Display for DeltaObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectStore({}://{})", self.scheme, self.root)
    }
}

impl DeltaObjectStore {
    /// Try creating a new instance of DeltaObjectStore from table uri and storage options
    pub fn try_new_with_options(
        table_uri: impl AsRef<str>,
        storage_options: Option<HashMap<String, String>>,
    ) -> ObjectStoreResult<Self> {
        let storage = get_backend_for_uri_with_options(
            table_uri.as_ref(),
            storage_options.unwrap_or_default(),
        )
        .map_err(|err| ObjectStoreError::Generic {
            store: "DeltaObjectStore",
            source: Box::new(err),
        })?;
        Self::try_new(table_uri, storage)
    }

    /// Try creating a new instance of DeltaObjectStore with specified storage
    pub fn try_new(
        table_uri: impl AsRef<str>,
        storage: Arc<dyn StorageBackend>,
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
        Ok(Self {
            scheme,
            root,
            storage,
        })
    }

    /// Get a reference to the underlying storage backend
    // TODO we should eventually be able to remove this
    pub fn storage_backend(&self) -> Arc<dyn StorageBackend> {
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
                cfg_if::cfg_if! {
                    if #[cfg(target_os = "windows")] {
                        format!("{}/{}", self.root, location.as_ref())
                    } else {
                        format!("/{}/{}", self.root, location.as_ref())
                    }
                }
            }
            _ => format!("{}://{}/{}", self.scheme, self.root, location.as_ref()),
        };
        uri.trim_end_matches('/').to_string()
    }

    #[cfg(feature = "datafusion-ext")]
    /// generate a unique url to identify the store in datafusion.
    /// The
    pub(crate) fn object_store_url(&self) -> ObjectStoreUrl {
        // we are certain, that the URL can be parsed, since
        // we make sure when we are parsing the table uri
        ObjectStoreUrl::parse(format!(
            "delta-rs://{}",
            self.root.as_ref().replace(DELIMITER, "-")
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
        Ok(self
            .storage
            .put_obj(&self.to_uri(location), bytes.as_ref())
            .await?)
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let data = self.storage.get_obj(&self.to_uri(location)).await?;
        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(data.into()) }).boxed(),
        ))
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let store = object_store::local::LocalFileSystem::default();
        let mut root_parts = self.root.parts().collect::<Vec<_>>();
        root_parts.extend(location.parts());
        let path = Path::from_iter(root_parts);
        store.get_range(&path, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let meta = self.storage.head_obj(&self.to_uri(location)).await?;
        convert_object_meta(self.root_uri(), meta)
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        Ok(self.storage.delete_obj(&self.to_uri(location)).await?)
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        let path = match prefix {
            Some(pre) => self.to_uri(pre),
            None => self.root_uri(),
        };
        let root_uri = self.root_uri();
        let stream = self
            .storage
            .list_objs(&path)
            .await?
            .map(|obj| match obj {
                Ok(meta) => convert_object_meta(root_uri.clone(), meta),
                Err(err) => Err(ObjectStoreError::from(err)),
            })
            .collect::<Vec<_>>()
            .await;
        Ok(Box::pin(futures::stream::iter(stream)))
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        todo!()
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        todo!()
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// By default, this is implemented as a copy and then delete source. It may not
    /// check when deleting source that it was the same object that was originally copied.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.copy(from, to).await?;
        self.delete(from).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        todo!()
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        Ok(self
            .storage
            .rename_obj_noreplace(&self.to_uri(from), &self.to_uri(to))
            .await?)
    }
}

#[inline]
/// Return path relative to parent_path
fn extract_rel_path<'a, 'b>(
    parent_path: &'b str,
    path: &'a str,
) -> Result<&'a str, ObjectStoreError> {
    if path.starts_with(&parent_path) {
        Ok(&path[parent_path.len()..])
    } else {
        Err(ObjectStoreError::Generic {
            store: "DeltaObjectStore",
            source: Box::new(StorageError::NotFound),
        })
    }
}

fn convert_object_meta(
    root_uri: String,
    storage_meta: StorageObjectMeta,
) -> ObjectStoreResult<ObjectMeta> {
    Ok(ObjectMeta {
        location: Path::from(extract_rel_path(
            root_uri.as_ref(),
            // HACK hopefully this will hold over until we have switches to object_store
            storage_meta.path.as_str().replace('\\', DELIMITER).as_ref(),
        )?),
        last_modified: storage_meta.modified,
        size: storage_meta.size.unwrap_or_default() as usize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use tokio::fs;

    #[tokio::test]
    async fn test_put() {
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
        let tmp_dir = tempdir::TempDir::new("").unwrap();
        let object_store =
            DeltaObjectStore::try_new_with_options(tmp_dir.path().to_str().unwrap(), None).unwrap();

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
