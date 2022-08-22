//! Local file storage backend. This backend read and write objects from local filesystem.
//!
//! The local file storage backend is multi-writer safe.

use super::StorageError;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path as ObjectStorePath, Error as ObjectStoreError, GetResult,
    ListResult, MultipartId, ObjectMeta as ObjStoreObjectMeta, ObjectStore,
    Result as ObjectStoreResult,
};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

mod rename;

/// Multi-writer support for different platforms:
///
/// * Modern Linux kernels are well supported. However because Linux implementation leverages
/// `RENAME_NOREPLACE`, older versions of the kernel might not work depending on what filesystem is
/// being used:
///   *  ext4 requires >= Linux 3.15
///   *  btrfs, shmem, and cif requires >= Linux 3.17
///   *  xfs requires >= Linux 4.0
///   *  ext2, minix, reiserfs, jfs, vfat, and bpf requires >= Linux 4.9
/// * Darwin is supported but not fully tested.
/// Patches welcome.
/// * Support for other platforms are not implemented at the moment.
#[derive(Debug, Default)]
pub struct FileStorageBackend {
    inner: Arc<LocalFileSystem>,
}

impl FileStorageBackend {
    /// Creates a new FileStorageBackend.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(LocalFileSystem::default()),
        }
    }
}

impl std::fmt::Display for FileStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileStorageBackend")
    }
}

/// Return an absolute filesystem path of the given location
fn path_to_filesystem(location: &ObjectStorePath) -> String {
    let mut url = url::Url::parse("file:///").unwrap();
    url.path_segments_mut()
        .expect("url path")
        // technically not necessary as Path ignores empty segments
        // but avoids creating paths with "//" which look odd in error messages.
        .pop_if_empty()
        .extend(location.parts());

    url.to_file_path().unwrap().to_str().unwrap().to_owned()
}

#[async_trait::async_trait]
impl ObjectStore for FileStorageBackend {
    async fn put(&self, location: &ObjectStorePath, bytes: Bytes) -> ObjectStoreResult<()> {
        self.inner.put(location, bytes).await
    }

    async fn get(&self, location: &ObjectStorePath) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_range(
        &self,
        location: &ObjectStorePath,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectStorePath) -> ObjectStoreResult<ObjStoreObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjStoreObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        _from: &ObjectStorePath,
        _to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn rename_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        let path_from = path_to_filesystem(from);
        let path_to = path_to_filesystem(to);
        rename::rename_noreplace(path_from.as_ref(), path_to.as_ref())
            .await
            .map_err(|err| match err {
                StorageError::AlreadyExists(ref path) => ObjectStoreError::AlreadyExists {
                    path: path.clone(),
                    source: Box::new(err),
                },
                StorageError::NotFound => ObjectStoreError::NotFound {
                    path: from.to_string(),
                    source: Box::new(err),
                },
                _ => ObjectStoreError::Generic {
                    store: "DeltaLocalFileSystem",
                    source: Box::new(err),
                },
            })
    }

    async fn put_multipart(
        &self,
        location: &ObjectStorePath,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &ObjectStorePath,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }
}
