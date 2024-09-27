//! Mount file storage backend. This backend read and write objects from mounted filesystem.
//!
//! The mount file storage backend is not multi-writer safe.

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path as ObjectStorePath, Error as ObjectStoreError, GetOptions,
    GetResult, ListResult, ObjectMeta, ObjectStore, PutOptions, PutResult,
    Result as ObjectStoreResult,
};
use object_store::{MultipartUpload, PutMode, PutMultipartOpts, PutPayload};
use std::ops::Range;
use std::sync::Arc;
use url::Url;

pub(crate) const STORE_NAME: &str = "MountObjectStore";

/// Error raised by storage lock client
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum LocalFileSystemError {
    /// Object exists already at path
    #[error("Object exists already at path: {} ({:?})", path, source)]
    AlreadyExists {
        /// Path of the already existing file
        path: String,
        /// Originating error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Object not found at the given path
    #[error("Object not found at path: {} ({:?})", path, source)]
    NotFound {
        /// Provided path which does not exist
        path: String,
        /// Originating error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Invalid argument sent to OS call
    #[error("Invalid argument in OS call for path: {} ({:?})", path, source)]
    InvalidArgument {
        /// Provided path
        path: String,
        /// Originating error
        source: errno::Errno,
    },

    /// Null error for path for FFI
    #[error("Null error in FFI for path: {} ({:?})", path, source)]
    NullError {
        /// Given path
        path: String,
        /// Originating error
        source: std::ffi::NulError,
    },

    /// Generic catch-all error for this store
    #[error("Generic error in store: {} ({:?})", store, source)]
    Generic {
        /// String name of the object store
        store: &'static str,
        /// Originating error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Errors from the Tokio runtime
    #[error("Error executing async task for path: {} ({:?})", path, source)]
    Tokio {
        /// Path
        path: String,
        /// Originating error
        source: tokio::task::JoinError,
    },
}

impl From<LocalFileSystemError> for ObjectStoreError {
    fn from(e: LocalFileSystemError) -> Self {
        match e {
            LocalFileSystemError::AlreadyExists { path, source } => {
                ObjectStoreError::AlreadyExists { path, source }
            }
            LocalFileSystemError::NotFound { path, source } => {
                ObjectStoreError::NotFound { path, source }
            }
            LocalFileSystemError::InvalidArgument { source, .. } => ObjectStoreError::Generic {
                store: STORE_NAME,
                source: Box::new(source),
            },
            LocalFileSystemError::NullError { source, .. } => ObjectStoreError::Generic {
                store: STORE_NAME,
                source: Box::new(source),
            },
            LocalFileSystemError::Tokio { source, .. } => ObjectStoreError::Generic {
                store: STORE_NAME,
                source: Box::new(source),
            },
            LocalFileSystemError::Generic { store, source } => {
                ObjectStoreError::Generic { store, source }
            }
        }
    }
}

/// Mount File Storage Backend.
/// Note that it's non-atomic writing and may leave the filesystem in an inconsistent state if it fails.
#[derive(Debug)]
pub struct MountFileStorageBackend {
    inner: Arc<LocalFileSystem>,
    root_url: Arc<Url>,
}

impl MountFileStorageBackend {
    /// Creates a new MountFileStorageBackend.
    pub fn try_new(path: impl AsRef<std::path::Path>) -> ObjectStoreResult<Self> {
        Ok(Self {
            root_url: Arc::new(Self::path_to_root_url(path.as_ref())?),
            inner: Arc::new(LocalFileSystem::new_with_prefix(path)?),
        })
    }

    fn path_to_root_url(path: &std::path::Path) -> ObjectStoreResult<Url> {
        let root_path =
            std::fs::canonicalize(path).map_err(|e| object_store::Error::InvalidPath {
                source: object_store::path::Error::Canonicalize {
                    path: path.into(),
                    source: e,
                },
            })?;

        Url::from_file_path(root_path).map_err(|_| object_store::Error::InvalidPath {
            source: object_store::path::Error::InvalidPath { path: path.into() },
        })
    }

    /// Return an absolute filesystem path of the given location
    fn path_to_filesystem(&self, location: &ObjectStorePath) -> String {
        let mut url = self.root_url.as_ref().clone();
        url.path_segments_mut()
            .expect("url path")
            // technically not necessary as Path ignores empty segments
            // but avoids creating paths with "//" which look odd in error messages.
            .pop_if_empty()
            .extend(location.parts());

        url.to_file_path().unwrap().to_str().unwrap().to_owned()
    }
}

impl std::fmt::Display for MountFileStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MountFileStorageBackend")
    }
}

#[async_trait::async_trait]
impl ObjectStore for MountFileStorageBackend {
    async fn put(
        &self,
        location: &ObjectStorePath,
        bytes: PutPayload,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        bytes: PutPayload,
        mut options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        // In mounted storage we do an unsafe rename/overwrite
        // We don't conditionally check whether the file already exists
        options.mode = PutMode::Overwrite;
        self.inner.put_opts(location, bytes, options).await
    }

    async fn get(&self, location: &ObjectStorePath) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectStorePath,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectStorePath) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectStorePath>,
        offset: &ObjectStorePath,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
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
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        let path_from = self.path_to_filesystem(from);
        let path_to = self.path_to_filesystem(to);
        Ok(regular_rename(path_from.as_ref(), path_to.as_ref()).await?)
    }

    async fn put_multipart(
        &self,
        location: &ObjectStorePath,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}

/// Regular renames `from` to `to`.
/// `from` has to exist, but `to` is not, otherwise the operation will fail.
/// It's not atomic and cannot be called in parallel with other operations on the same file.
#[inline]
async fn regular_rename(from: &str, to: &str) -> Result<(), LocalFileSystemError> {
    let from_path = String::from(from);
    let to_path = String::from(to);

    println!("rr {from_path} -> {to_path}");

    tokio::task::spawn_blocking(move || {
        if std::fs::metadata(&to_path).is_ok() {
            Err(LocalFileSystemError::AlreadyExists {
                path: to_path,
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "Already exists",
                )),
            })
        } else if std::path::Path::new(&from_path).exists() {
            std::fs::rename(&from_path, &to_path).map_err(|err| LocalFileSystemError::Generic {
                store: STORE_NAME,
                source: Box::new(err),
            })
        } else {
            Err(LocalFileSystemError::NotFound {
                path: from_path.clone(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Could not find {from_path}"),
                )),
            })
        }
    })
    .await
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    #[tokio::test]
    async fn test_regular_rename() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let a = create_file(tmp_dir.path(), "a");
        let b = create_file(tmp_dir.path(), "b");
        let c = &tmp_dir.path().join("c");

        // unsuccessful move not_exists to C, not_exists is missing
        let result = regular_rename("not_exists", c.to_str().unwrap()).await;
        assert!(matches!(
            result.expect_err("nonexistent should fail"),
            LocalFileSystemError::NotFound { .. }
        ));

        // successful move A to C
        assert!(a.exists());
        assert!(!c.exists());
        match regular_rename(a.to_str().unwrap(), c.to_str().unwrap()).await {
            Err(LocalFileSystemError::InvalidArgument {source, ..}) =>
                panic!("expected success, got: {source:?}. Note: atomically renaming Windows files from WSL2 is not supported."),
            Err(e) => panic!("expected success, got: {e:?}"),
            _ => {}
        }
        assert!(!a.exists());
        assert!(c.exists());

        // unsuccessful move B to C, C already exists, B is not deleted
        assert!(b.exists());
        match regular_rename(b.to_str().unwrap(), c.to_str().unwrap()).await {
            Err(LocalFileSystemError::AlreadyExists { path, .. }) => {
                assert_eq!(path, c.to_str().unwrap())
            }
            _ => panic!("unexpected"),
        }
        assert!(b.exists());
        assert_eq!(std::fs::read_to_string(c).unwrap(), "a");
    }

    fn create_file(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(name.as_bytes()).unwrap();
        path
    }
}
