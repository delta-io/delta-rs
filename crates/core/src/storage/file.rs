//! Local file storage backend. This backend read and write objects from local filesystem.
//!
//! The local file storage backend is multi-writer safe.
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path as ObjectStorePath, Error as ObjectStoreError, GetOptions,
    GetResult, ListResult, ObjectMeta, ObjectStore, PutOptions, PutResult,
    Result as ObjectStoreResult,
};
use object_store::{MultipartUpload, PutMultipartOpts, PutPayload};
use url::Url;

const STORE_NAME: &str = "DeltaLocalObjectStore";

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

/// Multi-writer support for different platforms:
///
/// * Modern Linux kernels are well supported. However because Linux implementation leverages
///   `RENAME_NOREPLACE`, older versions of the kernel might not work depending on what filesystem is
///   being used:
///   *  ext4 requires >= Linux 3.15
///   *  btrfs, shmem, and cif requires >= Linux 3.17
///   *  xfs requires >= Linux 4.0
///   *  ext2, minix, reiserfs, jfs, vfat, and bpf requires >= Linux 4.9
/// * Darwin is supported but not fully tested.
///   Patches welcome.
/// * Support for other platforms are not implemented at the moment.
#[derive(Debug)]
pub struct FileStorageBackend {
    inner: Arc<LocalFileSystem>,
    root_url: Arc<Url>,
}

impl FileStorageBackend {
    /// Creates a new FileStorageBackend.
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

impl std::fmt::Display for FileStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileStorageBackend")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FileStorageBackend {
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
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
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
        Ok(rename_noreplace(path_from.as_ref(), path_to.as_ref()).await?)
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

/// Atomically renames `from` to `to`.
/// `from` has to exist, but `to` is not, otherwise the operation will fail.
#[inline]
async fn rename_noreplace(from: &str, to: &str) -> Result<(), LocalFileSystemError> {
    imp::rename_noreplace(from, to).await
}

// Generic implementation (Requires 2 system calls)
#[cfg(not(any(all(target_os = "linux", target_env = "gnu"), target_os = "macos")))]
mod imp {
    use super::*;

    pub(super) async fn rename_noreplace(from: &str, to: &str) -> Result<(), LocalFileSystemError> {
        let from_path = String::from(from);
        let to_path = String::from(to);

        tokio::task::spawn_blocking(move || {
            std::fs::hard_link(&from_path, &to_path).map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    LocalFileSystemError::AlreadyExists {
                        path: to_path,
                        source: Box::new(err),
                    }
                } else if err.kind() == std::io::ErrorKind::NotFound {
                    LocalFileSystemError::NotFound {
                        path: from_path.clone(),
                        source: Box::new(err),
                    }
                } else {
                    LocalFileSystemError::Generic {
                        store: STORE_NAME,
                        source: Box::new(err),
                    }
                }
            })?;

            std::fs::remove_file(from_path).map_err(|err| LocalFileSystemError::Generic {
                store: STORE_NAME,
                source: Box::new(err),
            })?;

            Ok(())
        })
        .await
        .unwrap()
    }
}

// Optimized implementations (Only 1 system call)
#[cfg(any(all(target_os = "linux", target_env = "gnu"), target_os = "macos"))]
mod imp {
    use super::*;
    use std::ffi::CString;

    fn to_c_string(p: &str) -> Result<CString, LocalFileSystemError> {
        CString::new(p).map_err(|err| LocalFileSystemError::NullError {
            path: p.into(),
            source: err,
        })
    }

    pub(super) async fn rename_noreplace(from: &str, to: &str) -> Result<(), LocalFileSystemError> {
        let cs_from = to_c_string(from)?;
        let cs_to = to_c_string(to)?;

        let ret = unsafe {
            tokio::task::spawn_blocking(move || {
                let ret = platform_specific_rename(cs_from.as_ptr(), cs_to.as_ptr());
                if ret != 0 {
                    Err(errno::errno())
                } else {
                    Ok(())
                }
            })
            .await
            .map_err(|err| LocalFileSystemError::Tokio {
                path: from.into(),
                source: err,
            })?
        };

        match ret {
            Err(e) if e.0 == libc::EEXIST => Err(LocalFileSystemError::AlreadyExists {
                path: to.into(),
                source: Box::new(e),
            }),
            Err(e) if e.0 == libc::ENOENT => Err(LocalFileSystemError::NotFound {
                path: to.into(),
                source: Box::new(e),
            }),
            Err(e) if e.0 == libc::EINVAL => Err(LocalFileSystemError::InvalidArgument {
                path: to.into(),
                source: e,
            }),
            Err(e) => Err(LocalFileSystemError::Generic {
                store: STORE_NAME,
                source: Box::new(e),
            }),
            Ok(_) => Ok(()),
        }
    }

    #[allow(unused_variables)]
    unsafe fn platform_specific_rename(from: *const libc::c_char, to: *const libc::c_char) -> i32 {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", target_env = "gnu"))] {
                libc::renameat2(libc::AT_FDCWD, from, libc::AT_FDCWD, to, libc::RENAME_NOREPLACE)
            } else if #[cfg(target_os = "macos")] {
                libc::renamex_np(from, to, libc::RENAME_EXCL)
            } else {
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    #[tokio::test]
    async fn test_rename_noreplace() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let a = create_file(tmp_dir.path(), "a");
        let b = create_file(tmp_dir.path(), "b");
        let c = &tmp_dir.path().join("c");

        // unsuccessful move not_exists to C, not_exists is missing
        let result = rename_noreplace("not_exists", c.to_str().unwrap()).await;
        assert!(matches!(
            result.expect_err("nonexistent should fail"),
            LocalFileSystemError::NotFound { .. }
        ));

        // successful move A to C
        assert!(a.exists());
        assert!(!c.exists());
        match rename_noreplace(a.to_str().unwrap(), c.to_str().unwrap()).await {
            Err(LocalFileSystemError::InvalidArgument {source, ..}) =>
                panic!("expected success, got: {source:?}. Note: atomically renaming Windows files from WSL2 is not supported."),
            Err(e) => panic!("expected success, got: {e:?}"),
            _ => {}
        }
        assert!(!a.exists());
        assert!(c.exists());

        // unsuccessful move B to C, C already exists, B is not deleted
        assert!(b.exists());
        match rename_noreplace(b.to_str().unwrap(), c.to_str().unwrap()).await {
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
