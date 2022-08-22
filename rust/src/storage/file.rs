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

mod rename {
    use crate::StorageError;

    // Generic implementation (Requires 2 system calls)
    #[cfg(not(any(
        all(target_os = "linux", target_env = "gnu", glibc_renameat2),
        target_os = "macos"
    )))]
    mod imp {
        use super::*;

        pub async fn rename_noreplace(from: &str, to: &str) -> Result<(), StorageError> {
            let from_path = String::from(from);
            let to_path = String::from(to);

            tokio::task::spawn_blocking(move || {
                std::fs::hard_link(&from_path, &to_path).map_err(|err| {
                    if err.kind() == std::io::ErrorKind::AlreadyExists {
                        StorageError::AlreadyExists(to_path)
                    } else {
                        err.into()
                    }
                })?;

                std::fs::remove_file(from_path)?;

                Ok(())
            })
            .await
            .unwrap()
        }
    }

    // Optimized implementations (Only 1 system call)
    #[cfg(any(
        all(target_os = "linux", target_env = "gnu", glibc_renameat2),
        target_os = "macos"
    ))]
    mod imp {
        use super::*;
        use std::ffi::CString;

        fn to_c_string(p: &str) -> Result<CString, StorageError> {
            CString::new(p).map_err(|e| StorageError::Generic(format!("{}", e)))
        }

        pub async fn rename_noreplace(from: &str, to: &str) -> Result<(), StorageError> {
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
                .unwrap()
            };

            match ret {
                Err(e) => {
                    if let libc::EEXIST = e.0 {
                        return Err(StorageError::AlreadyExists(String::from(to)));
                    }
                    if let libc::EINVAL = e.0 {
                        return Err(StorageError::Generic(format!(
                            "rename_noreplace failed with message '{}'",
                            e
                        )));
                    }
                    Err(StorageError::other_std_io_err(format!(
                        "failed to rename {} to {}: {}",
                        from, to, e
                    )))
                }
                Ok(_) => Ok(()),
            }
        }

        #[allow(unused_variables)]
        unsafe fn platform_specific_rename(
            from: *const libc::c_char,
            to: *const libc::c_char,
        ) -> i32 {
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

    /// Atomically renames `from` to `to`.
    /// `from` has to exist, but `to` is not, otherwise the operation will fail.
    #[inline]
    pub async fn rename_noreplace(from: &str, to: &str) -> Result<(), StorageError> {
        imp::rename_noreplace(from, to).await
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::fs::File;
        use std::io::Write;
        use std::path::{Path, PathBuf};

        #[tokio::test()]
        async fn test_rename_noreplace() {
            let tmp_dir = tempdir::TempDir::new_in(".", "test_rename_noreplace").unwrap();
            let a = create_file(&tmp_dir.path(), "a");
            let b = create_file(&tmp_dir.path(), "b");
            let c = &tmp_dir.path().join("c");

            // unsuccessful move not_exists to C, not_exists is missing
            match rename_noreplace("not_exists", c.to_str().unwrap()).await {
                Err(StorageError::NotFound) => {}
                Err(StorageError::Io { source: e }) => {
                    cfg_if::cfg_if! {
                        if #[cfg(target_os = "windows")] {
                            assert_eq!(
                                e.to_string(),
                                format!(
                                    "failed to rename not_exists to {}: The system cannot find the file specified. (os error 2)",
                                    c.to_str().unwrap()
                                )
                            );
                        } else {
                            assert_eq!(
                                e.to_string(),
                                format!(
                                    "failed to rename not_exists to {}: No such file or directory",
                                    c.to_str().unwrap()
                                )
                            );
                        }
                    }
                }
                Err(e) => panic!("expect std::io::Error, got: {:#}", e),
                Ok(()) => panic!("{}", "expect rename to fail with Err, but got Ok"),
            }

            // successful move A to C
            assert!(a.exists());
            assert!(!c.exists());
            match rename_noreplace(a.to_str().unwrap(), c.to_str().unwrap()).await {
            Err(StorageError::Generic(e)) if e == "rename_noreplace failed with message 'Invalid argument'" =>
                panic!("expected success, got: {:?}. Note: atomically renaming Windows files from WSL2 is not supported.", e),
            Err(e) => panic!("expected success, got: {:?}", e),
            _ => {}
        }
            assert!(!a.exists());
            assert!(c.exists());

            // unsuccessful move B to C, C already exists, B is not deleted
            assert!(b.exists());
            match rename_noreplace(b.to_str().unwrap(), c.to_str().unwrap()).await {
                Err(StorageError::AlreadyExists(p)) => assert_eq!(p, c.to_str().unwrap()),
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
}
