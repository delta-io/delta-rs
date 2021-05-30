//! Local file storage backend. This backend read and write objects from local filesystem.
//!
//! The local file storage backend is multi-writer safe.

use std::path::{Path, PathBuf};
use std::pin::Pin;

use chrono::DateTime;
use futures::{Stream, TryStreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReadDirStream;

use super::{ObjectMeta, StorageBackend, StorageError};

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
/// * Windows is not supported because we are not using native atomic file rename system call.
/// Patches welcome.
/// * Support for other platforms are not implemented at the moment.
#[derive(Default, Debug)]
pub struct FileStorageBackend {
    root: String,
}

impl FileStorageBackend {
    /// Creates a new FileStorageBackend.
    pub fn new(root: &str) -> Self {
        Self {
            root: String::from(root),
        }
    }
}

#[async_trait::async_trait]
impl StorageBackend for FileStorageBackend {
    #[inline]
    fn join_path(&self, path: &str, path_to_join: &str) -> String {
        let new_path = Path::new(path);
        new_path
            .join(path_to_join)
            .into_os_string()
            .into_string()
            .unwrap()
    }

    #[inline]
    fn join_paths(&self, paths: &[&str]) -> String {
        let mut iter = paths.iter();
        let mut path = PathBuf::from(iter.next().unwrap_or(&""));
        iter.for_each(|s| path.push(s));
        path.into_os_string().into_string().unwrap()
    }

    #[inline]
    fn trim_path(&self, path: &str) -> String {
        path.trim_end_matches(std::path::MAIN_SEPARATOR).to_string()
    }

    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let attr = fs::metadata(path).await?;

        Ok(ObjectMeta {
            path: path.to_string(),
            modified: DateTime::from(attr.modified().unwrap()),
        })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        fs::read(path).await.map_err(StorageError::from)
    }

    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    > {
        let readdir = ReadDirStream::new(fs::read_dir(path).await?);

        Ok(Box::pin(readdir.err_into().and_then(|entry| async move {
            Ok(ObjectMeta {
                path: String::from(entry.path().to_str().unwrap()),
                modified: DateTime::from(entry.metadata().await.unwrap().modified().unwrap()),
            })
        })))
    }

    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .await?;

        f.write(obj_bytes).await?;

        Ok(())
    }

    async fn rename_obj(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        rename::atomic_rename(src, dst)
    }

    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        fs::remove_file(path).await.map_err(StorageError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse_uri;
    use super::*;

    #[tokio::test]
    async fn put_and_rename() {
        let tmp_dir = tempdir::TempDir::new("rename_test").unwrap();
        let backend = FileStorageBackend::new(tmp_dir.path().to_str().unwrap());

        let tmp_file_path = tmp_dir.path().join("tmp_file");
        let new_file_path = tmp_dir.path().join("new_file");

        let tmp_file = tmp_file_path.to_str().unwrap();
        let new_file = new_file_path.to_str().unwrap();

        // first try should result in successful rename
        backend.put_obj(tmp_file, b"hello").await.unwrap();
        if let Err(e) = backend.rename_obj(tmp_file, new_file).await {
            panic!("Expect put_obj to return Ok, got Err: {:#?}", e)
        }

        // second try should result in already exists error
        backend.put_obj(tmp_file, b"hello").await.unwrap();
        assert!(matches!(
            backend.rename_obj(tmp_file, new_file).await,
            Err(StorageError::AlreadyExists(s)) if s == new_file_path.to_str().unwrap(),
        ));
    }

    #[tokio::test]
    async fn delete_obj() {
        let tmp_dir = tempdir::TempDir::new("delete_test").unwrap();
        let tmp_file_path = tmp_dir.path().join("tmp_file");
        let backend = FileStorageBackend::new(tmp_dir.path().to_str().unwrap());

        // put object
        let path = tmp_file_path.to_str().unwrap();
        backend.put_obj(path, &[]).await.unwrap();
        assert_eq!(fs::metadata(path).await.is_ok(), true);

        // delete object
        backend.delete_obj(path).await.unwrap();
        assert_eq!(fs::metadata(path).await.is_ok(), false)
    }

    #[test]
    fn join_multiple_paths() {
        let backend = FileStorageBackend::new("./");
        assert_eq!(
            Path::new(&backend.join_paths(&["abc", "efg/", "123"])),
            Path::new("abc").join("efg").join("123"),
        );
        assert_eq!(
            &backend.join_paths(&["abc", "efg"]),
            &backend.join_path("abc", "efg"),
        );
        assert_eq!(&backend.join_paths(&["foo"]), "foo",);
        assert_eq!(&backend.join_paths(&[]), "",);
    }

    #[test]
    fn trim_path() {
        let be = FileStorageBackend::new("root");
        let path = be.join_paths(&["foo", "bar"]);
        assert_eq!(be.trim_path(&path), path);
        assert_eq!(
            be.trim_path(&format!("{}{}", path, std::path::MAIN_SEPARATOR)),
            path,
        );
        assert_eq!(
            be.trim_path(&format!(
                "{}{}{}",
                path,
                std::path::MAIN_SEPARATOR,
                std::path::MAIN_SEPARATOR
            )),
            path,
        );
    }

    #[test]
    fn test_parse_uri() {
        let uri = parse_uri("foo/bar").unwrap();
        assert_eq!(uri.path(), "foo/bar");
        assert_eq!(uri.into_localpath().unwrap(), "foo/bar");

        let uri2 = parse_uri("file:///foo/bar").unwrap();
        assert_eq!(uri2.path(), "/foo/bar");
        assert_eq!(uri2.into_localpath().unwrap(), "/foo/bar");
    }
}
