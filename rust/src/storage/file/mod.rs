use std::path::{Path, PathBuf};
use std::pin::Pin;

use chrono::DateTime;
use futures::{Stream, TryStreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReadDirStream;

use super::{ObjectMeta, StorageBackend, StorageError};
use uuid::Uuid;

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
    pub fn new(root: &str) -> Self {
        Self {
            root: String::from(root),
        }
    }
}

#[async_trait::async_trait]
impl StorageBackend for FileStorageBackend {
    fn get_separator(&self) -> char { std::path::MAIN_SEPARATOR }

    fn join_path(&self, path: &str, path_to_join: &str) -> String {
        let new_path = Path::new(path);
        new_path
            .join(path_to_join)
            .into_os_string()
            .into_string()
            .unwrap()
    }

    fn join_paths(&self, paths: &[&str]) -> String {
        let mut iter = paths.iter();
        let mut path = PathBuf::from(iter.next().unwrap_or(&""));
        iter.for_each(|s| path.push(s));
        path.into_os_string().into_string().unwrap()
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
    ) -> Result<Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + 'a>>, StorageError>
    {
        let readdir = ReadDirStream::new(fs::read_dir(path).await?);

        Ok(Box::pin(readdir.err_into().and_then(|entry| async move {
            Ok(ObjectMeta {
                path: String::from(entry.path().to_str().unwrap()),
                modified: DateTime::from(entry.metadata().await.unwrap().modified().unwrap()),
            })
        })))
    }

    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        let tmp_path = create_tmp_file_with_retry(self, path, obj_bytes).await?;

        if let Err(e) = rename::atomic_rename(&tmp_path, path) {
            fs::remove_file(tmp_path).await.unwrap_or(());
            return Err(e);
        }

        Ok(())
    }
}

const CREATE_TEMPORARY_FILE_MAX_TRIES: i32 = 5;

async fn create_tmp_file_with_retry(
    backend: &FileStorageBackend,
    path: &str,
    obj_bytes: &[u8],
) -> Result<String, StorageError> {
    let mut i = 0;

    while i < CREATE_TEMPORARY_FILE_MAX_TRIES {
        let tmp_file_name = format!("{}.temporary", Uuid::new_v4().to_string());
        let tmp_path = backend.join_path(&backend.root, &tmp_file_name);
        let rf = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .await;

        match rf {
            Ok(mut f) => {
                f.write(obj_bytes).await?;
                return Ok(tmp_path);
            }
            Err(e) => {
                std::fs::remove_file(&tmp_path).unwrap_or(());
                if e.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(StorageError::Io { source: e });
                }
            }
        }

        i += 1;
    }

    Err(StorageError::AlreadyExists(String::from(path)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_obj() {
        let tmp_dir = tempdir::TempDir::new("rename_test").unwrap();
        let backend = FileStorageBackend::new(tmp_dir.path().to_str().unwrap());
        let tmp_dir_path = tmp_dir.path();
        let new_file_path = tmp_dir_path.join("new_file");

        if let Err(e) = backend
            .put_obj(new_file_path.to_str().unwrap(), b"hello")
            .await
        {
            panic!("Expect put_obj to return Ok, got Err: {:#?}", e)
        }

        // second try should result in already exists error
        assert!(matches!(
            backend.put_obj(new_file_path.to_str().unwrap(), b"hello").await,
            Err(StorageError::AlreadyExists(s)) if s == new_file_path.to_str().unwrap(),
        ));

        // make sure rename failure doesn't leave any dangling temp file
        let paths = std::fs::read_dir(tmp_dir_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(paths, [new_file_path]);
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
}
