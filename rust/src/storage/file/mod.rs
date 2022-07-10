//! Local file storage backend. This backend read and write objects from local filesystem.
//!
//! The local file storage backend is multi-writer safe.

use super::{ObjectMeta, StorageBackend, StorageError};
use chrono::DateTime;
use futures::{stream::BoxStream, StreamExt};
use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;
use walkdir::WalkDir;

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
    #[allow(dead_code)]
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
            size: Some(attr.len().try_into().unwrap()),
        })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        fs::read(path).await.map_err(StorageError::from)
    }

    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<BoxStream<'a, Result<ObjectMeta, StorageError>>, StorageError> {
        let walkdir = WalkDir::new(path)
            // Don't include the root directory itself
            .min_depth(1);

        let s =
            walkdir.into_iter().flat_map(move |result_dir_entry| {
                match convert_walkdir_result(result_dir_entry) {
                    Err(e) => Some(Err(e)),
                    Ok(None) => None,
                    Ok(entry @ Some(_)) => entry
                        .filter(|dir_entry| dir_entry.file_type().is_file())
                        .map(|entry| {
                            let file_path =
                                String::from(entry.path().to_str().ok_or_else(|| {
                                    StorageError::Generic("invalid path".to_string())
                                })?);
                            match entry.metadata() {
                                Ok(meta) => Ok(ObjectMeta {
                                    path: file_path,
                                    modified: meta.modified()?.into(),
                                    size: Some(meta.len().try_into().map_err(|_| {
                                        StorageError::Generic("cannot convert to i64".to_string())
                                    })?),
                                }),
                                Err(err)
                                    if err.io_error().map(|e| e.kind())
                                        == Some(io::ErrorKind::NotFound) =>
                                {
                                    Err(StorageError::NotFound)
                                }
                                Err(err) => Err(StorageError::WalkDir { source: err }),
                            }
                        }),
                }
            });

        // list in batches of CHUNK_SIZE
        const CHUNK_SIZE: usize = 1024;

        let buffer = VecDeque::with_capacity(CHUNK_SIZE);
        let stream = futures::stream::try_unfold((s, buffer), |(mut s, mut buffer)| async move {
            if buffer.is_empty() {
                (s, buffer) = tokio::task::spawn_blocking(move || {
                    for _ in 0..CHUNK_SIZE {
                        match s.next() {
                            Some(r) => buffer.push_back(r),
                            None => break,
                        }
                    }
                    (s, buffer)
                })
                .await
                .map_err(|err| StorageError::Generic(err.to_string()))?;
            }

            match buffer.pop_front() {
                Some(Err(e)) => Err(e),
                Some(Ok(meta)) => Ok(Some((meta, (s, buffer)))),
                None => Ok(None),
            }
        });

        Ok(stream.boxed())
    }

    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp_path = &format!("{}_{}", path, Uuid::new_v4());
        let mut f = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(tmp_path)
            .await?;

        f.write_all(obj_bytes).await?;
        f.sync_all().await?;
        drop(f);

        // as temp path is transparent to end user, we could use syscall directly here
        match fs::rename(tmp_path, path).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // If rename failed, clean up the temp file.
                self.delete_obj(tmp_path).await?;
                Err(StorageError::from(e))
            }
        }
    }

    async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        rename::rename_noreplace(src, dst).await
    }

    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        fs::remove_file(path).await.map_err(StorageError::from)
    }
}

/// Convert walkdir results and converts not-found errors into `None`.
fn convert_walkdir_result(
    res: std::result::Result<walkdir::DirEntry, walkdir::Error>,
) -> Result<Option<walkdir::DirEntry>, StorageError> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(walkdir_err) => match walkdir_err.io_error() {
            Some(io_err) => match io_err.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(StorageError::Generic(io_err.to_string())),
            },
            None => Err(StorageError::Generic(walkdir_err.to_string())),
        },
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
        if let Err(e) = backend.rename_obj_noreplace(tmp_file, new_file).await {
            panic!("Expect put_obj to return Ok, got Err: {:#?}", e)
        }

        // second try should result in already exists error
        backend.put_obj(tmp_file, b"hello").await.unwrap();
        assert!(matches!(
            backend.rename_obj_noreplace(tmp_file, new_file).await,
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

    #[tokio::test]
    async fn delete_objs() {
        let tmp_dir = tempdir::TempDir::new("delete_test").unwrap();
        let tmp_file_path1 = tmp_dir.path().join("tmp_file1");
        let tmp_file_path2 = tmp_dir.path().join("tmp_file2");
        let backend = FileStorageBackend::new(tmp_dir.path().to_str().unwrap());

        // put object
        let path1 = tmp_file_path1.to_str().unwrap();
        let path2 = tmp_file_path2.to_str().unwrap();
        backend.put_obj(path1, &[]).await.unwrap();
        backend.put_obj(path2, &[]).await.unwrap();
        assert_eq!(fs::metadata(path1).await.is_ok(), true);
        assert_eq!(fs::metadata(path2).await.is_ok(), true);

        // delete object
        backend
            .delete_objs(&[path1.to_string(), path2.to_string()])
            .await
            .unwrap();
        assert_eq!(fs::metadata(path1).await.is_ok(), false);
        assert_eq!(fs::metadata(path2).await.is_ok(), false)
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
