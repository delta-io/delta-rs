use std::path::Path;
use std::pin::Pin;

use chrono::DateTime;
use futures::{Stream, TryStreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReadDirStream;

use super::{ObjectMeta, StorageBackend, StorageError};
use uuid::Uuid;

mod rename;

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
    fn join_path(&self, path: &str, path_to_join: &str) -> String {
        let new_path = Path::new(path);
        new_path
            .join(path_to_join)
            .into_os_string()
            .into_string()
            .unwrap()
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

        rename::rename(&tmp_path, path)
    }
}

async fn create_tmp_file_with_retry(
    backend: &FileStorageBackend,
    path: &str,
    obj_bytes: &[u8],
) -> Result<String, StorageError> {
    let mut i = 0;

    while i < 5 {
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
