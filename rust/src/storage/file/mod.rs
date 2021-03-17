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
    pub fn new(root: &str) -> std::io::Result<Self> {
        let abs_path = Path::new(root).canonicalize()?;

        abs_path
            .to_str()
            .ok_or_else(|| custom_io_error(format!("unable to get absolute path of {}", root)))
            .map(|abs_root| Self {
                root: String::from(abs_root),
            })
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
        let tmp_file_name = format!("{}.temporary", Uuid::new_v4().to_string());
        let tmp_path = self.join_path(&self.root, &tmp_file_name);

        // run this in loop in case tmp file with tmp_file_name already exists
        loop {
            let rf = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)
                .await;
            match rf {
                Ok(mut f) => {
                    f.write(obj_bytes).await?;
                    break;
                },
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    continue;
                },
                Err(e) => return Err(StorageError::Io { source: e }),
            }
        }

        rename::rename(&tmp_path, path)?;

        Ok(())
    }
}

fn custom_io_error(desc: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, desc)
}