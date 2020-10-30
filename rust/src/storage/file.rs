use std::fs;

use chrono::DateTime;

use super::{ObjectMeta, StorageBackend, StorageError};

#[derive(Default)]
pub struct FileStorageBackend {}

impl FileStorageBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl StorageBackend for FileStorageBackend {
    fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let attr = fs::metadata(path)?;

        Ok(ObjectMeta {
            path: path.to_string(),
            modified: DateTime::from(attr.modified().unwrap()),
        })
    }

    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        fs::read(path).map_err(StorageError::from)
    }

    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = ObjectMeta>>, StorageError> {
        let readdir = fs::read_dir(path)?;

        Ok(Box::new(readdir.filter_map(Result::ok).map(|entry| {
            ObjectMeta {
                path: String::from(entry.path().to_str().unwrap()),
                modified: DateTime::from(entry.metadata().unwrap().modified().unwrap()),
            }
        })))
    }
}
