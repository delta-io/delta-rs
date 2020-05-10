use std::fs;

use super::{StorageBackend, StorageError};

pub struct FileStorageBackend {}

impl FileStorageBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl StorageBackend for FileStorageBackend {
    fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        fs::read(path).map_err(|e| StorageError::from(e))
    }

    fn list_objs(&self, path: &str) -> Result<Box<dyn Iterator<Item = String>>, StorageError> {
        let readdir = fs::read_dir(path)?;
        Ok(Box::new(
            readdir
                .into_iter()
                .filter_map(Result::ok)
                .map(|entry| String::from(entry.path().to_str().unwrap())),
        ))
    }
}
