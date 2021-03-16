use std::path::{Path, PathBuf};
use std::pin::Pin;

use chrono::DateTime;
use futures::{Stream, TryStreamExt};
use libc::{c_char, c_int, c_uint};
use std::ffi::CString;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReadDirStream;

use super::{ObjectMeta, StorageBackend, StorageError};
use uuid::Uuid;

#[derive(Default, Debug)]
pub struct FileStorageBackend {
    root: PathBuf,
}

impl FileStorageBackend {
    pub fn new(root: &str) -> Self {
        Self {
            root: PathBuf::from(root),
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
        let tmp_file = self.root.join(format!("{}.temporary", Uuid::new_v4().to_string()));

        let mut f = fs::File::create(&tmp_file).await?;
        f.write(obj_bytes).await?;

        rename(tmp_file, PathBuf::from(path))?;

        Ok(())
    }
}

#[cfg(target_os = "linux")]
const RENAME_NOREPLACE: c_uint = 1;

#[cfg(target_os = "macos")]
const RENAME_EXCL: c_uint = 4;

extern "C" {
    #[cfg(target_os = "linux")]
    fn renameat2(
        olddirfd: c_int,
        oldpath: *const c_char,
        newdirfd: c_int,
        newpath: *const c_char,
        flags: c_uint,
    ) -> c_int;

    #[cfg(target_os = "macos")]
    fn renamex_np(from: *const c_char, to: *const c_char, flags: c_uint) -> c_int;
}

fn rename<P: AsRef<Path>>(from: P, to: P) -> Result<(), StorageError> {
    let from = path_to_cstring(from)?;
    let to = path_to_cstring(to)?;

    unsafe {
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                if renameat2(0, from.as_ptr(), 0, to.as_ptr(), RENAME_NOREPLACE) == 0 {
                   return Ok(())
                }
            } else if #[cfg(target_os = "macos")] {
                if renamex_np(from.as_ptr(), to.as_ptr(), RENAME_EXCL) == 0 {
                   return Ok(())
                }
            } else {
                return Err(StorageError::FileSystemNotSupported)
            }
        }
    }

    let path = to.into_string().map_err(|e| StorageError::Generic(format!("{}", e)))?;
    Err(StorageError::AlreadyExists(path))
}

fn path_to_cstring<P: AsRef<Path>>(p: P) -> Result<CString, StorageError> {
    let as_str = p.as_ref().as_os_str().to_str()
        .ok_or_else(|| StorageError::Generic(String::from("Unable to get &str from &Path")));
    CString::new(as_str?)
        .map_err(|e| StorageError::Generic(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn test_rename() {
        let tmp = std::env::temp_dir().join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&tmp).unwrap();
        let a = create_file(&tmp, "a");
        let b = create_file(&tmp, "b");
        let c = tmp.join("c");

        assert_eq!(a.exists(), true);
        assert_eq!(c.exists(), false);
        rename(a.to_str().unwrap(), c.to_str().unwrap()).unwrap();
        assert_eq!(a.exists(), false);
        assert_eq!(c.exists(), true);

        assert_eq!(b.exists(), true);
        match rename(b.to_str().unwrap(), c.to_str().unwrap()) {
            Err(StorageError::AlreadyExists(p)) => assert_eq!(p, c.to_str().unwrap()),
            _ => panic!("unexpected"),
        }
        assert_eq!(b.exists(), true);
        assert_eq!(std::fs::read_to_string(c).unwrap(), "a");

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    fn create_file(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(name.as_bytes()).unwrap();
        path
    }
}
