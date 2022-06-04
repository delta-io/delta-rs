use crate::StorageError;

/// Atomically renames `from` to `to`.
/// `from` has to exist, but `to` is not, otherwise the operation will fail.
#[inline]
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
            Err(e) => panic!("expect StorageError::NotFound, got: {:#}", e),
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
