use crate::StorageError;

#[cfg(windows)]
mod imp {
    use super::*;
    use lazy_static::lazy_static;
    use std::sync::Arc;
    use std::sync::Mutex;

    // async rename under Windows is flaky due to no native rename if not exists support.
    // Use a shared lock to prevent threads renaming at the same time.
    lazy_static! {
        static ref SHARED_LOCK: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
    }

    pub async fn rename_noreplace(from: &str, to: &str) -> Result<(), StorageError> {
        let from_path = String::from(from);
        let to_path = String::from(to);

        // rename in Windows already set the MOVEFILE_REPLACE_EXISTING flag
        // it should always succeed no matter destination filconcurrent_writes_teste exists or not
        tokio::task::spawn_blocking(move || {
            let lock = Arc::clone(&SHARED_LOCK);
            let mut data = lock.lock().unwrap();
            *data += 1;

            // doing best effort in windows since there is no native rename if not exists support
            let to_exists = std::fs::metadata(&to_path).is_ok();
            if to_exists {
                return Err(StorageError::AlreadyExists(to_path));
            }
            std::fs::rename(&from_path, &to_path).map_err(|e| {
                let to_exists = std::fs::metadata(&to_path).is_ok();
                if to_exists {
                    return StorageError::AlreadyExists(to_path);
                }

                StorageError::other_std_io_err(format!(
                    "failed to rename {} to {}: {}",
                    from_path, to_path, e
                ))
            })
        })
        .await
        .unwrap()
    }
}

#[cfg(unix)]
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
                return Err(StorageError::other_std_io_err(format!(
                    "failed to rename {} to {}: {}",
                    from, to, e
                )));
            }
            Ok(_) => Ok(()),
        }
    }

    #[allow(unused_variables)]
    unsafe fn platform_specific_rename(from: *const libc::c_char, to: *const libc::c_char) -> i32 {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", target_env = "gnu"))] {
                cfg_if::cfg_if! {
                    if #[cfg(glibc_renameat2)] {
                        libc::renameat2(libc::AT_FDCWD, from, libc::AT_FDCWD, to, libc::RENAME_NOREPLACE)
                    } else {
                        // target has old glibc (< 2.28), we would need to invoke syscall manually
                        unimplemented!()
                    }
                }
            } else if #[cfg(target_os = "macos")] {
                libc::renamex_np(from, to, libc::RENAME_EXCL)
            } else {
                unimplemented!()
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
