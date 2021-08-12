use crate::StorageError;

#[cfg(windows)]
mod imp {
    use super::*;

    pub fn atomic_rename(from: &str, to: &str, swap: bool) -> Result<(), StorageError> {
        // doing best effort in windows since there is no native atomic rename support
        let to_exists = std::fs::metadata(to).is_ok();
        // consistent behavior with Linux
        if !to_exists && swap {
            return Err(StorageError::other_std_io_err(format!(
                "failed to rename {} to {}: {} not exists and swap is set to true.",
                from, to, to
            )));
        } else if to_exists && !swap {
            return Err(StorageError::AlreadyExists(to.to_string()));
        }
        // rename in Windows already set the MOVEFILE_REPLACE_EXISTING flag
        // it should always succeed no matter destination file exists or not
        // TODO: with swap set to true, we should keep the from file in Windows?
        std::fs::rename(from, to).map_err(|e| {
            StorageError::other_std_io_err(format!("failed to rename {} to {}: {}", from, to, e))
        })
    }
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::ffi::CString;

    fn to_c_string(p: &str) -> Result<CString, StorageError> {
        CString::new(p).map_err(|e| StorageError::Generic(format!("{}", e)))
    }

    pub fn atomic_rename(from: &str, to: &str, swap: bool) -> Result<(), StorageError> {
        let cs_from = to_c_string(from)?;
        let cs_to = to_c_string(to)?;
        let ret = unsafe { platform_specific_rename(cs_from.as_ptr(), cs_to.as_ptr(), swap) };

        if ret != 0 {
            let e = errno::errno();
            if let libc::EEXIST = e.0 {
                return Err(StorageError::AlreadyExists(String::from(to)));
            }

            return Err(StorageError::other_std_io_err(format!(
                "failed to rename {} to {}: {}",
                from, to, e
            )));
        }

        Ok(())
    }

    #[allow(unused_variables)]
    unsafe fn platform_specific_rename(
        from: *const libc::c_char,
        to: *const libc::c_char,
        swap: bool,
    ) -> i32 {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", target_env = "gnu"))] {
                cfg_if::cfg_if! {
                    if #[cfg(glibc_renameat2)] {
                        let flag = if swap { libc::RENAME_EXCHANGE } else { libc::RENAME_NOREPLACE };
                        libc::renameat2(libc::AT_FDCWD, from, libc::AT_FDCWD, to, flag)
                    } else {
                        // target has old glibc (< 2.28), we would need to invoke syscall manually
                        unimplemented!()
                    }
                }
            } else if #[cfg(target_os = "macos")] {
                let flag = if swap { libc::RENAME_SWAP } else { libc::RENAME_EXCL };
                libc::renamex_np(from, to, flag)
            } else {
                unimplemented!()
            }
        }
    }
}

/// Atomically renames `from` to `to`.
/// - if `swap` is `true` then both `from` and `to` have to exist;
/// - if `swap` is `false` then `from` has to exist, but `to` is not;
/// otherwise the operation will fail.
#[inline]
pub fn atomic_rename(from: &str, to: &str, swap: bool) -> Result<(), StorageError> {
    imp::atomic_rename(from, to, swap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_atomic_rename() {
        let tmp_dir = tempdir::TempDir::new_in(".", "test_atomic_rename").unwrap();
        let a = create_file(&tmp_dir.path(), "a");
        let b = create_file(&tmp_dir.path(), "b");
        let c = &tmp_dir.path().join("c");

        // unsuccessful move not_exists to C, not_exists is missing
        match atomic_rename("not_exists", c.to_str().unwrap(), false) {
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
        atomic_rename(a.to_str().unwrap(), c.to_str().unwrap(), false).unwrap();
        assert!(!a.exists());
        assert!(c.exists());

        // unsuccessful move B to C, C already exists, B is not deleted
        assert!(b.exists());
        match atomic_rename(b.to_str().unwrap(), c.to_str().unwrap(), false) {
            Err(StorageError::AlreadyExists(p)) => assert_eq!(p, c.to_str().unwrap()),
            _ => panic!("unexpected"),
        }
        assert!(b.exists());
        assert_eq!(std::fs::read_to_string(c).unwrap(), "a");

        // successful swaps B to C
        atomic_rename(b.to_str().unwrap(), c.to_str().unwrap(), true).unwrap();
        cfg_if::cfg_if! {
            if #[cfg(target_os = "windows")] {
                assert!(!b.exists());
            } else{
                assert!(b.exists());
                assert_eq!(std::fs::read_to_string(b).unwrap(), "a");
            }
        }
        assert!(c.exists());
        assert_eq!(std::fs::read_to_string(c).unwrap(), "b");

        // unsuccessful swap C to D, D does not exist
        let d = &tmp_dir.path().join("d");
        assert!(!d.exists());
        match atomic_rename(c.to_str().unwrap(), d.to_str().unwrap(), true) {
            Err(StorageError::Io { source }) => {
                assert!(source.to_string().starts_with("failed to rename"));
            }
            _ => panic!("unexpected"),
        }
        assert!(c.exists());
        assert!(!d.exists());
    }

    fn create_file(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(name.as_bytes()).unwrap();
        path
    }
}
