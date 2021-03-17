use crate::StorageError;
use std::ffi::CString;

#[cfg(target_os = "linux")]
const RENAME_NOREPLACE: libc::c_uint = 1;

#[cfg(target_os = "linux")]
extern "C" {
    fn renameat2(
        olddirfd: libc::c_int,
        oldpath: *const libc::c_char,
        newdirfd: libc::c_int,
        newpath: *const libc::c_char,
        flags: libc::c_uint,
    ) -> libc::c_int;
}

pub fn rename(from: &str, to: &str) -> Result<(), StorageError> {
    let ret = unsafe {
        let from = to_c_string(from)?;
        let to = to_c_string(to)?;
        platform_specific_rename(from.as_ptr(), to.as_ptr())
    };

    if ret != 0 {
        let e = errno::errno();
        if let libc::EEXIST = e.0 {
            std::fs::remove_file(from).unwrap_or(());
            return Err(StorageError::AlreadyExists(String::from(to)));
        }

        return Err(StorageError::Io {
            source: super::custom_io_error(format!("{}", e))
        });
    }

    Ok(())
}

unsafe fn platform_specific_rename(from: *const libc::c_char, to: *const libc::c_char) -> i32 {
    cfg_if::cfg_if! {
        if #[cfg(target_os = "linux")] {
            renameat2(0, from, 0, to, RENAME_NOREPLACE)
        } else if #[cfg(target_os = "macos")] {
            libc::renamex_np(from, to, libc::RENAME_EXCL)
        } else {
            unimplemented!()
        }
    }
}

fn to_c_string(p: &str) -> Result<CString, StorageError> {
    CString::new(p).map_err(|e| StorageError::Generic(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use uuid::Uuid;

    #[test]
    fn test_rename() {
        let tmp = std::env::temp_dir().join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&tmp).unwrap();
        let a = create_file(&tmp, "a");
        let b = create_file(&tmp, "b");
        let c = &tmp.join("c");

        // successful move A to C
        assert!(a.exists());
        assert!(!c.exists());
        rename(a.to_str().unwrap(), c.to_str().unwrap()).unwrap();
        assert!(!a.exists());
        assert!(c.exists());

        // unsuccessful move B to C, C already exists, B is deleted afterwards
        assert!(b.exists());
        match rename(b.to_str().unwrap(), c.to_str().unwrap()) {
            Err(StorageError::AlreadyExists(p)) => assert_eq!(p, c.to_str().unwrap()),
            _ => panic!("unexpected"),
        }
        assert!(!b.exists());
        assert_eq!(std::fs::read_to_string(c).unwrap(), "a");

        // unsuccessful move B to C, B is missing
        match rename(b.to_str().unwrap(), c.to_str().unwrap()) {
            Err(StorageError::Io { source: e }) => {
                assert_eq!(e.to_string(), "No such file or directory")
            }
            _ => panic!("unexpected"),
        }

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    fn create_file(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(name.as_bytes()).unwrap();
        path
    }
}
