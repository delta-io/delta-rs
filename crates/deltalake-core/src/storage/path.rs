#![allow(dead_code)]
//! Utilities to make working with directory and file paths easier

use lazy_static::lazy_static;
use regex::Regex;
use url::Url;

use crate::{DeltaResult, DeltaTableError};
use object_store::path::Path;

/// The delimiter to separate object namespaces, creating a directory structure.
const DELIMITER: &str = "/";

lazy_static! {
    static ref CHECKPOINT_FILE_PATTERN: Regex =
        Regex::new(r"\d+\.checkpoint(\.\d+\.\d+)?\.parquet").unwrap();
    static ref DELTA_FILE_PATTERN: Regex = Regex::new(r"\d+\.json").unwrap();
}

/// The metadata that describes an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileMeta {
    /// The fully qualified path to the object
    pub location: LogPath,
    /// The last modified time
    pub last_modified: i64,
    /// The size in bytes of the object
    pub size: usize,
}

/// A file paths defined in the delta log
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogPath {
    /// Path relkative to the table root
    ObjectStore(Path),
    /// fully qualified url for file path
    Url(Url),
}

impl LogPath {
    pub(crate) fn child(&self, path: impl AsRef<str>) -> DeltaResult<LogPath> {
        match self {
            LogPath::ObjectStore(p) => Ok(Self::ObjectStore(p.child(path.as_ref()))),
            LogPath::Url(url) => Ok(Self::Url(url.join(path.as_ref()).map_err(|err| {
                DeltaTableError::GenericError {
                    source: Box::new(err),
                }
            })?)),
        }
    }

    /// Returns the last path segment containing the filename stored in this [`LogPath`]
    pub(crate) fn filename(&self) -> Option<&str> {
        match self {
            LogPath::ObjectStore(p) => p.filename(),
            LogPath::Url(url) => match url.path().is_empty() || url.path().ends_with('/') {
                true => None,
                false => url.path().split(DELIMITER).last(),
            },
        }
    }

    pub(crate) fn is_checkpoint_file(&self) -> bool {
        self.filename().map(is_checkpoint_file).unwrap_or(false)
    }

    pub(crate) fn is_commit_file(&self) -> bool {
        self.filename().map(is_commit_file).unwrap_or(false)
    }

    /// Parse the version number assuming a commit json or checkpoint parquet file
    pub(crate) fn commit_version(&self) -> Option<i64> {
        self.filename().and_then(commit_version)
    }
}

pub(crate) fn is_checkpoint_file(path: &str) -> bool {
    CHECKPOINT_FILE_PATTERN.captures(path).is_some()
}

pub(crate) fn is_commit_file(path: &str) -> bool {
    DELTA_FILE_PATTERN.captures(path).is_some()
}

pub(crate) fn commit_version(path: &str) -> Option<i64> {
    path.split_once('.').and_then(|(name, _)| name.parse().ok())
}

// impl<'a> AsRef<Url> for LogPath<'a> {
//     fn as_ref(&self) -> &Url {
//         self.0
//     }
// }

impl<'a> AsRef<str> for LogPath {
    fn as_ref(&self) -> &str {
        match self {
            LogPath::ObjectStore(p) => p.as_ref(),
            LogPath::Url(url) => url.as_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;
    use std::path::PathBuf;

    use super::*;

    fn table_url() -> Url {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        url::Url::from_file_path(path).unwrap()
    }

    fn test_path(log_path: LogPath) {
        let path = log_path
            .child("_delta_log")
            .unwrap()
            .child("00000000000000000000.json")
            .unwrap();

        assert_eq!("00000000000000000000.json", path.filename().unwrap());
        assert!(path.is_commit_file());
        assert!(!path.is_checkpoint_file());
        assert_eq!(path.commit_version(), Some(0));

        let path = log_path.child("00000000000000000005.json").unwrap();
        assert_eq!(path.commit_version(), Some(5));

        let path = log_path
            .child("00000000000000000002.checkpoint.parquet")
            .unwrap();
        assert_eq!(
            "00000000000000000002.checkpoint.parquet",
            path.filename().unwrap()
        );
        assert!(!path.is_commit_file());
        assert!(path.is_checkpoint_file());
        assert_eq!(path.commit_version(), Some(2));
    }

    #[test]
    fn test_file_patterns() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let log_path = LogPath::Url(url::Url::from_file_path(path.clone()).unwrap());
        test_path(log_path);

        let log_path = LogPath::ObjectStore(Path::from_filesystem_path(path).unwrap());
        test_path(log_path);
    }
}
