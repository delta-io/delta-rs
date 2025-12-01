//! Utility functions for working across Delta tables

use chrono::DateTime;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::path::Path as StdPath;
use url::Url;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;

/// Return the uri of commit version.
///
/// ```rust
/// # use deltalake_core::logstore::*;
/// use object_store::path::Path;
/// let uri = commit_uri_from_version(1);
/// assert_eq!(uri, Path::from("_delta_log/00000000000000000001.json"));
/// ```
pub fn commit_uri_from_version(version: i64) -> Path {
    let version = format!("{version:020}.json");
    super::DELTA_LOG_PATH.child(version.as_str())
}

/// Returns true if the provided string is either a fully-qualified URI or
/// an absolute filesystem path for the current platform.
pub fn is_absolute_uri_or_path(s: &str) -> bool {
    if Url::parse(s).is_ok() {
        true
    } else {
        StdPath::new(s).is_absolute()
    }
}

/// Normalize a data file path for tables using the file scheme.
///
/// Rules:
/// - If input is a file:// URI, convert to a platform filesystem path.
/// - If input is another fully-qualified URI or an absolute filesystem path, keep as-is.
/// - Otherwise, treat input as relative and join under the filesystem path of `root`.
pub fn normalize_path_for_file_scheme(root: &Url, input: &str) -> String {
    match Url::parse(input) {
        Ok(url) if url.scheme() == "file" => url.path().to_string(),
        Ok(_) => input.to_string(),
        Err(_) => {
            if StdPath::new(input).is_absolute() {
                input.to_string()
            } else {
                let root_fs_path = root
                    .to_file_path()
                    .unwrap_or_else(|_| StdPath::new(root.path()).to_path_buf());
                let rel = input.trim_start_matches(['/', '\\']);
                let full_path = root_fs_path.join(rel);
                full_path
                    .to_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| full_path.to_string_lossy().into_owned())
            }
        }
    }
}

/// For non-file schemes, if `input` is a fully-qualified URI beginning with `root`,
/// strip the table root prefix so the result is relative to the table root.
pub fn strip_table_root_from_full_uri(root: &Url, input: &str) -> String {
    let root_str = root.as_str().trim_end_matches('/');
    let root_with_sep = format!("{}/", root_str);
    if input.starts_with(&root_with_sep) {
        input[root_with_sep.len()..].to_string()
    } else if input == root_str {
        String::new()
    } else {
        input.to_string()
    }
}

impl TryFrom<Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: Add) -> DeltaResult<Self> {
        (&value).try_into()
    }
}

impl TryFrom<&Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: &Add) -> DeltaResult<Self> {
        let last_modified = DateTime::from_timestamp_millis(value.modification_time).ok_or(
            DeltaTableError::MetadataError(format!(
                "invalid modification_time: {:?}",
                value.modification_time
            )),
        )?;

        Ok(Self {
            // IMPORTANT: `ObjectMeta` is only constructed at the boundary where
            // a concrete object store is used. At that point, `value.path` must
            // already be normalized (either relative to table root for remote
            // schemes or an absolute filesystem path for file scheme).
            // We therefore parse it directly without attempting to alter it.
            location: Path::parse(value.path.as_str())?,
            last_modified,
            size: value.size as u64,
            e_tag: None,
            version: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_meta_from_add_action() {
        let add = Add {
            path: "x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
                .to_string(),
            size: 123,
            modification_time: 123456789,
            data_change: true,
            stats: None,
            partition_values: Default::default(),
            tags: Default::default(),
            base_row_id: None,
            default_row_commit_version: None,
            deletion_vector: None,
            clustering_provider: None,
        };

        let meta: ObjectMeta = (&add).try_into().unwrap();
        assert_eq!(
            meta.location,
            Path::parse(
                "x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
            )
            .unwrap()
        );
        assert_eq!(meta.size, 123);
        assert_eq!(meta.last_modified.timestamp_millis(), 123456789);
    }
}
