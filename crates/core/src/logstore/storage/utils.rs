//! Utility functions for working across Delta tables

use crate::kernel::Version;
use chrono::DateTime;
use object_store::ObjectMeta;
use object_store::path::Path;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;

/// Return the uri of commit version.
///
/// ```rust
/// # use deltalake_core::logstore::*;
/// use object_store::path::Path;
/// let uri = commit_uri_from_version(Some(1));
/// assert_eq!(uri, Path::from("_delta_log/00000000000000000001.json"));
/// ```
pub fn commit_uri_from_version(version: Option<Version>) -> Path {
    if let Some(version) = version {
        let version = format!("{version:020}.json");
        super::DELTA_LOG_PATH.clone().join(version.as_str())
    } else {
        /*
         * Currently there are some situations where we're relying on negative versions for silly
         * reasons like in load_with_datetime(). Handling the `None` case preserves this behavior
         */
        let version = -1;
        let version = format!("{version:020}.json");
        super::DELTA_LOG_PATH.clone().join(version.as_str())
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
            // TODO this won't work for absolute paths, since Paths are always relative to store.
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
    fn test_commit_uri_from_version() {
        let version = commit_uri_from_version(Some(0));
        assert_eq!(version, Path::from("_delta_log/00000000000000000000.json"));
        let version = commit_uri_from_version(Some(123));
        assert_eq!(version, Path::from("_delta_log/00000000000000000123.json"));
        let version = commit_uri_from_version(None);
        assert_eq!(version, Path::from("_delta_log/-0000000000000000001.json"));
    }

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
