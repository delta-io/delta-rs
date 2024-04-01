//! Utility functions for working across Delta tables

use chrono::DateTime;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta, Result as ObjectStoreResult};

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;

/// Collect list stream
pub async fn flatten_list_stream(
    storage: &DynObjectStore,
    prefix: Option<&Path>,
) -> ObjectStoreResult<Vec<Path>> {
    storage
        .list(prefix)
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<Path>>()
        .await
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
            DeltaTableError::from(crate::protocol::ProtocolError::InvalidField(format!(
                "invalid modification_time: {:?}",
                value.modification_time
            ))),
        )?;

        Ok(Self {
            // TODO this won't work for absolute paths, since Paths are always relative to store.
            location: Path::parse(value.path.as_str())?,
            last_modified,
            size: value.size as usize,
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
            stats_parsed: None,
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
