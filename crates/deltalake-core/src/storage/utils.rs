//! Utility functions for working across Delta tables

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDateTime, TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta, Result as ObjectStoreResult};

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;
use crate::table::builder::DeltaTableBuilder;

/// Copies the contents from the `from` location into the `to` location
pub async fn copy_table(
    from: impl AsRef<str>,
    from_options: Option<HashMap<String, String>>,
    to: impl AsRef<str>,
    to_options: Option<HashMap<String, String>>,
    allow_http: bool,
) -> Result<(), DeltaTableError> {
    let from_store = DeltaTableBuilder::from_uri(from)
        .with_storage_options(from_options.unwrap_or_default())
        .with_allow_http(allow_http)
        .build_storage()?;
    let to_store = DeltaTableBuilder::from_uri(to)
        .with_storage_options(to_options.unwrap_or_default())
        .with_allow_http(allow_http)
        .build_storage()?;
    sync_stores(from_store.object_store(), to_store.object_store()).await
}

/// Synchronize the contents of two object stores
pub async fn sync_stores(
    from_store: Arc<DynObjectStore>,
    to_store: Arc<DynObjectStore>,
) -> Result<(), DeltaTableError> {
    // TODO if a table is copied within the same root store (i.e bucket), using copy would be MUCH more efficient
    let mut meta_stream = from_store.list(None).await?;
    while let Some(file) = meta_stream.next().await {
        if let Ok(meta) = file {
            let bytes = from_store.get(&meta.location).await?.bytes().await?;
            to_store.put(&meta.location, bytes).await?;
        }
    }
    Ok(())
}

/// Collect list stream
pub async fn flatten_list_stream(
    storage: &DynObjectStore,
    prefix: Option<&Path>,
) -> ObjectStoreResult<Vec<Path>> {
    storage
        .list(prefix)
        .await?
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<Path>>()
        .await
}

pub(crate) fn str_is_truthy(val: &str) -> bool {
    val.eq_ignore_ascii_case("1")
        | val.eq_ignore_ascii_case("true")
        | val.eq_ignore_ascii_case("on")
        | val.eq_ignore_ascii_case("yes")
        | val.eq_ignore_ascii_case("y")
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
        let last_modified = Utc.from_utc_datetime(
            &NaiveDateTime::from_timestamp_millis(value.modification_time).ok_or(
                DeltaTableError::from(crate::protocol::ProtocolError::InvalidField(format!(
                    "invalid modification_time: {:?}",
                    value.modification_time
                ))),
            )?,
        );
        Ok(Self {
            // TODO this won't work for absolute paths, since Paths are always relative to store.
            location: Path::parse(value.path.as_str())?,
            last_modified,
            size: value.size as usize,
            e_tag: None,
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
            partition_values_parsed: None,
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
