//! Utility functions for working across Delta tables

use std::collections::HashMap;

use crate::action::Add;
use crate::builder::DeltaTableBuilder;
use crate::{DeltaDataTypeVersion, DeltaResult, DeltaTableError};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta, Result as ObjectStoreResult};
use std::sync::Arc;

const DELTA_LOG_FOLDER: &str = "_delta_log";

/// Return the uri of commit version.
pub(crate) fn commit_uri_from_version(version: DeltaDataTypeVersion) -> Path {
    let version = format!("{version:020}.json");
    Path::from_iter([DELTA_LOG_FOLDER, &version])
}

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
    sync_stores(from_store, to_store).await
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
        let last_modified = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_millis(value.modification_time).ok_or(
                DeltaTableError::InvalidAction {
                    source: crate::action::ActionError::InvalidField(format!(
                        "invalid modification_time: {:?}",
                        value.modification_time
                    )),
                },
            )?,
            Utc,
        );
        Ok(Self {
            // TODO this won't work for absoute paths, since Paths are always relative to store.
            location: Path::from(value.path.as_str()),
            last_modified,
            size: value.size as usize,
        })
    }
}
