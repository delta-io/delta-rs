//! Utility functions for working across Delta tables

use std::collections::HashMap;

use crate::builder::DeltaTableBuilder;
use crate::DeltaTableError;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{DynObjectStore, Result as ObjectStoreResult};
use std::sync::Arc;

/// Copies the contents from the `from` location into the `to` location
pub async fn copy_table(
    from: impl AsRef<str>,
    from_options: Option<HashMap<String, String>>,
    to: impl AsRef<str>,
    to_options: Option<HashMap<String, String>>,
) -> Result<(), DeltaTableError> {
    let from_store = DeltaTableBuilder::from_uri(from)
        .with_storage_options(from_options.unwrap_or_default())
        .build_storage()?;
    let to_store = DeltaTableBuilder::from_uri(to)
        .with_storage_options(to_options.unwrap_or_default())
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
