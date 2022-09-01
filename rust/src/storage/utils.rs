//! Utility functions for working across Delta tables

use std::collections::HashMap;

use crate::builder::DeltaTableBuilder;
use crate::DeltaTableError;
use futures::StreamExt;
use object_store::ObjectStore;

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
