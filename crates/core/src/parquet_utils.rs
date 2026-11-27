use std::collections::HashMap;

use parquet::basic::Compression;
use parquet::file::properties::{
    CdcOptions, DEFAULT_CDC_MAX_CHUNK_SIZE, DEFAULT_CDC_MIN_CHUNK_SIZE, DEFAULT_CDC_NORM_LEVEL,
    WriterProperties, WriterPropertiesBuilder,
};

use crate::datafile::properties::{FileContext, WriterPropertiesLayer};
use crate::errors::{DeltaResult, DeltaTableError};

/// Prefix of the `Metadata.format.options` keys that configure content-defined chunking.
const CDC_PREFIX: &str = "contentDefinedChunking.";
/// Content-defined chunking settings this version understands, without [`CDC_PREFIX`].
const CDC_SETTINGS: [&str; 4] = ["enabled", "minChunkSize", "maxChunkSize", "normLevel"];

pub(crate) fn default_writer_properties(compression: Compression) -> WriterProperties {
    WriterProperties::builder()
        .set_created_by(format!("delta-rs version {}", crate::crate_version()))
        .set_compression(compression)
        .build()
}

/// Reject format options a new table should not be created with: unknown content-defined
/// chunking settings (likely typos) and settings
/// [`ContentDefinedChunking::from_format_options`] rejects.
pub(crate) fn validate_format_options(format_options: &HashMap<String, String>) -> DeltaResult<()> {
    let unknown = format_options.keys().find(|key| {
        key.strip_prefix(CDC_PREFIX)
            .is_some_and(|setting| !CDC_SETTINGS.contains(&setting))
    });
    if let Some(key) = unknown {
        return Err(DeltaTableError::Generic(format!(
            "Unknown format option {key}"
        )));
    }
    ContentDefinedChunking::from_format_options(format_options).map(|_| ())
}

/// The content-defined chunking a table's `Metadata.format.options` enable, set on every
/// data file the table is written with.
#[derive(Debug)]
pub(crate) struct ContentDefinedChunking(CdcOptions);

impl ContentDefinedChunking {
    /// The chunking `format_options` enable, or `None` when they do not. Invalid settings are
    /// rejected rather than replaced by defaults, and parquet panics on chunk sizes it cannot
    /// use. Settings this version does not know, e.g. from a newer writer, are ignored so they
    /// do not block writes to the table.
    pub(crate) fn from_format_options(
        format_options: &HashMap<String, String>,
    ) -> DeltaResult<Option<Self>> {
        let mut enabled = false;
        let mut cdc = CdcOptions {
            min_chunk_size: DEFAULT_CDC_MIN_CHUNK_SIZE,
            max_chunk_size: DEFAULT_CDC_MAX_CHUNK_SIZE,
            norm_level: DEFAULT_CDC_NORM_LEVEL,
        };
        for (key, value) in format_options {
            let Some(name) = key.strip_prefix(CDC_PREFIX) else {
                continue;
            };
            let invalid =
                || DeltaTableError::Generic(format!("Invalid format option {key}: {value}"));
            match name {
                "enabled" => enabled = value.to_ascii_lowercase().parse().map_err(|_| invalid())?,
                "minChunkSize" => cdc.min_chunk_size = value.parse().map_err(|_| invalid())?,
                "maxChunkSize" => cdc.max_chunk_size = value.parse().map_err(|_| invalid())?,
                "normLevel" => cdc.norm_level = value.parse().map_err(|_| invalid())?,
                _ => {}
            }
        }
        if cdc.min_chunk_size == 0 || cdc.max_chunk_size <= cdc.min_chunk_size {
            return Err(DeltaTableError::Generic(format!(
                "Invalid content-defined chunking sizes: minChunkSize ({}) must be positive and \
                 smaller than maxChunkSize ({})",
                cdc.min_chunk_size, cdc.max_chunk_size
            )));
        }
        Ok(enabled.then_some(Self(cdc)))
    }
}

#[async_trait::async_trait]
impl WriterPropertiesLayer for ContentDefinedChunking {
    async fn apply(
        &self,
        builder: WriterPropertiesBuilder,
        _file: FileContext<'_>,
    ) -> DeltaResult<WriterPropertiesBuilder> {
        Ok(builder.set_content_defined_chunking(Some(self.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::schema::types::ColumnPath;

    #[test]
    fn default_writer_properties_sets_created_by_and_compression() {
        let writer_properties = default_writer_properties(Compression::SNAPPY);

        assert_eq!(
            writer_properties.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
        assert_eq!(
            writer_properties.compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
    }

    #[test]
    fn content_defined_chunking_ignores_unknown_keys() {
        // Written by a newer writer; must not block writes to the table.
        let format_options = HashMap::from([
            (
                "contentDefinedChunking.enabled".to_string(),
                "true".to_string(),
            ),
            (
                "contentDefinedChunking.futureKey".to_string(),
                "1".to_string(),
            ),
        ]);

        let cdc = ContentDefinedChunking::from_format_options(&format_options).unwrap();

        assert!(cdc.is_some());
    }
}
