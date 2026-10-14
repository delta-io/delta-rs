use std::collections::HashMap;

use parquet::basic::Compression;
use parquet::file::properties::{
    CdcOptions, DEFAULT_CDC_MAX_CHUNK_SIZE, DEFAULT_CDC_MIN_CHUNK_SIZE, DEFAULT_CDC_NORM_LEVEL,
    WriterProperties,
};

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
/// chunking settings (likely typos) and settings [`content_defined_chunking`] rejects.
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
    content_defined_chunking(format_options).map(|_| ())
}

/// Content-defined chunking settings from a table's `Metadata.format.options`, or `None` when
/// they do not enable it. Invalid settings are rejected rather than replaced by defaults, and
/// parquet panics on chunk sizes it cannot use. Settings this version does not know, e.g. from a
/// newer writer, are ignored so they do not block writes to the table.
fn content_defined_chunking(
    format_options: &HashMap<String, String>,
) -> DeltaResult<Option<CdcOptions>> {
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
        let invalid = || DeltaTableError::Generic(format!("Invalid format option {key}: {value}"));
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
    Ok(enabled.then_some(cdc))
}

/// `props` with the content-defined chunking that a table's `Metadata.format.options` enable.
pub(crate) fn apply_format_options(
    props: WriterProperties,
    format_options: &HashMap<String, String>,
) -> DeltaResult<WriterProperties> {
    Ok(match content_defined_chunking(format_options)? {
        Some(cdc) => props
            .into_builder()
            .set_content_defined_chunking(Some(cdc))
            .build(),
        None => props,
    })
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
    fn apply_format_options_ignores_unknown_content_defined_chunking_keys() {
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

        let writer_properties = apply_format_options(
            default_writer_properties(Compression::SNAPPY),
            &format_options,
        )
        .unwrap();

        assert!(writer_properties.content_defined_chunking().is_some());
    }
}
