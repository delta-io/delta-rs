//! Async factory for creating per-file [`WriterProperties`].
//!
//! This module is intentionally free of `datafusion` dependencies so that the
//! legacy writers (`JsonWriter`, `RecordBatchWriter`) can use the factory API
//! without requiring the `datafusion` feature. KMS/encrypted factories live in
//! `crate::operations::write::encryption` (datafusion-gated).

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

use crate::crate_version;
use crate::errors::DeltaResult;

/// Async factory for creating per-file [`WriterProperties`].
///
/// The async signature allows implementations to fetch per-file encryption keys from a
/// remote KMS based on the file path (required for AAD encryption where the file path
/// is incorporated into the key material).
#[async_trait]
pub trait WriterPropertiesFactory: Send + Sync + Debug + 'static {
    /// Returns the compression for a given column path. Called synchronously before
    /// the async key fetch so callers can compute the file-name extension.
    fn compression(&self, column_path: &ColumnPath) -> Compression;

    /// Create [`WriterProperties`] for the given `file_path` and `file_schema`.
    ///
    /// Called once per new parquet file, immediately before the `AsyncArrowWriter` is
    /// constructed. KMS implementations that use AAD must incorporate `file_path` into
    /// key derivation to bind ciphertext to the correct file location.
    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties>;
}

/// Convenience type alias.
pub type WriterPropertiesFactoryRef = Arc<dyn WriterPropertiesFactory>;

/// A [`WriterPropertiesFactory`] that returns the same static [`WriterProperties`] for
/// every file (no encryption, no per-file key derivation).
#[derive(Clone, Debug)]
pub struct DefaultWriterPropertiesFactory {
    writer_properties: WriterProperties,
}

impl DefaultWriterPropertiesFactory {
    pub fn new(writer_properties: WriterProperties) -> Self {
        Self { writer_properties }
    }

    pub fn snappy() -> Self {
        Self::new(snappy_writer_properties())
    }
}

/// Build the standard delta-rs base [`WriterProperties`]: SNAPPY compression with
/// the delta-rs `created_by` tag. Used as the base for both plain and encrypted writers.
pub fn snappy_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(format!("delta-rs version {}", crate_version()))
        .build()
}

#[async_trait]
impl WriterPropertiesFactory for DefaultWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.writer_properties.compression(column_path)
    }

    async fn create_writer_properties(
        &self,
        _file_path: &Path,
        _file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        Ok(self.writer_properties.clone())
    }
}

/// Build a default [`WriterPropertiesFactoryRef`] (SNAPPY compression, no encryption).
pub fn default_writer_properties_factory() -> WriterPropertiesFactoryRef {
    Arc::new(DefaultWriterPropertiesFactory::snappy())
}

/// Wrap a static [`WriterProperties`] in a factory.
pub fn factory_from_writer_properties(wp: WriterProperties) -> WriterPropertiesFactoryRef {
    Arc::new(DefaultWriterPropertiesFactory::new(wp))
}
