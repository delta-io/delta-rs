#[cfg(feature = "datafusion")]
pub use datafusion::config::{ConfigFileType, TableOptions, TableParquetOptions};
#[cfg(feature = "datafusion")]
use datafusion::execution::{SessionState, SessionStateBuilder};
#[cfg(feature = "datafusion")]
use datafusion::catalog::Session;
use std::fmt::{Debug, Formatter};

use crate::{crate_version, DeltaResult};
use arrow_schema::Schema as ArrowSchema;

#[cfg(feature = "datafusion")]
use crate::operations::encryption::TableEncryption;
use async_trait::async_trait;

use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[cfg(not(feature = "datafusion"))]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableOptions {}

// Top level trait for file format options used by a DeltaTable
#[cfg(feature = "datafusion")]
pub trait FileFormatOptions: Send + Sync + std::fmt::Debug + 'static {
    fn table_options(&self) -> TableOptions;

    fn writer_properties_factory(&self) -> Arc<dyn WriterPropertiesFactory>;

    fn update_session(&self, session: &dyn Session) -> DeltaResult<()> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Convenience alias for file format options reference used across the codebase
pub type FileFormatRef = Arc<dyn FileFormatOptions>;

#[derive(Clone, Debug, Default)]
pub struct SimpleFileFormatOptions {
    table_options: TableOptions,
}

impl SimpleFileFormatOptions {
    pub fn new(table_options: TableOptions) -> Self {
        Self { table_options }
    }
}

#[cfg(feature = "datafusion")]
impl FileFormatOptions for SimpleFileFormatOptions {
    fn table_options(&self) -> TableOptions {
        self.table_options.clone()
    }

    fn writer_properties_factory(&self) -> Arc<dyn WriterPropertiesFactory> {
        build_writer_properties_factory_tpo(&Some(self.table_options.parquet.clone())).unwrap()
    }
}

#[cfg(feature = "datafusion")]
pub fn build_writer_properties_factory_ffo(
    file_format_options: Option<FileFormatRef>,
) -> Option<Arc<dyn WriterPropertiesFactory>> {
    file_format_options.map(|ffo| ffo.writer_properties_factory())
}

#[cfg(feature = "datafusion")]
pub fn build_writer_properties_factory_or_default_ffo(
    file_format_options: Option<FileFormatRef>,
) -> Arc<dyn WriterPropertiesFactory> {
    build_writer_properties_factory_ffo(file_format_options)
        .unwrap_or_else(|| build_writer_properties_factory_default())
}

#[cfg(feature = "datafusion")]
pub fn to_table_parquet_options_from_ffo(
    file_format_options: Option<&FileFormatRef>,
) -> Option<TableParquetOptions> {
    file_format_options.map(|ffo| ffo.table_options().parquet)
}

#[cfg(feature = "datafusion")]
pub fn state_with_file_format_options(
    state: SessionState,
    file_format_options: Option<&FileFormatRef>,
) -> DeltaResult<SessionState> {
    if let Some(ffo) = file_format_options {
        ffo.update_session(&state)?;
    }
    Ok(state)
}

#[cfg(feature = "datafusion")]
fn build_writer_properties_tpo(
    table_parquet_options: &Option<TableParquetOptions>,
) -> Option<WriterProperties> {
    use datafusion::common::file_options::parquet_writer::ParquetWriterOptions;
    table_parquet_options.as_ref().map(|tpo| {
        let mut tpo = tpo.clone();
        tpo.global.skip_arrow_metadata = true;
        ParquetWriterOptions::try_from(&tpo)
            .expect("Failed to convert TableParquetOptions to ParquetWriterOptions")
            .writer_options()
            .clone()
    })
}

#[cfg(feature = "datafusion")]
fn build_writer_properties_factory_tpo(
    table_parquet_options: &Option<TableParquetOptions>,
) -> Option<Arc<dyn WriterPropertiesFactory>> {
    let props = build_writer_properties_tpo(table_parquet_options);
    props.map(|wp| {
        Arc::new(SimpleWriterPropertiesFactory::new(wp)) as Arc<dyn WriterPropertiesFactory>
    })
}

pub fn build_writer_properties_factory_wp(
    writer_properties: WriterProperties,
) -> Arc<dyn WriterPropertiesFactory> {
    Arc::new(SimpleWriterPropertiesFactory::new(writer_properties))
}

pub fn build_writer_properties_factory_default() -> Arc<dyn WriterPropertiesFactory> {
    Arc::new(SimpleWriterPropertiesFactory::default())
}

#[async_trait]
pub trait WriterPropertiesFactory: Send + Sync + std::fmt::Debug + 'static {
    fn compression(&self, column_path: &ColumnPath) -> Compression;
    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties>;
}

#[derive(Clone, Debug)]
pub struct SimpleWriterPropertiesFactory {
    writer_properties: WriterProperties,
}

impl SimpleWriterPropertiesFactory {
    pub fn new(writer_properties: WriterProperties) -> Self {
        Self { writer_properties }
    }
}

impl Default for SimpleWriterPropertiesFactory {
    fn default() -> Self {
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY) // Code assumes Snappy by default
            .set_created_by(format!("delta-rs version {}", crate_version()))
            .build();
        Self { writer_properties }
    }
}

#[async_trait]
impl WriterPropertiesFactory for SimpleWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.writer_properties.compression(column_path)
    }

    async fn create_writer_properties(
        &self,
        file_path: &Path,
        _file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        info!("Called create_writer_properties for file: {file_path}");
        Ok(self.writer_properties.clone())
    }
}

// More advanced factory with KMS support
#[cfg(feature = "datafusion")]
#[derive(Clone, Debug)]
pub struct KMSWriterPropertiesFactory {
    writer_properties: WriterProperties,
    encryption: Option<crate::operations::encryption::TableEncryption>,
}

#[cfg(feature = "datafusion")]
impl KMSWriterPropertiesFactory {
    pub fn with_encryption(table_encryption: TableEncryption) -> Self {
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY) // Code assumes Snappy by default
            .set_created_by(format!("delta-rs version {}", crate_version()))
            .build();
        Self {
            writer_properties,
            encryption: Some(table_encryption),
        }
    }
}

#[cfg(feature = "datafusion")]
#[async_trait]
impl WriterPropertiesFactory for KMSWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.writer_properties.compression(column_path)
    }

    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        let mut builder = self.writer_properties.to_builder();
        if let Some(encryption) = self.encryption.as_ref() {
            builder = encryption
                .update_writer_properties(builder, file_path, file_schema)
                .await?;
        }
        Ok(builder.build())
    }
}

// -------------------------------------------------------------------------------------------------
// FileFormatOptions for KMS encryption based on settings in TableEncryption
// -------------------------------------------------------------------------------------------------
#[cfg(feature = "datafusion")]
pub struct KmsFileFormatOptions {
    table_encryption: TableEncryption,
    writer_properties_factory: Arc<dyn WriterPropertiesFactory>,
    encryption_factory_id: String,
}

#[cfg(feature = "datafusion")]
impl KmsFileFormatOptions {
    pub fn new(table_encryption: TableEncryption) -> Self {
        let encryption_factory_id = format!("delta-{}", Uuid::new_v4().to_string());
        let writer_properties_factory = Arc::new(KMSWriterPropertiesFactory::with_encryption(
            table_encryption.clone(),
        ));
        Self {
            table_encryption,
            writer_properties_factory,
            encryption_factory_id,
        }
    }
}

#[cfg(feature = "datafusion")]
impl Debug for KmsFileFormatOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsFileFormatOptions")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "datafusion")]
impl FileFormatOptions for KmsFileFormatOptions {
    fn table_options(&self) -> TableOptions {
        let mut table_options = TableOptions::default();
        table_options.parquet.crypto.factory_id = Some(self.encryption_factory_id.clone());
        table_options.parquet.crypto.factory_options =
            self.table_encryption.configuration().clone();
        table_options
    }

    fn writer_properties_factory(&self) -> Arc<dyn WriterPropertiesFactory> {
        Arc::clone(&self.writer_properties_factory)
    }

    fn update_session(&self, session: &dyn Session) -> DeltaResult<()> {
        // Ensure DataFusion has the encryption factory registered
        session.runtime_env().register_parquet_encryption_factory(
            &self.encryption_factory_id,
            Arc::clone(self.table_encryption.encryption_factory()),
        );
        Ok(())
    }
}

/// AI generated code to get builder from existing WriterProperties
/// Can be removed with PR to arrow-rs
/// https://github.com/apache/arrow-rs/pull/8272
pub trait WriterPropertiesExt {
    fn to_builder(&self) -> WriterPropertiesBuilder;
}

impl WriterPropertiesExt for WriterProperties {
    fn to_builder(&self) -> WriterPropertiesBuilder {
        // Start with copying top-level writer properties
        let mut builder = WriterProperties::builder()
            .set_writer_version(self.writer_version())
            .set_data_page_size_limit(self.data_page_size_limit())
            .set_data_page_row_count_limit(self.data_page_row_count_limit())
            .set_dictionary_page_size_limit(self.dictionary_page_size_limit())
            .set_write_batch_size(self.write_batch_size())
            .set_max_row_group_size(self.max_row_group_size())
            .set_bloom_filter_position(self.bloom_filter_position())
            .set_created_by(self.created_by().to_string())
            .set_offset_index_disabled(self.offset_index_disabled())
            .set_key_value_metadata(self.key_value_metadata().cloned())
            .set_sorting_columns(self.sorting_columns().cloned())
            .set_column_index_truncate_length(self.column_index_truncate_length())
            .set_statistics_truncate_length(self.statistics_truncate_length())
            .set_coerce_types(self.coerce_types());

        // Use an empty ColumnPath to read default column-level properties
        let empty = ColumnPath::new(Vec::new());

        // Default compression
        let default_compression = self.compression(&empty);
        builder = builder.set_compression(default_compression);

        // Default encoding (if explicitly set)
        if let Some(enc) = self.encoding(&empty) {
            builder = builder.set_encoding(enc);
        }

        // Default dictionary enabled
        builder = builder.set_dictionary_enabled(self.dictionary_enabled(&empty));

        // Default statistics setting
        builder = builder.set_statistics_enabled(self.statistics_enabled(&empty));

        // Default max statistics size (deprecated in parquet, but preserve value if used)
        #[allow(deprecated)]
        {
            let max_stats = self.max_statistics_size(&empty);
            builder = builder.set_max_statistics_size(max_stats);
        }

        // Default bloom filter settings
        if let Some(bfp) = self.bloom_filter_properties(&empty) {
            builder = builder
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_fpp(bfp.fpp)
                .set_bloom_filter_ndv(bfp.ndv);
        } else {
            // Ensure bloom filters are disabled if not present
            builder = builder.set_bloom_filter_enabled(false);
        }

        // Preserve encryption properties if present
        if let Some(fep) = self.file_encryption_properties() {
            builder = builder.with_file_encryption_properties(fep.clone());
        }
        builder
    }
}
