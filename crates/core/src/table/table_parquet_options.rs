use std::fmt::Debug;
#[cfg(feature = "datafusion")]
pub use datafusion::config::{ConfigFileType, TableOptions, TableParquetOptions};
#[cfg(feature = "datafusion")]
use datafusion::execution::{SessionState, SessionStateBuilder};

use crate::{crate_version, DeltaResult};
use arrow_schema::Schema as ArrowSchema;

use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableParquetOptions {}

#[cfg(feature = "datafusion")]
pub fn build_writer_properties(
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
pub fn build_writer_properties_factory(
    table_parquet_options: &Option<TableParquetOptions>,
) -> Option<Arc<dyn WriterPropertiesFactory>> {
    let props = build_writer_properties(table_parquet_options);
    props.map(|wp| Arc::new(DefaultWriterPropertiesFactory::new(wp)) as Arc<dyn WriterPropertiesFactory>)
}

#[cfg(feature = "datafusion")]
pub fn build_writer_properties_factory_or_default(
    table_parquet_options: &Option<TableParquetOptions>,
) -> Arc<dyn WriterPropertiesFactory> {
    let props = build_writer_properties(table_parquet_options);
    let maybe_wp = props.map(|wp| Arc::new(DefaultWriterPropertiesFactory::new(wp)) as Arc<dyn WriterPropertiesFactory>);
    maybe_wp.unwrap_or_else(|| Arc::new(DefaultWriterPropertiesFactory::default()))
}

#[cfg(not(feature = "datafusion"))]
pub fn build_writer_properties_factory_or_default(
    table_parquet_options: &Option<TableParquetOptions>,
) -> Arc<dyn WriterPropertiesFactory> {
    Arc::new(DefaultWriterPropertiesFactory::default())
}


#[cfg(feature = "datafusion")]
pub fn state_with_parquet_options(
    state: SessionState,
    parquet_options: Option<&TableParquetOptions>,
) -> SessionState {
    if parquet_options.is_some() {
        let mut sb = SessionStateBuilder::new_from_existing(state.clone());
        let mut tbl_opts = TableOptions::new();
        tbl_opts.parquet = parquet_options.unwrap().clone();
        tbl_opts.set_config_format(ConfigFileType::PARQUET);
        sb = sb.with_table_options(tbl_opts);
        let state = sb.build();
        return state;
    }
    state
}

pub trait WriterPropertiesFactory: Send + Sync + std::fmt::Debug + 'static {
    fn compression(&self, column_path: &ColumnPath) -> Compression;
    fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties>;
}

#[derive(Clone, Debug, Default)]
pub struct DefaultWriterPropertiesFactory {
    writer_properties: WriterProperties,
}

impl DefaultWriterPropertiesFactory {
    pub fn new(writer_properties: WriterProperties) -> Self {
        Self {
            writer_properties
        }
    }
}

impl WriterPropertiesFactory for DefaultWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.writer_properties.compression(column_path)
    }

    fn create_writer_properties(&self, _file_path: &Path, _file_schema: &Arc<ArrowSchema>) -> DeltaResult<WriterProperties> {
        Ok(self.writer_properties.clone())
    }
}


// More advanced factory with KMS support

#[derive(Clone, Debug, Default)]
pub struct KMSWriterPropertiesFactory {
    writer_properties: WriterProperties,
    encryption: Option<crate::operations::encryption::TableEncryption>,
}

impl WriterPropertiesFactory for KMSWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.writer_properties.compression(column_path)
    }

    fn create_writer_properties(&self, file_path: &Path, file_schema: &Arc<ArrowSchema>) -> DeltaResult<WriterProperties> {
        let mut builder = self.writer_properties.to_builder();
        if let Some(encryption) = self.encryption.as_ref() {
            builder = encryption.update_writer_properties(builder, file_path, file_schema)?;
        }
        Ok(builder.build())
    }
}



/// AI generated code to get builder from existing WriterProperties
/// May not be right
///
/// Extension to construct a WriterPropertiesBuilder from existing WriterProperties
pub trait WriterPropertiesExt {
    fn to_builder(&self) -> WriterPropertiesBuilder;
}

impl WriterPropertiesExt for WriterProperties {
    fn to_builder(&self) -> WriterPropertiesBuilder {
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

        // Set default compression (use empty column path to retrieve default)
        let default_compression = self.compression(&ColumnPath::new(Vec::new()));
        builder = builder.set_compression(default_compression);

        // Preserve encryption properties if present
        if let Some(fep) = self.file_encryption_properties() {
            builder = builder.with_file_encryption_properties(fep.clone());
        }
        builder
    }
}
