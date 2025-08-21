use std::fmt::Debug;
#[cfg(feature = "datafusion")]
pub use datafusion::config::{ConfigFileType, TableOptions, TableParquetOptions};
#[cfg(feature = "datafusion")]
use datafusion::execution::{SessionState, SessionStateBuilder};

use crate::{DeltaResult};
use arrow_schema::Schema;
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
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
        file_schema: &Arc<Schema>,
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

    fn create_writer_properties(&self, _file_path: &Path, _file_schema: &Arc<Schema>) -> DeltaResult<WriterProperties> {
        Ok(self.writer_properties.clone())
    }
}


// More advanced factory with KMS support
/*
#[derive(Clone, Debug, Default)]
pub struct DefaultWriterPropertiesFactory {
    overridden_properties: Option<WriterProperties>,
    encryption: Option<TableEncryption>,
    compression: Option<Compression>,
}

impl DefaultWriterPropertiesFactory {
    const DEFAULT_COMPRESSION: Compression = Compression::SNAPPY;

    pub fn for_table(table: &DeltaTable) -> WriterPropertiesFactory {
        let mut writer_properties_factory = WriterPropertiesFactory::default();
        if let Some(encryption) = &table.encryption_config {
            writer_properties_factory.set_encryption(encryption.clone());
        }
        writer_properties_factory
    }

    pub fn set_properties(&mut self, properties: WriterProperties) {
        self.overridden_properties = Some(properties);
    }

    pub fn set_encryption(&mut self, encryption: TableEncryption) {
        self.encryption = Some(encryption);
    }

    pub fn set_compression(&mut self, compression: Compression) {
        self.compression = Some(compression);
    }

    pub(crate) fn compression(&self, column_path: &ColumnPath) -> Compression {
        if let Some(properties) = self.overridden_properties.as_ref() {
            properties.compression(column_path)
        } else if let Some(compression) = self.compression {
            compression
        } else {
            Self::DEFAULT_COMPRESSION
        }
    }

    pub(crate) fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<Schema>,
    ) -> DeltaResult<WriterProperties> {
        if let Some(properties) = self.overridden_properties.as_ref() {
            if self.encryption.is_some() {
                return Err(DeltaTableError::Generic(
                    "Cannot specify both Parquet WriterProperties and table encryption".to_owned(),
                ));
            }
            Ok(properties.clone())
        } else {
            let compression = self.compression.unwrap_or(Self::DEFAULT_COMPRESSION);
            let mut builder = WriterProperties::builder()
                .set_compression(compression)
                .set_created_by(format!("delta-rs version {}", crate_version()));
            if let Some(encryption) = self.encryption.as_ref() {
                builder = encryption.update_writer_properties(builder, file_path, file_schema)?;
            }
            Ok(builder.build())
        }
    }
}

 */
