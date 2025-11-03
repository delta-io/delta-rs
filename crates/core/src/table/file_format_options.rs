#[cfg(feature = "datafusion")]
use datafusion::catalog::Session;
#[cfg(feature = "datafusion")]
pub use datafusion::config::{ConfigFileType, TableOptions, TableParquetOptions};
#[cfg(feature = "datafusion")]
use datafusion::execution::SessionState;
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
use tracing::debug;
use uuid::Uuid;

// Top level trait for file format options used by a DeltaTable
pub trait FileFormatOptions: Send + Sync + std::fmt::Debug + 'static {
    #[cfg(feature = "datafusion")]
    fn table_options(&self) -> TableOptions;

    fn writer_properties_factory(&self) -> WriterPropertiesFactoryRef;

    #[cfg(feature = "datafusion")]
    fn update_session(&self, _session: &dyn Session) -> DeltaResult<()> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Convenience alias for file format options reference used across the codebase
pub type FileFormatRef = Arc<dyn FileFormatOptions>;

/// Convenience alias for writer properties factory reference used across the codebase
pub type WriterPropertiesFactoryRef = Arc<dyn WriterPropertiesFactory>;

#[cfg(feature = "datafusion")]
#[derive(Clone, Debug, Default)]
pub struct SimpleFileFormatOptions {
    table_options: TableOptions,
}

#[cfg(feature = "datafusion")]
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

    fn writer_properties_factory(&self) -> WriterPropertiesFactoryRef {
        build_writer_properties_factory_tpo(&Some(self.table_options.parquet.clone())).unwrap()
    }
}

pub fn build_writer_properties_factory_or_default_ffo(
    file_format_options: Option<FileFormatRef>,
) -> WriterPropertiesFactoryRef {
    file_format_options
        .map(|ffo| ffo.writer_properties_factory())
        .unwrap_or_else(|| build_writer_properties_factory_default())
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
    table_parquet_options.as_ref().map(|tpo| {
        let mut tpo = tpo.clone();
        tpo.global.skip_arrow_metadata = true;
        let mut wp_build = WriterPropertiesBuilder::try_from(&tpo)
            .expect("Failed to convert TableParquetOptions to ParquetWriterOptions");
        if tpo.crypto.file_encryption.is_some() {
            wp_build = wp_build
                .with_file_encryption_properties(tpo.crypto.file_encryption.unwrap().into());
        }
        wp_build.build()
    })
}

#[cfg(feature = "datafusion")]
fn build_writer_properties_factory_tpo(
    table_parquet_options: &Option<TableParquetOptions>,
) -> Option<WriterPropertiesFactoryRef> {
    let props = build_writer_properties_tpo(table_parquet_options);
    props.map(|wp| Arc::new(SimpleWriterPropertiesFactory::new(wp)) as WriterPropertiesFactoryRef)
}

pub trait IntoWriterPropertiesFactoryRef {
    fn into_factory_ref(self) -> WriterPropertiesFactoryRef;
}

impl IntoWriterPropertiesFactoryRef for WriterProperties {
    fn into_factory_ref(self) -> WriterPropertiesFactoryRef {
        Arc::new(SimpleWriterPropertiesFactory::new(self))
    }
}

pub fn build_writer_properties_factory_default() -> WriterPropertiesFactoryRef {
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
        debug!("Called create_writer_properties for file: {file_path}");
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
        let mut builder: WriterPropertiesBuilder = self.writer_properties.clone().into();
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
    writer_properties_factory: WriterPropertiesFactoryRef,
    encryption_factory_id: String,
}

#[cfg(feature = "datafusion")]
impl KmsFileFormatOptions {
    pub fn new(table_encryption: TableEncryption) -> Self {
        let encryption_factory_id = format!("delta-{}", Uuid::new_v4());
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

    fn writer_properties_factory(&self) -> WriterPropertiesFactoryRef {
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
