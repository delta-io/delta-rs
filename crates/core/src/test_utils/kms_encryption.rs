use crate::table::file_format_options::{
    FileFormatOptions, TableOptions, WriterPropertiesFactory, WriterPropertiesFactoryRef,
};
use crate::{crate_version, DeltaResult};
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use uuid::Uuid;

#[cfg(feature = "datafusion")]
use crate::operations::encryption::TableEncryption;

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
