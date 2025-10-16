//! Configuration for Parquet modular encryption

use crate::DeltaResult;
use arrow_schema::Schema as ArrowSchema;
use object_store::path::Path;
use parquet::file::properties::WriterPropertiesBuilder;
use std::sync::Arc;

pub type SchemaRef = Arc<ArrowSchema>;
use datafusion::config::{ConfigField, EncryptionFactoryOptions, ExtensionOptions};
use datafusion::execution::parquet_encryption::EncryptionFactory;

#[derive(Clone, Debug)]
pub struct TableEncryption {
    encryption_factory: Arc<dyn EncryptionFactory>,
    configuration: EncryptionFactoryOptions,
}

impl TableEncryption {
    pub fn new(
        encryption_factory: Arc<dyn EncryptionFactory>,
        configuration: EncryptionFactoryOptions,
    ) -> Self {
        Self {
            encryption_factory,
            configuration,
        }
    }

    pub fn new_with_extension_options<T: ExtensionOptions>(
        encryption_factory: Arc<dyn EncryptionFactory>,
        options: &T,
    ) -> DeltaResult<Self> {
        let mut configuration = EncryptionFactoryOptions::default();
        for entry in options.entries() {
            if let Some(value) = &entry.value {
                configuration.set(&entry.key, value)?;
            }
        }
        Ok(Self {
            encryption_factory,
            configuration,
        })
    }

    pub fn encryption_factory(&self) -> &Arc<dyn EncryptionFactory> {
        &self.encryption_factory
    }

    pub fn configuration(&self) -> &EncryptionFactoryOptions {
        &self.configuration
    }

    pub async fn update_writer_properties(
        &self,
        mut builder: WriterPropertiesBuilder,
        file_path: &Path,
        file_schema: &SchemaRef,
    ) -> DeltaResult<WriterPropertiesBuilder> {
        let encryption_properties = self
            .encryption_factory
            .get_file_encryption_properties(&self.configuration, file_schema, file_path)
            .await?;
        if let Some(encryption_properties) = encryption_properties {
            builder = builder.with_file_encryption_properties(encryption_properties);
        }
        Ok(builder)
    }
}
