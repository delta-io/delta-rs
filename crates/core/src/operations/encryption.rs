//! Configuration for Parquet modular encryption
use crate::{DeltaResult};
use object_store::path::Path;
use parquet::file::properties::WriterPropertiesBuilder;
use std::sync::Arc;
use datafusion::common::{DataFusionError, HashMap};
use datafusion::config::{ConfigField, ExtensionOptions, Visit};
use arrow_schema::Schema as ArrowSchema;

pub type SchemaRef = Arc<ArrowSchema>;
/*
use datafusion::config::EncryptionFactoryOptions;
use datafusion::execution::parquet_encryption::EncryptionFactory;
 */


///////////////////////////////////////////////////////////////////////////////
// Copy code from datafusion
// Updating to version  > 49.0.1 is a bigger change
use std::result;
use delta_kernel::schema::StructType;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;

/// Result type for operations that could result in an [DataFusionError]
pub type Result<T, E = DataFusionError> = result::Result<T, E>;

/// Holds implementation-specific options for an encryption factory
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EncryptionFactoryOptions {
    pub options: HashMap<String, String>,
}

impl ConfigField for EncryptionFactoryOptions {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, _description: &'static str) {
        for (option_key, option_value) in &self.options {
            v.some(
                &format!("{key}.{option_key}"),
                option_value,
                "Encryption factory specific option",
            );
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.options.insert(key.to_owned(), value.to_owned());
        Ok(())
    }
}

impl EncryptionFactoryOptions {
    /// Convert these encryption factory options to an [`ExtensionOptions`] instance.
    pub fn to_extension_options<T: ExtensionOptions + Default>(&self) -> Result<T> {
        let mut options = T::default();
        for (key, value) in &self.options {
            options.set(key, value)?;
        }
        Ok(options)
    }
}

/// Trait for types that generate file encryption and decryption properties to
/// write and read encrypted Parquet files.
/// This allows flexibility in how encryption keys are managed, for example, to
/// integrate with a user's key management service (KMS).
/// For example usage, see the [`parquet_encrypted_with_kms` example].
///
/// [`parquet_encrypted_with_kms` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/parquet_encrypted_with_kms.rs
pub trait EncryptionFactory: Send + Sync + std::fmt::Debug + 'static {
    /// Generate file encryption properties to use when writing a Parquet file.
    fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        schema: &SchemaRef,
        file_path: &Path,
    ) -> Result<Option<FileEncryptionProperties>>;

    /// Generate file decryption properties to use when reading a Parquet file.
    fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> Result<Option<FileDecryptionProperties>>;
}

// End Copy code from datafusion
///////////////////////////////////////////////////////////////////////////////

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

    pub fn update_writer_properties(
        &self,
        mut builder: WriterPropertiesBuilder,
        file_path: &Path,
        file_schema: &SchemaRef,
    ) -> DeltaResult<WriterPropertiesBuilder> {
        let encryption_properties = self.encryption_factory.get_file_encryption_properties(
            &self.configuration,
            file_schema,
            file_path,
        )?;
        if let Some(encryption_properties) = encryption_properties {
            builder = builder.with_file_encryption_properties(encryption_properties);
        }
        Ok(builder)
    }
}