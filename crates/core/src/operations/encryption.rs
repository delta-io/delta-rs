//! Configuration for Parquet modular encryption

use crate::DeltaResult;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::common::{extensions_options, DataFusionError};
use object_store::path::Path;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet_key_management::{
    crypto_factory::{CryptoFactory, DecryptionConfiguration, EncryptionConfiguration},
    kms::KmsConnectionConfig,
};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub type SchemaRef = Arc<ArrowSchema>;
use datafusion::config::{ConfigEntry, ConfigField, EncryptionFactoryOptions, ExtensionOptions};
use datafusion::execution::parquet_encryption::EncryptionFactory;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;

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

// -------------------------------------------------------------------------------------------------
// Below code should all be part of parquet-key-management-rs.
// See PR to add DataFusion integration:
// https://github.com/G-Research/parquet-key-management-rs/pull/20
// -------------------------------------------------------------------------------------------------

pub struct KmsEncryptionFactory {
    crypto_factory: CryptoFactory,
    kms_connection_config: Arc<KmsConnectionConfig>,
}

impl KmsEncryptionFactory {
    /// Create a new [`KmsEncryptionFactory`] with the provided [`CryptoFactory`] and [`KmsConnectionConfig`].
    pub fn new(
        crypto_factory: CryptoFactory,
        kms_connection_config: Arc<KmsConnectionConfig>,
    ) -> Self {
        Self {
            crypto_factory,
            kms_connection_config,
        }
    }
}

#[async_trait]
impl EncryptionFactory for KmsEncryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _schema: &arrow_schema::SchemaRef,
        _file_path: &Path,
    ) -> datafusion::common::Result<Option<FileEncryptionProperties>> {
        let encryption_configuration = build_encryption_configuration(config)?;
        Ok(Some(self.crypto_factory.file_encryption_properties(
            Arc::clone(&self.kms_connection_config),
            &encryption_configuration,
        )?))
    }

    async fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _file_path: &Path,
    ) -> datafusion::common::Result<Option<FileDecryptionProperties>> {
        let decryption_configuration = build_decryption_configuration(config)?;
        Ok(Some(self.crypto_factory.file_decryption_properties(
            Arc::clone(&self.kms_connection_config),
            decryption_configuration,
        )?))
    }
}

impl std::fmt::Debug for KmsEncryptionFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsEncryptionFactory")
            .finish_non_exhaustive()
    }
}

/// DataFusion compatible options used to configure generation of encryption and decryption
/// properties for a Table when using a [`KmsEncryptionFactory`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KmsEncryptionFactoryOptions {
    /// The configuration to use when writing encrypted Parquet
    pub encryption: EncryptionOptions,
    /// The configuration to use when reading encrypted Parquet
    pub decryption: DecryptionOptions,
}

impl KmsEncryptionFactoryOptions {
    /// Create a new [`KmsEncryptionFactoryOptions `] from encryption and decryption configurations
    pub fn new(
        encryption_config: EncryptionConfiguration,
        decryption_config: DecryptionConfiguration,
    ) -> Self {
        Self {
            encryption: encryption_config.into(),
            decryption: decryption_config.into(),
        }
    }
}

extensions_options! {
    /// DataFusion compatible configuration options related to file encryption
    pub struct EncryptionOptions {
        /// Master key identifier for footer key encryption
        pub footer_key_id: String, default = "".to_owned()
        /// List of master key ids and the columns to be encrypted with each key,
        /// formatted like "columnKeyId1:column1,column2;columnKeyId2:column3"
        pub column_key_ids: String, default = "".to_owned()
        /// Whether to write footer metadata unencrypted
        pub plaintext_footer: bool, default = false
        /// Whether to encrypt data encryption keys with key encryption keys, before wrapping with the master key
        pub double_wrapping: bool, default = true
        /// How long in seconds to cache objects used during encryption
        pub cache_lifetime_s: Option<u64>, default = None
        /// Whether to store encryption key material inside Parquet files
        pub internal_key_material: bool, default = true
        /// Length of data encryption keys to generate
        pub data_key_length_bits: u64, default = 128
    }
}

extensions_options! {
    /// DataFusion compatible configuration options related to file decryption
    pub struct DecryptionOptions {
        /// How long in seconds to cache objects used during decryption
        pub cache_lifetime_s: Option<u64>, default = None
    }
}

// Manually implement PartialEq as using #[derive] isn't compatible with extensions_options macro
impl PartialEq for EncryptionOptions {
    fn eq(&self, other: &Self) -> bool {
        self.footer_key_id == other.footer_key_id
            && self.column_key_ids == other.column_key_ids
            && self.plaintext_footer == other.plaintext_footer
            && self.double_wrapping == other.double_wrapping
            && self.cache_lifetime_s == other.cache_lifetime_s
            && self.internal_key_material == other.internal_key_material
            && self.data_key_length_bits == other.data_key_length_bits
    }
}

impl PartialEq for DecryptionOptions {
    fn eq(&self, other: &Self) -> bool {
        self.cache_lifetime_s == other.cache_lifetime_s
    }
}

impl ExtensionOptions for KmsEncryptionFactoryOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::common::Result<()> {
        let (parent, key) = key.split_once('.').ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Invalid configuration key for KMS encryption: {key}"
            ))
        })?;
        match parent {
            "decryption" => ExtensionOptions::set(&mut self.decryption, key, value),
            "encryption" => ExtensionOptions::set(&mut self.encryption, key, value),
            _ => Err(DataFusionError::Configuration(format!(
                "Invalid configuration key for KMS encryption: {parent}"
            ))),
        }
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        let mut decryption_entries = self.decryption.entries();
        for entry in decryption_entries.iter_mut() {
            entry.key = format!("decryption.{}", entry.key);
        }
        let mut encryption_entries = self.encryption.entries();
        for entry in encryption_entries.iter_mut() {
            entry.key = format!("encryption.{}", entry.key);
        }
        decryption_entries.append(&mut encryption_entries);
        decryption_entries
    }
}

impl From<EncryptionConfiguration> for EncryptionOptions {
    fn from(config: EncryptionConfiguration) -> Self {
        EncryptionOptions {
            footer_key_id: config.footer_key_id().to_owned(),
            column_key_ids: serialize_column_keys(config.column_key_ids()),
            plaintext_footer: config.plaintext_footer(),
            double_wrapping: config.double_wrapping(),
            cache_lifetime_s: config.cache_lifetime().map(|lifetime| lifetime.as_secs()),
            internal_key_material: config.internal_key_material(),
            data_key_length_bits: config.data_key_length_bits() as u64,
        }
    }
}

impl TryInto<EncryptionConfiguration> for EncryptionOptions {
    type Error = DataFusionError;

    fn try_into(self) -> Result<EncryptionConfiguration, Self::Error> {
        let mut builder = EncryptionConfiguration::builder(self.footer_key_id.clone())
            .set_double_wrapping(self.double_wrapping)
            .set_plaintext_footer(self.plaintext_footer)
            .set_cache_lifetime(self.cache_lifetime_s.map(Duration::from_secs));
        let column_keys = deserialize_column_keys(&self.column_key_ids)?;
        for (key, value) in column_keys.into_iter() {
            builder = builder.add_column_key(key, value);
        }
        Ok(builder.build()?)
    }
}

impl From<DecryptionConfiguration> for DecryptionOptions {
    fn from(config: DecryptionConfiguration) -> Self {
        DecryptionOptions {
            cache_lifetime_s: config.cache_lifetime().map(|lifetime| lifetime.as_secs()),
        }
    }
}

impl TryInto<DecryptionConfiguration> for DecryptionOptions {
    type Error = DataFusionError;

    fn try_into(self) -> Result<DecryptionConfiguration, Self::Error> {
        Ok(DecryptionConfiguration::builder()
            .set_cache_lifetime(self.cache_lifetime_s.map(Duration::from_secs))
            .build())
    }
}

fn serialize_column_keys(column_key_ids: &HashMap<String, Vec<String>>) -> String {
    let mut result = String::new();
    let mut first = true;
    for (key_id, columns) in column_key_ids.iter() {
        if !first {
            result.push(';');
        }
        result.push_str(key_id);
        result.push(':');
        result.push_str(&columns.join(","));
        first = false;
    }
    result
}

fn deserialize_column_keys(
    column_key_ids: &str,
) -> datafusion::common::Result<HashMap<String, Vec<String>>> {
    let mut keys = HashMap::new();
    for key_with_cols in column_key_ids.split(';') {
        if key_with_cols.is_empty() {
            continue;
        }
        let (key_id, cols) = key_with_cols.split_once(':').ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Invalid column_key_ids format in encryption configuration: '{column_key_ids}'"
            ))
        })?;
        let cols = cols
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        keys.insert(key_id.to_owned(), cols);
    }
    Ok(keys)
}

fn build_decryption_configuration(
    options: &EncryptionFactoryOptions,
) -> datafusion::common::Result<DecryptionConfiguration> {
    let kms_options: KmsEncryptionFactoryOptions = options.to_extension_options()?;
    kms_options.decryption.try_into()
}

fn build_encryption_configuration(
    options: &EncryptionFactoryOptions,
) -> datafusion::common::Result<EncryptionConfiguration> {
    let kms_options: KmsEncryptionFactoryOptions = options.to_extension_options()?;
    kms_options.encryption.try_into()
}
