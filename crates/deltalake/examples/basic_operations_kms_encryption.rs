use async_trait::async_trait;
use parquet_key_management::crypto_factory::{
    CryptoFactory, DecryptionConfiguration, EncryptionConfiguration,
};
use parquet_key_management::kms::KmsConnectionConfig;
use parquet_key_management::test_kms::TestKmsClientFactory;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::time::Duration;
use std::{fs, path::PathBuf, sync::Arc};
use tempfile::TempDir;

use deltalake::arrow::datatypes::SchemaRef;
use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::datafusion::common::{extensions_options, DataFusionError};
use deltalake::datafusion::config::{
    ConfigEntry, ConfigField, EncryptionFactoryOptions, ExtensionOptions, TableOptions,
};
use deltalake::datafusion::execution::parquet_encryption::EncryptionFactory;
use deltalake::datafusion::{
    assert_batches_sorted_eq,
    dataframe::DataFrame,
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::encryption::{
    decrypt::FileDecryptionProperties, encrypt::FileEncryptionProperties,
};
use deltalake::{arrow, DeltaOps};
use deltalake_core::operations::encryption::TableEncryption;
use deltalake_core::table::file_format_options::{
    FileFormatOptions, KMSWriterPropertiesFactory, WriterPropertiesFactory,
};
use deltalake_core::{
    checkpoints, datafusion::common::test_util::format_batches, operations::optimize::OptimizeType,
    DeltaTable, DeltaTableError,
};

fn get_table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            String::from("int"),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            String::from("string"),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            String::from("timestamp"),
            DataType::Primitive(PrimitiveType::TimestampNtz),
            true,
        ),
    ]
}

fn get_table_schema() -> Arc<Schema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

fn get_table_batches() -> RecordBatch {
    let schema = get_table_schema();

    let int_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_values = StringArray::from(vec!["A", "B", "C", "B", "A", "C", "A", "B", "B", "A", "A"]);
    let ts_values = TimestampMicrosecondArray::from(vec![
        1000000012, 1000000012, 1000000012, 1000000012, 500012305, 500012305, 500012305, 500012305,
        500012305, 500012305, 500012305,
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(int_values),
            Arc::new(str_values),
            Arc::new(ts_values),
        ],
    )
    .unwrap()
}

struct KmsFileFormatOptions {
    table_encryption: TableEncryption,
    writer_properties_factory: Arc<dyn WriterPropertiesFactory>,
}

impl KmsFileFormatOptions {
    fn new(table_encryption: TableEncryption) -> Self {
        let writer_properties_factory = Arc::new(KMSWriterPropertiesFactory::with_encryption(
            table_encryption.clone(),
        ));
        Self {
            table_encryption,
            writer_properties_factory,
        }
    }
}

impl Debug for KmsFileFormatOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsFileFormatOptions")
            .finish_non_exhaustive()
    }
}

impl FileFormatOptions for KmsFileFormatOptions {
    fn table_options(&self) -> TableOptions {
        // TODO: Allow access to session when creating these?
        let mut table_options = TableOptions::default();
        // TODO: Need to have random per-factory id somehow?
        table_options.parquet.crypto.factory_id = Some("temp_id".to_owned());
        table_options.parquet.crypto.factory_options = self.table_encryption.configuration.clone();
        table_options
    }

    fn writer_properties_factory(&self) -> Arc<dyn WriterPropertiesFactory> {
        Arc::clone(&self.writer_properties_factory)
    }
}

async fn ops_with_crypto(
    uri: &str,
    table_encryption: &TableEncryption,
) -> Result<DeltaOps, DeltaTableError> {
    let ops = DeltaOps::try_from_uri(uri).await?;
    let file_format_options = Arc::new(KmsFileFormatOptions::new(table_encryption.clone()));
    Ok(ops.with_file_format_options(file_format_options))
}

async fn create_table(
    uri: &str,
    table_name: &str,
    table_encryption: &TableEncryption,
) -> Result<DeltaTable, DeltaTableError> {
    fs::remove_dir_all(uri)?;
    let ops = ops_with_crypto(uri, table_encryption).await?;

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = ops
        .create()
        .with_columns(get_table_columns())
        .with_table_name(table_name)
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), Some(0));

    let batch = get_table_batches();
    let table = DeltaOps(table).write(vec![batch.clone()]).await?;

    assert_eq!(table.version(), Some(1));

    // Append records to the table
    let table = DeltaOps(table).write(vec![batch.clone()]).await?;

    assert_eq!(table.version(), Some(2));

    Ok(table)
}

async fn read_table(uri: &str, table_encryption: &TableEncryption) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, table_encryption).await?;
    let (_table, stream) = ops.load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    // println!("{data:?}");
    let formatted = format_batches(&*data)?.to_string();
    println!("Final table:");
    println!("{}", formatted);

    Ok(())
}

async fn update_table(
    uri: &str,
    table_encryption: &TableEncryption,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, table_encryption).await?;
    let table: DeltaTable = ops.into();
    let version = table.version();
    let ops: DeltaOps = table.into();

    let (table, _metrics) = ops
        .update()
        .with_predicate(col("int").eq(lit(1)))
        .with_update("int", "100")
        .await
        .unwrap();

    assert_eq!(table.version(), Some(version.unwrap() + 1));

    Ok(())
}

async fn delete_from_table(
    uri: &str,
    table_encryption: &TableEncryption,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, table_encryption).await?;
    let table: DeltaTable = ops.into();
    let version = table.version();
    let ops: DeltaOps = table.into();

    let (table, _metrics) = ops
        .delete()
        .with_predicate(col("int").eq(lit(2)))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(version.unwrap() + 1));

    if false {
        println!("Table after delete:");
        let (_table, stream) = DeltaOps(table).load().await?;
        let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

        println!("{data:?}");
    }

    Ok(())
}

fn merge_source(schema: Arc<ArrowSchema>) -> DataFrame {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
            Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1000000012, 1000000012, 1000000012,
            ])),
        ],
    )
    .unwrap();
    ctx.read_batch(batch).unwrap()
}

async fn merge_table(uri: &str, table_encryption: &TableEncryption) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, table_encryption).await?;

    let schema = get_table_schema();
    let source = merge_source(schema);

    let (table, _metrics) = ops
        .merge(source, col("target.int").eq(col("source.int")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_by_source_delete(|delete| delete)
        .unwrap()
        .await
        .unwrap();

    let expected = vec![
        "+-----+--------+----------------------------+",
        "| int | string | timestamp                  |",
        "+-----+--------+----------------------------+",
        "| 10  | A      | 1970-01-01T00:08:20.012305 |",
        "| 10  | A      | 1970-01-01T00:08:20.012305 |",
        "+-----+--------+----------------------------+",
    ];

    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    // println!("{data:?}");

    assert_batches_sorted_eq!(&expected, &data);
    Ok(())
}

async fn optimize_table_z_order(
    uri: &str,
    table_encryption: &TableEncryption,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, table_encryption).await?;
    let (_table, metrics) = ops
        .optimize()
        .with_type(OptimizeType::ZOrder(vec![
            "timestamp".to_string(),
            "int".to_string(),
        ]))
        .await?;
    println!("\nOptimize Z-Order:\n{metrics:?}\n");
    Ok(())
}

async fn optimize_table_compact(
    uri: &str,
    table_encryption: &TableEncryption,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, table_encryption).await?;
    let (_table, metrics) = ops.optimize().with_type(OptimizeType::Compact).await?;
    println!("\nOptimize Compact:\n{metrics:?}\n");
    Ok(())
}

/*
I guess this test isn't needed since checkpoints only summarize what files to use.
 */
async fn checkpoint_table(
    uri: &str,
    table_encryption: &TableEncryption,
) -> Result<(), DeltaTableError> {
    let table_location = uri;
    let table_path = PathBuf::from(table_location);
    let log_path = table_path.join("_delta_log");

    let ops = ops_with_crypto(uri, table_encryption).await?;
    let table: DeltaTable = ops.into();
    let version = table.version();

    // Write a checkpoint
    checkpoints::create_checkpoint(&table, None).await.unwrap();

    // checkpoint should exist
    let filename = format!(
        "00000000000000000{:03}.checkpoint.parquet",
        version.unwrap()
    );
    let checkpoint_path = log_path.join(filename);
    assert!(checkpoint_path.as_path().exists());

    Ok(())
}

async fn round_trip_test() -> Result<(), deltalake::errors::DeltaTableError> {
    let temp_dir = TempDir::new()?;
    let uri = temp_dir.path().to_str().unwrap();

    let table_name = "roundtrip";

    let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());

    let kms_connection_config = Arc::new(KmsConnectionConfig::default());
    let encryption_factory = Arc::new(KmsEncryptionFactory::new(
        crypto_factory,
        kms_connection_config,
    ));

    let encryption_config = EncryptionConfiguration::builder("kf".into()).build()?;
    let decryption_config = DecryptionConfiguration::builder().build();
    let kms_options = KmsEncryptionFactoryOptions::new(encryption_config, decryption_config);

    // TODO: Move this into TableEncryption ctor?
    let mut encryption_factory_options = EncryptionFactoryOptions::default();
    for entry in kms_options.entries() {
        if let Some(value) = &entry.value {
            encryption_factory_options.set(&entry.key, value)?;
        }
    }

    let table_encryption = TableEncryption::new(encryption_factory, encryption_factory_options);

    create_table(uri, table_name, &table_encryption).await?;
    //optimize_table_z_order(uri, &table_encryption).await?;
    //optimize_table_compact(uri, &table_encryption).await?;
    //checkpoint_table(uri, &table_encryption).await?;
    //update_table(uri, &table_encryption).await?;
    //delete_from_table(uri, &table_encryption).await?;
    //merge_table(uri, &table_encryption).await?;
    read_table(uri, &table_encryption).await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), DeltaTableError> {
    round_trip_test().await?;
    Ok(())
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
        _schema: &SchemaRef,
        _file_path: &object_store::path::Path,
    ) -> deltalake::datafusion::common::Result<Option<FileEncryptionProperties>> {
        let encryption_configuration = build_encryption_configuration(config)?;
        Ok(Some(self.crypto_factory.file_encryption_properties(
            Arc::clone(&self.kms_connection_config),
            &encryption_configuration,
        )?))
    }

    async fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _file_path: &object_store::path::Path,
    ) -> deltalake::datafusion::common::Result<Option<FileDecryptionProperties>> {
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

    fn set(&mut self, key: &str, value: &str) -> deltalake::datafusion::common::Result<()> {
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
) -> deltalake::datafusion::common::Result<HashMap<String, Vec<String>>> {
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
) -> deltalake::datafusion::common::Result<DecryptionConfiguration> {
    let kms_options: KmsEncryptionFactoryOptions = options.to_extension_options()?;
    kms_options.decryption.try_into()
}

fn build_encryption_configuration(
    options: &EncryptionFactoryOptions,
) -> deltalake::datafusion::common::Result<EncryptionConfiguration> {
    let kms_options: KmsEncryptionFactoryOptions = options.to_extension_options()?;
    kms_options.encryption.try_into()
}
