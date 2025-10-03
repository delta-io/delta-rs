use arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::{
    assert_batches_sorted_eq,
    config::{ConfigFileType, TableOptions, TableParquetOptions},
    dataframe::DataFrame,
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::collect_sendable_stream;
use deltalake_core::operations::encryption::TableEncryption;
use deltalake_core::parquet::encryption::decrypt::FileDecryptionProperties;
use deltalake_core::table::file_format_options::{
    FileFormatRef, KmsFileFormatOptions, SimpleFileFormatOptions,
};
use deltalake_core::{arrow, parquet, DeltaOps};
use deltalake_core::{operations::optimize::OptimizeType, DeltaTable, DeltaTableError};
use parquet_key_management::datafusion::{KmsEncryptionFactory, KmsEncryptionFactoryOptions};
use parquet_key_management::{
    crypto_factory::{CryptoFactory, DecryptionConfiguration, EncryptionConfiguration},
    kms::KmsConnectionConfig,
    test_kms::TestKmsClientFactory,
};
use std::{fs, sync::Arc};
use tempfile::TempDir;
use url::Url;

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

// Create a DeltaOps instance with the specified file_format_options to apply crypto settings.
async fn ops_with_crypto(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<DeltaOps, DeltaTableError> {
    let prefix_uri = format!("file://{}", uri);
    let url = Url::parse(&*prefix_uri).unwrap();
    let ops = DeltaOps::try_from_uri(url).await?;
    Ok(ops.with_file_format_options(file_format_options.clone()))
}

async fn create_table(
    uri: &str,
    table_name: &str,
    file_format_options: &FileFormatRef,
) -> Result<DeltaTable, DeltaTableError> {
    fs::remove_dir_all(uri)?;
    fs::create_dir(uri)?;
    let ops = ops_with_crypto(uri, file_format_options).await?;

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
    let table = DeltaOps(table).write(vec![batch]).await?;

    assert_eq!(table.version(), Some(2));

    Ok(table)
}

async fn read_table(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<Vec<RecordBatch>, DeltaTableError> {
    let ops = ops_with_crypto(uri, file_format_options).await?;
    let (_table, stream) = ops.load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;
    Ok(data)
}

async fn update_table(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, file_format_options).await?;
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
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, file_format_options).await?;
    let table: DeltaTable = ops.into();
    let version = table.version();
    let ops: DeltaOps = table.into();

    let (table, _metrics) = ops
        .delete()
        .with_predicate(col("int").eq(lit(2)))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(version.unwrap() + 1));

    Ok(())
}

// Secondary table to merge with primary data
fn merge_source() -> DataFrame {
    let ctx = SessionContext::new();
    let schema = get_table_schema();
    let batch = RecordBatch::try_new(
        schema,
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

// Apply merge operation to the primary table
async fn merge_table(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, file_format_options).await?;

    let source = merge_source();

    let (_table, _metrics) = ops
        .merge(source, col("target.int").eq(col("source.int")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_by_source_delete(|delete| delete)
        .unwrap()
        .await?;
    Ok(())
}

async fn optimize_table(
    uri: &str,
    file_format_options: &FileFormatRef,
    optimize_type: OptimizeType,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, file_format_options).await?;
    let (_table, _metrics) = ops.optimize().with_type(optimize_type).await?;
    Ok(())
}

async fn optimize_table_z_order(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    optimize_table(
        uri,
        file_format_options,
        OptimizeType::ZOrder(vec!["timestamp".to_string(), "int".to_string()]),
    )
    .await
}

async fn optimize_table_compact(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    optimize_table(uri, file_format_options, OptimizeType::Compact).await
}

fn plain_crypto_format() -> Result<FileFormatRef, DeltaTableError> {
    let key: Vec<_> = b"1234567890123450".to_vec();

    let crypt = parquet::encryption::encrypt::FileEncryptionProperties::builder(key.clone())
        .with_column_key("int", key.clone())
        .with_column_key("string", key.clone())
        .build()?;

    let decrypt = FileDecryptionProperties::builder(key.clone())
        .with_column_key("int", key.clone())
        .with_column_key("string", key.clone())
        .build()?;

    let mut tpo: TableParquetOptions = TableParquetOptions::default();
    tpo.crypto.file_encryption = Some((&crypt).into());
    tpo.crypto.file_decryption = Some((&decrypt).into());
    let mut tbl_options = TableOptions::new();
    tbl_options.parquet = tpo;
    tbl_options.current_format = Some(ConfigFileType::PARQUET);
    let file_format_options = Arc::new(SimpleFileFormatOptions::new(tbl_options)) as FileFormatRef;
    Ok(file_format_options)
}
fn kms_crypto_format() -> Result<FileFormatRef, DeltaTableError> {
    let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());

    let kms_connection_config = Arc::new(KmsConnectionConfig::default());
    let encryption_factory = Arc::new(KmsEncryptionFactory::new(
        crypto_factory,
        kms_connection_config,
    ));

    let encryption_config = EncryptionConfiguration::builder("kf".into()).build()?;
    let decryption_config = DecryptionConfiguration::builder().build();
    let kms_options = KmsEncryptionFactoryOptions::new(encryption_config, decryption_config);

    let table_encryption =
        TableEncryption::new_with_extension_options(encryption_factory, &kms_options)?;

    let file_format_options =
        Arc::new(KmsFileFormatOptions::new(table_encryption.clone())) as FileFormatRef;
    Ok(file_format_options)
}

fn full_table_data() -> Vec<&'static str> {
    vec![
        "+-----+--------+----------------------------+",
        "| int | string | timestamp                  |",
        "+-----+--------+----------------------------+",
        "| 1   | A      | 1970-01-01T00:16:40.000012 |",
        "| 2   | B      | 1970-01-01T00:16:40.000012 |",
        "| 3   | C      | 1970-01-01T00:16:40.000012 |",
        "| 4   | B      | 1970-01-01T00:16:40.000012 |",
        "| 5   | A      | 1970-01-01T00:08:20.012305 |",
        "| 6   | C      | 1970-01-01T00:08:20.012305 |",
        "| 7   | A      | 1970-01-01T00:08:20.012305 |",
        "| 8   | B      | 1970-01-01T00:08:20.012305 |",
        "| 9   | B      | 1970-01-01T00:08:20.012305 |",
        "| 10  | A      | 1970-01-01T00:08:20.012305 |",
        "| 11  | A      | 1970-01-01T00:08:20.012305 |",
        "| 1   | A      | 1970-01-01T00:16:40.000012 |",
        "| 2   | B      | 1970-01-01T00:16:40.000012 |",
        "| 3   | C      | 1970-01-01T00:16:40.000012 |",
        "| 4   | B      | 1970-01-01T00:16:40.000012 |",
        "| 5   | A      | 1970-01-01T00:08:20.012305 |",
        "| 6   | C      | 1970-01-01T00:08:20.012305 |",
        "| 7   | A      | 1970-01-01T00:08:20.012305 |",
        "| 8   | B      | 1970-01-01T00:08:20.012305 |",
        "| 9   | B      | 1970-01-01T00:08:20.012305 |",
        "| 10  | A      | 1970-01-01T00:08:20.012305 |",
        "| 11  | A      | 1970-01-01T00:08:20.012305 |",
        "+-----+--------+----------------------------+",
    ]
}

type ModifyFn = for<'a> fn(
    uri: &'a str,
    file_format_options: &'a FileFormatRef,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(), DeltaTableError>> + Send + 'a>,
>;

// Create the table, modify it, and read it back. Verify that the final data is as expected.
async fn run_modify_test(
    file_format_options: FileFormatRef,
    modifier: ModifyFn,
    expected: Vec<String>,
) {
    let temp_dir = TempDir::new().unwrap();
    let uri = temp_dir.path().to_str().unwrap();
    let table_name = "test";
    create_table(uri, table_name, &file_format_options)
        .await
        .expect("Failed to create encrypted table");
    modifier(uri, &file_format_options)
        .await
        .expect("Failed to modify encrypted table");
    let data = read_table(uri, &file_format_options)
        .await
        .expect("Failed to read encrypted table");
    let expected_refs: Vec<&str> = expected.iter().map(AsRef::as_ref).collect();
    assert_batches_sorted_eq!(&expected_refs, &data);
}

async fn test_create_and_read(file_format_options: FileFormatRef) {
    // Use the shared modify test template with a no-op modifier
    let expected: Vec<String> = full_table_data().iter().map(|s| s.to_string()).collect();
    run_modify_test(
        file_format_options,
        |_uri, _opts| Box::pin(async { Ok(()) }),
        expected,
    )
    .await;
}

#[tokio::test]
async fn test_create_and_read_plain_crypto() {
    let file_format_options = plain_crypto_format().unwrap();
    test_create_and_read(file_format_options).await;
}

#[tokio::test]
async fn test_create_and_read_kms() {
    let file_format_options = kms_crypto_format().unwrap();
    test_create_and_read(file_format_options).await;
}

async fn test_optimize(file_format_options: FileFormatRef) {
    // Use the shared modify test template; perform optimization steps inside the modifier
    let expected: Vec<String> = full_table_data().iter().map(|s| s.to_string()).collect();
    run_modify_test(
        file_format_options.clone(),
        |uri, opts| Box::pin(optimize_table_z_order(uri, opts)),
        expected.clone(),
    )
    .await;
    run_modify_test(
        file_format_options,
        |uri, opts| Box::pin(optimize_table_compact(uri, opts)),
        expected,
    )
    .await;
}

#[tokio::test]
async fn test_optimize_plain_crypto() {
    let file_format_options = plain_crypto_format().unwrap();
    test_optimize(file_format_options).await;
}

#[tokio::test]
async fn test_optimize_kms() {
    let file_format_options = kms_crypto_format().unwrap();
    test_optimize(file_format_options).await;
}

async fn test_update(file_format_options: FileFormatRef) {
    let base = full_table_data();
    let expected: Vec<String> = base
        .iter()
        // If the value of the int column is 1, so we expect the value to be updated to 100
        .map(|s| s.to_string().replace("| 1   |", "| 100 |"))
        .collect();
    run_modify_test(
        file_format_options,
        |uri, opts| Box::pin(update_table(uri, opts)),
        expected,
    )
    .await;
}

#[tokio::test]
async fn test_update_plain_crypto() {
    let file_format_options = plain_crypto_format().unwrap();
    test_update(file_format_options).await;
}

#[tokio::test]
async fn test_update_kms() {
    let file_format_options = kms_crypto_format().unwrap();
    test_update(file_format_options).await;
}

async fn test_delete(file_format_options: FileFormatRef) {
    let base = full_table_data();
    let expected: Vec<String> = base
        .iter()
        // If the value of the int column is 2, we expect the row to be deleted
        .filter(|s| !s.contains("| 2   |"))
        .map(|s| s.to_string())
        .collect();
    run_modify_test(
        file_format_options,
        |uri, opts| Box::pin(delete_from_table(uri, opts)),
        expected,
    )
    .await;
}

#[tokio::test]
async fn test_delete_plain_crypto() {
    let file_format_options = plain_crypto_format().unwrap();
    test_delete(file_format_options).await;
}

#[tokio::test]
async fn test_delete_kms() {
    let file_format_options = kms_crypto_format().unwrap();
    test_delete(file_format_options).await;
}

async fn test_merge(file_format_options: FileFormatRef) {
    let expected_str = vec![
        "+-----+--------+----------------------------+",
        "| int | string | timestamp                  |",
        "+-----+--------+----------------------------+",
        "| 10  | A      | 1970-01-01T00:08:20.012305 |",
        "| 10  | A      | 1970-01-01T00:08:20.012305 |",
        "+-----+--------+----------------------------+",
    ];
    let expected: Vec<String> = expected_str.iter().map(|s| s.to_string()).collect();
    run_modify_test(
        file_format_options,
        |uri, opts| Box::pin(merge_table(uri, opts)),
        expected,
    )
    .await;
}

#[tokio::test]
async fn test_merge_plain_crypto() {
    let file_format_options = plain_crypto_format().unwrap();
    test_merge(file_format_options).await;
}

#[tokio::test]
async fn test_merge_kms() {
    let file_format_options = kms_crypto_format().unwrap();
    test_merge(file_format_options).await;
}
