use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::datafusion::{
    assert_batches_sorted_eq,
    config::{ConfigFileType, TableOptions, TableParquetOptions},
    dataframe::DataFrame,
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::encryption::decrypt::FileDecryptionProperties;
use deltalake::{arrow, parquet, DeltaTable, DeltaTableError};
use deltalake_core::datafusion::common::test_util::format_batches;
use deltalake_core::operations::optimize::OptimizeType;
use deltalake_core::table::builder::DeltaTableBuilder;
use deltalake_core::table::file_format_options::{FileFormatRef, SimpleFileFormatOptions};
use deltalake_core::test_utils::kms_encryption::{
    KmsFileFormatOptions, MockKmsClient, TableEncryption,
};

use deltalake::datafusion::config::EncryptionFactoryOptions;
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

fn table_with_crypto(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<DeltaTable, DeltaTableError> {
    let prefix_uri = format!("file://{}", uri);
    let url = Url::parse(&*prefix_uri).unwrap();
    let table = DeltaTableBuilder::from_url(url)?
        .with_file_format_options(file_format_options.clone())
        .build()?;
    Ok(table)
}

async fn create_table(
    uri: &str,
    table_name: &str,
    file_format_options: &FileFormatRef,
) -> Result<DeltaTable, DeltaTableError> {
    fs::remove_dir_all(uri)?;
    fs::create_dir(uri)?;
    let table = table_with_crypto(uri, file_format_options)?;

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = table
        .create()
        .with_columns(get_table_columns())
        .with_table_name(table_name)
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), Some(0));

    let batch = get_table_batches();
    let table = table.write(vec![batch.clone()]).await?;

    assert_eq!(table.version(), Some(1));

    // Append records to the table
    let table = table.write(vec![batch.clone()]).await?;

    assert_eq!(table.version(), Some(2));

    Ok(table)
}

async fn read_table(uri: &str, file_format_options: &FileFormatRef) -> Result<(), DeltaTableError> {
    let mut table = table_with_crypto(uri, file_format_options)?;
    table.load().await?;
    let (_table, stream) = table.scan_table().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    let formatted = format_batches(&*data)?.to_string();
    println!("Final table:");
    println!("{}", formatted);

    Ok(())
}

async fn update_table(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    let mut table = table_with_crypto(uri, file_format_options)?;
    table.load().await?;
    let version = table.version();

    let (table, _metrics) = table
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
    let mut table = table_with_crypto(uri, file_format_options)?;
    table.load().await?;
    let version = table.version();

    let (table, _metrics) = table
        .delete()
        .with_predicate(col("int").eq(lit(2)))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(version.unwrap() + 1));

    if false {
        println!("Table after delete:");
        let (_table, stream) = table.scan_table().await?;
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

async fn merge_table(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    let mut table = table_with_crypto(uri, file_format_options)?;
    table.load().await?;

    let schema = get_table_schema();
    let source = merge_source(schema);

    let (table, _metrics) = table
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

    let (_table, stream) = table.scan_table().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    assert_batches_sorted_eq!(&expected, &data);
    Ok(())
}

async fn optimize_table_z_order(
    uri: &str,
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    let mut table = table_with_crypto(uri, file_format_options)?;
    table.load().await?;
    let (_table, metrics) = table
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
    file_format_options: &FileFormatRef,
) -> Result<(), DeltaTableError> {
    let mut table = table_with_crypto(uri, file_format_options)?;
    table.load().await?;
    let (_table, metrics) = table.optimize().with_type(OptimizeType::Compact).await?;
    println!("\nOptimize Compact:\n{metrics:?}\n");
    Ok(())
}

fn plain_crypto_format() -> Result<FileFormatRef, DeltaTableError> {
    let key: Vec<_> = b"1234567890123450".to_vec();
    let _wrong_key: Vec<_> = b"9234567890123450".to_vec(); // Can use to check encryption

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
    let encryption_factory = Arc::new(MockKmsClient::new());
    let configuration = EncryptionFactoryOptions::default();
    let table_encryption = TableEncryption::new(encryption_factory, configuration);
    let file_format_options =
        Arc::new(KmsFileFormatOptions::new(table_encryption)) as FileFormatRef;
    Ok(file_format_options)
}

async fn round_trip_test(
    file_format_options: FileFormatRef,
) -> Result<(), deltalake::errors::DeltaTableError> {
    let temp_dir = TempDir::new()?;
    let uri = temp_dir.path().to_str().unwrap();

    let table_name = "roundtrip";

    create_table(uri, table_name, &file_format_options).await?;
    optimize_table_z_order(uri, &file_format_options).await?;
    // Re-create and append to table again so compact has work to do
    create_table(uri, table_name, &file_format_options).await?;
    optimize_table_compact(uri, &file_format_options).await?;
    update_table(uri, &file_format_options).await?;
    delete_from_table(uri, &file_format_options).await?;
    merge_table(uri, &file_format_options).await?;
    read_table(uri, &file_format_options).await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<(), DeltaTableError> {
    println!("====================");
    println!("Begin Plain encryption test");
    let file_format_options = plain_crypto_format()?;
    round_trip_test(file_format_options).await?;
    println!("End Plain encryption test");
    println!("====================");

    println!("\n\n");
    println!("====================");
    println!("Begin KMS encryption test");
    let file_format_options = kms_crypto_format()?;
    round_trip_test(file_format_options).await?;
    println!("End KMS encryption test");
    println!("====================");

    Ok(())
}
