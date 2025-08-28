use std::{fs, path::PathBuf, sync::Arc};

use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::datafusion::{
    assert_batches_sorted_eq,
    config::{TableOptions, TableParquetOptions},
    dataframe::DataFrame,
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::encryption::{
    decrypt::FileDecryptionProperties, encrypt::FileEncryptionProperties,
};
use deltalake::{arrow, parquet, DeltaOps};

use deltalake_core::{
    checkpoints,
    datafusion::{common::test_util::format_batches, config::ConfigFileType},
    operations::optimize::OptimizeType,
    table::file_format_options::SimpleFileFormatOptions,
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
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    schema
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

async fn ops_with_crypto(
    uri: &str,
    enc: Option<&FileEncryptionProperties>,
    dec: Option<&FileDecryptionProperties>,
) -> Result<DeltaOps, DeltaTableError> {
    let ops = DeltaOps::try_from_uri(uri).await?;
    let mut tpo: TableParquetOptions = TableParquetOptions::default();
    if let Some(enc) = enc {
        tpo.crypto.file_encryption = Some(enc.into());
    }
    if let Some(dec) = dec {
        tpo.crypto.file_decryption = Some(dec.into());
    }
    let mut tbl_options = TableOptions::new();
    tbl_options.parquet = tpo;
    tbl_options.current_format = Some(ConfigFileType::PARQUET);
    let format_options = Arc::new(SimpleFileFormatOptions::new(tbl_options));
    Ok(ops.with_file_format_options(format_options))
}

async fn create_table(
    uri: &str,
    table_name: &str,
    crypt: &FileEncryptionProperties,
) -> Result<DeltaTable, DeltaTableError> {
    fs::remove_dir_all(uri)?;
    let ops = ops_with_crypto(uri, Some(crypt), None).await?;

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

async fn read_table(
    uri: &str,
    decryption_properties: &FileDecryptionProperties,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, None, Some(decryption_properties)).await?;
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
    decryption_properties: &FileDecryptionProperties,
    crypt: &FileEncryptionProperties,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, Some(crypt), Some(decryption_properties)).await?;
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
    decryption_properties: &FileDecryptionProperties,
    crypt: &FileEncryptionProperties,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, Some(crypt), Some(decryption_properties)).await?;
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

async fn merge_table(
    uri: &str,
    decryption_properties: &FileDecryptionProperties,
    crypt: &FileEncryptionProperties,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, Some(crypt), Some(decryption_properties)).await?;

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
    decryption_properties: &FileDecryptionProperties,
    crypt: &FileEncryptionProperties,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, Some(crypt), Some(decryption_properties)).await?;
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
    decryption_properties: &FileDecryptionProperties,
    crypt: &FileEncryptionProperties,
) -> Result<(), DeltaTableError> {
    let ops = ops_with_crypto(uri, Some(crypt), Some(decryption_properties)).await?;
    let (_table, metrics) = ops.optimize().with_type(OptimizeType::Compact).await?;
    println!("\nOptimize Compact:\n{metrics:?}\n");
    Ok(())
}

/*
I guess this test isn't needed since checkpoints only summarize what files to use.
 */
async fn checkpoint_table(
    uri: &str,
    decryption_properties: &FileDecryptionProperties,
    crypt: &FileEncryptionProperties,
) -> Result<(), DeltaTableError> {
    let table_location = uri;
    let table_path = PathBuf::from(table_location);
    let log_path = table_path.join("_delta_log");

    let ops = ops_with_crypto(uri, Some(crypt), Some(decryption_properties)).await?;
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
    let uri = "/home/cjoy/src/delta-rs/crates/deltalake/examples/encrypted_roundtrip";
    let table_name = "roundtrip";
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

    create_table(uri, table_name, &crypt).await?;
    optimize_table_z_order(uri, &decrypt, &crypt).await?;
    optimize_table_compact(uri, &decrypt, &crypt).await?;
    checkpoint_table(uri, &decrypt, &crypt).await?;
    update_table(uri, &decrypt, &crypt).await?;
    delete_from_table(uri, &decrypt, &crypt).await?;
    merge_table(uri, &decrypt, &crypt).await?;
    read_table(uri, &decrypt).await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), DeltaTableError> {
    round_trip_test().await?;
    Ok(())
}
