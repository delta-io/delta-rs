//! End-to-end example: Parquet encryption via Delta table properties.
//!
//! Encryption is configured by setting `delta.encryption.*` table properties at table
//! creation time.  A factory is registered once globally (or per-session) and all
//! subsequent read and write operations on the table automatically apply encryption.
//!
//! Run with:
//! ```shell
//! cargo run --example basic_operations_encryption \
//!     --features "datafusion integration-test" -p deltalake
//! ```

use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::datafusion::{
    assert_batches_sorted_eq,
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::optimize::OptimizeType;
use deltalake::{DeltaTable, DeltaTableError, arrow};
use deltalake_core::operations::write::encryption::register_encryption_factory;
use deltalake_core::test_utils::kms_encryption::MockKmsFactory;
use std::sync::Arc;
use tempfile::TempDir;

const KMS_ID: &str = "my-test-kms";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 10, 10])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "A"])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                500012305, 500012305, 500012305, 500012305,
            ])),
        ],
    )
    .unwrap()
}

fn table_url(uri: &str) -> url::Url {
    url::Url::parse(&format!("file://{}", uri)).unwrap()
}

async fn table_from_uri(uri: &str) -> DeltaTable {
    deltalake_core::DeltaTableBuilder::from_url(table_url(uri))
        .unwrap()
        .load()
        .await
        .expect("Failed to load table")
}

async fn read(uri: &str) -> Vec<RecordBatch> {
    let table = table_from_uri(uri).await;
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    ctx.sql("SELECT * FROM t ORDER BY int")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), DeltaTableError> {
    // -----------------------------------------------------------------------
    // Step 1: Register the KMS factory (done once at application startup).
    // -----------------------------------------------------------------------
    let factory = Arc::new(MockKmsFactory::new());
    register_encryption_factory(KMS_ID, factory);
    println!("Registered KMS factory '{KMS_ID}'");

    // -----------------------------------------------------------------------
    // Step 2: Create an encrypted table using delta.encryption.* properties.
    // -----------------------------------------------------------------------
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;

    table
        .create()
        .with_columns(vec![
            StructField::new("int", DataType::Primitive(PrimitiveType::Integer), false),
            StructField::new("string", DataType::Primitive(PrimitiveType::String), true),
            StructField::new(
                "timestamp",
                DataType::Primitive(PrimitiveType::TimestampNtz),
                true,
            ),
        ])
        .with_table_name("encrypted_table")
        // These properties are stored in the delta log and automatically applied to all
        // subsequent operations — no per-operation encryption config needed.
        .with_property("delta.encryption.kms.id", KMS_ID)
        .with_property("delta.encryption.footer.key", "my-footer-master-key")
        .await?;

    println!("Created encrypted table at {uri}");

    // -----------------------------------------------------------------------
    // Step 3: Write data — automatically encrypted.
    // -----------------------------------------------------------------------
    let table = table_from_uri(uri).await;
    let table = table.write(vec![batch()]).await?;
    let table = table.write(vec![batch()]).await?;
    println!("Wrote 2 batches (encrypted)");

    // -----------------------------------------------------------------------
    // Step 4: Optimize (Z-order + compact) — reads encrypted, writes encrypted.
    // -----------------------------------------------------------------------
    let table = table_from_uri(uri).await;
    let (table, metrics) = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["int".to_string()]))
        .await?;
    println!("Z-order: {metrics:?}");

    let table = table_from_uri(uri).await;
    let (table, metrics) = table.optimize().await?;
    println!("Compact: {metrics:?}");

    // -----------------------------------------------------------------------
    // Step 5: Read back — automatically decrypted.
    // -----------------------------------------------------------------------
    let batches = read(uri).await;
    println!("Final table:");
    assert_batches_sorted_eq!(
        &[
            "+-----+--------+----------------------------+",
            "| int | string | timestamp                  |",
            "+-----+--------+----------------------------+",
            "| 1   | A      | 1970-01-01T00:08:20.012305 |",
            "| 1   | A      | 1970-01-01T00:08:20.012305 |",
            "| 2   | B      | 1970-01-01T00:08:20.012305 |",
            "| 2   | B      | 1970-01-01T00:08:20.012305 |",
            "| 10  | A      | 1970-01-01T00:08:20.012305 |",
            "| 10  | A      | 1970-01-01T00:08:20.012305 |",
            "| 10  | A      | 1970-01-01T00:08:20.012305 |",
            "| 10  | A      | 1970-01-01T00:08:20.012305 |",
            "+-----+--------+----------------------------+",
        ],
        &batches
    );
    println!("All data verified successfully.");
    Ok(())
}
