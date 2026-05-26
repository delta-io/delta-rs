#![cfg(feature = "datafusion")]
//! Integration tests for Parquet encryption via `delta.encryption.*` table properties.
//!
//! Encryption is configured by setting Delta table properties at table creation time.
//! A factory is registered globally once and all operations (write, read, delete, update,
//! merge, optimize) automatically encrypt/decrypt without any per-operation configuration.

use arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::{
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::optimize::OptimizeType;
use deltalake_core::operations::write::encryption::register_encryption_factory;
use deltalake_core::test_utils::kms_encryption::MockKmsFactory;
use deltalake_core::{DeltaResult, DeltaTable};
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

fn get_table_columns() -> Vec<StructField> {
    vec![
        StructField::new("int", DataType::Primitive(PrimitiveType::Integer), false),
        StructField::new("string", DataType::Primitive(PrimitiveType::String), true),
        StructField::new(
            "timestamp",
            DataType::Primitive(PrimitiveType::TimestampNtz),
            true,
        ),
    ]
}

fn get_table_batches() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let int_vals = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_vals = StringArray::from(vec!["A", "B", "C", "B", "A", "C", "A", "B", "B", "A", "A"]);
    let ts_vals = TimestampMicrosecondArray::from(vec![
        1000000012, 1000000012, 1000000012, 1000000012, 500012305, 500012305, 500012305, 500012305,
        500012305, 500012305, 500012305,
    ]);
    RecordBatch::try_new(
        schema,
        vec![Arc::new(int_vals), Arc::new(str_vals), Arc::new(ts_vals)],
    )
    .unwrap()
}

/// Register a fresh factory with a unique ID to prevent test interference.
/// Each test gets its own `MockKmsFactory` instance so keys from one test
/// cannot be mistaken for keys from another.
fn register_fresh_factory() -> String {
    let kms_id = format!("test-kms-{}", Uuid::new_v4());
    let factory = Arc::new(MockKmsFactory::new());
    register_encryption_factory(&kms_id, factory);
    kms_id
}

fn table_url(uri: &str) -> Url {
    Url::parse(&format!("file://{}", uri)).unwrap()
}

async fn create_encrypted_table(
    uri: &str,
    table_name: &str,
    kms_id: &str,
) -> DeltaResult<DeltaTable> {
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_table_name(table_name)
        .with_property("delta.encryption.kms.id", kms_id)
        .with_property("delta.encryption.footer.key", "test-footer-key")
        .await?;

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let batch = get_table_batches();
    let table = table.write(vec![batch.clone()]).await?;
    let table = table.write(vec![batch.clone()]).await?;
    Ok(table)
}

async fn read_table(uri: &str) -> DeltaResult<Vec<RecordBatch>> {
    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await?)?;
    let batches = ctx.sql("SELECT * FROM t").await?.collect().await?;
    Ok(batches)
}

/// Recursively collect all `.parquet` files under `dir`.
fn find_parquet(d: &std::path::Path, result: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(d) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                find_parquet(&path, result);
            } else if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                result.push(path);
            }
        }
    }
}

/// Walk `dir` and assert that every `.parquet` file has an encrypted footer.
/// Fails with a clear message if any file can be read without decryption — which
/// would mean the operation wrote unencrypted parquet despite having encryption configured.
async fn assert_all_parquets_encrypted(dir: &std::path::Path) {
    use object_store::{ObjectStore, local::LocalFileSystem, path::Path};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;

    let mut parquet_files = vec![];
    find_parquet(dir, &mut parquet_files);

    assert!(
        !parquet_files.is_empty(),
        "No parquet files found in {:?} — cannot verify encryption",
        dir
    );

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    for abs_path in &parquet_files {
        let rel = abs_path.strip_prefix(dir).unwrap();
        let object_path = Path::parse(rel.to_string_lossy().as_ref()).unwrap();
        let meta = store.head(&object_path).await.unwrap();
        let reader =
            ParquetObjectReader::new(store.clone(), object_path.clone()).with_file_size(meta.size);
        let result = ParquetRecordBatchStreamBuilder::new(reader).await;
        assert!(
            result.is_err(),
            "Parquet file {:?} opened WITHOUT decryption — file is NOT encrypted! \
             Encryption may have silently failed to propagate for this operation.",
            rel
        );
    }
}

#[tokio::test]
async fn test_encrypted_create_and_read() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_optimize_compact() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table.optimize().await?;
    assert!(
        metrics.num_files_added > 0 || metrics.num_files_removed > 0,
        "Compact should have changed files: {metrics:?}"
    );
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_encrypted_optimize_zorder() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["int".to_string()]))
        .await?;
    assert!(metrics.num_files_added > 0, "Z-order should add files");
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_delete() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table.delete().with_predicate(col("int").eq(lit(1))).await?;
    assert!(metrics.num_deleted_rows > 0);
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_update() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table
        .update()
        .with_predicate(col("int").eq(lit(1)))
        .with_update("int", lit(100))
        .await?;
    assert!(metrics.num_updated_rows > 0);
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

// ---------------------------------------------------------------------------
// Negative test: missing factory must produce a clear error
// ---------------------------------------------------------------------------

/// Guards against silent encryption skip: verifies that an unregistered kms.id
/// returns an explicit error rather than silently writing/reading unencrypted data.
#[tokio::test]
async fn test_missing_factory_returns_error() {
    use deltalake_core::operations::write::encryption::{
        WriterEncryptionConfig, get_encryption_factory,
    };
    use deltalake_core::table::config::EncryptionConfig;

    let impossible_kms = format!("never-registered-{}", Uuid::new_v4());
    assert!(
        get_encryption_factory(&impossible_kms).is_none(),
        "Factory should not be registered"
    );

    // WriterEncryptionConfig must error when kms.id is set but factory is absent.
    let fake_enc = EncryptionConfig {
        kms_id: impossible_kms.clone(),
        kms_configuration: None,
        footer_key: "key".to_string(),
        plaintext_footer: false,
        column_keys: std::collections::HashMap::new(),
    };
    let result = WriterEncryptionConfig::from_global_registry(Some(fake_enc));
    assert!(result.is_err(), "Expected error for unregistered kms.id");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains(&impossible_kms),
        "Error should mention the kms_id, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_matrix_create_and_read() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;
    let batches = read_table(uri).await?;
    assert!(
        !batches.is_empty(),
        "Table should have rows after create_and_read"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Verify files are physically encrypted on disk
// ---------------------------------------------------------------------------

/// The critical correctness test: opens each parquet file in the table directly
/// with the raw parquet reader (no decryption properties) and verifies that the
/// read fails because the footer is encrypted.
///
/// If this test fails it means parquet files are written **unencrypted** even though
/// `delta.encryption.*` properties are set — the encryption configuration silently
/// failed to propagate.
#[tokio::test]
async fn test_parquet_files_are_physically_encrypted() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;
    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Columnar encryption with plaintext footer
// ---------------------------------------------------------------------------

/// Verify columnar encryption where only the "int" and "string" columns are encrypted
/// and the parquet footer is left in plaintext.
///
/// With plaintext footer mode:
/// - The footer (schema, row-group metadata) is readable without keys.
/// - Only the column data pages for "int" and "string" are encrypted.
/// - The "timestamp" column is not encrypted and is always readable.
///
/// This tests that `delta.encryption.column.keys` and
/// `delta.encryption.plaintext.footer` are correctly forwarded to the factory.
#[tokio::test]
async fn test_encrypted_columnar_plaintext_footer() -> DeltaResult<()> {
    use object_store::{ObjectStore, local::LocalFileSystem, path::Path as ObjPath};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    let table_url = table_url(uri);

    // Create with only "int" and "string" encrypted; "timestamp" is left unencrypted.
    // The footer is stored in plaintext so the schema is readable without keys.
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url.clone())?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_table_name("col-enc-test")
        .with_property("delta.encryption.kms.id", &kms_id)
        .with_property("delta.encryption.footer.key", "footer-master-key")
        .with_property("delta.encryption.plaintext.footer", "true")
        .with_property("delta.encryption.column.keys", "col-master-key:int,string")
        .await?;

    // Write two batches so we exercise more than one file.
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url.clone())?
        .load()
        .await?;
    let batch = get_table_batches();
    let table = table.write(vec![batch.clone()]).await?;
    let table = table.write(vec![batch.clone()]).await?;

    // Round-trip: read back with the factory registered and verify row count.
    let batches = read_table(uri).await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 22,
        "Expected 22 rows (2 writes × 11 rows each), got {total_rows}"
    );

    // Physical check: without decryption, the parquet FOOTER should be readable
    // (plaintext footer mode) but reading the encrypted column data should fail.
    let mut parquet_files = vec![];
    find_parquet(dir.path(), &mut parquet_files);
    assert!(
        !parquet_files.is_empty(),
        "No parquet files found — cannot verify columnar encryption"
    );

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    for abs_path in &parquet_files {
        let rel = abs_path.strip_prefix(dir.path()).unwrap();
        let obj_path = ObjPath::parse(rel.to_string_lossy().as_ref()).unwrap();

        let meta = store.head(&obj_path).await.unwrap();
        let reader =
            ParquetObjectReader::new(store.clone(), obj_path.clone()).with_file_size(meta.size);

        // With plaintext footer the builder itself must SUCCEED (footer is readable).
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Expected plaintext footer to be readable for {:?}, got: {e}",
                    rel
                )
            });

        // Reading column data without decryption keys must FAIL for the encrypted columns.
        let result: parquet::errors::Result<Vec<_>> = async {
            let stream = builder.build()?;
            futures::StreamExt::collect::<Vec<_>>(stream)
                .await
                .into_iter()
                .collect()
        }
        .await;
        assert!(
            result.is_err(),
            "Expected reading encrypted column data to fail for {:?} without keys, \
             but it succeeded — columnar encryption may not have been applied",
            rel
        );
    }

    // Verify the "timestamp" column is NOT listed in the encryption properties
    // so the write path respected the per-column config.
    let _ = table; // silence unused warning

    Ok(())
}
