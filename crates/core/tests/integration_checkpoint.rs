use chrono::Utc;
use deltalake_core::DeltaTable;
use deltalake_core::checkpoints::{cleanup_expired_logs_for, create_checkpoint};
use deltalake_core::kernel::{DataType, PrimitiveType};
use deltalake_core::writer::{DeltaWriter, JsonWriter};
use deltalake_core::{DeltaTableBuilder, ObjectStore, ensure_table_uri, errors::DeltaResult};
use deltalake_test::utils::*;
use object_store::path::Path;
use serde_json::json;
use serial_test::serial;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use url::Url;

/// Clone an existing test table from the tests crate into a [TempDir] for use in an integration
/// test
pub fn clone_table(table_name: impl AsRef<str> + std::fmt::Display) -> TempDir {
    // Create a temporary directory
    let tmp_dir = TempDir::new().expect("Failed to make temp dir");

    // Copy recursively from the test data directory to the temporary directory
    let source_path = format!("../test/tests/data/{table_name}");
    let options = fs_extra::dir::CopyOptions {
        content_only: true,
        ..Default::default()
    };
    println!("copying from {source_path}");
    fs_extra::dir::copy(source_path, tmp_dir.path(), &options).unwrap();
    tmp_dir
}

#[tokio::test]
#[serial]
// This test requires refactoring and a revisit
#[ignore]
async fn cleanup_metadata_fs_test() -> TestResult {
    let storage = Box::new(LocalStorageIntegration::default());
    let context = IntegrationContext::new(storage)?;
    cleanup_metadata_test(&context).await?;
    Ok(())
}

// Last-Modified for S3 could not be altered by user, hence using system pauses which makes
// test to run longer but reliable
async fn cleanup_metadata_test(context: &IntegrationContext) -> TestResult {
    let table_uri = context.root_uri();
    let table_url = deltalake_core::table::builder::parse_table_uri(table_uri).unwrap();
    let log_store = DeltaTableBuilder::from_url(table_url)?
        .with_allow_http(true)
        .build_storage()?;
    let object_store = log_store.object_store(None);

    let log_path = |version| log_store.log_path().child(format!("{version:020}.json"));

    // we don't need to actually populate files with content as cleanup works only with file's metadata
    object_store
        .put(&log_path(0), bytes::Bytes::from("foo").into())
        .await?;

    // since we cannot alter s3 object metadata, we mimic it with pauses
    // also we forced to use 2 seconds since Last-Modified is stored in seconds
    std::thread::sleep(Duration::from_secs(2));
    object_store
        .put(&log_path(1), bytes::Bytes::from("foo").into())
        .await?;

    std::thread::sleep(Duration::from_secs(3));
    object_store
        .put(&log_path(2), bytes::Bytes::from("foo").into())
        .await?;

    let v0time = object_store.head(&log_path(0)).await?.last_modified;
    let v1time = object_store.head(&log_path(1)).await?.last_modified;
    let v2time = object_store.head(&log_path(2)).await?.last_modified;

    // we choose the retention timestamp to be between v1 and v2 so v2 will be kept but other removed.
    let retention_timestamp =
        v1time.timestamp_millis() + (v2time.timestamp_millis() - v1time.timestamp_millis()) / 2;

    assert!(retention_timestamp > v0time.timestamp_millis());
    assert!(retention_timestamp > v1time.timestamp_millis());
    assert!(retention_timestamp < v2time.timestamp_millis());

    let removed =
        cleanup_expired_logs_for(3, log_store.as_ref(), retention_timestamp, None).await?;

    assert_eq!(removed, 2);
    assert!(object_store.head(&log_path(0)).await.is_err());
    assert!(object_store.head(&log_path(1)).await.is_err());
    assert!(object_store.head(&log_path(2)).await.is_ok());

    // after test cleanup
    object_store.delete(&log_path(2)).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_issue_1420_cleanup_expired_logs_for() -> DeltaResult<()> {
    let _ = std::fs::remove_dir_all("./tests/data/issue_1420");

    // Create the directory and get absolute path
    std::fs::create_dir_all("./tests/data/issue_1420").unwrap();
    let path = std::path::Path::new("./tests/data/issue_1420")
        .canonicalize()
        .unwrap();
    let mut table = DeltaTable::try_from_url(url::Url::from_directory_path(path).unwrap())
        .await?
        .create()
        .with_column(
            "id",
            DataType::Primitive(PrimitiveType::Integer),
            false,
            None,
        )
        .await?;

    let mut writer = JsonWriter::for_table(&table)?;
    writer.write(vec![json!({"id": 1})]).await?;
    writer.flush_and_commit(&mut table).await?; // v1

    writer.write(vec![json!({"id": 2})]).await?;
    writer.flush_and_commit(&mut table).await?; // v2
    assert_eq!(table.version(), Some(2));

    create_checkpoint(&table, None).await.unwrap(); // v2.checkpoint.parquet

    sleep(Duration::from_secs(1)).await;
    let ts = Utc::now(); // use this ts for log retention expiry

    // Should delete v1 but not v2 or v2.checkpoint.parquet
    cleanup_expired_logs_for(
        table.version().unwrap(),
        table.log_store().as_ref(),
        ts.timestamp_millis(),
        None,
    )
    .await?;

    assert!(
        table
            .log_store()
            .object_store(None)
            .head(&Path::from(format!("_delta_log/{:020}.json", 1)))
            .await
            .is_err(),
        "commit should not exist"
    );

    assert!(
        table
            .log_store()
            .object_store(None)
            .head(&Path::from(format!("_delta_log/{:020}.json", 2)))
            .await
            .is_ok(),
        "commit should exist"
    );

    assert!(
        table
            .log_store()
            .object_store(None)
            .head(&Path::from(format!(
                "_delta_log/{:020}.checkpoint.parquet",
                2
            )))
            .await
            .is_ok(),
        "checkpoint should exist"
    );

    // pretend time advanced but there is no new versions after v2
    // v2 and v2.checkpoint.parquet should still be there
    let ts = Utc::now();
    sleep(Duration::from_secs(1)).await;

    cleanup_expired_logs_for(
        table.version().unwrap(),
        table.log_store().as_ref(),
        ts.timestamp_millis(),
        None,
    )
    .await?;

    assert!(
        table
            .log_store()
            .object_store(None)
            .head(&Path::from(format!("_delta_log/{:020}.json", 2)))
            .await
            .is_ok(),
        "commit should exist"
    );

    assert!(
        table
            .log_store()
            .object_store(None)
            .head(&Path::from(format!(
                "_delta_log/{:020}.checkpoint.parquet",
                2
            )))
            .await
            .is_ok(),
        "checkpoint should exist"
    );

    Ok(())
}

#[tokio::test]
/// This test validates a checkpoint can be updated on a pre deltalake (python) 1.x table
/// see also: <https://github.com/delta-io/delta-rs/issues/3527>
async fn test_older_checkpoint_reads() -> DeltaResult<()> {
    let temp_table = clone_table("python-0.25.5-checkpoint");
    let table_path = temp_table.path().to_str().unwrap();
    let table_url = ensure_table_uri(table_path).unwrap();
    let table = deltalake_core::open_table(table_url).await?;
    assert_eq!(table.version(), Some(1));
    create_checkpoint(&table, None).await?;
    Ok(())
}

#[tokio::test]
/// This test validates that we can read a table with v2 checkpoints
async fn test_v2_checkpoint_json() -> DeltaResult<()> {
    let temp_table = clone_table("checkpoint-v2-table");
    let table_path = temp_table.path().to_str().unwrap();
    let table_url = ensure_table_uri(table_path).unwrap();
    let table = deltalake_core::open_table(table_url).await?;
    assert_eq!(table.version(), Some(9));
    create_checkpoint(&table, None).await?;
    Ok(())
}

#[tokio::test]
/// This test that we can read a table with domain metadata. Since we cannot
/// write domain metadata atm, we can at least test, that accessing restricted
/// domain metadata in the table fails with a proper error.
async fn test_checkpoint_with_domain_meta() -> DeltaResult<()> {
    let temp_table = clone_table("table-with-domain-metadata");
    let table_path = temp_table.path().to_str().unwrap();
    let table =
        deltalake_core::open_table(Url::parse(&format!("file://{table_path}")).unwrap()).await?;
    assert_eq!(table.version(), Some(108));
    let metadata = table
        .snapshot()
        .unwrap()
        .snapshot()
        .domain_metadata(&table.log_store(), "delta.clustering")
        .await;
    assert!(
        metadata.unwrap_err().to_string().contains(
            "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain"
        )
    );
    Ok(())
}
