use chrono::Utc;
use deltalake_core::checkpoints::{cleanup_expired_logs_for, create_checkpoint};
use deltalake_core::kernel::{DataType, PrimitiveType};
use deltalake_core::writer::{DeltaWriter, JsonWriter};
use deltalake_core::{errors::DeltaResult, DeltaOps, DeltaTableBuilder, ObjectStore};
use deltalake_test::utils::*;
use object_store::path::Path;
use serde_json::json;
use serial_test::serial;
use std::time::Duration;
use tokio::time::sleep;

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
    let log_store = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .build_storage()?;
    let object_store = log_store.object_store();

    let log_path = |version| log_store.log_path().child(format!("{:020}.json", version));

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

    let removed = cleanup_expired_logs_for(3, log_store.as_ref(), retention_timestamp).await?;

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

    let mut table = DeltaOps::try_from_uri("./tests/data/issue_1420")
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

    let ts = Utc::now(); // use this ts for log retention expiry
    sleep(Duration::from_secs(1)).await;

    writer.write(vec![json!({"id": 2})]).await?;
    writer.flush_and_commit(&mut table).await?; // v2
    assert_eq!(table.version(), 2);

    create_checkpoint(&table).await.unwrap(); // v2.checkpoint.parquet

    // Should delete v1 but not v2 or v2.checkpoint.parquet
    cleanup_expired_logs_for(
        table.version(),
        table.log_store().as_ref(),
        ts.timestamp_millis(),
    )
    .await?;

    assert!(
        table
            .object_store()
            .head(&Path::from(format!("_delta_log/{:020}.json", 1)))
            .await
            .is_err(),
        "commit should not exist"
    );

    assert!(
        table
            .object_store()
            .head(&Path::from(format!("_delta_log/{:020}.json", 2)))
            .await
            .is_ok(),
        "commit should exist"
    );

    assert!(
        table
            .object_store()
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
        table.version(),
        table.log_store().as_ref(),
        ts.timestamp_millis(),
    )
    .await?;

    assert!(
        table
            .object_store()
            .head(&Path::from(format!("_delta_log/{:020}.json", 2)))
            .await
            .is_ok(),
        "commit should exist"
    );

    assert!(
        table
            .object_store()
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
