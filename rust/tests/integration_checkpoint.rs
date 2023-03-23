#![cfg(feature = "integration_test")]

use deltalake::checkpoints::cleanup_expired_logs_for;
use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult};
use deltalake::{DeltaTableBuilder, ObjectStore};
use serial_test::serial;
use std::time::Duration;

#[tokio::test]
async fn cleanup_metadata_fs_test() -> TestResult {
    let context = IntegrationContext::new(StorageIntegration::Local)?;
    cleanup_metadata_test(&context).await?;
    Ok(())
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
#[tokio::test]
#[serial]
async fn cleanup_metadata_aws_test() -> TestResult {
    let context = IntegrationContext::new(StorageIntegration::Amazon)?;
    cleanup_metadata_test(&context).await?;
    Ok(())
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn cleanup_metadata_azure_test() -> TestResult {
    let context = IntegrationContext::new(StorageIntegration::Microsoft)?;
    cleanup_metadata_test(&context).await?;
    Ok(())
}

#[cfg(feature = "gcs")]
#[tokio::test]
#[serial]
async fn cleanup_metadata_gcp_test() -> TestResult {
    let context = IntegrationContext::new(StorageIntegration::Google)?;
    cleanup_metadata_test(&context).await?;
    Ok(())
}

#[cfg(feature = "hdfs")]
#[tokio::test]
#[serial]
async fn cleanup_metadata_hdfs_test() -> TestResult {
    let context = IntegrationContext::new(StorageIntegration::Hdfs)?;
    cleanup_metadata_test(&context).await?;
    Ok(())
}

// Last-Modified for S3 could not be altered by user, hence using system pauses which makes
// test to run longer but reliable
async fn cleanup_metadata_test(context: &IntegrationContext) -> TestResult {
    let table_uri = context.root_uri();
    let object_store = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .build_storage()?;

    let log_path = |version| {
        object_store
            .log_path()
            .child(format!("{:020}.json", version))
    };

    // we don't need to actually populate files with content as cleanup works only with file's metadata
    object_store
        .put(&log_path(0), bytes::Bytes::from("foo"))
        .await?;

    // since we cannot alter s3 object metadata, we mimic it with pauses
    // also we forced to use 2 seconds since Last-Modified is stored in seconds
    std::thread::sleep(Duration::from_secs(2));
    object_store
        .put(&log_path(1), bytes::Bytes::from("foo"))
        .await?;

    std::thread::sleep(Duration::from_secs(3));
    object_store
        .put(&log_path(2), bytes::Bytes::from("foo"))
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

    let removed = cleanup_expired_logs_for(3, object_store.as_ref(), retention_timestamp).await?;

    assert_eq!(removed, 2);
    assert!(object_store.head(&log_path(0)).await.is_err());
    assert!(object_store.head(&log_path(1)).await.is_err());
    assert!(object_store.head(&log_path(2)).await.is_ok());

    // after test cleanup
    object_store.delete(&log_path(2)).await.unwrap();

    Ok(())
}
