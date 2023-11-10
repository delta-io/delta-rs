#![cfg(feature = "integration_test")]

use deltalake_core::test_utils::{
    set_env_if_not_set, IntegrationContext, StorageIntegration, TestResult, TestTables,
};
use deltalake_core::Path;
use deltalake_core::{errors::DeltaTableError, DeltaOps};
use serial_test::serial;

mod common;

#[tokio::test]
#[serial]
async fn test_filesystem_check_local() -> TestResult {
    test_filesystem_check(StorageIntegration::Local).await
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
#[tokio::test]
#[serial]
async fn test_filesystem_check_aws() -> TestResult {
    set_env_if_not_set("AWS_S3_ALLOW_UNSAFE_RENAME", "true");
    set_env_if_not_set("AWS_S3_LOCKING_PROVIDER", "none");
    test_filesystem_check(StorageIntegration::Amazon).await
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_filesystem_check_azure() -> TestResult {
    test_filesystem_check(StorageIntegration::Microsoft).await
}

#[cfg(feature = "gcs")]
#[tokio::test]
#[serial]
async fn test_filesystem_check_gcp() -> TestResult {
    test_filesystem_check(StorageIntegration::Google).await
}

#[cfg(feature = "hdfs")]
#[tokio::test]
#[serial]
async fn test_filesystem_check_hdfs() -> TestResult {
    Ok(test_filesystem_check(StorageIntegration::Hdfs).await?)
}

async fn test_filesystem_check(storage: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(storage)?;
    context.load_table(TestTables::Simple).await?;
    let file = "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet";
    let path = Path::from_iter([&TestTables::Simple.as_name(), file]);

    // Delete an active file from underlying storage without an update to the log to simulate an external fault
    context.object_store().delete(&path).await?;

    let table = context.table_builder(TestTables::Simple).load().await?;
    let version = table.state.version();
    let active = table.state.files().len();

    // Validate a Dry run does not mutate the table log and indentifies orphaned add actions
    let op = DeltaOps::from(table);
    let (table, metrics) = op.filesystem_check().with_dry_run(true).await?;
    assert_eq!(version, table.state.version());
    assert_eq!(active, table.state.files().len());
    assert_eq!(vec![file.to_string()], metrics.files_removed);

    // Validate a run updates the table version with proper remove actions
    let op = DeltaOps::from(table);
    let (table, metrics) = op.filesystem_check().await?;
    assert_eq!(version + 1, table.state.version());
    assert_eq!(active - 1, table.state.files().len());
    assert_eq!(vec![file.to_string()], metrics.files_removed);

    let remove = table.state.all_tombstones().get(file).unwrap();
    assert!(remove.data_change);

    // An additional run should return an empty list of orphaned actions
    let op = DeltaOps::from(table);
    let (table, metrics) = op.filesystem_check().await?;
    assert_eq!(version + 1, table.state.version());
    assert_eq!(active - 1, table.state.files().len());
    assert!(metrics.files_removed.is_empty());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_filesystem_check_partitioned() -> TestResult {
    let storage = StorageIntegration::Local;
    let context = IntegrationContext::new(storage)?;
    context
        .load_table(TestTables::Delta0_8_0Partitioned)
        .await?;
    let file = "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet";
    let path = Path::parse(TestTables::Delta0_8_0Partitioned.as_name() + "/" + file).unwrap();

    // Delete an active file from underlying storage without an update to the log to simulate an external fault
    context.object_store().delete(&path).await?;

    let table = context
        .table_builder(TestTables::Delta0_8_0Partitioned)
        .load()
        .await?;
    let version = table.state.version();
    let active = table.state.files().len();

    // Validate a run updates the table version with proper remove actions
    let op = DeltaOps::from(table);
    let (table, metrics) = op.filesystem_check().await?;
    assert_eq!(version + 1, table.state.version());
    assert_eq!(active - 1, table.state.files().len());
    assert_eq!(vec![file.to_string()], metrics.files_removed);

    let remove = table.state.all_tombstones().get(file).unwrap();
    assert!(remove.data_change);
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_filesystem_check_fails_for_concurrent_delete() -> TestResult {
    // Validate failure when a non dry only executes on the latest version
    let context = IntegrationContext::new(StorageIntegration::Local)?;
    context.load_table(TestTables::Simple).await?;
    let file = "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet";
    let path = Path::from_iter([&TestTables::Simple.as_name(), file]);

    // Delete an active file from underlying storage without an update to the log to simulate an external fault
    context.object_store().delete(&path).await?;

    let table = context
        .table_builder(TestTables::Simple)
        .with_version(2)
        .load()
        .await?;

    let op = DeltaOps::from(table);
    let res = op.filesystem_check().with_dry_run(false).await;

    // TODO check more specific error
    assert!(matches!(res, Err(DeltaTableError::Transaction { .. })));

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "should this actually fail? with conflict resolution, we are re-trying again."]
async fn test_filesystem_check_outdated() -> TestResult {
    // Validate failure when a non dry only executes on the latest version
    let context = IntegrationContext::new(StorageIntegration::Local)?;
    context.load_table(TestTables::Simple).await?;
    let file = "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet";
    let path = Path::from_iter([&TestTables::Simple.as_name(), file]);

    // Delete an active file from underlying storage without an update to the log to simulate an external fault
    context.object_store().delete(&path).await?;

    let table = context
        .table_builder(TestTables::Simple)
        .with_version(2)
        .load()
        .await?;

    let op = DeltaOps::from(table);
    let res = op.filesystem_check().with_dry_run(false).await;
    println!("{:?}", res);
    if let Err(DeltaTableError::VersionAlreadyExists(version)) = res {
        assert!(version == 3);
    } else {
        panic!();
    }

    Ok(())
}
