#![cfg(all(feature = "integration_test"))]

use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use deltalake::DeltaOps;
use deltalake::Path;
use serial_test::serial;

mod common;

#[tokio::test]
#[serial]
async fn test_filesystem_check_local() -> TestResult {
    Ok(test_filesystem_check(StorageIntegration::Local).await?)
}

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
#[tokio::test]
#[serial]
async fn test_filesystem_check_aws() -> TestResult {
    Ok(test_filesystem_check(StorageIntegration::Amazon).await?)
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_filesystem_check_azure() -> TestResult {
    Ok(test_filesystem_check(StorageIntegration::Microsoft).await?)
}

#[cfg(feature = "gcs")]
#[tokio::test]
#[serial]
async fn test_filesystem_check_gcp() -> TestResult {
    Ok(test_filesystem_check(StorageIntegration::Google).await?)
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
    assert_eq!(remove.data_change, true);

    // An additonal run should return an empty list of orphaned actions
    let op = DeltaOps::from(table);
    let (table, metrics) = op.filesystem_check().await?;
    assert_eq!(version + 1, table.state.version());
    assert_eq!(active - 1, table.state.files().len());
    assert!(metrics.files_removed.is_empty());

    Ok(())
}
