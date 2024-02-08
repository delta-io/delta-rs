use deltalake_test::read::read_table_paths;
use deltalake_test::utils::*;
use deltalake_test::{test_concurrent_writes, test_read_tables};
use object_store::path::Path;
use serial_test::serial;

#[allow(dead_code)]
mod fs_common;

static TEST_PREFIXES: &[&str] = &["my table", "你好/😊"];

#[tokio::test]
#[serial]
async fn test_integration_local() -> TestResult {
    let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;

    test_read_tables(&context).await?;

    for prefix in TEST_PREFIXES {
        read_table_paths(&context, prefix, prefix).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_concurrency_local() -> TestResult {
    let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;

    test_concurrent_writes(&context).await?;

    Ok(())
}

#[tokio::test]
async fn test_action_reconciliation() {
    let path = "./tests/data/action_reconciliation";
    let mut table = fs_common::create_table(path, None).await;

    // Add a file.
    let a = fs_common::add(3 * 60 * 1000);
    assert_eq!(1, fs_common::commit_add(&mut table, &a).await);
    assert_eq!(
        table.get_files_iter().unwrap().collect::<Vec<_>>(),
        vec![Path::from(a.path.clone())]
    );

    // Remove added file.
    let r = deltalake_core::kernel::Remove {
        path: a.path.clone(),
        deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        data_change: false,
        extended_file_metadata: None,
        partition_values: None,
        size: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
    };

    assert_eq!(2, fs_common::commit_removes(&mut table, vec![&r]).await);
    assert_eq!(table.get_files_iter().unwrap().count(), 0);
    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .all_tombstones(table.object_store().clone())
            .await
            .unwrap()
            .map(|r| r.path.clone())
            .collect::<Vec<_>>(),
        vec![a.path.clone()]
    );
}
