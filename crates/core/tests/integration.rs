use deltalake_test::read::read_table_paths;
use deltalake_test::utils::*;
use deltalake_test::{test_concurrent_writes, test_read_tables};
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
