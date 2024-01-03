#![cfg(feature = "integration_test")]

use deltalake_test::read::read_table_paths;
use deltalake_test::{test_concurrent_writes, test_read_tables, IntegrationContext, TestResult};
use serial_test::serial;

mod context;
use context::*;

static TEST_PREFIXES: &[&str] = &["my table", "你好/😊"];
/// TEST_PREFIXES as they should appear in object stores.
static TEST_PREFIXES_ENCODED: &[&str] = &["my%20table", "%E4%BD%A0%E5%A5%BD/%F0%9F%98%8A"];

#[tokio::test]
#[serial]
async fn test_read_tables_azure() -> TestResult {
    let context = IntegrationContext::new(Box::new(MsftIntegration::default()))?;

    test_read_tables(&context).await?;

    for (prefix, prefix_encoded) in TEST_PREFIXES.iter().zip(TEST_PREFIXES_ENCODED.iter()) {
        read_table_paths(&context, prefix, prefix_encoded).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_concurrency_azure() -> TestResult {
    let context = IntegrationContext::new(Box::new(MsftIntegration::default()))?;

    test_concurrent_writes(&context).await?;

    Ok(())
}
