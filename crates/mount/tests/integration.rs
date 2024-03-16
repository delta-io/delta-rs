#![cfg(feature = "integration_test")]

use deltalake_test::read::read_table_paths;
use deltalake_test::{test_read_tables, IntegrationContext, TestResult};
use serial_test::serial;

mod context;
use context::*;

static TEST_PREFIXES: &[&str] = &["my table", "你好/😊"];

#[tokio::test]
#[serial]
async fn test_integration_local() -> TestResult {
    let context = IntegrationContext::new(Box::<MountIntegration>::default())?;

    test_read_tables(&context).await?;

    for prefix in TEST_PREFIXES {
        read_table_paths(&context, prefix, prefix).await?;
    }

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "The DBFS tests currently hang due to CI pipeline cannot write to /dbfs"]
async fn test_integration_dbfs() -> TestResult {
    let context = IntegrationContext::new(Box::<DbfsIntegration>::default())?;

    test_read_tables(&context).await?;

    for prefix in TEST_PREFIXES {
        read_table_paths(&context, prefix, prefix).await?;
    }

    Ok(())
}
