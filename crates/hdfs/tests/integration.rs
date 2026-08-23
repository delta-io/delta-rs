#![cfg(feature = "integration_test")]
use deltalake_test::{IntegrationContext, TestResult, test_read_tables};
use serial_test::serial;

mod context;
use context::*;

#[tokio::test]
#[serial]
async fn test_read_tables_hdfs() -> TestResult {
    let context = IntegrationContext::new(Box::<HdfsIntegration>::default())?;

    test_read_tables(&context).await?;

    Ok(())
}
