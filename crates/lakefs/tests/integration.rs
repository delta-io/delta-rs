#![cfg(feature = "integration_test_lakefs")]
use deltalake_test::{IntegrationContext, TestResult, test_read_tables};
use serial_test::serial;

mod context;
use context::*;
//
#[tokio::test]
#[serial]
async fn test_read_tables_lakefs() -> TestResult {
    let context = IntegrationContext::new(Box::<LakeFSIntegration>::default())?;

    test_read_tables(&context).await?;

    Ok(())
}
