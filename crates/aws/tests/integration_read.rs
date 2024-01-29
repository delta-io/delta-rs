#![cfg(feature = "integration_test")]

use deltalake_test::read::{read_table_paths, test_read_tables};
use deltalake_test::utils::*;
use serial_test::serial;

mod common;
use common::*;

static TEST_PREFIXES: &[&str] = &["my table", "你好/😊"];
/// TEST_PREFIXES as they should appear in object stores.
static TEST_PREFIXES_ENCODED: &[&str] = &["my%20table", "%E4%BD%A0%E5%A5%BD/%F0%9F%98%8A"];

#[tokio::test]
#[serial]
async fn test_read_tables_aws() -> TestResult {
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;

    test_read_tables(&context).await?;

    for (prefix, prefix_encoded) in TEST_PREFIXES.iter().zip(TEST_PREFIXES_ENCODED.iter()) {
        read_table_paths(&context, prefix, prefix_encoded).await?;
    }

    Ok(())
}
