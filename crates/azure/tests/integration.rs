#![cfg(feature = "integration_test")]

use bytes::Bytes;
use deltalake_core::DeltaTableBuilder;
use deltalake_test::read::read_table_paths;
use deltalake_test::{test_concurrent_writes, test_read_tables, IntegrationContext, TestResult};
use object_store::path::Path;
use serial_test::serial;

mod context;
use context::*;

static TEST_PREFIXES: &[&str] = &["my table", "你好/😊"];
/// TEST_PREFIXES as they should appear in object stores.
static TEST_PREFIXES_ENCODED: &[&str] = &["my table", "%E4%BD%A0%E5%A5%BD/%F0%9F%98%8A"];

#[tokio::test]
#[serial]
async fn test_read_tables_azure() -> TestResult {
    let context = IntegrationContext::new(Box::new(MsftIntegration::default()))?;

    test_read_tables(&context).await?;

    for (prefix, _prefix_encoded) in TEST_PREFIXES.iter().zip(TEST_PREFIXES_ENCODED.iter()) {
        read_table_paths(&context, prefix, prefix).await?;
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

// NOTE: This test is ignored based on [this
// comment](https://github.com/delta-io/delta-rs/pull/1564#issuecomment-1721048753) and we should
// figure out a way to re-enable this test at least in the GitHub Actions CI environment
#[ignore]
#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_object_store_onelake() -> TestResult {
    let path = Path::from("17d3977c-d46e-4bae-8fed-ff467e674aed/Files/SampleCustomerList.csv");
    let context = IntegrationContext::new(Box::new(MsftIntegration::Onelake))?;
    read_write_test_onelake(&context, &path).await?;
    Ok(())
}

// NOTE: This test is ignored based on [this
// comment](https://github.com/delta-io/delta-rs/pull/1564#issuecomment-1721048753) and we should
// figure out a way to re-enable this test at least in the GitHub Actions CI environment
#[ignore]
#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_object_store_onelake_abfs() -> TestResult {
    let path = Path::from("17d3977c-d46e-4bae-8fed-ff467e674aed/Files/SampleCustomerList.csv");
    let context = IntegrationContext::new(Box::new(MsftIntegration::OnelakeAbfs))?;
    read_write_test_onelake(&context, &path).await?;
    Ok(())
}

#[allow(dead_code)]
async fn read_write_test_onelake(context: &IntegrationContext, path: &Path) -> TestResult {
    let delta_store = DeltaTableBuilder::from_uri(&context.root_uri())
        .with_allow_http(true)
        .build_storage()?
        .object_store();

    let expected = Bytes::from_static(b"test world from delta-rs on friday");

    delta_store
        .put(path, expected.clone().into())
        .await
        .unwrap();
    let fetched = delta_store.get(path).await.unwrap().bytes().await.unwrap();
    assert_eq!(expected, fetched);

    for range in [0..10, 3..5, 0..expected.len()] {
        let data = delta_store.get_range(path, range.clone()).await.unwrap();
        assert_eq!(&data[..], &expected[range])
    }

    Ok(())
}
