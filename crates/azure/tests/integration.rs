#![cfg(feature = "integration_test")]

use std::assert_eq;
use std::collections::HashMap;

use bytes::Bytes;
use deltalake_core::DeltaTableBuilder;
use deltalake_core::data_catalog::storage::ListingSchemaProvider;
use deltalake_test::read::read_table_paths;
use deltalake_test::{IntegrationContext, TestResult, test_concurrent_writes, test_read_tables};
use object_store::path::Path;
use serial_test::serial;
use url::Url;

mod context;
use context::*;

static TEST_PREFIXES: &[&str] = &["my table", "你好/😊"];
/// TEST_PREFIXES as they should appear in object stores.
static TEST_PREFIXES_ENCODED: &[&str] = &["my table", "%E4%BD%A0%E5%A5%BD/%F0%9F%98%8A"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    let table_uri = Url::parse(&context.root_uri()).unwrap();
    let delta_store = DeltaTableBuilder::from_url(table_uri)
        .unwrap()
        .with_allow_http(true)
        .build_storage()?
        .object_store(None);

    let expected = Bytes::from_static(b"test world from delta-rs on friday");

    delta_store
        .put(path, expected.clone().into())
        .await
        .unwrap();
    let fetched = delta_store.get(path).await.unwrap().bytes().await.unwrap();
    assert_eq!(expected, fetched);

    for range in [0..10, 3..5, 0..expected.len()] {
        let range_u64 = range.start as u64..range.end as u64;
        let data = delta_store.get_range(path, range_u64).await.unwrap();
        assert_eq!(&data[..], &expected[range])
    }

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_delta_tables_using_listing_provider_with_missing_account_name() -> TestResult {
    let context = IntegrationContext::new(Box::new(MsftIntegration::default()))?;
    unsafe {
        // Removing the envs set by the `IntegrationContext (az_cli::prepare_env())` to illustrate the issue if e.g. account_name is not set from custom `storage_options`, but still preserving the use of the `IntegrationContext`
        std::env::remove_var("AZURE_STORAGE_USE_EMULATOR");
        std::env::remove_var("AZURE_STORAGE_ACCOUNT_NAME");
        std::env::remove_var("AZURE_STORAGE_TOKEN");
        std::env::remove_var("AZURE_STORAGE_ACCOUNT_KEY");
    }

    let storage_options = HashMap::<String, String>::new();
    if let Err(read_error) =
        ListingSchemaProvider::try_new(context.root_uri(), Some(storage_options))
    {
        assert_eq!(read_error.to_string(), "Failed to read delta log object: Generic MicrosoftAzure error: Account must be specified".to_string());
    };
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_delta_tables_using_listing_provider_with_account_name() -> TestResult {
    let context = IntegrationContext::new(Box::new(MsftIntegration::default()))?;
    unsafe {
        // Removing the envs set by the `IntegrationContext (az_cli::prepare_env())` to illustrate the issue if e.g. account_name is not set from custom `storage_options`, but still preserving the use of the `IntegrationContext`
        std::env::remove_var("AZURE_STORAGE_USE_EMULATOR");
        std::env::remove_var("AZURE_STORAGE_ACCOUNT_NAME");
        std::env::remove_var("AZURE_STORAGE_TOKEN");
        std::env::remove_var("AZURE_STORAGE_ACCOUNT_KEY");
    }

    let mut storage_options = HashMap::<String, String>::new();
    storage_options.insert("account_name".to_string(), "test_account".to_string());
    let schema = ListingSchemaProvider::try_new(context.root_uri(), Some(storage_options));
    assert!(
        schema.is_ok(),
        "Capable of reading the storage options. Fails if e.g. `account_name` is missing"
    );
    Ok(())
}
