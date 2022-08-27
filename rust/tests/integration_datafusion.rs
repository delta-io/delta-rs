#![cfg(all(feature = "integration_test", feature = "datafusion-ext"))]

use datafusion::arrow::array::Int64Array;
use datafusion::execution::context::SessionContext;
use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use deltalake::DeltaTableBuilder;
use maplit::hashmap;
use serial_test::serial;
use std::sync::Arc;

#[tokio::test]
#[serial]
async fn test_datafusion_local() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Local).await?)
}

#[cfg(feature = "s3")]
#[tokio::test]
#[serial]
async fn test_datafusion_aws() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Amazon).await?)
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_datafusion_azure() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Microsoft).await?)
}

async fn test_datafusion(storage: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new_with_tables(storage, [TestTables::Simple])?;

    simple_query(&context).await?;

    Ok(())
}

async fn simple_query(context: &IntegrationContext) -> TestResult {
    let table_uri = context.uri_for_table(TestTables::Simple);

    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .with_storage_options(hashmap! {
            "DYNAMO_LOCK_OWNER_NAME".to_string() => "s3::deltars/simple".to_string(),
        })
        .load()
        .await?;

    let ctx = SessionContext::new();
    ctx.register_table("demo", Arc::new(table))?;

    let batches = ctx
        .sql("SELECT id FROM demo WHERE id > 5 ORDER BY id ASC")
        .await?
        .collect()
        .await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(
        batch.column(0).as_ref(),
        Arc::new(Int64Array::from(vec![7, 9])).as_ref(),
    );

    Ok(())
}
