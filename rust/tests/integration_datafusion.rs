#![cfg(all(feature = "integration_test", feature = "datafusion"))]

use arrow::array::Int64Array;
use common::datafusion::context_with_delta_table_factory;
use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use serial_test::serial;
use std::sync::Arc;

mod common;

#[tokio::test]
#[serial]
async fn test_datafusion_local() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Local).await?)
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
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

#[cfg(feature = "gcs")]
#[tokio::test]
#[serial]
async fn test_datafusion_gcp() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Google).await?)
}

#[cfg(feature = "hdfs")]
#[tokio::test]
#[serial]
async fn test_datafusion_hdfs() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Hdfs).await?)
}

async fn test_datafusion(storage: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(storage)?;
    context.load_table(TestTables::Simple).await?;

    simple_query(&context).await?;

    Ok(())
}

async fn simple_query(context: &IntegrationContext) -> TestResult {
    let table_uri = context.uri_for_table(TestTables::Simple);

    let dynamo_lock_option = "'DYNAMO_LOCK_OWNER_NAME' 's3::deltars/simple'".to_string();
    let options = match context.integration {
        StorageIntegration::Amazon => format!("'AWS_STORAGE_ALLOW_HTTP' '1', {dynamo_lock_option}"),
        StorageIntegration::Microsoft => {
            format!("'AZURE_STORAGE_ALLOW_HTTP' '1', {dynamo_lock_option}")
        }
        _ => dynamo_lock_option,
    };

    let sql = format!(
        "CREATE EXTERNAL TABLE demo \
        STORED AS DELTATABLE \
        OPTIONS ({options}) \
        LOCATION '{table_uri}'",
    );

    let ctx = context_with_delta_table_factory();
    let _ = ctx
        .sql(sql.as_str())
        .await
        .expect("Failed to register table!");

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
