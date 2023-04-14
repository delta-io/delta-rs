#![cfg(feature = "integration_test")]

use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use deltalake::DeltaTableBuilder;
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use dynamodb_lock::dynamo_lock_options;
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use maplit::hashmap;
use object_store::path::Path;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_read_tables_local() -> TestResult {
    Ok(read_tables(StorageIntegration::Local).await?)
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_read_tables_azure() -> TestResult {
    Ok(read_tables(StorageIntegration::Microsoft).await?)
}

#[cfg(feature = "hdfs")]
#[tokio::test]
#[serial]
async fn test_read_tables_hdfs() -> TestResult {
    Ok(read_tables(StorageIntegration::Hdfs).await?)
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
#[tokio::test]
#[serial]
async fn test_read_tables_aws() -> TestResult {
    Ok(read_tables(StorageIntegration::Amazon).await?)
}

async fn read_tables(storage: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(storage)?;
    context.load_table(TestTables::Simple).await?;
    context.load_table(TestTables::Golden).await?;

    read_simple_table(&context).await?;
    read_simple_table_with_version(&context).await?;
    read_golden(&context).await?;

    Ok(())
}

async fn read_simple_table(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Simple);
    // the s3 options don't hurt us for other integrations ...
    #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
    let table = DeltaTableBuilder::from_uri(table_uri).with_allow_http(true).with_storage_options(hashmap! {
        dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME.to_string() => "s3::deltars/simple".to_string(),
    }).load().await?;
    #[cfg(not(any(feature = "s3", feature = "s3-native-tls")))]
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .load()
        .await?;

    assert_eq!(table.version(), 4);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet"),
            Path::from("part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet"),
            Path::from("part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet"),
            Path::from("part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet"),
            Path::from("part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet"),
        ]
    );
    let tombstones = table.get_state().all_tombstones();
    assert_eq!(tombstones.len(), 31);
    assert!(tombstones.contains(&deltalake::action::Remove {
        path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
        deletion_timestamp: Some(1587968596250),
        data_change: true,
        extended_file_metadata: None,
        ..Default::default()
    }));

    Ok(())
}

async fn read_simple_table_with_version(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Simple);

    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .with_version(3)
        .load()
        .await?;

    assert_eq!(table.version(), 3);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet"),
            Path::from("part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet"),
            Path::from("part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet"),
            Path::from("part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet"),
            Path::from("part-00000-f17fcbf5-e0dc-40ba-adae-ce66d1fcaef6-c000.snappy.parquet"),
            Path::from("part-00001-bb70d2ba-c196-4df2-9c85-f34969ad3aa9-c000.snappy.parquet"),
        ]
    );
    let tombstones = table.get_state().all_tombstones();
    assert_eq!(tombstones.len(), 29);
    assert!(tombstones.contains(&deltalake::action::Remove {
        path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
        deletion_timestamp: Some(1587968596250),
        data_change: true,
        ..Default::default()
    }));

    Ok(())
}

async fn read_golden(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Golden);

    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .load()
        .await
        .unwrap();

    assert_eq!(table.version(), 0);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);

    Ok(())
}

// TODO we keep teh gcs test around until we can also integrate with CI
#[cfg(feature = "gcs")]
mod gcs {
    use object_store::path::Path;
    /*
     * The storage account to run this test must be provided by the developer and test are executed locally.
     *
     * To prepare test execution, create a gcs bucket and upload the contents of ./rust/tests/data/simple_table
     * into that bucket.
     *
     * Set the environment variables used for authentication as outlined in rust/src/storage/gcs/mod.rs
     * Also set GCS_DELTA_BUCKET for the created bucket name.
     *
     * remove the ignore statement below and execute tests via 'cargo test --features gcs'
     */
    #[ignore]
    #[tokio::test]
    async fn test_gcs_simple() {
        let bucket = std::env::var("GCS_DELTA_BUCKET").unwrap();
        let table = deltalake::open_table(format!("gs://{}/simple_table", bucket).as_str())
            .await
            .unwrap();
        assert_eq!(table.version(), 4);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet"),
                Path::from("part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet"),
                Path::from("part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet"),
                Path::from("part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet"),
                Path::from("part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet"),
            ]
        );
        let tombstones = table.get_state().all_tombstones();
        assert_eq!(tombstones.len(), 31);
        assert!(tombstones.contains(&deltalake::action::Remove {
            path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1587968596250),
            data_change: true,
            ..Default::default()
        }));
    }
}
