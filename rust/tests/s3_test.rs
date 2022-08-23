#![cfg(feature = "integration_test")]
#![cfg(feature = "s3")]
mod s3_common;

use crate::s3_common::setup;
use bytes::Bytes;
use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use deltalake::DeltaTableBuilder;
use deltalake::ObjectStoreError;
use dynamodb_lock::dynamo_lock_options;
use maplit::hashmap;
use object_store::path::Path;
use serial_test::serial;

#[cfg(feature = "azure")]
#[serial]
async fn test_read_tables_azure() -> TestResult {
    Ok(read_tables(StorageIntegration::Microsoft).await?)
}

#[cfg(feature = "s3")]
#[tokio::test]
#[serial]
async fn test_read_tables_aws() -> TestResult {
    Ok(read_tables(StorageIntegration::Amazon).await?)
}

async fn read_tables(storage: StorageIntegration) -> TestResult {
    let context =
        IntegrationContext::new_with_tables(storage, [TestTables::Simple, TestTables::Golden])?;

    read_simple_table(&context).await?;
    read_simple_table_with_version(&context).await?;
    read_golden(&context).await?;

    Ok(())
}

async fn read_simple_table(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Simple);
    // the s3 options don't hurt us for other integrations ...
    let table = DeltaTableBuilder::from_uri(table_uri).with_allow_http(true).with_storage_options(hashmap! {
        dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME.to_string() => "s3::deltars/simple".to_string(),
    }).load().await?;

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

#[tokio::test]
#[serial]
async fn test_s3_head_obj() {
    setup();

    let key = "s3://deltars/";
    let backend = DeltaTableBuilder::from_uri(key)
        .with_allow_http(true)
        .build_storage()
        .unwrap()
        .storage_backend();
    let err = backend.head(&Path::from("missing")).await.err().unwrap();

    assert!(matches!(err, ObjectStoreError::NotFound { .. }));

    let path = Path::from("head_test");
    let data = Bytes::from("Hello world!");
    backend.put(&path, data.clone()).await.unwrap();
    let head_data = backend.head(&path).await.unwrap();
    assert_eq!(head_data.size, data.len());
    assert_eq!(head_data.location, path);
    assert!(head_data.last_modified > (chrono::offset::Utc::now() - chrono::Duration::seconds(30)));
}

#[tokio::test]
#[serial]
async fn test_s3_delete_obj() {
    setup();

    let root = "s3://deltars/";
    let path = Path::from("delete.snappy.parquet");
    let backend = DeltaTableBuilder::from_uri(root)
        .with_allow_http(true)
        .build_storage()
        .unwrap()
        .storage_backend();
    backend.put(&path, Bytes::from("")).await.unwrap();
    backend.delete(&path).await.unwrap();
    let err = backend.head(&path).await.err().unwrap();

    assert!(matches!(err, ObjectStoreError::NotFound { .. }));
}

// TODO batch delete not yet supported in object store.
#[ignore]
#[tokio::test]
#[serial]
async fn test_s3_delete_objs() {
    setup();

    let root = "s3://deltars/";
    let path1 = Path::from("delete1.snappy.parquet");
    let path2 = Path::from("delete2.snappy.parquet");
    let backend = DeltaTableBuilder::from_uri(root)
        .with_allow_http(true)
        .build_storage()
        .unwrap()
        .storage_backend();

    backend.put(&path1, Bytes::from("")).await.unwrap();
    backend.put(&path2, Bytes::from("")).await.unwrap();
    // backend
    //     .delete_batch(&[path1.to_string(), path2.to_string()])
    //     .await
    //     .unwrap();
    // let err1 = backend.head_obj(path1).await.err().unwrap();
    // let err2 = backend.head_obj(path2).await.err().unwrap();
    //
    // assert!(matches!(err1, StorageError::NotFound));
    // assert!(matches!(err2, StorageError::NotFound));
}
