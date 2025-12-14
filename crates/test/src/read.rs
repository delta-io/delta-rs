use deltalake_core::DeltaTableBuilder;
use futures::TryStreamExt as _;
use object_store::path::Path;

use crate::utils::{IntegrationContext, TestResult, TestTables};

pub async fn test_read_tables(context: &IntegrationContext) -> TestResult {
    context.load_table(TestTables::Simple).await?;
    context.load_table(TestTables::Golden).await?;
    context
        .load_table(TestTables::Delta0_8_0SpecialPartitioned)
        .await?;

    read_simple_table(context).await?;
    read_simple_table_with_version(context).await?;
    read_golden(context).await?;

    Ok(())
}

pub async fn read_table_paths(
    context: &IntegrationContext,
    table_root: &str,
    upload_path: &str,
) -> TestResult {
    context
        .load_table_with_name(TestTables::Delta0_8_0SpecialPartitioned, upload_path)
        .await?;

    verify_store(context, table_root).await?;

    read_encoded_table(context, table_root).await?;

    Ok(())
}

async fn read_simple_table(integration: &IntegrationContext) -> TestResult {
    use futures::stream::TryStreamExt;

    let table_uri = integration.uri_for_table(TestTables::Simple);
    let table_url = url::Url::parse(&table_uri)?;
    let table = DeltaTableBuilder::from_url(table_url)?
        .with_allow_http(true)
        .load()
        .await?;
    let snapshot = table.snapshot()?;
    assert_eq!(snapshot.version(), 4);
    assert_eq!(snapshot.protocol().min_writer_version(), 2);
    assert_eq!(snapshot.protocol().min_reader_version(), 1);
    assert_eq!(
        snapshot
            .snapshot()
            .file_views(&table.log_store(), None)
            .try_collect::<Vec<_>>()
            .await?
            .iter()
            .map(|lfv| lfv.path().to_string())
            .collect::<Vec<_>>(),
        vec![
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
        ]
    );
    let tombstones = snapshot
        .all_tombstones(&table.log_store())
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(tombstones.len(), 31);
    let paths = tombstones
        .iter()
        .map(|tombstone| tombstone.path().to_string())
        .collect::<Vec<_>>();
    assert!(paths.contains(
        &"part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string()
    ));
    let deletion_ts = tombstones
        .iter()
        .map(|tombstone| tombstone.deletion_timestamp())
        .collect::<Vec<_>>();
    assert!(deletion_ts.contains(&Some(1587968596250)));

    Ok(())
}

async fn read_simple_table_with_version(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Simple);
    let table_url = url::Url::parse(&table_uri)?;

    let table = DeltaTableBuilder::from_url(table_url)?
        .with_allow_http(true)
        .with_version(3)
        .load()
        .await?;
    let snapshot = table.snapshot()?;
    assert_eq!(snapshot.version(), 3);
    assert_eq!(snapshot.protocol().min_writer_version(), 2);
    assert_eq!(snapshot.protocol().min_reader_version(), 1);
    assert_eq!(
        snapshot
            .snapshot()
            .file_views(&table.log_store(), None)
            .try_collect::<Vec<_>>()
            .await?
            .iter()
            .map(|lfv| lfv.path().to_string())
            .collect::<Vec<_>>(),
        vec![
            "part-00000-f17fcbf5-e0dc-40ba-adae-ce66d1fcaef6-c000.snappy.parquet",
            "part-00001-bb70d2ba-c196-4df2-9c85-f34969ad3aa9-c000.snappy.parquet",
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
        ]
    );
    let tombstones = snapshot
        .all_tombstones(&table.log_store())
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(tombstones.len(), 29);
    let paths = tombstones
        .iter()
        .map(|tombstone| tombstone.path().to_string())
        .collect::<Vec<_>>();
    assert!(paths.contains(
        &"part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string()
    ));
    let deletion_ts = tombstones
        .iter()
        .map(|tombstone| tombstone.deletion_timestamp())
        .collect::<Vec<_>>();
    assert!(deletion_ts.contains(&Some(1587968596250)));

    Ok(())
}

pub async fn read_golden(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Golden);
    let table_url = url::Url::parse(&table_uri)?;

    let table = DeltaTableBuilder::from_url(table_url)?
        .with_allow_http(true)
        .load()
        .await
        .unwrap();
    let snapshot = table.snapshot()?;
    assert_eq!(snapshot.version(), 0);
    assert_eq!(snapshot.protocol().min_writer_version(), 2);
    assert_eq!(snapshot.protocol().min_reader_version(), 1);

    Ok(())
}

async fn verify_store(integration: &IntegrationContext, root_path: &str) -> TestResult {
    let table_uri = format!("{}/{root_path}", integration.root_uri());
    let table_url = url::Url::parse(&table_uri)?;

    let storage = DeltaTableBuilder::from_url(table_url)?
        .with_allow_http(true)
        .build_storage()?
        .object_store(None);

    let files = storage.list_with_delimiter(None).await?;
    assert_eq!(
        vec![
            Path::parse("_delta_log").unwrap(),
            Path::parse("x=A%2FA").unwrap(),
            Path::parse("x=B%20B").unwrap(),
        ],
        files.common_prefixes
    );

    Ok(())
}

async fn read_encoded_table(integration: &IntegrationContext, root_path: &str) -> TestResult {
    let table_uri = format!("{}/{root_path}", integration.root_uri());
    let table_url = url::Url::parse(&table_uri)?;

    let table = DeltaTableBuilder::from_url(table_url)?
        .with_allow_http(true)
        .load()
        .await?;
    let snapshot = table.snapshot()?;
    assert_eq!(snapshot.version(), 0);
    assert_eq!(snapshot.log_data().num_files(), 2);

    Ok(())
}
