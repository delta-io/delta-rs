extern crate deltalake;

use pretty_assertions::assert_eq;
use std::collections::HashMap;

#[tokio::test]
async fn read_delta_2_0_table_without_version() {
    let table = deltalake::open_table("./tests/data/delta-0.2.0")
        .await
        .unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet",
            "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
            "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        ]
    );
    let tombstones = table.get_tombstones();
    assert_eq!(tombstones.len(), 4);
    assert_eq!(
        tombstones[0],
        deltalake::action::Remove {
            path: "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet".to_string(),
            deletionTimestamp: 1564524298213,
            dataChange: false,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn read_delta_table_with_update() {
    let path = "./tests/data/simple_table_with_checkpoint/";
    let table_newest_version = deltalake::open_table(path).await.unwrap();
    let mut table_to_update = deltalake::open_table_with_version(path, 0).await.unwrap();
    table_to_update.update().await.unwrap();

    assert_eq!(
        table_newest_version.get_files(),
        table_to_update.get_files()
    );
}

#[tokio::test]
async fn read_delta_2_0_table_with_version() {
    let mut table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 0)
        .await
        .unwrap();
    assert_eq!(table.version, 0);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
            "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
        ],
    );

    table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 2)
        .await
        .unwrap();
    assert_eq!(table.version, 2);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
            "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        ]
    );

    table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 3)
        .await
        .unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet",
            "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
            "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        ]
    );
}

#[tokio::test]
async fn read_delta_8_0_table_without_version() {
    let table = deltalake::open_table("./tests/data/delta-0.8.0")
        .await
        .unwrap();
    assert_eq!(table.version, 1);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
            "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet"
        ]
    );
    let tombstones = table.get_tombstones();
    assert_eq!(tombstones.len(), 1);
    assert_eq!(
        tombstones[0],
        deltalake::action::Remove {
            path: "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet".to_string(),
            deletionTimestamp: 1615043776198,
            dataChange: true,
            extendedFileMetadata: Some(true),
            partitionValues: Some(HashMap::new()),
            size: Some(445),
            ..Default::default()
        }
    );
}
