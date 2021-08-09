extern crate deltalake;

use deltalake::storage::file::FileStorageBackend;
use deltalake::StorageBackend;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::time::SystemTime;

#[tokio::test]
async fn read_delta_2_0_table_without_version() {
    let table = deltalake::open_table("./tests/data/delta-0.2.0")
        .await
        .unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
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
            deletion_timestamp: 1564524298213,
            data_change: false,
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
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
            "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
        ],
    );

    table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 2)
        .await
        .unwrap();
    assert_eq!(table.version, 2);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
            "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        ]
    );

    table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 3)
        .await
        .unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
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
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
            "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet"
        ]
    );
    assert_eq!(table.get_stats().len(), 2);

    assert_eq!(
        table
            .get_stats()
            .into_iter()
            .map(|x| x.unwrap().unwrap().num_records)
            .sum::<i64>(),
        4
    );

    assert_eq!(
        table
            .get_stats()
            .into_iter()
            .map(|x| x.unwrap().unwrap().null_count["value"].as_value().unwrap())
            .collect::<Vec<i64>>(),
        vec![0, 0]
    );
    let tombstones = table.get_tombstones();
    assert_eq!(tombstones.len(), 1);
    assert_eq!(
        tombstones[0],
        deltalake::action::Remove {
            path: "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet".to_string(),
            deletion_timestamp: 1615043776198,
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(HashMap::new()),
            size: Some(445),
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn read_delta_8_0_table_with_load_version() {
    let mut table = deltalake::open_table("./tests/data/delta-0.8.0")
        .await
        .unwrap();
    assert_eq!(table.version, 1);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
            "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet",
        ]
    );
    table.load_version(0).await.unwrap();
    assert_eq!(table.version, 0);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
            "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet",
        ]
    );
}

#[tokio::test]
async fn read_delta_8_0_table_with_partitions() {
    let table = deltalake::open_table("./tests/data/delta-0.8.0-partitioned")
        .await
        .unwrap();

    let filters = vec![
        deltalake::PartitionFilter {
            key: "month",
            value: deltalake::PartitionValue::Equal("2"),
        },
        deltalake::PartitionFilter {
            key: "year",
            value: deltalake::PartitionValue::Equal("2020"),
        },
    ];

    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            "year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet".to_string(),
            "year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet".to_string()
        ]
    );

    #[cfg(unix)]
    assert_eq!(
        table.get_file_uris_by_partitions(&filters).unwrap(),
        vec![
            "./tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet".to_string(),
            "./tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet".to_string()
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        table.get_file_uris_by_partitions(&filters).unwrap(),
        vec![
            "./tests/data/delta-0.8.0-partitioned\\year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet".to_string(),
            "./tests/data/delta-0.8.0-partitioned\\year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet".to_string()
        ]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::NotEqual("2"),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            "year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet".to_string(),
            "year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet".to_string(),
            "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet".to_string(),
            "year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet".to_string()
        ]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::In(vec!["2", "12"]),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            "year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet".to_string(),
            "year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet".to_string(),
            "year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet".to_string(),
            "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet".to_string()
        ]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::NotIn(vec!["2", "12"]),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            "year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet".to_string(),
            "year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet".to_string()
        ]
    );
}

#[tokio::test]
async fn read_delta_8_0_table_with_null_partition() {
    let table = deltalake::open_table("./tests/data/delta-0.8.0-null-partition")
        .await
        .unwrap();

    let filters = vec![deltalake::PartitionFilter {
        key: "k",
        value: deltalake::PartitionValue::Equal("A"),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec!["k=A/part-00000-b1f1dbbb-70bc-4970-893f-9bb772bf246e.c000.snappy.parquet".to_string()]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "k",
        value: deltalake::PartitionValue::Equal(""),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            "k=__HIVE_DEFAULT_PARTITION__/part-00001-8474ac85-360b-4f58-b3ea-23990c71b932.c000.snappy.parquet".to_string()
        ]
    );
}

#[tokio::test]
async fn vacuum_delta_8_0_table() {
    let backend = FileStorageBackend::new("");
    let mut table = deltalake::open_table(&backend.join_paths(&["tests", "data", "delta-0.8.0"]))
        .await
        .unwrap();

    let retention_hours = 1;
    let dry_run = true;

    assert!(matches!(
        table.vacuum(retention_hours, dry_run).await.unwrap_err(),
        deltalake::DeltaTableError::InvalidVacuumRetentionPeriod,
    ));

    let retention_hours = 169;

    assert_eq!(
        table.vacuum(retention_hours, dry_run).await.unwrap(),
        vec![backend.join_paths(&[
            "tests",
            "data",
            "delta-0.8.0",
            "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet",
        ])]
    );

    let retention_hours = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 3600;
    let empty: Vec<String> = Vec::new();

    assert_eq!(table.vacuum(retention_hours, dry_run).await.unwrap(), empty);
}
