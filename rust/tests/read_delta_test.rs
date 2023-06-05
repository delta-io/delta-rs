use chrono::Utc;
use deltalake::DeltaTableBuilder;
use deltalake::PeekCommit;
use object_store::path::Path;
use pretty_assertions::assert_eq;
use std::collections::HashMap;

#[allow(dead_code)]
mod fs_common;

#[tokio::test]
async fn read_delta_2_0_table_without_version() {
    let table = deltalake::open_table("./tests/data/delta-0.2.0")
        .await
        .unwrap();
    assert_eq!(table.version(), 3);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet"),
            Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
            Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
        ]
    );
    let tombstones = table.get_state().all_tombstones();
    assert_eq!(tombstones.len(), 4);
    assert!(tombstones.contains(&deltalake::action::Remove {
        path: "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet".to_string(),
        deletion_timestamp: Some(1564524298213),
        data_change: false,
        extended_file_metadata: Some(false),
        ..Default::default()
    }));
}

#[tokio::test]
async fn read_delta_table_with_update() {
    let path = "./tests/data/simple_table_with_checkpoint/";
    let table_newest_version = deltalake::open_table(path).await.unwrap();
    let mut table_to_update = deltalake::open_table_with_version(path, 0).await.unwrap();
    // calling update several times should not produce any duplicates
    table_to_update.update().await.unwrap();
    table_to_update.update().await.unwrap();
    table_to_update.update().await.unwrap();

    assert_eq!(
        table_newest_version.get_files(),
        table_to_update.get_files()
    );
}

#[tokio::test]
async fn read_delta_table_ignoring_tombstones() {
    let table = DeltaTableBuilder::from_uri("./tests/data/delta-0.8.0")
        .without_tombstones()
        .load()
        .await
        .unwrap();
    assert!(
        table.get_state().all_tombstones().is_empty(),
        "loading without tombstones should skip tombstones"
    );

    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
            Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet")
        ]
    );
}

#[tokio::test]
async fn read_delta_table_ignoring_files() {
    let table = DeltaTableBuilder::from_uri("./tests/data/delta-0.8.0")
        .without_files()
        .load()
        .await
        .unwrap();

    assert!(table.get_files().is_empty(), "files should be empty");
    assert!(
        table.get_tombstones().next().is_none(),
        "tombstones should be empty"
    );
}

#[tokio::test]
async fn read_delta_table_with_ignoring_files_on_apply_log() {
    let mut table = DeltaTableBuilder::from_uri("./tests/data/delta-0.8.0")
        .with_version(0)
        .without_files()
        .load()
        .await
        .unwrap();

    assert_eq!(table.version(), 0);
    assert!(table.get_files().is_empty(), "files should be empty");
    assert!(
        table.get_tombstones().next().is_none(),
        "tombstones should be empty"
    );

    table.update().await.unwrap();
    assert_eq!(table.version(), 1);
    assert!(table.get_files().is_empty(), "files should be empty");
    assert!(
        table.get_tombstones().next().is_none(),
        "tombstones should be empty"
    );
}

#[tokio::test]
async fn read_delta_2_0_table_with_version() {
    let mut table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 0)
        .await
        .unwrap();
    assert_eq!(table.version(), 0);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet"),
            Path::from("part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet"),
        ],
    );

    table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 2)
        .await
        .unwrap();
    assert_eq!(table.version(), 2);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
            Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
        ]
    );

    table = deltalake::open_table_with_version("./tests/data/delta-0.2.0", 3)
        .await
        .unwrap();
    assert_eq!(table.version(), 3);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet"),
            Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
            Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
        ]
    );
}

#[tokio::test]
async fn read_delta_8_0_table_without_version() {
    let table = deltalake::open_table("./tests/data/delta-0.8.0")
        .await
        .unwrap();
    assert_eq!(table.version(), 1);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
            Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet")
        ]
    );
    assert_eq!(table.get_stats().count(), 2);

    assert_eq!(
        table
            .get_stats()
            .map(|x| x.unwrap().unwrap().num_records)
            .sum::<i64>(),
        4
    );

    assert_eq!(
        table
            .get_stats()
            .map(|x| x.unwrap().unwrap().null_count["value"].as_value().unwrap())
            .collect::<Vec<i64>>(),
        vec![0, 0]
    );
    let tombstones = table.get_state().all_tombstones();
    assert_eq!(tombstones.len(), 1);
    assert!(tombstones.contains(&deltalake::action::Remove {
        path: "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet".to_string(),
        deletion_timestamp: Some(1615043776198),
        data_change: true,
        extended_file_metadata: Some(true),
        partition_values: Some(HashMap::new()),
        size: Some(445),
        ..Default::default()
    }));
}

#[tokio::test]
async fn read_delta_8_0_table_with_load_version() {
    let mut table = deltalake::open_table("./tests/data/delta-0.8.0")
        .await
        .unwrap();
    assert_eq!(table.version(), 1);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
            Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet"),
        ]
    );
    table.load_version(0).await.unwrap();
    assert_eq!(table.version(), 0);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
            Path::from("part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"),
        ]
    );
}

#[tokio::test]
async fn read_delta_8_0_table_with_partitions() {
    let current_dir = Path::from_filesystem_path(std::env::current_dir().unwrap()).unwrap();
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
            Path::from("year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet"),
            Path::from("year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet")
        ]
    );

    #[cfg(unix)]
    assert_eq!(
        table.get_file_uris_by_partitions(&filters).unwrap(),
        vec![
            format!("/{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet", current_dir.as_ref()),
            format!("/{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet", current_dir.as_ref())
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        table.get_file_uris_by_partitions(&filters).unwrap(),
        vec![
            format!("{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet", current_dir.as_ref()),
            format!("{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet", current_dir.as_ref())
        ]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::NotEqual("2"),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            Path::from("year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet"),
            Path::from("year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet"),
            Path::from("year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet"),
            Path::from("year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet")
        ]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::In(vec!["2", "12"]),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            Path::from("year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet"),
            Path::from("year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet"),
            Path::from("year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet"),
            Path::from("year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet")
        ]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::NotIn(vec!["2", "12"]),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            Path::from("year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet"),
            Path::from("year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet")
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
        vec![Path::from(
            "k=A/part-00000-b1f1dbbb-70bc-4970-893f-9bb772bf246e.c000.snappy.parquet"
        )]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "k",
        value: deltalake::PartitionValue::Equal(""),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            Path::from("k=__HIVE_DEFAULT_PARTITION__/part-00001-8474ac85-360b-4f58-b3ea-23990c71b932.c000.snappy.parquet")
        ]
    );
}

#[tokio::test]
async fn read_delta_8_0_table_with_special_partition() {
    let table = deltalake::open_table("./tests/data/delta-0.8.0-special-partition")
        .await
        .unwrap();

    assert_eq!(
        table.get_files(),
        vec![
            Path::from(
                "x=A%2FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
            ),
            Path::from(
                "x=B%20B/part-00015-e9abbc6f-85e9-457b-be8e-e9f5b8a22890.c000.snappy.parquet"
            )
        ]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "x",
        value: deltalake::PartitionValue::Equal("A/A"),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![Path::from(
            "x=A%2FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
        )]
    );
}

#[tokio::test]
async fn read_delta_8_0_table_partition_with_compare_op() {
    let table = deltalake::open_table("./tests/data/delta-0.8.0-numeric-partition")
        .await
        .unwrap();

    let filters = vec![deltalake::PartitionFilter {
        key: "x",
        value: deltalake::PartitionValue::LessThanOrEqual("9"),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![Path::from(
            "x=9/y=9.9/part-00007-3c50fba1-4264-446c-9c67-d8e24a1ccf83.c000.snappy.parquet"
        )]
    );

    let filters = vec![deltalake::PartitionFilter {
        key: "y",
        value: deltalake::PartitionValue::LessThan("10.0"),
    }];
    assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![Path::from(
            "x=9/y=9.9/part-00007-3c50fba1-4264-446c-9c67-d8e24a1ccf83.c000.snappy.parquet"
        )]
    );
}

// TODO: enable this for parquet2
#[cfg(feature = "parquet")]
#[tokio::test]
async fn read_delta_1_2_1_struct_stats_table() {
    let table_uri = "./tests/data/delta-1.2.1-only-struct-stats";
    let table_from_struct_stats = deltalake::open_table(table_uri).await.unwrap();
    let table_from_json_stats = deltalake::open_table_with_version(table_uri, 1)
        .await
        .unwrap();

    fn get_stats_for_file(
        table: &deltalake::DeltaTable,
        file_name: &str,
    ) -> deltalake::action::Stats {
        table
            .get_file_uris()
            .zip(table.get_stats())
            .filter_map(|(file_uri, file_stats)| {
                if file_uri.ends_with(file_name) {
                    file_stats.unwrap()
                } else {
                    None
                }
            })
            .next()
            .unwrap()
    }

    let file_to_compare = "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet";

    assert_eq!(
        get_stats_for_file(&table_from_struct_stats, file_to_compare),
        get_stats_for_file(&table_from_json_stats, file_to_compare),
    );
}

#[tokio::test]
async fn test_action_reconciliation() {
    let path = "./tests/data/action_reconciliation";
    let mut table = fs_common::create_table(path, None).await;

    // Add a file.
    let a = fs_common::add(3 * 60 * 1000);
    assert_eq!(1, fs_common::commit_add(&mut table, &a).await);
    assert_eq!(table.get_files(), vec![Path::from(a.path.clone())]);

    // Remove added file.
    let r = deltalake::action::Remove {
        path: a.path.clone(),
        deletion_timestamp: Some(Utc::now().timestamp_millis()),
        data_change: false,
        extended_file_metadata: None,
        partition_values: None,
        size: None,
        tags: None,
    };

    assert_eq!(2, fs_common::commit_removes(&mut table, vec![&r]).await);
    assert_eq!(table.get_files().len(), 0);
    assert_eq!(
        table
            .get_state()
            .all_tombstones()
            .iter()
            .map(|r| r.path.as_str())
            .collect::<Vec<_>>(),
        vec![a.path.as_str()]
    );

    // Add removed file back.
    assert_eq!(3, fs_common::commit_add(&mut table, &a).await);
    assert_eq!(table.get_files(), vec![Path::from(a.path)]);
    // tombstone is removed.
    assert_eq!(table.get_state().all_tombstones().len(), 0);
}

#[tokio::test]
async fn test_table_history() {
    let path = "./tests/data/simple_table_with_checkpoint";
    let mut latest_table = deltalake::open_table(path).await.unwrap();

    let mut table = deltalake::open_table_with_version(path, 1).await.unwrap();

    let history1 = table.history(None).await.expect("Cannot get table history");
    let history2 = latest_table
        .history(None)
        .await
        .expect("Cannot get table history");

    assert_eq!(history1, history2);

    let history3 = latest_table
        .history(Some(5))
        .await
        .expect("Cannot get table history");
    assert_eq!(history3.len(), 5);
}

#[tokio::test]
async fn test_poll_table_commits() {
    let path = "./tests/data/simple_table_with_checkpoint";
    let mut table = deltalake::open_table_with_version(path, 9).await.unwrap();
    let peek = table.peek_next_commit(table.version()).await.unwrap();
    assert!(matches!(peek, PeekCommit::New(..)));

    if let PeekCommit::New(version, actions) = peek {
        assert_eq!(table.version(), 9);
        assert!(!table.get_files_iter().any(|f| f
            == Path::from("part-00000-f0e955c5-a1e3-4eec-834e-dcc098fc9005-c000.snappy.parquet")));

        assert_eq!(version, 10);
        assert_eq!(actions.len(), 2);

        table.update_incremental(None).await.unwrap();

        assert_eq!(table.version(), 10);
        assert!(table.get_files_iter().any(|f| f
            == Path::from("part-00000-f0e955c5-a1e3-4eec-834e-dcc098fc9005-c000.snappy.parquet")));
    };

    let peek = table.peek_next_commit(table.version()).await.unwrap();
    assert!(matches!(peek, PeekCommit::UpToDate));
}

#[tokio::test]
async fn test_read_vacuumed_log() {
    let path = "./tests/data/checkpoints_vacuumed";
    let table = deltalake::open_table(path).await.unwrap();
    assert_eq!(table.version(), 12);
}

#[tokio::test]
async fn test_read_vacuumed_log_history() {
    let path = "./tests/data/checkpoints_vacuumed";
    let mut table = deltalake::open_table(path).await.unwrap();

    // load history for table version with available log file
    let history = table
        .history(Some(5))
        .await
        .expect("Cannot get table history");

    assert_eq!(history.len(), 5);

    // load history for table version without log file
    let history = table
        .history(Some(10))
        .await
        .expect("Cannot get table history");

    assert_eq!(history.len(), 8);
}

#[tokio::test]
async fn read_empty_folder() {
    let dir = std::env::temp_dir();
    let result = deltalake::open_table(&dir.into_os_string().into_string().unwrap()).await;

    assert!(matches!(
        result.unwrap_err(),
        deltalake::errors::DeltaTableError::NotATable(_),
    ));

    let dir = std::env::temp_dir();
    let result = deltalake::open_table_with_ds(
        &dir.into_os_string().into_string().unwrap(),
        "2021-08-09T13:18:31+08:00",
    )
    .await;

    assert!(matches!(
        result.unwrap_err(),
        deltalake::errors::DeltaTableError::NotATable(_),
    ));
}

#[tokio::test]
async fn read_delta_table_with_cdc() {
    let table = deltalake::open_table("./tests/data/simple_table_with_cdc")
        .await
        .unwrap();
    assert_eq!(table.version(), 2);
    assert_eq!(
        table.get_files(),
        vec![Path::from(
            "part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet"
        ),]
    );
}
