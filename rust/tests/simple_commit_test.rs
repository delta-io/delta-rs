extern crate chrono;
extern crate deltalake;
extern crate utime;

#[cfg(feature = "s3")]
#[allow(dead_code)]
mod s3_common;

#[allow(dead_code)]
mod fs_common;

use std::collections::HashMap;

use deltalake::action;

#[tokio::test]
async fn test_two_commits_fs() {
    prepare_fs();
    test_two_commits("./tests/data/simple_commit").await;
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn test_two_commits_s3() {
    prepare_s3().await;
    test_two_commits("s3://deltars/simple_commit_rw").await;
}

async fn test_two_commits(table_path: &str) {
    let mut table = deltalake::open_table(table_path).await.unwrap();

    assert_eq!(0, table.version);
    assert_eq!(0, table.get_files().len());

    let tx1_actions = vec![
        action::Action::add(action::Add {
            path: String::from(
                "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
            ),
            size: 396,
            partitionValues: HashMap::new(),
            partitionValues_parsed: None,
            modificationTime: 1564524294000,
            dataChange: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
        action::Action::add(action::Add {
            path: String::from(
                "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
            ),
            size: 400,
            partitionValues: HashMap::new(),
            partitionValues_parsed: None,
            modificationTime: 1564524294000,
            dataChange: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
    ];

    let mut tx1 = table.create_transaction(None);
    let version = tx1.commit_with(tx1_actions.as_slice(), None).await.unwrap();

    assert_eq!(1, version);
    assert_eq!(version, table.version);
    assert_eq!(2, table.get_files().len());

    let tx2_actions = vec![
        action::Action::add(action::Add {
            path: String::from(
                "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet",
            ),
            size: 396,
            partitionValues: HashMap::new(),
            partitionValues_parsed: None,
            modificationTime: 1564524296000,
            dataChange: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
        action::Action::add(action::Add {
            path: String::from(
                "part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet",
            ),
            size: 400,
            partitionValues: HashMap::new(),
            partitionValues_parsed: None,
            modificationTime: 1564524296000,
            dataChange: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
    ];

    let mut tx2 = table.create_transaction(None);
    let version = tx2.commit_with(tx2_actions.as_slice(), None).await.unwrap();

    assert_eq!(2, version);
    assert_eq!(version, table.version);
    assert_eq!(4, table.get_files().len());
}

fn prepare_fs() {
    fs_common::cleanup_dir_except(
        "./tests/data/simple_commit/_delta_log",
        vec!["00000000000000000000.json".to_string()],
    );
}

async fn prepare_s3() {
    s3_common::cleanup_dir_except(
        "s3://deltars/simple_commit_rw/_delta_log",
        vec!["00000000000000000000.json".to_string()],
    )
    .await;
}
