extern crate chrono;
extern crate deltalake;
extern crate utime;

use std::{collections::HashMap};
use deltalake::action;
use std::path::PathBuf;
use std::fs;

#[tokio::test]
async fn test_two_commits() {
    cleanup_log_dir();

    let mut table = deltalake::open_table("./tests/data/simple_commit")
        .await
        .unwrap();

    assert_eq!(0, table.version);
    assert_eq!(0, table.get_files().len());

    let tx1_actions = vec![
        action::Action::add(action::Add {
            path: String::from("part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet"),
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
            path: String::from("part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet"),
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

    let mut tx1 = table.create_transaction();
    let version = tx1.commit_all(tx1_actions.as_slice(), None).await.unwrap();

    assert_eq!(1, version);
    assert_eq!(version, table.version);
    assert_eq!(2, table.get_files().len());

    let tx2_actions = vec![
        action::Action::add(action::Add {
            path: String::from("part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet"),
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
            path: String::from("part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet"),
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

    let mut tx2 = table.create_transaction();
    let version = tx2.commit_all(tx2_actions.as_slice(), None).await.unwrap();

    assert_eq!(2, version);
    assert_eq!(version, table.version);
    assert_eq!(4, table.get_files().len());
}

fn cleanup_log_dir() {
    let log_dir = PathBuf::from("./tests/data/simple_commit/_delta_log");
    let paths = fs::read_dir(log_dir.as_path()).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let file_path = d.path();

                if let Some(extension) = file_path.extension() {
                    if extension == "json" && file_path.file_stem().unwrap() != "00000000000000000000" {
                        fs::remove_file(file_path).unwrap();
                    }
                }
            }
            _ => {}
        }
    }
}