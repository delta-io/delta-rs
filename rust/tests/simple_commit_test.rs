extern crate chrono;
extern crate deltalake;
extern crate utime;

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use rusoto_core::Region;
use rusoto_s3::{DeleteObjectRequest, S3Client, S3};

use deltalake::action;

#[tokio::test]
async fn test_two_commits_fs() {
    cleanup_log_dir_fs();
    test_two_commits("./tests/data/simple_commit").await;
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn test_two_commits_s3() {
    cleanup_log_dir_s3().await;
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

fn cleanup_log_dir_fs() {
    let log_dir = PathBuf::from("./tests/data/simple_commit/_delta_log");
    let paths = fs::read_dir(log_dir.as_path()).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let file_path = d.path();

                if let Some(extension) = file_path.extension() {
                    if extension == "json"
                        && file_path.file_stem().unwrap() != "00000000000000000000"
                    {
                        fs::remove_file(file_path).unwrap();
                    }
                }
            }
            _ => {}
        }
    }
}

async fn cleanup_log_dir_s3() {
    let endpoint = "http://localhost:4566";
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    let client = S3Client::new(Region::Custom {
        name: "custom".to_string(),
        endpoint: endpoint.to_string(),
    });
    delete_obj(&client, "00000000000000000001.json").await;
    delete_obj(&client, "00000000000000000002.json").await;
}

async fn delete_obj(client: &S3Client, name: &str) {
    let req = DeleteObjectRequest {
        bucket: "deltars".to_string(),
        key: format!("simple_commit_rw/_delta_log/{}", name),
        ..Default::default()
    };
    client.delete_object(req).await.unwrap();
}
