#![cfg(feature = "integration_test")]

#[allow(dead_code)]
mod fs_common;

use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use deltalake::{action, errors::DeltaTableError, DeltaTableBuilder};
use serial_test::serial;
use std::collections::HashMap;

#[tokio::test]
#[serial]
async fn test_commit_tables_local() {
    commit_tables(StorageIntegration::Local).await.unwrap();
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
#[tokio::test]
#[serial]
async fn test_commit_tables_aws() {
    std::env::set_var("AWS_S3_LOCKING_PROVIDER", "dynamodb");
    commit_tables(StorageIntegration::Amazon).await.unwrap();
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_commit_tables_azure() {
    commit_tables(StorageIntegration::Microsoft).await.unwrap();
}

#[cfg(feature = "gcs")]
#[tokio::test]
#[serial]
async fn test_commit_tables_gcp() {
    commit_tables(StorageIntegration::Google).await.unwrap();
}

#[cfg(feature = "hdfs")]
#[tokio::test]
#[serial]
async fn test_commit_tables_hdfs() {
    commit_tables(StorageIntegration::Hdfs).await.unwrap();
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
#[tokio::test]
#[serial]
async fn test_two_commits_s3_fails_with_no_lock() -> TestResult {
    std::env::set_var("AWS_S3_LOCKING_PROVIDER", "none  ");
    let context = IntegrationContext::new(StorageIntegration::Amazon)?;
    context.load_table(TestTables::SimpleCommit).await?;
    let table_uri = context.uri_for_table(TestTables::SimpleCommit);

    let result = test_two_commits(&table_uri).await;
    assert!(result.is_err());

    let err_msg = result.err().unwrap().to_string();
    assert!(err_msg.contains("Atomic rename requires a LockClient for S3 backends."));

    Ok(())
}

async fn commit_tables(storage: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(storage)?;

    context
        .load_table_with_name(TestTables::SimpleCommit, "simple_commit_1")
        .await?;
    let table_uri = context.uri_for_table(TestTables::Custom("simple_commit_1".into()));
    test_two_commits(&table_uri).await?;

    context
        .load_table_with_name(TestTables::SimpleCommit, "simple_commit_2")
        .await?;
    let table_uri = context.uri_for_table(TestTables::Custom("simple_commit_2".into()));
    test_commit_version_succeeds_if_version_does_not_exist(&table_uri).await?;

    Ok(())
}

async fn test_commit_version_succeeds_if_version_does_not_exist(
    table_path: &str,
) -> Result<(), DeltaTableError> {
    let mut table = DeltaTableBuilder::from_uri(table_path)
        .with_allow_http(true)
        .load()
        .await?;

    assert_eq!(0, table.version());
    assert_eq!(0, table.get_files().len());

    let mut tx1 = table.create_transaction(None);
    tx1.add_actions(tx1_actions());
    let commit = tx1.prepare_commit(None, None).await?;
    let result = table.try_commit_transaction(&commit, 1).await?;

    assert_eq!(1, result);
    assert_eq!(1, table.version());
    assert_eq!(2, table.get_files().len());

    Ok(())
}

mod simple_commit_fs {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_commit_version_succeeds_if_version_does_not_exist() {
        prepare_fs();

        let table_path = "./tests/data/simple_commit";
        let mut table = deltalake::open_table(table_path).await.unwrap();

        assert_eq!(0, table.version());
        assert_eq!(0, table.get_files().len());

        let mut tx1 = table.create_transaction(None);
        tx1.add_actions(tx1_actions());
        let commit = tx1.prepare_commit(None, None).await.unwrap();
        let result = table.try_commit_transaction(&commit, 1).await.unwrap();

        assert_eq!(1, result);
        assert_eq!(1, table.version());
        assert_eq!(2, table.get_files().len());

        prepare_fs();
    }

    #[tokio::test]
    #[serial]
    async fn test_commit_version_fails_if_version_exists() {
        prepare_fs();

        let table_path = "./tests/data/simple_commit";
        let mut table = deltalake::open_table(table_path).await.unwrap();

        assert_eq!(0, table.version());
        assert_eq!(0, table.get_files().len());

        let mut tx1 = table.create_transaction(None);
        tx1.add_actions(tx1_actions());
        let commit = tx1.prepare_commit(None, None).await.unwrap();
        let _ = table.try_commit_transaction(&commit, 1).await.unwrap();

        let mut tx2 = table.create_transaction(None);
        tx2.add_actions(tx2_actions());
        // we already committed version 1 - this should fail and return error for caller to handle.
        let commit = tx2.prepare_commit(None, None).await.unwrap();
        let result = table.try_commit_transaction(&commit, 1).await;

        match result {
            Err(DeltaTableError::VersionAlreadyExists(_)) => {
                assert!(true, "Delta version already exists.");
            }
            _ => {
                assert!(false, "Delta version should already exist.");
            }
        }

        assert!(result.is_err());
        assert_eq!(1, table.version());
        assert_eq!(2, table.get_files().len());

        prepare_fs();
    }

    // This test shows an example on how to use low-level transaction API with custom optimistic
    // concurrency loop and retry logic.
    #[tokio::test]
    #[serial]
    async fn test_low_level_tx_api() {
        prepare_fs();

        let table_path = "./tests/data/simple_commit";
        let mut table = deltalake::open_table(table_path).await.unwrap();

        assert_eq!(0, table.version());
        assert_eq!(0, table.get_files().len());

        let mut attempt = 0;
        let prepared_commit = {
            let mut tx = table.create_transaction(None);
            tx.add_actions(tx1_actions());
            tx.prepare_commit(None, None).await.unwrap()
        };

        loop {
            table.update().await.unwrap();

            let version = table.version() + 1;
            match table
                .try_commit_transaction(&prepared_commit, version)
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(DeltaTableError::VersionAlreadyExists(_)) => {
                    attempt += 1;
                }
                Err(e) => {
                    panic!("{}", e)
                }
            }
        }

        assert_eq!(0, attempt);
        assert_eq!(1, table.version());
        assert_eq!(2, table.get_files().len());

        prepare_fs();
    }

    fn prepare_fs() {
        fs_common::cleanup_dir_except(
            "./tests/data/simple_commit/_delta_log",
            vec!["00000000000000000000.json".to_string()],
        );
    }
}

async fn test_two_commits(table_path: &str) -> Result<(), DeltaTableError> {
    let mut table = DeltaTableBuilder::from_uri(table_path)
        .with_allow_http(true)
        .load()
        .await?;

    assert_eq!(0, table.version());
    assert_eq!(0, table.get_files().len());

    let mut tx1 = table.create_transaction(None);
    tx1.add_actions(tx1_actions());
    let version = tx1.commit(None, None).await?;

    assert_eq!(1, version);
    assert_eq!(version, table.version());
    assert_eq!(2, table.get_files().len());

    let mut tx2 = table.create_transaction(None);
    tx2.add_actions(tx2_actions());
    let version = tx2.commit(None, None).await.unwrap();

    assert_eq!(2, version);
    assert_eq!(version, table.version());
    assert_eq!(4, table.get_files().len());
    Ok(())
}

fn tx1_actions() -> Vec<action::Action> {
    vec![
        action::Action::add(action::Add {
            path: String::from(
                "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
            ),
            size: 396,
            partition_values: HashMap::new(),
            partition_values_parsed: None,
            modification_time: 1564524294000,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
        action::Action::add(action::Add {
            path: String::from(
                "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
            ),
            size: 400,
            partition_values: HashMap::new(),
            partition_values_parsed: None,
            modification_time: 1564524294000,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
    ]
}

fn tx2_actions() -> Vec<action::Action> {
    vec![
        action::Action::add(action::Add {
            path: String::from(
                "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet",
            ),
            size: 396,
            partition_values: HashMap::new(),
            partition_values_parsed: None,
            modification_time: 1564524296000,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
        action::Action::add(action::Add {
            path: String::from(
                "part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet",
            ),
            size: 400,
            partition_values: HashMap::new(),
            partition_values_parsed: None,
            modification_time: 1564524296000,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }),
    ]
}
