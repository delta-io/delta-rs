mod fs_common;
use deltalake_core::protocol::DeltaOperation;

// NOTE: The below is a useful external command for inspecting the written checkpoint schema visually:
// parquet-tools inspect tests/data/checkpoints/_delta_log/00000000000000000005.checkpoint.parquet

mod simple_checkpoint {
    use deltalake_core::*;
    use parquet::basic::Encoding;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use pretty_assertions::assert_eq;
    use regex::Regex;
    use serial_test::serial;
    use std::fs::{self, File};
    use std::path::{Path, PathBuf};

    #[tokio::test]
    #[serial]
    async fn simple_checkpoint_test() {
        let table_location = "../test/tests/data/checkpoints";
        let table_path = PathBuf::from(table_location);
        let log_path = table_path.join("_delta_log");

        // Delete checkpoint files from previous runs
        cleanup_checkpoint_files(log_path.as_path());

        // Load the delta table at version 5
        let mut table = deltalake_core::open_table_with_version(table_location, 5)
            .await
            .unwrap();

        // Write a checkpoint
        checkpoints::create_checkpoint(&table, None).await.unwrap();

        // checkpoint should exist
        let checkpoint_path = log_path.join("00000000000000000005.checkpoint.parquet");
        assert!(checkpoint_path.as_path().exists());

        // Check that the checkpoint does use run length encoding
        assert_column_rle_encoding(checkpoint_path, true);

        // _last_checkpoint should exist and point to the correct version
        let version = get_last_checkpoint_version(&log_path);
        assert_eq!(5, version);

        table.load_version(10).await.unwrap();
        checkpoints::create_checkpoint(&table, None).await.unwrap();

        // checkpoint should exist
        let checkpoint_path = log_path.join("00000000000000000010.checkpoint.parquet");
        assert!(checkpoint_path.as_path().exists());

        // Check that the checkpoint does use run length encoding
        assert_column_rle_encoding(checkpoint_path, true);

        // _last_checkpoint should exist and point to the correct version
        let version = get_last_checkpoint_version(&log_path);
        assert_eq!(10, version);

        // delta table should load just fine with the checkpoint in place
        let table_result = deltalake_core::open_table(table_location).await.unwrap();
        let table = table_result;
        let files = table.get_files_iter().unwrap();
        assert_eq!(12, files.count());
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn checkpoint_run_length_encoding_test() {
        let table_location = "../test/tests/data/checkpoints";
        let table_path = PathBuf::from(table_location);
        let log_path = table_path.join("_delta_log");

        // Delete checkpoint files from previous runs
        cleanup_checkpoint_files(log_path.as_path());

        // Load the delta table
        let base_table = deltalake_core::open_table(table_location).await.unwrap();

        // Set the table properties to disable run length encoding
        // this alters table version and should be done in a more principled way
        let table = DeltaOps(base_table)
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::<String, String>::from([(
                "delta-rs.checkpoint.useRunLengthEncoding".to_string(),
                "false".to_string(),
            )]))
            .await
            .unwrap();

        // Write a checkpoint
        checkpoints::create_checkpoint(&table, None).await.unwrap();

        // checkpoint should exist
        let checkpoint_path = log_path.join("00000000000000000013.checkpoint.parquet");
        assert!(checkpoint_path.as_path().exists());

        // Check that the checkpoint does not use run length encoding
        assert_column_rle_encoding(checkpoint_path, false);

        // _last_checkpoint should exist and point to the correct version
        let version = get_last_checkpoint_version(&log_path);
        assert_eq!(table.version(), Some(version));

        // delta table should load just fine with the checkpoint in place
        let table_result = deltalake_core::open_table(table_location).await.unwrap();
        let table = table_result;
        let files = table.get_files_iter().unwrap();
        assert_eq!(12, files.count());
    }

    fn assert_column_rle_encoding(file_path: PathBuf, should_be_rle: bool) {
        let file = File::open(&file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let meta = reader.metadata();
        let mut found_rle = false;

        for i in 0..meta.num_row_groups() {
            let row_group = meta.row_group(i);
            for j in 0..row_group.num_columns() {
                let column_chunk: &parquet::file::metadata::ColumnChunkMetaData =
                    row_group.column(j);

                for encoding in column_chunk.encodings() {
                    if *encoding == Encoding::RLE_DICTIONARY {
                        found_rle = true;
                    }
                }
            }
        }

        if should_be_rle {
            assert!(found_rle, "Expected RLE_DICTIONARY encoding");
        } else {
            assert!(!found_rle, "Expected no RLE_DICTIONARY encoding");
        }
    }

    fn get_last_checkpoint_version(log_path: &Path) -> i64 {
        let last_checkpoint_path = log_path.join("_last_checkpoint");
        assert!(last_checkpoint_path.as_path().exists());

        let last_checkpoint_content = fs::read_to_string(last_checkpoint_path.as_path()).unwrap();
        let last_checkpoint_content: serde_json::Value =
            serde_json::from_str(last_checkpoint_content.trim()).unwrap();

        last_checkpoint_content
            .get("version")
            .unwrap()
            .as_i64()
            .unwrap()
    }

    fn cleanup_checkpoint_files(log_path: &Path) {
        let re = Regex::new(r"^(\d{20})\.json$").unwrap();
        for entry in fs::read_dir(log_path).unwrap().flatten() {
            let path = entry.path();
            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if let Some(caps) = re.captures(filename) {
                if let Ok(num) = caps[1].parse::<u64>() {
                    if num <= 12 {
                        continue;
                    }
                }
            }
            let _ = fs::remove_file(path);
        }
    }
}

mod delete_expired_delta_log_in_checkpoint {
    use super::*;
    use std::fs::{FileTimes, OpenOptions};
    use std::ops::Sub;
    use std::time::{Duration, SystemTime};

    use ::object_store::path::Path as ObjectStorePath;
    use deltalake_core::table::config::TableProperty;
    use deltalake_core::*;
    use maplit::hashmap;

    #[tokio::test]
    async fn test_delete_expired_logs() {
        let mut table = fs_common::create_table(
            "../test/tests/data/checkpoints_with_expired_logs/expired",
            Some(hashmap! {
                TableProperty::LogRetentionDuration.as_ref().into() => Some("interval 10 minute".to_string()),
                TableProperty::EnableExpiredLogCleanup.as_ref().into() => Some("true".to_string())
            }),
        )
        .await;

        let table_path = table.table_uri();
        let set_file_last_modified = |version: usize, last_modified_millis: u64| {
            let path = format!("{table_path}_delta_log/{version:020}.json");
            let file = OpenOptions::new().write(true).open(path).unwrap();
            let last_modified = SystemTime::now().sub(Duration::from_millis(last_modified_millis));
            let times = FileTimes::new()
                .set_modified(last_modified)
                .set_accessed(last_modified);
            file.set_times(times).unwrap();
        };

        // create 2 commits
        let a1 = fs_common::add(0);
        let a2 = fs_common::add(0);
        assert_eq!(1, fs_common::commit_add(&mut table, &a1).await);
        assert_eq!(2, fs_common::commit_add(&mut table, &a2).await);

        // set last_modified
        set_file_last_modified(0, 25 * 60 * 1000); // 25 mins ago, should be deleted
        set_file_last_modified(1, 15 * 60 * 1000); // 25 mins ago, should be deleted
        set_file_last_modified(2, 5 * 60 * 1000); // 25 mins ago, should be kept

        table.load_version(0).await.expect("Cannot load version 0");
        table.load_version(1).await.expect("Cannot load version 1");
        table.load_version(2).await.expect("Cannot load version 2");

        checkpoints::create_checkpoint_from_table_uri_and_cleanup(
            &table.table_uri(),
            table.version().unwrap(),
            None,
            None,
        )
        .await
        .unwrap();

        table.update().await.unwrap(); // make table to read the checkpoint
        assert_eq!(
            table.get_files_iter().unwrap().collect::<Vec<_>>(),
            vec![
                ObjectStorePath::from(a2.path.as_ref()),
                ObjectStorePath::from(a1.path.as_ref()),
            ]
        );

        // log files 0 and 1 are deleted
        table
            .load_version(0)
            .await
            .expect_err("Should not load version 0");
        table
            .load_version(1)
            .await
            .expect_err("Should not load version 1");
        // log file 2 is kept
        table.load_version(2).await.expect("Cannot load version 2");
    }

    #[tokio::test]
    async fn test_not_delete_expired_logs() {
        let mut table = fs_common::create_table(
            "../test/tests/data/checkpoints_with_expired_logs/not_delete_expired",
            Some(hashmap! {
                TableProperty::LogRetentionDuration.as_ref().into() => Some("interval 1 second".to_string()),
                TableProperty::EnableExpiredLogCleanup.as_ref().into() => Some("false".to_string())
            }),
        )
        .await;

        let a1 = fs_common::add(3 * 60 * 1000); // 3 mins ago,
        let a2 = fs_common::add(2 * 60 * 1000); // 2 mins ago,
        assert_eq!(1, fs_common::commit_add(&mut table, &a1).await);
        assert_eq!(2, fs_common::commit_add(&mut table, &a2).await);

        table.load_version(0).await.expect("Cannot load version 0");
        table.load_version(1).await.expect("Cannot load version 1");

        checkpoints::create_checkpoint_from_table_uri_and_cleanup(
            &table.table_uri(),
            table.version().unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
        table.update().await.unwrap(); // make table to read the checkpoint
        assert_eq!(
            table.get_files_iter().unwrap().collect::<Vec<_>>(),
            vec![
                ObjectStorePath::from(a2.path.as_ref()),
                ObjectStorePath::from(a1.path.as_ref()),
            ]
        );

        table
            .load_version(0)
            .await
            .expect("Should not delete version 0");
        table
            .load_version(1)
            .await
            .expect("Should not delete version 1");

        table.load_version(2).await.expect("Cannot load version 2");
    }
}

mod checkpoints_with_tombstones {
    use super::*;

    use ::object_store::path::Path as ObjectStorePath;
    use chrono::Utc;
    use deltalake_core::kernel::*;
    use deltalake_core::table::config::TableProperty;
    use deltalake_core::*;
    use maplit::hashmap;
    use pretty_assertions::assert_eq;
    use std::collections::{HashMap, HashSet};

    #[tokio::test]
    #[ignore]
    async fn test_expired_tombstones() {
        let mut table = fs_common::create_table("../test/tests/data/checkpoints_tombstones/expired", Some(hashmap! {
            TableProperty::DeletedFileRetentionDuration.as_ref().into() => Some("interval 1 minute".to_string())
        })).await;

        let a1 = fs_common::add(3 * 60 * 1000); // 3 mins ago,
        let a2 = fs_common::add(2 * 60 * 1000); // 2 mins ago,

        assert_eq!(1, fs_common::commit_add(&mut table, &a1).await);
        assert_eq!(2, fs_common::commit_add(&mut table, &a2).await);
        checkpoints::create_checkpoint(&table, None).await.unwrap();
        table.update().await.unwrap(); // make table to read the checkpoint
        assert_eq!(
            table.get_files_iter().unwrap().collect::<Vec<_>>(),
            vec![
                ObjectStorePath::from(a2.path.as_ref()),
                ObjectStorePath::from(a1.path.as_ref()),
            ]
        );

        let (removes1, opt1) = pseudo_optimize(&mut table, 5 * 59 * 1000).await;
        assert_eq!(
            table.get_files_iter().unwrap().collect::<Vec<_>>(),
            vec![ObjectStorePath::from(opt1.path.as_ref())]
        );

        assert_eq!(
            table
                .snapshot()
                .unwrap()
                .all_tombstones(&table.log_store())
                .await
                .unwrap()
                .collect::<HashSet<_>>(),
            removes1
        );

        checkpoints::create_checkpoint(&table, None).await.unwrap();
        table.update().await.unwrap(); // make table to read the checkpoint
        assert_eq!(
            table.get_files_iter().unwrap().collect::<Vec<_>>(),
            vec![ObjectStorePath::from(opt1.path.as_ref())]
        );
        assert_eq!(
            table
                .snapshot()
                .unwrap()
                .all_tombstones(&table.log_store())
                .await
                .unwrap()
                .count(),
            0
        ); // stale removes are deleted from the state
    }

    async fn pseudo_optimize(table: &mut DeltaTable, offset_millis: i64) -> (HashSet<Remove>, Add) {
        let removes: HashSet<Remove> = table
            .get_files_iter()
            .unwrap()
            .map(|p| Remove {
                path: p.to_string(),
                deletion_timestamp: Some(Utc::now().timestamp_millis() - offset_millis),
                data_change: false,
                extended_file_metadata: None,
                partition_values: Some(HashMap::new()),
                size: None,
                tags: Some(HashMap::new()),
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
            })
            .collect();

        let add = Add {
            data_change: false,
            ..fs_common::add(offset_millis)
        };

        let actions = removes
            .iter()
            .cloned()
            .map(Action::Remove)
            .chain(std::iter::once(Action::Add(add.clone())))
            .collect();
        let operation = DeltaOperation::Optimize {
            predicate: None,
            target_size: 1000000,
        };
        fs_common::commit_actions(table, actions, operation).await;
        (removes, add)
    }
}
