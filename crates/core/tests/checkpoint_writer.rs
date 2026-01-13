mod fs_common;

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
        let table_url = deltalake_core::table::builder::parse_table_uri(table_location).unwrap();
        let mut table = deltalake_core::open_table_with_version(table_url.clone(), 5)
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
        let table_result = deltalake_core::open_table(table_url.clone()).await.unwrap();
        let table = table_result;
        let files = table.snapshot().unwrap().log_data().num_files();
        assert_eq!(12, files);
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
                    if encoding == Encoding::RLE_DICTIONARY {
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

            if let Some(caps) = re.captures(filename)
                && let Ok(num) = caps[1].parse::<u64>()
                && num <= 12
            {
                continue;
            }
            let _ = fs::remove_file(path);
        }
    }
}

mod delete_expired_delta_log_in_checkpoint {
    use super::*;
    use std::collections::HashMap;
    use std::fs::{FileTimes, OpenOptions};
    use std::ops::Sub;
    use std::time::{Duration, SystemTime};

    use ::object_store::path::Path as ObjectStorePath;
    use deltalake_core::table::config::TableProperty;
    use deltalake_core::*;

    #[tokio::test]
    async fn test_delete_expired_logs() -> DeltaResult<()> {
        let mut table = fs_common::create_table(
            "../test/tests/data/checkpoints_with_expired_logs/expired",
            Some(HashMap::from([
                (
                    TableProperty::LogRetentionDuration.as_ref().into(),
                    Some("interval 0 minute".to_string()),
                ),
                (
                    TableProperty::EnableExpiredLogCleanup.as_ref().into(),
                    Some("true".to_string()),
                ),
            ])),
        )
        .await;

        let table_path = table
            .table_url()
            .to_file_path()
            .expect("Failed toc convert the table's Url to a file path");
        let set_file_last_modified = |version: usize, last_modified_millis: u64| {
            let mut path = table_path.clone();
            path.push("_delta_log");
            path.push(format!("{version:020}.json"));
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
        set_file_last_modified(1, 15 * 60 * 1000); // 15 mins ago, should be deleted
        set_file_last_modified(2, 5 * 60 * 1000); // 5 mins ago, should be kept as last safe checkpoint

        table.load_version(0).await.expect("Cannot load version 0");
        table.load_version(1).await.expect("Cannot load version 1");
        table.load_version(2).await.expect("Cannot load version 2");

        checkpoints::create_checkpoint_from_table_url_and_cleanup(
            table.table_url().clone(),
            table.version().expect("Failed to load version() on table"),
            None,
            None,
        )
        .await
        .expect("Failed to create a checkpoint and cleanup");

        table
            .load()
            .await
            .expect("Failed to read the checkpoint back");
        assert_eq!(
            table
                .get_files_by_partitions(&[])
                .await
                .expect("Failed to get_files_by_partitions()"),
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
        Ok(())
    }

    // Test to verify that intermediate versions can still be loaded after the checkpoint is created.
    // This is to verify the behavior of `cleanup_expired_logs_for` and its use of safe checkpoints.
    #[tokio::test]
    async fn test_delete_expired_logs_safe_checkpoint() -> DeltaResult<()> {
        // For additional tracing:
        // let _ = pretty_env_logger::try_init();
        let mut table = fs_common::create_table(
            "../test/tests/data/checkpoints_with_expired_logs/expired_with_checkpoint",
            Some(HashMap::from([
                (
                    TableProperty::LogRetentionDuration.as_ref().into(),
                    Some("interval 10 minute".to_string()),
                ),
                (
                    TableProperty::EnableExpiredLogCleanup.as_ref().into(),
                    Some("true".to_string()),
                ),
            ])),
        )
        .await;

        let table_path = table
            .table_url()
            .to_file_path()
            .expect("Failed toc convert the table's Url to a file path");
        let set_file_last_modified = |version: usize, last_modified_millis: u64, suffix: &str| {
            let mut path = table_path.clone();
            path.push("_delta_log");
            path.push(format!("{version:020}.{suffix}"));
            let file = OpenOptions::new().write(true).open(path).unwrap();
            let last_modified = SystemTime::now().sub(Duration::from_millis(last_modified_millis));
            let times = FileTimes::new()
                .set_modified(last_modified)
                .set_accessed(last_modified);
            file.set_times(times).unwrap();
        };

        // create 4 commits
        let a1 = fs_common::add(0);
        let a2 = fs_common::add(0);
        let a3 = fs_common::add(0);
        let a4 = fs_common::add(0);
        assert_eq!(1, fs_common::commit_add(&mut table, &a1).await);
        assert_eq!(2, fs_common::commit_add(&mut table, &a2).await);
        assert_eq!(3, fs_common::commit_add(&mut table, &a3).await);
        assert_eq!(4, fs_common::commit_add(&mut table, &a4).await);

        // set last_modified
        set_file_last_modified(0, 25 * 60 * 1000, "json"); // 25 mins ago, should be deleted
        set_file_last_modified(1, 20 * 60 * 1000, "json"); // 20 mins ago, last safe checkpoint, should be kept
        set_file_last_modified(2, 15 * 60 * 1000, "json"); // 15 mins ago, fails retention cutoff, but needed by v3
        set_file_last_modified(3, 6 * 60 * 1000, "json"); // 6 mins ago, should be kept by retention cutoff
        set_file_last_modified(4, 5 * 60 * 1000, "json"); // 5 mins ago, should be kept by retention cutoff

        table.load_version(0).await.expect("Cannot load version 0");
        table.load_version(1).await.expect("Cannot load version 1");
        table.load_version(2).await.expect("Cannot load version 2");
        table.load_version(3).await.expect("Cannot load version 3");
        table.load_version(4).await.expect("Cannot load version 4");

        // Create checkpoint for version 1
        checkpoints::create_checkpoint_from_table_url_and_cleanup(
            deltalake_core::ensure_table_uri(table.table_url()).unwrap(),
            1,
            Some(false),
            None,
        )
        .await
        .unwrap();

        // Update checkpoint time for version 1 to be just after version 1 data
        set_file_last_modified(1, 20 * 60 * 1000 - 10, "checkpoint.parquet");

        // Checkpoint final version
        checkpoints::create_checkpoint_from_table_url_and_cleanup(
            deltalake_core::ensure_table_uri(table.table_url()).unwrap(),
            table.version().unwrap(),
            None,
            None,
        )
        .await
        .unwrap();

        table.update_state().await.unwrap(); // make table to read the checkpoint
        assert_eq!(
            table.get_files_by_partitions(&[]).await?,
            vec![
                ObjectStorePath::from(a4.path.as_ref()),
                ObjectStorePath::from(a3.path.as_ref()),
                ObjectStorePath::from(a2.path.as_ref()),
                ObjectStorePath::from(a1.path.as_ref()),
            ]
        );

        // Without going back to a safe checkpoint, previously loading version 3 would fail.
        table.load_version(3).await.expect("Cannot load version 3");
        table.load_version(4).await.expect("Cannot load version 4");
        Ok(())
    }

    #[tokio::test]
    async fn test_not_delete_expired_logs() -> DeltaResult<()> {
        let mut table = fs_common::create_table(
            "../test/tests/data/checkpoints_with_expired_logs/not_delete_expired",
            Some(HashMap::from([
                (
                    TableProperty::LogRetentionDuration.as_ref().into(),
                    Some("interval 1 second".to_string()),
                ),
                (
                    TableProperty::EnableExpiredLogCleanup.as_ref().into(),
                    Some("false".to_string()),
                ),
            ])),
        )
        .await;

        let a1 = fs_common::add(3 * 60 * 1000); // 3 mins ago,
        let a2 = fs_common::add(2 * 60 * 1000); // 2 mins ago,
        assert_eq!(1, fs_common::commit_add(&mut table, &a1).await);
        assert_eq!(2, fs_common::commit_add(&mut table, &a2).await);

        table.load_version(0).await.expect("Cannot load version 0");
        table.load_version(1).await.expect("Cannot load version 1");

        checkpoints::create_checkpoint_from_table_url_and_cleanup(
            deltalake_core::ensure_table_uri(table.table_url()).unwrap(),
            table.version().unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
        table.update_state().await.unwrap(); // make table to read the checkpoint
        assert_eq!(
            table.get_files_by_partitions(&[]).await?,
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
        Ok(())
    }
}
