mod fs_common;
use deltalake_core::protocol::DeltaOperation;

// NOTE: The below is a useful external command for inspecting the written checkpoint schema visually:
// parquet-tools inspect tests/data/checkpoints/_delta_log/00000000000000000005.checkpoint.parquet

mod simple_checkpoint {
    use deltalake_core::*;
    use pretty_assertions::assert_eq;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[tokio::test]
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
        checkpoints::create_checkpoint(&table).await.unwrap();

        // checkpoint should exist
        let checkpoint_path = log_path.join("00000000000000000005.checkpoint.parquet");
        assert!(checkpoint_path.as_path().exists());

        // _last_checkpoint should exist and point to the correct version
        let version = get_last_checkpoint_version(&log_path);
        assert_eq!(5, version);

        table.load_version(10).await.unwrap();
        checkpoints::create_checkpoint(&table).await.unwrap();

        // checkpoint should exist
        let checkpoint_path = log_path.join("00000000000000000010.checkpoint.parquet");
        assert!(checkpoint_path.as_path().exists());

        // _last_checkpoint should exist and point to the correct version
        let version = get_last_checkpoint_version(&log_path);
        assert_eq!(10, version);

        // delta table should load just fine with the checkpoint in place
        let table_result = deltalake_core::open_table(table_location).await.unwrap();
        let table = table_result;
        let files = table.get_files_iter().unwrap();
        assert_eq!(12, files.count());
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
        let paths = fs::read_dir(log_path).unwrap();
        for d in paths.flatten() {
            let path = d.path();

            if path.file_name().unwrap() == "_last_checkpoint"
                || (path.extension().is_some() && path.extension().unwrap() == "parquet")
            {
                fs::remove_file(path).unwrap();
            }
        }
    }
}

mod delete_expired_delta_log_in_checkpoint {
    use super::*;

    use ::object_store::path::Path as ObjectStorePath;
    use chrono::Utc;
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
        let set_file_last_modified = |version: usize, last_modified_millis: i64| {
            let last_modified_secs = last_modified_millis / 1000;
            let path = format!("{}/_delta_log/{:020}.json", &table_path, version);
            utime::set_file_times(path, last_modified_secs, last_modified_secs).unwrap();
        };

        // create 2 commits
        let a1 = fs_common::add(0);
        let a2 = fs_common::add(0);
        assert_eq!(1, fs_common::commit_add(&mut table, &a1).await);
        assert_eq!(2, fs_common::commit_add(&mut table, &a2).await);

        // set last_modified
        let now = Utc::now().timestamp_millis();
        set_file_last_modified(0, now - 25 * 60 * 1000); // 25 mins ago, should be deleted
        set_file_last_modified(1, now - 15 * 60 * 1000); // 25 mins ago, should be deleted
        set_file_last_modified(2, now - 5 * 60 * 1000); // 25 mins ago, should be kept

        table.load_version(0).await.expect("Cannot load version 0");
        table.load_version(1).await.expect("Cannot load version 1");
        table.load_version(2).await.expect("Cannot load version 2");

        checkpoints::create_checkpoint_from_table_uri_and_cleanup(
            &table.table_uri(),
            table.version(),
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
            table.version(),
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
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::schema::types::Type;
    use pretty_assertions::assert_eq;
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::iter::FromIterator;
    use uuid::Uuid;

    async fn read_checkpoint(path: &str) -> (Type, Vec<Action>) {
        let file = File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let schema = reader.metadata().file_metadata().schema();
        let row_iter = reader.get_row_iter(None).unwrap();
        let mut actions = Vec::new();
        for record in row_iter {
            actions.push(Action::from_parquet_record(schema, &record.unwrap()).unwrap())
        }
        (schema.clone(), actions)
    }

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
        checkpoints::create_checkpoint(&table).await.unwrap();
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
                .all_tombstones(table.object_store().clone())
                .await
                .unwrap()
                .collect::<HashSet<_>>(),
            removes1
        );

        checkpoints::create_checkpoint(&table).await.unwrap();
        table.update().await.unwrap(); // make table to read the checkpoint
        assert_eq!(
            table.get_files_iter().unwrap().collect::<Vec<_>>(),
            vec![ObjectStorePath::from(opt1.path.as_ref())]
        );
        assert_eq!(
            table
                .snapshot()
                .unwrap()
                .all_tombstones(table.object_store().clone())
                .await
                .unwrap()
                .count(),
            0
        ); // stale removes are deleted from the state
    }

    #[tokio::test]
    #[ignore]
    async fn test_checkpoint_with_extended_file_metadata_true() {
        let path = "../test/tests/data/checkpoints_tombstones/metadata_true";
        let mut table = fs_common::create_table(path, None).await;
        let r1 = remove_metadata_true();
        let r2 = remove_metadata_true();
        let version = fs_common::commit_removes(&mut table, vec![&r1, &r2]).await;
        let (schema, actions) = create_checkpoint_and_parse(&table, path, version).await;

        assert!(actions.contains(&r1));
        assert!(actions.contains(&r2));
        assert!(schema.contains("size"));
        assert!(schema.contains("partitionValues"));
        assert!(schema.contains("tags"));
    }

    #[tokio::test]
    async fn test_checkpoint_with_extended_file_metadata_false() {
        let path = "../test/tests/data/checkpoints_tombstones/metadata_false";
        let mut table = fs_common::create_table(path, None).await;
        let r1 = remove_metadata_true();
        let r2 = remove_metadata_false();
        let version = fs_common::commit_removes(&mut table, vec![&r1, &r2]).await;
        let (schema, actions) = create_checkpoint_and_parse(&table, path, version).await;

        // r2 has extended_file_metadata=false, then every tombstone should be so, even r1
        assert_ne!(actions, vec![r1.clone(), r2.clone()]);
        assert!(!schema.contains("size"));
        assert!(!schema.contains("partitionValues"));
        assert!(!schema.contains("tags"));
        let r1_updated = Remove {
            extended_file_metadata: Some(false),
            size: None,
            ..r1
        };
        assert!(actions.contains(&r1_updated));
        assert!(actions.contains(&r2));
    }

    #[tokio::test]
    async fn test_checkpoint_with_extended_file_metadata_broken() {
        let path = "../test/tests/data/checkpoints_tombstones/metadata_broken";
        let mut table = fs_common::create_table(path, None).await;
        let r1 = remove_metadata_broken();
        let r2 = remove_metadata_false();
        let version = fs_common::commit_removes(&mut table, vec![&r1, &r2]).await;
        let (schema, actions) = create_checkpoint_and_parse(&table, path, version).await;

        // r1 extended_file_metadata=true, but the size is null.
        // We should fix this by setting extended_file_metadata=false
        assert!(!schema.contains("size"));
        assert!(!schema.contains("partitionValues"));
        assert!(!schema.contains("tags"));
        assert!(actions.contains(&Remove {
            extended_file_metadata: Some(false),
            size: None,
            ..r1
        }));
        assert!(actions.contains(&r2));
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

    async fn create_checkpoint_and_parse(
        table: &DeltaTable,
        path: &str,
        version: i64,
    ) -> (HashSet<String>, Vec<Remove>) {
        checkpoints::create_checkpoint(table).await.unwrap();
        let cp_path = format!("{path}/_delta_log/0000000000000000000{version}.checkpoint.parquet");
        let (schema, actions) = read_checkpoint(&cp_path).await;

        let fields = schema
            .get_fields()
            .iter()
            .find(|p| p.name() == "remove")
            .unwrap()
            .get_fields()
            .iter()
            .map(|f| f.name().to_string());

        let actions = actions
            .iter()
            .filter_map(|a| match a {
                Action::Remove(r) => Some(r.clone()),
                _ => None,
            })
            .collect();

        (HashSet::from_iter(fields), actions)
    }

    // The tags and partition values could be missing, but the size has to present
    fn remove_metadata_true() -> Remove {
        Remove {
            path: Uuid::new_v4().to_string(),
            deletion_timestamp: Some(Utc::now().timestamp_millis()),
            data_change: false,
            extended_file_metadata: Some(true),
            partition_values: None,
            size: Some(100),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        }
    }

    // when metadata is false, then extended fields are null
    fn remove_metadata_false() -> Remove {
        Remove {
            extended_file_metadata: Some(false),
            size: None,
            partition_values: None,
            tags: None,
            ..remove_metadata_true()
        }
    }

    // broken record is when fields are null (the size especially) but metadata is true
    fn remove_metadata_broken() -> Remove {
        Remove {
            extended_file_metadata: Some(true),
            ..remove_metadata_false()
        }
    }
}
