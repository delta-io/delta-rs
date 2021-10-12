use std::collections::{HashMap, HashSet};
use std::fs;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

use chrono::Utc;
use maplit::hashmap;
use uuid::Uuid;

use deltalake::action::*;
use deltalake::*;

mod fs_common;

// NOTE: The below is a useful external command for inspecting the written checkpoint schema visually:
// parquet-tools inspect tests/data/checkpoints/_delta_log/00000000000000000005.checkpoint.parquet

mod simple_checkpoint {
    use super::*;

    #[tokio::test]
    async fn simple_checkpoint_test() {
        let table_location = "./tests/data/checkpoints";
        let table_path = PathBuf::from(table_location);
        let log_path = table_path.join("_delta_log");

        // Delete checkpoint files from previous runs
        cleanup_checkpoint_files(log_path.as_path());

        // Load the delta table at version 5
        let mut table = deltalake::open_table_with_version(table_location, 5)
            .await
            .unwrap();

        // Write a checkpoint
        checkpoints::create_checkpoint_from_table(&table)
            .await
            .unwrap();

        // checkpoint should exist
        let checkpoint_path = log_path.join("00000000000000000005.checkpoint.parquet");
        assert!(checkpoint_path.as_path().exists());

        // _last_checkpoint should exist and point to the correct version
        let version = get_last_checkpoint_version(&log_path);
        assert_eq!(5, version);

        table.load_version(10).await.unwrap();
        checkpoints::create_checkpoint_from_table(&table)
            .await
            .unwrap();

        // checkpoint should exist
        let checkpoint_path = log_path.join("00000000000000000010.checkpoint.parquet");
        assert!(checkpoint_path.as_path().exists());

        // _last_checkpoint should exist and point to the correct version
        let version = get_last_checkpoint_version(&log_path);
        assert_eq!(10, version);

        // delta table should load just fine with the checkpoint in place
        let table_result = deltalake::open_table(table_location).await.unwrap();
        let table = table_result;
        let files = table.get_files();
        assert_eq!(12, files.len());
    }

    fn get_last_checkpoint_version(log_path: &PathBuf) -> i64 {
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
        for p in paths {
            match p {
                Ok(d) => {
                    let path = d.path();

                    if path.file_name().unwrap() == "_last_checkpoint"
                        || (path.extension().is_some() && path.extension().unwrap() == "parquet")
                    {
                        fs::remove_file(path).unwrap();
                    }
                }
                _ => {}
            }
        }
    }
}

mod checkpoints_with_tombstones {
    use super::*;

    #[tokio::test]
    async fn test_expired_tombstones() {
        let mut table = create_table("./tests/data/checkpoints_tombstones/expired", Some(hashmap! {
            delta_config::TOMBSTONE_RETENTION.key.clone() => Some("interval 1 minute".to_string())
        })).await;

        let a1 = add(3 * 60 * 1000); // 3 mins ago,
        let a2 = add(2 * 60 * 1000); // 2 mins ago,

        assert_eq!(1, commit_add(&mut table, &a1).await);
        assert_eq!(2, commit_add(&mut table, &a2).await);
        checkpoints::create_checkpoint_from_table(&table)
            .await
            .unwrap();
        table.update().await.unwrap(); // make table to read the checkpoint
        assert_eq!(table.get_files(), vec![a1.path.as_str(), a2.path.as_str()]);

        let (removes1, opt1) = pseudo_optimize(&mut table, 5 * 59 * 1000).await;
        assert_eq!(table.get_files(), vec![opt1.path.as_str()]);
        assert_eq!(table.get_state().all_tombstones(), &removes1);

        checkpoints::create_checkpoint_from_table(&table)
            .await
            .unwrap();
        table.update().await.unwrap(); // make table to read the checkpoint
        assert_eq!(table.get_files(), vec![opt1.path.as_str()]);
        assert_eq!(table.get_state().all_tombstones().len(), 0); // stale removes are deleted from the state
    }

    #[tokio::test]
    async fn test_checkpoint_with_extended_file_metadata_true() {
        let path = "./tests/data/checkpoints_tombstones/metadata_true";
        let mut table = create_table(path, None).await;
        let r1 = remove_metadata_true();
        let r2 = remove_metadata_true();
        let version = commit_removes(&mut table, vec![&r1, &r2]).await;
        let (schema, actions) = create_checkpoint_and_parse(&table, &path, version).await;

        assert!(actions.contains(&r1));
        assert!(actions.contains(&r2));
        assert!(schema.contains("size"));
        assert!(schema.contains("partitionValues"));
        assert!(schema.contains("tags"));
    }

    #[tokio::test]
    async fn test_checkpoint_with_extended_file_metadata_false() {
        let path = "./tests/data/checkpoints_tombstones/metadata_false";
        let mut table = create_table(path, None).await;
        let r1 = remove_metadata_true();
        let r2 = remove_metadata_false();
        let version = commit_removes(&mut table, vec![&r1, &r2]).await;
        let (schema, actions) = create_checkpoint_and_parse(&table, &path, version).await;

        // r2 has extended_file_metadata=false, then every tombstone should be so, even r1
        assert_ne!(actions, vec![r1.clone(), r2.clone()]);
        // assert!(!actions.contains(&r1));
        // assert!(!actions.contains(&r2));
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
        let path = "./tests/data/checkpoints_tombstones/metadata_broken";
        let mut table = create_table(path, None).await;
        let r1 = remove_metadata_broken();
        let r2 = remove_metadata_false();
        let version = commit_removes(&mut table, vec![&r1, &r2]).await;
        let (schema, actions) = create_checkpoint_and_parse(&table, &path, version).await;

        // r1 extended_file_metadata=true, but the size is null.
        // We should fix this by setting extended_file_metadata=false
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
    async fn test_checkpoint_with_action_reconciliation() {
        let path = "./tests/data/checkpoints_tombstones/action_reconciliation";
        let mut table = create_table(path, None).await;

        let a1 = add(3 * 60 * 1000);
        assert_eq!(1, commit_add(&mut table, &a1).await);
        assert_eq!(table.get_files(), vec![a1.path.as_str()]);

        // Remove added file.
        let (_, opt1) = pseudo_optimize(&mut table, 5 * 59 * 1000).await;
        assert_eq!(table.get_files(), vec![opt1.path.as_str()]);
        assert_eq!(
            table
                .get_state()
                .all_tombstones()
                .iter()
                .map(|r| r.path.as_str())
                .collect::<Vec<_>>(),
            vec![a1.path.as_str()]
        );

        // Add removed file back.
        assert_eq!(3, commit_add(&mut table, &a1).await);
        assert_eq!(
            table.get_files(),
            vec![opt1.path.as_str(), a1.path.as_str()]
        );
        // tombstone is removed.
        assert_eq!(table.get_state().all_tombstones().len(), 0);
    }

    async fn create_table(
        path: &str,
        config: Option<HashMap<String, Option<String>>>,
    ) -> DeltaTable {
        let log_dir = Path::new(path).join("_delta_log");
        fs::create_dir_all(&log_dir).unwrap();
        fs_common::cleanup_dir_except(log_dir, vec![]);

        let schema = Schema::new(vec![SchemaField::new(
            "id".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        )]);

        fs_common::create_test_table(path, schema, config.unwrap_or(HashMap::new())).await
    }

    async fn pseudo_optimize(table: &mut DeltaTable, offset_millis: i64) -> (HashSet<Remove>, Add) {
        let removes: HashSet<Remove> = table
            .get_files()
            .iter()
            .map(|p| Remove {
                path: p.to_string(),
                deletion_timestamp: Some(Utc::now().timestamp_millis() - offset_millis),
                data_change: false,
                extended_file_metadata: None,
                partition_values: None,
                size: None,
                tags: None,
            })
            .collect();

        let add = Add {
            data_change: false,
            ..add(offset_millis)
        };

        let actions = removes
            .iter()
            .cloned()
            .map(Action::remove)
            .chain(std::iter::once(Action::add(add.clone())))
            .collect();

        commit_actions(table, actions).await;
        (removes, add)
    }

    fn add(offset_millis: i64) -> Add {
        Add {
            path: Uuid::new_v4().to_string(),
            size: 100,
            partition_values: Default::default(),
            partition_values_parsed: None,
            modification_time: Utc::now().timestamp_millis() - offset_millis,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        }
    }

    async fn create_checkpoint_and_parse(
        table: &DeltaTable,
        path: &str,
        version: i64,
    ) -> (HashSet<String>, Vec<Remove>) {
        checkpoints::create_checkpoint_from_table(&table)
            .await
            .unwrap();
        let cp_path = format!(
            "{}/_delta_log/0000000000000000000{}.checkpoint.parquet",
            path, version
        );
        let (schema, actions) = fs_common::read_checkpoint(&cp_path).await;

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
                Action::remove(r) => Some(r.clone()),
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

    async fn commit_add(table: &mut DeltaTable, add: &Add) -> i64 {
        commit_actions(table, vec![Action::add(add.clone())]).await
    }

    async fn commit_removes(table: &mut DeltaTable, removes: Vec<&Remove>) -> i64 {
        let vec = removes
            .iter()
            .map(|r| Action::remove((*r).clone()))
            .collect();
        commit_actions(table, vec).await
    }

    async fn commit_actions(table: &mut DeltaTable, actions: Vec<Action>) -> i64 {
        let mut tx = table.create_transaction(None);
        tx.add_actions(actions);
        tx.commit(None).await.unwrap()
    }
}
