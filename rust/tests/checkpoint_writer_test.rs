use chrono::Utc;
use deltalake::action::*;
use deltalake::*;
use maplit::hashmap;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

// NOTE: The below is a useful external command for inspecting the written checkpoint schema visually:
// parquet-tools inspect tests/data/checkpoints/_delta_log/00000000000000000005.checkpoint.parquet

// TODO Add Remove actions to checkpoints tests as well!
#[tokio::test]
async fn write_simple_checkpoint() {
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

mod fs_common;

#[tokio::test]
async fn test_checkpoints_with_tombstones() {
    let main_branch = false;
    if main_branch {
        test_checkpoints_with_tombstones_main().await
    } else {
        test_checkpoints_with_tombstones_map_support().await
    }
}

async fn test_checkpoints_with_tombstones_main() {}

async fn test_checkpoints_with_tombstones_map_support() {
    let path = "./tests/data/checkpoints_rw";
    let log_dir = Path::new(path).join("_delta_log");
    fs::create_dir_all(&log_dir).unwrap();
    fs_common::cleanup_dir_except(log_dir, vec![]);

    let schema = Schema::new(vec![SchemaField::new(
        "id".to_string(),
        SchemaDataType::primitive("integer".to_string()),
        true,
        HashMap::new(),
    )]);
    let config = hashmap! {
        delta_config::TOMBSTONE_RETENTION.key.clone() => Some("interval 1 minute".to_string())
    };
    let mut table = fs_common::create_test_table(path, schema, config).await;

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
    assert_eq!(table.get_state().all_tombstones(), &vec![]); // stale removes are deleted from the state
}

async fn pseudo_optimize(table: &mut DeltaTable, offset_millis: i64) -> (Vec<Remove>, Add) {
    let removes: Vec<Remove> = table
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

async fn commit_add(table: &mut DeltaTable, add: &Add) -> i64 {
    commit_actions(table, vec![Action::add(add.clone())]).await
}

async fn commit_actions(table: &mut DeltaTable, actions: Vec<Action>) -> i64 {
    let mut tx = table.create_transaction(None);
    tx.add_actions(actions);
    tx.commit(None).await.unwrap()
}
