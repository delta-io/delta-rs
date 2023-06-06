use chrono::Utc;
use deltalake::action::{Action, Add, DeltaOperation, Remove, SaveMode};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::commit;
use deltalake::{DeltaTable, Schema, SchemaDataType, SchemaField};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use uuid::Uuid;

pub fn cleanup_dir_except<P: AsRef<Path>>(path: P, ignore_files: Vec<String>) {
    for d in fs::read_dir(path).unwrap().flatten() {
        let path = d.path();
        let name = d.path().file_name().unwrap().to_str().unwrap().to_string();

        if !ignore_files.contains(&name) && !name.starts_with('.') {
            fs::remove_file(path).unwrap();
        }
    }
}

// TODO: should we drop this
#[allow(dead_code)]
pub async fn create_table_from_json(
    path: &str,
    schema: Value,
    partition_columns: Vec<&str>,
    config: Value,
) -> DeltaTable {
    assert!(path.starts_with("./tests/data"));
    std::fs::create_dir_all(path).unwrap();
    std::fs::remove_dir_all(path).unwrap();
    std::fs::create_dir_all(path).unwrap();
    let schema: Schema = serde_json::from_value(schema).unwrap();
    let config: HashMap<String, Option<String>> = serde_json::from_value(config).unwrap();
    create_test_table(path, schema, partition_columns, config).await
}

pub async fn create_test_table(
    path: &str,
    schema: Schema,
    partition_columns: Vec<&str>,
    config: HashMap<String, Option<String>>,
) -> DeltaTable {
    CreateBuilder::new()
        .with_location(path)
        .with_table_name("test-table")
        .with_comment("A table for running tests")
        .with_columns(schema.get_fields().clone())
        .with_partition_columns(partition_columns)
        .with_configuration(config)
        .await
        .unwrap()
}

pub async fn create_table(
    path: &str,
    config: Option<HashMap<String, Option<String>>>,
) -> DeltaTable {
    let log_dir = Path::new(path).join("_delta_log");
    fs::create_dir_all(&log_dir).unwrap();
    cleanup_dir_except(log_dir, vec![]);

    let schema = Schema::new(vec![SchemaField::new(
        "id".to_string(),
        SchemaDataType::primitive("integer".to_string()),
        true,
        HashMap::new(),
    )]);

    create_test_table(path, schema, Vec::new(), config.unwrap_or_default()).await
}

pub fn add(offset_millis: i64) -> Add {
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

pub async fn commit_add(table: &mut DeltaTable, add: &Add) -> i64 {
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    commit_actions(table, vec![Action::add(add.clone())], operation).await
}

pub async fn commit_removes(table: &mut DeltaTable, removes: Vec<&Remove>) -> i64 {
    let vec = removes
        .iter()
        .map(|r| Action::remove((*r).clone()))
        .collect();
    let operation = DeltaOperation::Delete { predicate: None };
    commit_actions(table, vec, operation).await
}

pub async fn commit_actions(
    table: &mut DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
) -> i64 {
    let version = commit(
        table.object_store().as_ref(),
        &actions,
        operation,
        &table.state,
        None,
    )
    .await
    .unwrap();
    table.update().await.unwrap();
    version
}
