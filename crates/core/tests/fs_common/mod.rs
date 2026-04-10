use chrono::Utc;
use deltalake_core::DeltaTable;
use deltalake_core::kernel::transaction::CommitBuilder;
use deltalake_core::kernel::{
    Action, Add, DataType, PrimitiveType, StructField, StructType, Version,
};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
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
    assert!(path.starts_with("../test/tests/data"));
    std::fs::create_dir_all(path).unwrap();
    std::fs::remove_dir_all(path).unwrap();
    std::fs::create_dir_all(path).unwrap();
    let schema: StructType = serde_json::from_value(schema).unwrap();
    let config: HashMap<String, Option<String>> = serde_json::from_value(config).unwrap();
    create_test_table(path, schema, partition_columns, config).await
}

pub async fn create_test_table(
    path: &str,
    schema: StructType,
    partition_columns: Vec<&str>,
    config: HashMap<String, Option<String>>,
) -> DeltaTable {
    CreateBuilder::new()
        .with_location(path)
        .with_table_name("test-table")
        .with_comment("A table for running tests")
        .with_columns(schema.fields().cloned())
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

    let schema = StructType::try_new(vec![StructField::new(
        "id".to_string(),
        DataType::Primitive(PrimitiveType::Integer),
        true,
    )])
    .unwrap();

    create_test_table(path, schema, Vec::new(), config.unwrap_or_default()).await
}

pub fn add(offset_millis: i64) -> Add {
    Add {
        path: Uuid::new_v4().to_string(),
        size: 100,
        partition_values: Default::default(),
        modification_time: Utc::now().timestamp_millis() - offset_millis,
        data_change: true,
        stats: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    }
}

pub async fn commit_add(table: &mut DeltaTable, add: &Add) -> Version {
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    commit_actions(table, vec![Action::Add(add.clone())], operation).await
}

pub async fn commit_actions(
    table: &mut DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
) -> Version {
    let version = CommitBuilder::default()
        .with_actions(actions)
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store().clone(),
            operation,
        )
        .await
        .unwrap()
        .version();
    table
        .update_state()
        .await
        .expect("Failed to commit_actions: {actions:?}");
    version
}
