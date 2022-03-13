use chrono::Utc;
use deltalake::action::{Action, Add, Protocol, Remove};
use deltalake::{
    storage, DeltaTable, DeltaTableConfig, DeltaTableMetaData, Schema, SchemaDataType, SchemaField,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::Path;
use uuid::Uuid;

pub fn cleanup_dir_except<P: AsRef<Path>>(path: P, ignore_files: Vec<String>) {
    for p in fs::read_dir(path).unwrap() {
        if let Ok(d) = p {
            let path = d.path();
            let name = d.path().file_name().unwrap().to_str().unwrap().to_string();

            if !ignore_files.contains(&name) && !name.starts_with(".") {
                fs::remove_file(&path).unwrap();
            }
        }
    }
}

pub async fn create_table_from_json(
    path: &str,
    schema: Value,
    partition_columns: Vec<&str>,
    config: Value,
) -> DeltaTable {
    assert!(path.starts_with("./tests/data"));
    std::fs::create_dir_all(path).unwrap();
    std::fs::remove_dir_all(path).unwrap();
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
    let backend = storage::get_backend_for_uri(path).unwrap();
    let mut table = DeltaTable::new(path, backend, DeltaTableConfig::default()).unwrap();
    let partition_columns = partition_columns.iter().map(|s| s.to_string()).collect();
    let md = DeltaTableMetaData::new(None, None, None, schema, partition_columns, config);
    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    };
    table.create(md, protocol, None, None).await.unwrap();
    table
}

pub async fn read_checkpoint(path: &str) -> (Type, Vec<Action>) {
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let schema = reader.metadata().file_metadata().schema();
    let mut row_iter = reader.get_row_iter(None).unwrap();
    let mut actions = Vec::new();
    while let Some(record) = row_iter.next() {
        actions.push(Action::from_parquet_record(schema, &record).unwrap())
    }
    (schema.clone(), actions)
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

    create_test_table(path, schema, Vec::new(), config.unwrap_or(HashMap::new())).await
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
    commit_actions(table, vec![Action::add(add.clone())]).await
}

pub async fn commit_removes(table: &mut DeltaTable, removes: Vec<&Remove>) -> i64 {
    let vec = removes
        .iter()
        .map(|r| Action::remove((*r).clone()))
        .collect();
    commit_actions(table, vec).await
}

pub async fn commit_actions(table: &mut DeltaTable, actions: Vec<Action>) -> i64 {
    let mut tx = table.create_transaction(None);
    tx.add_actions(actions);
    tx.commit(None).await.unwrap()
}
