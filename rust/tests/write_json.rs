use deltalake::{
    action::Protocol,
    schema::{Schema, SchemaDataType, SchemaField},
    write::json::*,
    write::*,
    DeltaTable, DeltaTableConfig, DeltaTableError, DeltaTableMetaData,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

async fn create_temp_table(
    table_uri: &str,
    partition_cols: Vec<String>,
) -> Result<DeltaTable, DeltaTableError> {
    let table_schema = Schema::new(vec![
        SchemaField::new(
            "id".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "value".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "modified".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
    ]);

    let table_metadata = DeltaTableMetaData::new(
        Some("Test Table Create".to_string()),
        Some("A table for testing.".to_string()),
        None,
        table_schema,
        partition_cols,
        HashMap::new(),
    );

    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    };

    let backend = Box::new(deltalake::storage::file::FileStorageBackend::new(table_uri));

    let mut dt = DeltaTable::new(
        table_uri,
        backend,
        DeltaTableConfig {
            require_tombstones: true,
        },
    )
    .unwrap();

    let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    commit_info.insert(
        "userName".to_string(),
        serde_json::Value::String("test user".to_string()),
    );
    // Action
    dt.create(table_metadata.clone(), protocol.clone(), Some(commit_info))
        .await
        .unwrap();

    Ok(dt)
}

fn json_data_part_1() -> Vec<Value> {
    vec![
        json!({ "id": "A", "value": 42, "modified": "2021-02-01" }),
        json!({ "id": "B", "value": 44, "modified": "2021-02-01" }),
        json!({ "id": "C", "value": 46, "modified": "2021-02-01" }),
        json!({ "id": "D", "value": 48, "modified": "2021-02-01" }),
        json!({ "id": "E", "value": 50, "modified": "2021-02-01" }),
        json!({ "id": "F", "value": 52, "modified": "2021-02-01" }),
        json!({ "id": "G", "value": 54, "modified": "2021-02-01" }),
        json!({ "id": "H", "value": 56, "modified": "2021-02-01" }),
    ]
}

fn json_data_part_2() -> Vec<Value> {
    vec![
        json!({ "id": "D", "value": 148, "modified": "2021-02-02" }),
        json!({ "id": "E", "value": 150, "modified": "2021-02-02" }),
        json!({ "id": "F", "value": 152, "modified": "2021-02-02" }),
    ]
}
