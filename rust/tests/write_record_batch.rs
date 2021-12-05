use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use deltalake::{
    action::Protocol,
    schema::{Schema, SchemaDataType, SchemaField},
    write::handlers::record_batch::*,
    write::*,
    DeltaTable, DeltaTableConfig, DeltaTableError, DeltaTableMetaData,
};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn write_and_commit_no_partition() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_path = temp_dir.path();

    let mut table = create_temp_table(table_path.to_str().unwrap(), vec![])
        .await
        .unwrap();
    let message_handler = Arc::new(RecordBatchHandler {});
    let mut writer = DataWriter::for_table(&table, message_handler, HashMap::new()).unwrap();
    let data = get_record_batch();

    writer
        .write_and_commit(&mut table, vec![data], None)
        .await
        .unwrap();

    let log_path = table_path.join("_delta_log/00000000000000000001.json");
    assert!(log_path.exists());
    assert!(log_path.is_file());
}

fn get_record_batch() -> RecordBatch {
    let int_values = Int32Array::from(vec![42, 44, 46, 48, 50, 52, 54, 56, 148, 150, 152]);
    let id_values = StringArray::from(vec!["A", "B", "C", "D", "E", "F", "G", "H", "D", "E", "F"]);
    let modified_values = StringArray::from(vec![
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-02",
        "2021-02-02",
        "2021-02-02",
    ]);

    // expected results from parsing json payload
    let schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
        Field::new("modified", DataType::Utf8, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_values),
            Arc::new(int_values),
            Arc::new(modified_values),
        ],
    )
    .unwrap()
}

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
