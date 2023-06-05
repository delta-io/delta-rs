#![allow(deprecated)]
//! Utilities for writing unit tests
use crate::{
    action::Protocol, schema::Schema, DeltaTable, DeltaTableBuilder, DeltaTableMetaData,
    SchemaDataType, SchemaField,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{Int32Array, StringArray, UInt32Array},
    compute::take,
    datatypes::Schema as ArrowSchema,
};
use std::collections::HashMap;
use std::sync::Arc;

pub type TestResult = Result<(), Box<dyn std::error::Error + 'static>>;

pub fn get_record_batch(part: Option<String>, with_null: bool) -> RecordBatch {
    let (base_int, base_str, base_mod) = if with_null {
        data_with_null()
    } else {
        data_without_null()
    };

    let indices = match &part {
        Some(key) if key == "modified=2021-02-01" => {
            UInt32Array::from(vec![3, 4, 5, 6, 7, 8, 9, 10])
        }
        Some(key) if key == "modified=2021-02-01/id=A" => UInt32Array::from(vec![4, 5, 6, 9, 10]),
        Some(key) if key == "modified=2021-02-01/id=B" => UInt32Array::from(vec![3, 7, 8]),
        Some(key) if key == "modified=2021-02-02" => UInt32Array::from(vec![0, 1, 2]),
        Some(key) if key == "modified=2021-02-02/id=A" => UInt32Array::from(vec![0, 2]),
        Some(key) if key == "modified=2021-02-02/id=B" => UInt32Array::from(vec![1]),
        _ => UInt32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    };

    let int_values = take(&base_int, &indices, None).unwrap();
    let str_values = take(&base_str, &indices, None).unwrap();
    let mod_values = take(&base_mod, &indices, None).unwrap();

    let schema = get_arrow_schema(&part);

    match &part {
        Some(key) if key.contains("/id=") => {
            RecordBatch::try_new(schema, vec![int_values]).unwrap()
        }
        Some(_) => RecordBatch::try_new(schema, vec![str_values, int_values]).unwrap(),
        _ => RecordBatch::try_new(schema, vec![str_values, int_values, mod_values]).unwrap(),
    }
}

pub fn get_arrow_schema(part: &Option<String>) -> Arc<ArrowSchema> {
    match part {
        Some(key) if key.contains("/id=") => Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )])),
        Some(_) => Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ])),
        _ => Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
        ])),
    }
}

fn data_with_null() -> (Int32Array, StringArray, StringArray) {
    let base_int = Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        None,
        Some(7),
        Some(8),
        Some(9),
        Some(10),
        Some(11),
    ]);
    let base_str = StringArray::from(vec![
        Some("A"),
        Some("B"),
        None,
        Some("B"),
        Some("A"),
        Some("A"),
        None,
        None,
        Some("B"),
        Some("A"),
        Some("A"),
    ]);
    let base_mod = StringArray::from(vec![
        Some("2021-02-02"),
        Some("2021-02-02"),
        Some("2021-02-02"),
        Some("2021-02-01"),
        Some("2021-02-01"),
        Some("2021-02-01"),
        Some("2021-02-01"),
        Some("2021-02-01"),
        Some("2021-02-01"),
        Some("2021-02-01"),
        Some("2021-02-01"),
    ]);

    (base_int, base_str, base_mod)
}

fn data_without_null() -> (Int32Array, StringArray, StringArray) {
    let base_int = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let base_str = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);
    let base_mod = StringArray::from(vec![
        "2021-02-02",
        "2021-02-02",
        "2021-02-02",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
        "2021-02-01",
    ]);

    (base_int, base_str, base_mod)
}

pub fn get_delta_schema() -> Schema {
    Schema::new(vec![
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
    ])
}

pub fn get_delta_metadata(partition_cols: &[String]) -> DeltaTableMetaData {
    let table_schema = get_delta_schema();
    DeltaTableMetaData::new(
        None,
        None,
        None,
        table_schema,
        partition_cols.to_vec(),
        HashMap::new(),
    )
}

pub fn create_bare_table() -> DeltaTable {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();
    DeltaTableBuilder::from_uri(table_path.to_str().unwrap())
        .build()
        .unwrap()
}

pub async fn create_initialized_table(partition_cols: &[String]) -> DeltaTable {
    let mut table = create_bare_table();
    let table_schema = get_delta_schema();

    let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    commit_info.insert(
        "userName".to_string(),
        serde_json::Value::String("test user".to_string()),
    );

    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 1,
    };

    let metadata = DeltaTableMetaData::new(
        None,
        None,
        None,
        table_schema,
        partition_cols.to_vec(),
        HashMap::new(),
    );

    table
        .create(metadata, protocol, Some(commit_info), None)
        .await
        .unwrap();

    table
}

#[cfg(feature = "datafusion")]
pub mod datafusion {
    use crate::DeltaTable;
    use arrow_array::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    pub async fn get_data(table: &DeltaTable) -> Vec<RecordBatch> {
        let table = DeltaTable::new_with_state(table.object_store(), table.state.clone());
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table)).unwrap();
        ctx.sql("select * from test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }
}
