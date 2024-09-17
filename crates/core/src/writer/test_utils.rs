//! Utilities for writing unit tests

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray, StructArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use arrow_select::take::take;

use crate::kernel::{DataType as DeltaDataType, Metadata, PrimitiveType, StructField, StructType};
use crate::operations::create::CreateBuilder;
use crate::operations::DeltaOps;
use crate::{DeltaTable, DeltaTableBuilder, TableProperty};

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

pub fn get_delta_schema() -> StructType {
    StructType::new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "modified".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ])
}

pub fn get_delta_metadata(partition_cols: &[String]) -> Metadata {
    let table_schema = get_delta_schema();
    Metadata::try_new(table_schema, partition_cols.to_vec(), HashMap::new()).unwrap()
}

pub fn get_record_batch_with_nested_struct() -> RecordBatch {
    let nested_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::Int64,
        true,
    )]));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
        Field::new("modified", DataType::Utf8, true),
        Field::new(
            "nested",
            DataType::Struct(nested_schema.fields().clone()),
            true,
        ),
    ]));

    let count_array = Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(9),
        Some(10),
        Some(11),
    ]);
    let id_array = StringArray::from(vec![
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
    let value_array = Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(9),
        Some(10),
        Some(11),
    ]);
    let modified_array = StringArray::from(vec![
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

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(value_array),
            Arc::new(modified_array),
            Arc::new(StructArray::from(
                RecordBatch::try_new(nested_schema, vec![Arc::new(count_array)]).unwrap(),
            )),
        ],
    )
    .unwrap()
}

pub fn get_delta_schema_with_nested_struct() -> StructType {
    StructType::new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "modified".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            String::from("nested"),
            DeltaDataType::Struct(Box::new(StructType::new(vec![StructField::new(
                String::from("count"),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            )]))),
            true,
        ),
    ])
}

pub async fn setup_table_with_configuration(
    key: TableProperty,
    value: Option<impl Into<String>>,
) -> DeltaTable {
    let table_schema = get_delta_schema();
    DeltaOps::new_in_memory()
        .create()
        .with_columns(table_schema.fields().cloned())
        .with_configuration_property(key, value)
        .await
        .expect("Failed to create table")
}

pub fn create_bare_table() -> DeltaTable {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();
    DeltaTableBuilder::from_uri(table_path.to_str().unwrap())
        .build()
        .unwrap()
}

pub async fn create_initialized_table(partition_cols: &[String]) -> DeltaTable {
    let table_schema = get_delta_schema();
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();

    CreateBuilder::new()
        .with_location(table_path.to_str().unwrap())
        .with_table_name("test-table")
        .with_comment("A table for running tests")
        .with_columns(table_schema.fields().cloned())
        .with_partition_columns(partition_cols)
        .await
        .unwrap()
}

#[cfg(feature = "datafusion")]
pub mod datafusion {
    use crate::operations::DeltaOps;
    use crate::writer::SaveMode;
    use crate::DeltaTable;
    use arrow_array::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    pub async fn get_data(table: &DeltaTable) -> Vec<RecordBatch> {
        let table =
            DeltaTable::new_with_state(table.log_store.clone(), table.snapshot().unwrap().clone());
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table)).unwrap();
        ctx.sql("select * from test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    pub async fn get_data_sorted(table: &DeltaTable, columns: &str) -> Vec<RecordBatch> {
        let table = DeltaTable::new_with_state(
            table.log_store.clone(),
            table.state.as_ref().unwrap().clone(),
        );
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table)).unwrap();
        ctx.sql(&format!(
            "select {} from test order by {}",
            columns, columns
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
    }

    pub async fn write_batch(table: DeltaTable, batch: RecordBatch) -> DeltaTable {
        DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("Failed to append")
    }
}
