//! Shared end-to-end helpers used by the generic-service integration tests.
// Each test binary compiles the whole module but uses only a subset.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake_core::DeltaTable;
use deltalake_core::DeltaTableBuilder;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use url::Url;

/// Create a table and write one batch of rows, committing version 1.
/// Returns the table handle (still bound to its original operator).
pub async fn create_and_write(
    table_uri: &str,
    storage_options: HashMap<String, String>,
) -> DeltaTable {
    let columns = vec![
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true),
        StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
    ];

    let mut table = CreateBuilder::new()
        .with_location(table_uri)
        .with_storage_options(storage_options)
        .with_columns(columns)
        .await
        .expect("create table");
    assert_eq!(table.version(), Some(0), "table created at version 0");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, true),
        Field::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let mut writer = RecordBatchWriter::for_table(&table).expect("writer for table");
    writer.write(batch).await.expect("buffer batch");
    let version = writer
        .flush_and_commit(&mut table)
        .await
        .expect("flush and commit");
    assert_eq!(version, 1, "write commits version 1");

    table
}

fn assert_committed(table: &DeltaTable) {
    assert_eq!(table.version(), Some(1), "table at version 1");
    let files: Vec<_> = table.get_file_uris().unwrap().collect();
    assert_eq!(files.len(), 1, "exactly one data file was committed");
}

/// Full round-trip for persistent backends: create + write, then reopen from a
/// fresh table builder to prove the committed log is readable through the store.
pub async fn roundtrip(table_uri: &str, storage_options: HashMap<String, String>) {
    create_and_write(table_uri, storage_options.clone()).await;

    let reopened = DeltaTableBuilder::from_url(Url::parse(table_uri).unwrap())
        .unwrap()
        .with_storage_options(storage_options)
        .load()
        .await
        .expect("reopen table");
    assert_committed(&reopened);
}

/// Round-trip for the in-memory backend, whose OpenDAL operator is not shared
/// across instances: reload the same handle (retaining its operator) rather
/// than building a fresh one, which still re-reads the log from the store.
pub async fn roundtrip_same_handle(table_uri: &str, storage_options: HashMap<String, String>) {
    let mut table = create_and_write(table_uri, storage_options).await;
    table.load().await.expect("reload table");
    assert_committed(&table);
}
