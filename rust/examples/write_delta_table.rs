extern crate anyhow;
extern crate deltalake;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use deltalake::{commands::DeltaCommands, DeltaTable, DeltaTableConfig};
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // let name = env::args().skip(1).next();

    let table_path = std::env::current_dir()?; // .join("/data");
    let table_path = table_path.as_path().join("data");

    let backend = Box::new(deltalake::storage::file::FileStorageBackend::new(
        table_path.as_path().to_str().unwrap(),
    ));

    let mut dt = DeltaTable::new(
        table_path.as_path().to_str().unwrap(),
        backend,
        DeltaTableConfig::default(),
    )
    .unwrap();

    let mut commands = DeltaCommands::try_from_uri(dt.table_uri.to_string())
        .await
        .unwrap();

    let batch = get_record_batch();
    commands
        .write(vec![batch], None, Some(vec!["modified".to_string()]))
        .await
        .unwrap();

    dt.update().await.unwrap();
    println!("{}", dt.version);

    Ok(())
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
