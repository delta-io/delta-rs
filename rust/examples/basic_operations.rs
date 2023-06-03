use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::operations::collect_sendable_stream;
use deltalake::{action::SaveMode, DeltaOps, SchemaDataType, SchemaField};
use std::sync::Arc;

fn get_table_columns() -> Vec<SchemaField> {
    vec![
        SchemaField::new(
            String::from("int"),
            SchemaDataType::primitive(String::from("integer")),
            false,
            Default::default(),
        ),
        SchemaField::new(
            String::from("string"),
            SchemaDataType::primitive(String::from("string")),
            true,
            Default::default(),
        ),
    ]
}

fn get_table_batches() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("int", DataType::Int32, false),
        Field::new("string", DataType::Utf8, true),
    ]));

    let int_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_values = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);

    RecordBatch::try_new(schema, vec![Arc::new(int_values), Arc::new(str_values)]).unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Create a delta operations client pointing at an un-initialized in-memory location.
    // In a production environment this would be created with "try_new" and point at
    // a real storage location.
    let ops = DeltaOps::new_in_memory();

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = ops
        .create()
        .with_columns(get_table_columns())
        .with_table_name("my_table")
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), 0);

    let batch = get_table_batches();
    let table = DeltaOps(table).write(vec![batch.clone()]).await?;

    assert_eq!(table.version(), 1);

    // To overwrite instead of append (which is the default), use `.with_save_mode`:
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    assert_eq!(table.version(), 2);

    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{:?}", data);

    Ok(())
}
