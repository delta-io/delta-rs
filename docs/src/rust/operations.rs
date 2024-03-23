use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:replace_where]
    // Assuming there is already a table in this location with some records where `id = '1'` which we want to overwrite
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use deltalake::datafusion::logical_expr::{col, lit};
    use deltalake::protocol::SaveMode;
    use deltalake::DeltaOps;

    let schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]);

    let data = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["1", "1"])),
            Arc::new(arrow::array::Int32Array::from(vec![11, 12])),
        ],
    )
    .unwrap();

    let table = deltalake::open_table("/tmp/my_table").await.unwrap();
    let _table = DeltaOps(table)
        .write(vec![data])
        .with_save_mode(SaveMode::Overwrite)
        .with_replace_where(col("id").eq(lit("1")))
        .await
        .unwrap();
    // --8<-- [end:replace_where]

    Ok(())
}