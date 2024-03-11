use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // --8<-- [start:add_constraint]
    let table = deltalake::open_table("../rust/tests/data/simple_table").await?;
    let ops = DeltaOps(table);
    ops.with_constraint("id_gt_0", "id > 0").await?;
    // --8<-- [end:add_constraint]

    // --8<-- [start:add_data]
    let table = deltalake::open_table("../rust/tests/data/simple_table").await?;
    let schema = table.get_state().arrow_schema()?;
    let invalid_values: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int32Array::from(vec![-10]))
    ];
    let batch = RecordBatch::try_new(schema, invalid_values)?;
    table.write(WriteData::Vecs(vec![batch])).await?;
    // --8<-- [end:add_data]

    Ok(())
}