use std::{path::Path, sync::Arc};
use url::Url;

use arrow_array::{Array, Int32Array, RecordBatch};
use deltalake::{writer::{DeltaWriter, RecordBatchWriter}};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // --8<-- [start:add_constraint]
    let path = Path::new("../rust/tests/data/simple_table").canonicalize().unwrap();
    let delta_path = Url::from_directory_path(path).unwrap();
    let table = deltalake::open_table(delta_path).await?;
    
    table.add_constraint().with_constraint("id_gt_0", "id > 0").await?;
    // --8<-- [end:add_constraint]

    // --8<-- [start:add_data]
    let mut table = deltalake::open_table(delta_path).await?;
    let schema = table.snapshot()?.snapshot().arrow_schema();
    let invalid_values: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int32Array::from(vec![-10]))
    ];
    let batch = RecordBatch::try_new(schema, invalid_values)?;
    let mut writer = RecordBatchWriter::for_table(&table)?;
    writer.write(batch).await?;
    writer.flush_and_commit(&mut table).await?;
    // --8<-- [end:add_data]

    Ok(())
}