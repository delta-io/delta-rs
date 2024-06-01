use deltalake::arrow::record_batch::RecordBatch;
use deltalake::operations::collect_sendable_stream;
use deltalake::DeltaOps;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Create a delta operations client pointing at an un-initialized location.
    let ops = if let Ok(table_uri) = std::env::var("TABLE_URI") {
        DeltaOps::try_from_uri(table_uri).await?
    } else {
        DeltaOps::try_from_uri("../test/tests/data/delta-0.8.0").await?
    };

    let (_table, stream) = ops.load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{:?}", data);

    Ok(())
}
