use deltalake::arrow::record_batch::RecordBatch;
use deltalake::operations::collect_sendable_stream;
use deltalake::{DeltaTable, DeltaTableError};
use url::Url;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Create a delta operations client pointing at an un-initialized location.
    let ops = if let Ok(table_uri) = std::env::var("TABLE_URI") {
        let table_url = Url::parse(&table_uri)
            .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
        DeltaTable::try_from_url(table_url).await?
    } else {
        let table_url = Url::from_directory_path(
            std::path::Path::new("../test/tests/data/delta-0.8.0")
                .canonicalize()
                .unwrap(),
        )
        .unwrap();
        DeltaTable::try_from_url(table_url).await?
    };

    let (_table, stream) = ops.scan_table().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{data:?}");

    Ok(())
}
