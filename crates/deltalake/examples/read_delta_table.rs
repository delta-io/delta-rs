use std::path::Path;
use url::Url;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    let table_path = "../test/tests/data/delta-0.8.0";
    let abs_path = std::fs::canonicalize(Path::new(table_path))?;
    let table_url = Url::from_file_path(&abs_path).map_err(|_| "Failed to convert path to URL")?;
    let table = deltalake::open_table(table_url).await?;
    println!("{table}");
    Ok(())
}
