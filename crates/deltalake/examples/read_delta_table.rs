use url::Url;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    let table_path = "../test/tests/data/delta-0.8.0";
    let table_url = Url::from_directory_path(table_path).unwrap();
    let table = deltalake::open_table(table_url).await?;
    println!("{table}");
    Ok(())
}
