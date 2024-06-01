#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    let table_path = "../test/tests/data/delta-0.8.0";
    let table = deltalake::open_table(table_path).await?;
    println!("{table}");
    Ok(())
}
