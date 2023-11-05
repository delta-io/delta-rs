#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:get_table_info]
    let table = deltalake::open_table("../rust/tests/data/simple_table").await.unwrap();
    println!("Version: {}", table.version());
    println!("Files: {}", table.get_files());
    // --8<-- [end:get_table_info]

    Ok(())
}