use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:get_table_info]
    let delta_path = Url::from_directory_path("/abs/path/or/build/url/to/simple_table").unwrap();
    let table = deltalake::open_table(delta_path).await.unwrap();
    println!("Version: {:?}", table.version());
    println!("Files: {:?}", table.get_file_uris().unwrap().collect::<Vec<_>>());
    // --8<-- [end:get_table_info]

    Ok(())
}