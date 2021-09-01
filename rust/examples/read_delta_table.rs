extern crate anyhow;
extern crate deltalake;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let table_path = "./tests/data/delta-0.8.0";
    let table = deltalake::open_table(table_path).await?;
    println!("{}", table);
    Ok(())
}
