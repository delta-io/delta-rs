#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let table = deltalake::open_table("../rust/tests/data/cdf-table").await?;
    let ops = DeltaOps(table);
    let cdf = ops.load_cdf()
        .with_starting_version(0)
        .with_ending_version(4)
        .build()
        .await?;

    arrow_cast::pretty::print_batches(&cdf)?;

    Ok(())
}