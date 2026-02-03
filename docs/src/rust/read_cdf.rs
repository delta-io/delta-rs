#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let table = deltalake::open_table("/abs/tmp/some-table").await?;
    let ctx = SessionContext::new();
    let cdf = table.scan_cdf()
        .with_starting_version(0)
        .with_ending_version(4)
        .build(&ctx.state(), None)
        .await?;

    let batches = collect_batches(
        cdf.properties().output_partitioning().partition_count(),
        cdf.as_ref(),
        ctx,
    ).await?;
    arrow_cast::pretty::print_batches(&batches)?;


    Ok(())
}

async fn collect_batches(
    num_partitions: usize,
    stream: &dyn ExecutionPlan,
    ctx: SessionContext,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let mut batches = vec![];
    for p in 0..num_partitions {
        let data: Vec<RecordBatch> =
            collect_sendable_stream(stream.execute(p, ctx.task_ctx())?).await?;
        batches.extend_from_slice(&data);
    }
    Ok(batches)
}
