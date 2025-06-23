//! Example for pure-Rust: write synthetic data, optimize with global sort, and verify ordering
use std::sync::Arc;
use tempfile::TempDir;

use deltalake::DeltaOps;
use deltalake::kernel::{StructField, DataType as KernelDataType, PrimitiveType};
use deltalake::delta_datafusion::{DeltaTableProvider, DeltaScanConfig};
use deltalake::arrow::{
    array::StringArray,
    record_batch::RecordBatch,
    datatypes::{Schema, Field, DataType},
};
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::datafusion::logical_expr::ident;
use futures_util::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the Delta table
    let tmp = TempDir::new()?;
    let path = tmp.path().to_str().unwrap();
    println!("Creating Delta table at {}", path);

    // Initialize an empty Delta table (in-place)
    let ops = DeltaOps::try_from_uri(path).await?;
    let mut table = ops.create()
        .with_columns(vec![
            StructField::new(
                "objectId".to_string(),
                KernelDataType::Primitive(PrimitiveType::String),
                false,
            ),
            StructField::new(
                "dateTime".to_string(),
                KernelDataType::Primitive(PrimitiveType::String),
                false,
            ),
        ])
        .await?;

    // Define the schema for RecordBatches
    let schema = Arc::new(Schema::new(vec![
        Field::new("objectId", DataType::Utf8, false),
        Field::new("dateTime", DataType::Utf8, false),
    ]));

    // Write 5 small RecordBatches (different shapes)
    for batch in vec![
        (vec!["B","A","B","A"], vec!["2021-02-02","2021-02-01","2021-01-01","2021-03-01"]),
        (vec!["X","Y","X","Y"], vec!["2021-04-02","2021-04-01","2021-04-03","2021-04-04"]),
        (vec!["A","A","B","B"], vec!["2021-05-01","2021-05-02","2021-05-03","2021-05-04"]),
    ] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(batch.0.clone())),
                Arc::new(StringArray::from(batch.1.clone())),
            ],
        )?;
        table = DeltaOps(table)
            .write(vec![batch])
            .await?;
    }
    println!("Written {} files", table.get_files_count());

    // Optimize with global sort
    println!("Running optimize with global sort...");
    let (table_opt, metrics) = DeltaOps(table)
        .optimize()
        .with_sort_columns(&["objectId", "dateTime"])
        .await?;
    println!("Metrics: {:?}", metrics);

    // Read back via DataFusion to verify ordering
    let ctx = SessionContext::new();
    let provider = DeltaTableProvider::try_new(
        table_opt.snapshot()?.clone(),
        table_opt.log_store().clone(),
        DeltaScanConfig::default(),
    )?;
    let df = ctx.read_table(Arc::new(provider))?;
    let sorted_df = df.sort(vec![
        ident("objectId").sort(true, true),
        ident("dateTime").sort(true, true),
    ])?;
    let mut batches = sorted_df.execute_stream().await?;
    let mut prev: Option<(String, String)> = None;
    while let Some(Ok(batch)) = batches.next().await {
        let a0 = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let a1 = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            let curr = (a0.value(i).to_string(), a1.value(i).to_string());
            if let Some(p) = &prev {
                if &curr < p {
                    println!("❌ Out of order: {:?} < {:?}", curr, p);
                    return Ok(());
                }
            }
            prev = Some(curr);
        }
    }
    println!("✅ Global ordering verified");
    Ok(())
}