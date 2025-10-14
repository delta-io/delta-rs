use std::{path::PathBuf, time::Instant};

use clap::{Parser, ValueEnum};

use delta_benchmarks::{
    merge_delete, merge_insert, merge_upsert, prepare_source_and_table, MergeOp, MergePerfParams,
};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OpKind {
    Upsert,
    Delete,
    Insert,
}

#[derive(Parser, Debug)]
#[command(about = "Run a merge benchmark with configurable parameters")]
struct Cli {
    /// Operation to benchmark
    #[arg(value_enum)]
    op: OpKind,

    /// Fraction of rows that match an existing key (0.0-1.0)
    #[arg(long, default_value_t = 0.01)]
    matched: f32,

    /// Fraction of rows that do not match (0.0-1.0)
    #[arg(long, default_value_t = 0.10)]
    not_matched: f32,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let op_fn: MergeOp = match cli.op {
        OpKind::Upsert => merge_upsert,
        OpKind::Delete => merge_delete,
        OpKind::Insert => merge_insert,
    };

    let params = MergePerfParams {
        sample_matched_rows: cli.matched,
        sample_not_matched_rows: cli.not_matched,
    };

    let tmp_dir = tempfile::tempdir().expect("create tmp dir");

    let parquet_dir = PathBuf::from(
        std::env::var("TPCDS_PARQUET_DIR")
            .unwrap_or_else(|_| "crates/benchmarks/data/tpcds_parquet".to_string()),
    );

    let (source, table) = prepare_source_and_table(&params, &tmp_dir, &parquet_dir)
        .await
        .expect("prepare inputs");

    let start = Instant::now();
    let (_table, metrics) = op_fn(source, table)
        .expect("build merge")
        .await
        .expect("execute merge");
    let elapsed = start.elapsed();

    println!("duration_ms={} metrics={:?}", elapsed.as_millis(), metrics)
}
