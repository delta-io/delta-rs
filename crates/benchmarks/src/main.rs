use std::{path::PathBuf, time::Instant};

use clap::{Parser, Subcommand, ValueEnum};

use delta_benchmarks::{
    merge_case_by_name, merge_case_names, merge_delete, merge_insert, merge_upsert,
    prepare_source_and_table, run_smoke_once, MergeOp, MergePerfParams, MergeTestCase, SmokeParams,
};
use deltalake_core::ensure_table_uri;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OpKind {
    Upsert,
    Delete,
    Insert,
}

#[derive(Parser, Debug)]
#[command(about = "Run delta-rs benchmarks")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a merge benchmark with configurable parameters
    Merge {
        /// Operation to benchmark
        #[arg(value_enum)]
        op: Option<OpKind>,

        /// Fraction of rows that match an existing key (0.0-1.0)
        #[arg(long, default_value_t = 0.01)]
        matched: f32,

        /// Fraction of rows that do not match (0.0-1.0)
        #[arg(long, default_value_t = 0.10)]
        not_matched: f32,

        /// Named test case to run (overrides manual parameters)
        #[arg(long)]
        case: Option<String>,
    },

    /// Run the smoke workload to validate delta-rs read/write operations
    Smoke {
        /// Number of rows to write into the smoke table
        #[arg(long, default_value_t = 2)]
        rows: usize,

        /// Optional table path to reuse for the smoke run (defaults to a temporary directory)
        #[arg(long)]
        table_path: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Merge {
            op,
            matched,
            not_matched,
            case,
        } => {
            if let Some(case_name) = case.as_deref() {
                let merge_case = merge_case_by_name(case_name).ok_or_else(|| {
                    anyhow::anyhow!(
                        "unknown merge case '{}'. Available cases: {}",
                        case_name,
                        merge_case_names().join(", ")
                    )
                })?;

                run_merge_case(merge_case).await?;
            } else {
                let op = op.ok_or_else(|| {
                    anyhow::anyhow!("specify an operation (upsert/delete/insert) or provide --case")
                })?;

                let op_fn: MergeOp = match op {
                    OpKind::Upsert => merge_upsert,
                    OpKind::Delete => merge_delete,
                    OpKind::Insert => merge_insert,
                };

                let params = MergePerfParams {
                    sample_matched_rows: matched,
                    sample_not_matched_rows: not_matched,
                };

                run_merge_with_params(op_fn, &params).await?;
            }
        }
        Command::Smoke { rows, table_path } => {
            let params = SmokeParams { rows };
            let (table_url, _guard) = match table_path {
                Some(path) => (ensure_table_uri(path.to_string_lossy().as_ref())?, None),
                None => {
                    let dir = tempfile::tempdir()?;
                    let url = ensure_table_uri(dir.path().to_string_lossy().as_ref())?;
                    (url, Some(dir))
                }
            };

            let start = Instant::now();
            run_smoke_once(&table_url, &params).await?;
            let elapsed = start.elapsed();

            println!(
                "smoke_duration_ms={} table_uri={}",
                elapsed.as_millis(),
                table_url
            );
        }
    }

    Ok(())
}

async fn run_merge_with_params(op_fn: MergeOp, params: &MergePerfParams) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let parquet_dir = PathBuf::from(
        std::env::var("TPCDS_PARQUET_DIR")
            .unwrap_or_else(|_| "crates/benchmarks/data/tpcds_parquet".to_string()),
    );

    let (source, table) = prepare_source_and_table(params, &tmp_dir, &parquet_dir).await?;

    let start = Instant::now();
    let (_table, metrics) = op_fn(source, table)?.await?;
    let elapsed = start.elapsed();

    println!(
        "merge_duration_ms={} metrics={:?}",
        elapsed.as_millis(),
        metrics
    );

    Ok(())
}

async fn run_merge_case(case: &MergeTestCase) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let parquet_dir = PathBuf::from(
        std::env::var("TPCDS_PARQUET_DIR")
            .unwrap_or_else(|_| "crates/benchmarks/data/tpcds_parquet".to_string()),
    );

    let (source, table) = prepare_source_and_table(&case.params, &tmp_dir, &parquet_dir).await?;

    let start = Instant::now();
    let (_table, metrics) = case.execute(source, table).await?;
    case.validate(&metrics)?;
    let elapsed = start.elapsed();

    println!(
        "merge_case={} merge_duration_ms={} metrics={:?}",
        case.name,
        elapsed.as_millis(),
        metrics
    );

    Ok(())
}
