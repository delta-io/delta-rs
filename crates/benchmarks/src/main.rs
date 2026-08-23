use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand, ValueEnum};

use delta_benchmarks::{
    default_fixture_dir, generate_vacuum_fixture, merge_case_by_name, merge_case_names,
    merge_delete, merge_insert, merge_noop_heavy_upsert, merge_upsert,
    open_vacuum_fixture_with_list_latency, prepare_source_and_table, register_tpcds_tables,
    run_smoke_once, run_vacuum_full_dry_run, tpcds_queries, tpcds_query, MergeOp, MergePerfParams,
    MergeTestCase, SmokeParams, VacuumFixtureParams, VacuumScanMode,
};
use deltalake_core::ensure_table_uri;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OpKind {
    Upsert,
    NoopHeavyUpsert,
    Delete,
    Insert,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum VacuumScanCli {
    Parallel,
    Flat,
}

impl From<VacuumScanCli> for VacuumScanMode {
    fn from(value: VacuumScanCli) -> Self {
        match value {
            VacuumScanCli::Parallel => VacuumScanMode::Parallel,
            VacuumScanCli::Flat => VacuumScanMode::Flat,
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "Run delta-rs benchmarks")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Path to the parquet directory
    #[arg(
        long,
        env = "TPCDS_PARQUET_DIR",
        default_value = "crates/benchmarks/data/tpcds_parquet"
    )]
    parquet_dir: PathBuf,
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

    /// Inspect the bundled TPC-DS queries
    Tpcds {
        /// Query identifier to print (for example `q1`)
        #[arg(long)]
        case: Option<String>,

        /// List all available query identifiers
        #[arg(long, conflicts_with = "case")]
        list: bool,

        /// Run the query and measure execution time
        #[arg(long, conflicts_with = "list", requires = "case")]
        run: bool,
    },

    /// Generate a multi-level vacuum listing fixture (once) for reuse by benches
    GenerateVacuumFixture {
        /// Output directory for the Delta table fixture
        #[arg(long, default_value_os_t = default_fixture_dir())]
        out: PathBuf,

        /// Number of day partitions (one commit per day)
        #[arg(long, default_value_t = VacuumFixtureParams::default().days)]
        days: usize,

        /// Number of group values per day
        #[arg(long, default_value_t = VacuumFixtureParams::default().groups)]
        groups: usize,

        /// Rows per (date, group) partition
        #[arg(long, default_value_t = VacuumFixtureParams::default().rows_per_partition)]
        rows_per_partition: usize,

        /// Rebuild even if a fixture already exists at --out
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    /// Dry-run full vacuum against a generated fixture (for profiling / sanity)
    Vacuum {
        /// Fixture directory (must already exist unless --generate is set)
        #[arg(
            long,
            env = "VACUUM_BENCH_FIXTURE",
            default_value_os_t = default_fixture_dir()
        )]
        fixture: PathBuf,

        /// Scan strategy
        #[arg(long, value_enum, default_value_t = VacuumScanCli::Parallel)]
        scan: VacuumScanCli,

        /// Optional LIST concurrency override (parallel path)
        #[arg(long)]
        scan_concurrency: Option<usize>,

        /// Artificial LIST latency in milliseconds (simulates cloud RTT on local FS)
        #[arg(long, env = "VACUUM_BENCH_LIST_LATENCY_MS", default_value_t = 0)]
        list_latency_ms: u64,

        /// Generate the default-sized fixture at --fixture if missing
        #[arg(long, default_value_t = false)]
        generate: bool,

        /// Number of timed dry-run samples after open (same name as Divan `--sample-count`)
        #[arg(long = "sample-count", default_value_t = 1)]
        sample_count: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let parquet_dir = cli.parquet_dir;

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

                run_merge_case(merge_case, &parquet_dir).await?;
            } else {
                let op = op.ok_or_else(|| {
                    anyhow::anyhow!(
                        "specify an operation (upsert/noop-heavy-upsert/delete/insert) or provide --case"
                    )
                })?;

                let op_fn: MergeOp = match op {
                    OpKind::Upsert => merge_upsert,
                    OpKind::NoopHeavyUpsert => merge_noop_heavy_upsert,
                    OpKind::Delete => merge_delete,
                    OpKind::Insert => merge_insert,
                };

                let params = MergePerfParams {
                    sample_matched_rows: matched,
                    sample_not_matched_rows: not_matched,
                };

                run_merge_with_params(op_fn, &params, &parquet_dir).await?;
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
        Command::Tpcds { case, list, run } => {
            if list {
                for name in tpcds_queries().keys() {
                    println!("{name}");
                }
            } else if let Some(name) = case {
                let sql = match tpcds_queries().get(name.as_str()) {
                    Some(sql) => sql,
                    None => anyhow::bail!(
                        "unknown TPC-DS query '{name}'. Available: {:?}",
                        tpcds_queries().keys(),
                    ),
                };

                if run {
                    run_tpcds_query(&name, &parquet_dir).await?;
                } else {
                    println!("-- {name}\n{}", sql.trim());
                }
            } else {
                anyhow::bail!("specify --case <id> or --list");
            }
        }
        Command::GenerateVacuumFixture {
            out,
            days,
            groups,
            rows_per_partition,
            force,
        } => {
            let params = VacuumFixtureParams {
                days,
                groups,
                rows_per_partition,
            };
            let start = Instant::now();
            let path = generate_vacuum_fixture(&out, &params, force).await?;
            println!(
                "vacuum_fixture_path={} days={} groups={} partitions_approx={} generate_duration_ms={}",
                path.display(),
                params.days,
                params.groups,
                params.days.saturating_mul(params.groups),
                start.elapsed().as_millis()
            );
        }
        Command::Vacuum {
            fixture,
            scan,
            scan_concurrency,
            list_latency_ms,
            generate,
            sample_count,
        } => {
            if generate {
                let params = VacuumFixtureParams::default();
                generate_vacuum_fixture(&fixture, &params, false).await?;
            }
            let list_latency = Duration::from_millis(list_latency_ms);
            let table = open_vacuum_fixture_with_list_latency(&fixture, list_latency).await?;
            let mode = VacuumScanMode::from(scan);
            for i in 0..sample_count {
                let start = Instant::now();
                let (_table, metrics) =
                    run_vacuum_full_dry_run(&table, mode, scan_concurrency).await?;
                println!(
                    "sample={} scan={} list_latency_ms={} vacuum_duration_ms={} files_deleted={}",
                    i,
                    mode.name(),
                    list_latency_ms,
                    start.elapsed().as_millis(),
                    metrics.files_deleted.len()
                );
            }
        }
    }

    Ok(())
}

async fn run_merge_with_params(
    op_fn: MergeOp,
    params: &MergePerfParams,
    parquet_dir: &Path,
) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;

    let (source, table) = prepare_source_and_table(params, &tmp_dir, parquet_dir).await?;

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

async fn run_merge_case(case: &MergeTestCase, parquet_dir: &Path) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;
    let (source, table) = prepare_source_and_table(&case.params, &tmp_dir, parquet_dir).await?;

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

async fn run_tpcds_query(query_name: &str, parquet_dir: &Path) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir()?;

    println!("Loading TPC-DS tables from {}...", parquet_dir.display());
    let setup_start = Instant::now();
    let ctx = register_tpcds_tables(&tmp_dir, parquet_dir).await?;
    let setup_elapsed = setup_start.elapsed();
    println!("Setup completed in {} ms", setup_elapsed.as_millis());

    let sql = tpcds_query(query_name)
        .ok_or_else(|| anyhow::anyhow!("query '{}' not found", query_name))?;

    println!("\nExecuting query {}...", query_name);
    let start = Instant::now();
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    let elapsed = start.elapsed();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    println!(
        "query={} duration_ms={} rows={}",
        query_name,
        elapsed.as_millis(),
        total_rows
    );

    Ok(())
}
