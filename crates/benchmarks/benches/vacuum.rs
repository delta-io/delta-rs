//! Full-mode vacuum listing bench (parallel vs flat).
//!
//! **Prep once, then measure only** (fixture is never built inside timed samples).
//!
//! If the fixture path is missing, the bench exits and tells you to generate it.
//!
//! ## Generate fixture
//!
//! ```bash
//! # Default: crates/benchmarks/data/vacuum_bench/d30_g500 (~30 days × 500 groups)
//! cargo run --release -p delta-benchmarks -- generate-vacuum-fixture
//!
//! # Larger / custom path
//! cargo run --release -p delta-benchmarks -- generate-vacuum-fixture \
//!   --out path/to/fixture --days 90 --groups 2000
//! ```
//!
//! ## Run this vacuum bench
//!
//! ```bash
//! cargo bench -p delta-benchmarks --bench vacuum -- \
//!   --fixture path/to/fixture \
//!   --list-latency-ms 100 \
//!   --sample-count 2
//! ```
//!
//! Benches that will be run: `full_dry_run_flat`, `full_dry_run_parallel`.
//!
//! Same flags also work on the one-shot CLI:
//! `cargo run -p delta-benchmarks -- vacuum --scan flat|parallel ...`
//!
//! Env aliases: `VACUUM_BENCH_FIXTURE`, `VACUUM_BENCH_LIST_LATENCY_MS`.

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use delta_benchmarks::{
    default_fixture_dir, fixture_exists, open_vacuum_fixture_with_list_latency,
    run_vacuum_full_dry_run, VacuumScanMode,
};
use deltalake_core::DeltaTable;
use divan::{AllocProfiler, Bencher};

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

/// Shared with CLI: `--fixture` / `VACUUM_BENCH_FIXTURE`,
/// `--list-latency-ms` / `VACUUM_BENCH_LIST_LATENCY_MS`.
struct BenchConfig {
    fixture: PathBuf,
    list_latency_ms: u64,
}

static CONFIG: OnceLock<BenchConfig> = OnceLock::new();

fn config() -> &'static BenchConfig {
    CONFIG.get().expect("bench config initialized in main")
}

fn main() {
    let cfg = parse_config_and_maybe_reexec();
    let _ = CONFIG.set(cfg);

    let cfg = config();
    if !fixture_exists(&cfg.fixture) {
        eprintln!(
            "vacuum bench fixture not found at {}\n\n\
             Prepare it once, then re-run the bench:\n\n\
             cargo run --release -p delta-benchmarks -- generate-vacuum-fixture\n\n\
             Or point at an existing fixture (same flags as the vacuum CLI):\n\n\
             cargo bench -p delta-benchmarks --bench vacuum -- \\\n\
               --fixture /path/to/fixture --list-latency-ms 100\n",
            cfg.fixture.display()
        );
        std::process::exit(1);
    }

    divan::main();
}

/// Read `--fixture` / `--list-latency-ms` (CLI-compatible). If present on argv,
/// strip them and re-exec so Divan only sees its own flags/filters.
fn parse_config_and_maybe_reexec() -> BenchConfig {
    let mut fixture = std::env::var_os("VACUUM_BENCH_FIXTURE")
        .map(PathBuf::from)
        .unwrap_or_else(default_fixture_dir);
    let mut list_latency_ms = std::env::var("VACUUM_BENCH_LIST_LATENCY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let args: Vec<String> = std::env::args().collect();
    let mut stripped = Vec::with_capacity(args.len());
    stripped.push(args[0].clone());
    let mut i = 1;
    let mut saw_ours = false;

    while i < args.len() {
        let a = &args[i];
        if a == "--fixture" {
            let v = args
                .get(i + 1)
                .unwrap_or_else(|| usage_exit("--fixture needs a value"));
            fixture = PathBuf::from(v);
            saw_ours = true;
            i += 2;
        } else if let Some(v) = a.strip_prefix("--fixture=") {
            fixture = PathBuf::from(v);
            saw_ours = true;
            i += 1;
        } else if a == "--list-latency-ms" {
            let v = args
                .get(i + 1)
                .unwrap_or_else(|| usage_exit("--list-latency-ms needs a value"));
            list_latency_ms = v
                .parse()
                .unwrap_or_else(|_| usage_exit("--list-latency-ms needs an integer"));
            saw_ours = true;
            i += 2;
        } else if let Some(v) = a.strip_prefix("--list-latency-ms=") {
            list_latency_ms = v
                .parse()
                .unwrap_or_else(|_| usage_exit("--list-latency-ms needs an integer"));
            saw_ours = true;
            i += 1;
        } else if a == "--help" || a == "-h" {
            eprint_our_flags();
            stripped.push(a.clone());
            i += 1;
        } else {
            stripped.push(a.clone());
            i += 1;
        }
    }

    if saw_ours {
        // Re-exec without our flags; pass config via the same env names as CLI.
        let status = Command::new(&stripped[0])
            .args(&stripped[1..])
            .env("VACUUM_BENCH_FIXTURE", &fixture)
            .env("VACUUM_BENCH_LIST_LATENCY_MS", list_latency_ms.to_string())
            .status()
            .expect("re-exec vacuum bench");
        std::process::exit(status.code().unwrap_or(1));
    }

    BenchConfig {
        fixture,
        list_latency_ms,
    }
}

fn eprint_our_flags() {
    eprintln!(
        "Vacuum bench options (same as `delta-benchmarks vacuum` CLI):\n\
         \n\
         \t--fixture <PATH>           Fixture directory [env: VACUUM_BENCH_FIXTURE]\n\
         \t--list-latency-ms <MS>     Artificial LIST latency [env: VACUUM_BENCH_LIST_LATENCY_MS]\n"
    );
}

fn usage_exit(msg: &str) -> ! {
    eprintln!("error: {msg}");
    eprint_our_flags();
    std::process::exit(2);
}

fn shared_table() -> &'static DeltaTable {
    static TABLE: OnceLock<DeltaTable> = OnceLock::new();
    TABLE.get_or_init(|| {
        let cfg = config();
        let list_latency = Duration::from_millis(cfg.list_latency_ms);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            open_vacuum_fixture_with_list_latency(&cfg.fixture, list_latency)
                .await
                .expect("open vacuum fixture")
        })
    })
}

fn bench_vacuum(bencher: Bencher, mode: VacuumScanMode, scan_concurrency: Option<usize>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = shared_table();
    bencher.bench_local(|| {
        rt.block_on(async {
            let result = run_vacuum_full_dry_run(table, mode, scan_concurrency)
                .await
                .expect("vacuum dry-run");
            divan::black_box(result);
        });
    });
}

/// Flat full scan (concurrency N/A).
#[divan::bench]
fn full_dry_run_flat(bencher: Bencher) {
    bench_vacuum(bencher, VacuumScanMode::Flat, None);
}

/// Parallel full scan with default scan concurrency.
#[divan::bench]
fn full_dry_run_parallel(bencher: Bencher) {
    bench_vacuum(bencher, VacuumScanMode::Parallel, None);
}
