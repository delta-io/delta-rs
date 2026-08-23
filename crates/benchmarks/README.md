# Benchmarks

The merge benchmarks are similar to the ones used by [Delta Spark](https://github.com/delta-io/delta/pull/1835).


## Dataset

To generate the database, `duckdb` can be used. Install `duckdb` by following [these instructions](https://duckdb.org/#quickinstall).

Run the following commands:

```bash
❯ duckdb
D CALL dsdgen(sf = 1);
100% ▕██████████████████████████████████████▏ (00:00:05.76 elapsed)
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ 0 rows  │
└─────────┘
D EXPORT DATABASE 'tpcds_parquet' (FORMAT PARQUET);
```

This will generate a folder called `tpcds_parquet` containing many parquet files. Place it at `crates/benchmarks/data/tpcds_parquet` (or set `TPCDS_PARQUET_DIR`). Credits to [Xuanwo's Blog](https://xuanwo.io/links/2025/02/duckdb-is-the-best-tpc-data-generator/).

## Running benchmarks

Benchmarks use Divan and time only the merge operation. A temporary Delta table is created per iteration from `web_returns.parquet` and removed afterwards.

Environment variables:
- `TPCDS_PARQUET_DIR` (optional): directory containing `web_returns.parquet`. Default: `crates/benchmarks/data/tpcds_parquet`.

From the repo root:
```
cargo bench -p delta-benchmarks --bench merge
```

Filter a specific suite:
```
cargo bench -p delta-benchmarks --bench merge -- delete_only
cargo bench -p delta-benchmarks --bench merge -- multiple_insert_only
cargo bench -p delta-benchmarks --bench merge -- upsert_file_matched
cargo bench -p delta-benchmarks --bench merge -- noop_heavy_upsert
```

## Profiling script

A simple CLI is available to run a single merge with configurable parameters (useful for profiling or ad-hoc runs). It creates a fresh temporary Delta table per sample from `web_returns.parquet`, times only the merge, and prints duration and metrics.

Run (from repo root):
```bash
cargo run --profile profiling -p delta-benchmarks -- merge upsert --matched 0.01 --not-matched 0.10
cargo run --profile profiling -p delta-benchmarks -- merge noop-heavy-upsert --matched 1.0 --not-matched 0.05
```

Options:
- `<upsert|noop-heavy-upsert|delete|insert>`: operation to benchmark
- `--matched <fraction>`: fraction of rows that match existing keys (default 0.01)
- `--not-matched <fraction>`: fraction of rows that do not match (default 0.10)
- `--case <name>`: run one of the predefined merge scenarios mirrored from the Delta Spark suite

The `noop-heavy-upsert` operation profiles matched rows with a false update predicate
and inserted rows.

List cases with:
```bash
cargo run --release -p delta-benchmarks -- merge --case single_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_0.05
```

### Flamegraphs using `samply`

Using `samply`, you can generate flamegraphs from the profile script.

To start,

```bash
cargo install samply --locked
cargo build --profile profiling -p delta-benchmarks
samply record ./target/profiling/delta-benchmarks merge upsert
```

## Vacuum full-scan listing

Compares full-mode vacuum dry-run listing cost (plan/scan only; no deletes):

- **parallel** — multi-level prefix expansion + concurrent leaf `list(prefix)`
- **flat** — `.disable_parallel_scan()` → single recursive `list(None)`

CLI and Divan use the **same flag names**:

| Flag | Env fallback | Meaning |
|------|----------------|---------|
| `--fixture <PATH>` | `VACUUM_BENCH_FIXTURE` | Fixture table directory |
| `--list-latency-ms <MS>` | `VACUUM_BENCH_LIST_LATENCY_MS` | Artificial LIST latency (cloud RTT sim) |
| `--sample-count <N>` | Divan: also `DIVAN_SAMPLE_COUNT` | Timed runs / Divan samples (default CLI **1**, Divan **100**) |

On local FS with high `--scan-concurrency`, raise open-file limits if you hit
`Too many open files (os error 24)`:

```bash
ulimit -n 10240
```

### Prep once, then measure

```bash
# 1) Prepare fixture once (skips if already present at default path)
cargo run --release -p delta-benchmarks -- generate-vacuum-fixture

# 2) Divan bench (flat + parallel at default concurrency)
cargo bench -p delta-benchmarks --bench vacuum
```

Default fixture: ~30 days × 500 groups ≈ **15k leaf partitions**, one commit per
day, partitions `date` + `group`, under
`crates/benchmarks/data/vacuum_bench/d30_g500`.

Custom / larger fixture:

```bash
cargo run --release -p delta-benchmarks -- generate-vacuum-fixture \
  --out crates/benchmarks/data/vacuum_bench/custom \
  --days 90 --groups 2000 --force

cargo bench -p delta-benchmarks --bench vacuum -- \
  --fixture crates/benchmarks/data/vacuum_bench/custom \
  --list-latency-ms 100 \
  --sample-count 2
```

### Divan benches

- `full_dry_run_flat` — flat scan
- `full_dry_run_parallel` — parallel scan with library default LIST concurrency
  (`None` → env / default **10**)

```bash
# Both benches, 2 samples each (simple A/B)
ulimit -n 10240
cargo bench -p delta-benchmarks --bench vacuum -- \
  --fixture crates/benchmarks/data/vacuum_bench/d1095_g5001 \
  --list-latency-ms 100 \
  --sample-count 2

# Parallel only
cargo bench -p delta-benchmarks --bench vacuum -- \
  --fixture crates/benchmarks/data/vacuum_bench/d1095_g5001 \
  --list-latency-ms 100 \
  --sample-count 2 \
  full_dry_run_parallel

# Flat only
cargo bench -p delta-benchmarks --bench vacuum -- \
  --fixture crates/benchmarks/data/vacuum_bench/d1095_g5001 \
  --list-latency-ms 100 \
  --sample-count 2 \
  full_dry_run_flat
```

`--list-latency-ms` delays every LIST / `list_with_delimiter`; flat `list`
streams are also delayed every 1000 keys (simulated S3 page size).

Env vars still work as fallbacks if you prefer not to pass flags:

```bash
VACUUM_BENCH_FIXTURE=crates/benchmarks/data/vacuum_bench/custom \
VACUUM_BENCH_LIST_LATENCY_MS=100 \
  cargo bench -p delta-benchmarks --bench vacuum -- --sample-count 1
```

### Ad-hoc / profiling CLI

Same flags as Divan (`--fixture`, `--list-latency-ms`, `--sample-count`), plus
scan controls:

```bash
# Optional: --generate builds the default fixture if missing
cargo run --release -p delta-benchmarks -- vacuum \
  --generate --scan parallel --sample-count 5

cargo run --release -p delta-benchmarks -- vacuum \
  --scan flat --sample-count 5

cargo run --release -p delta-benchmarks -- vacuum \
  --scan parallel --scan-concurrency 20

# Local "cloud-like" LIST latency
cargo run --release -p delta-benchmarks -- vacuum \
  --fixture crates/benchmarks/data/vacuum_bench/d1095_g5001 \
  --scan parallel --scan-concurrency 32 \
  --list-latency-ms 100 --sample-count 1

cargo run --release -p delta-benchmarks -- vacuum \
  --fixture crates/benchmarks/data/vacuum_bench/d1095_g5001 \
  --scan flat --list-latency-ms 100 --sample-count 1
```

CLI also accepts `VACUUM_BENCH_FIXTURE` / `VACUUM_BENCH_LIST_LATENCY_MS` as env
aliases for those flags.
