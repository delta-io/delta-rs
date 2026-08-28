use delta_benchmarks::{create_table, generate_batches, run_write, write_cases, WriteParams};
use divan::{AllocProfiler, Bencher};
use url::Url;

fn main() {
    divan::main();
}

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

type Runtime = tokio::runtime::Runtime;

fn bench_write(bencher: Bencher, params: &WriteParams) {
    let rt = Runtime::new().expect("create tokio runtime");
    bencher
        .with_inputs(|| {
            // Untimed setup: temp table + generated batches.
            let tmp_dir = tempfile::tempdir().expect("create temp dir");
            let url = Url::from_directory_path(tmp_dir.path()).expect("tmp dir url");
            let batches = generate_batches(params);
            let table = rt
                .block_on(create_table(&url, params))
                .expect("create table");
            (tmp_dir, table, batches)
        })
        .bench_local_values(|(tmp_dir, table, batches)| {
            // Timed: the write + commit only.
            rt.block_on(async move {
                run_write(table, batches, params).await.expect("write");
            });
            // Return the temp dir so divan drops it (recursive delete) untimed.
            tmp_dir
        });
}

// Writes are expensive (and regenerate large inputs per iteration), so cap the
// sample count rather than let Divan pick its default of ~100.
#[divan::bench(args = write_cases(), sample_count = 25, sample_size = 1)]
fn write(bencher: Bencher, params: &WriteParams) {
    bench_write(bencher, params);
}
