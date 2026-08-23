use delta_benchmarks::{run_smoke_once, SmokeParams};
use divan::{AllocProfiler, Bencher};
use url::Url;

fn main() {
    divan::main();
}

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

type Runtime = tokio::runtime::Runtime;

fn bench_smoke(bencher: Bencher, params: &SmokeParams) {
    let rt = Runtime::new().expect("create tokio runtime");
    bencher
        .with_inputs(|| tempfile::tempdir().expect("create temp dir"))
        .bench_local_values(|tmp_dir| {
            let table_url = Url::from_directory_path(tmp_dir.path()).expect("tmp dir url");
            rt.block_on(async {
                run_smoke_once(&table_url, params).await.expect("smoke run");
            });
            drop(tmp_dir);
        });
}

#[divan::bench(args = [
    SmokeParams { rows: 2 },
    SmokeParams { rows: 10 },
    SmokeParams { rows: 100 },
    SmokeParams { rows: 1_000 },
])]
fn smoke(bencher: Bencher, params: &SmokeParams) {
    bench_smoke(bencher, params);
}
