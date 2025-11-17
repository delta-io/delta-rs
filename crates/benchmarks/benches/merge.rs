use std::path::PathBuf;

use delta_benchmarks::{
    delete_only_cases, insert_only_cases, prepare_source_and_table, upsert_cases, MergeTestCase,
};

use divan::{AllocProfiler, Bencher};

fn main() {
    divan::main();
}

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn bench_merge_case(bencher: Bencher, case: &MergeTestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let tmp_dir = tempfile::tempdir().unwrap();
            let parquet_dir = PathBuf::from(
                std::env::var("TPCDS_PARQUET_DIR")
                    .unwrap_or_else(|_| "data/tpcds_parquet".to_string()),
            );
            rt.block_on(async move {
                let (source, table) =
                    prepare_source_and_table(&case.params, &tmp_dir, &parquet_dir)
                        .await
                        .expect("prepare inputs");
                (case, source, table, tmp_dir)
            })
        })
        .bench_local_values(|(case, source, table, tmp_dir)| {
            rt.block_on(async move {
                divan::black_box(case.execute(source, table).await.expect("execute merge"));
            });
            drop(tmp_dir);
        });
}

#[divan::bench(args = insert_only_cases())]
fn insert_only(bencher: Bencher, case: &MergeTestCase) {
    bench_merge_case(bencher, case);
}

#[divan::bench(args = delete_only_cases())]
fn delete_only(bencher: Bencher, case: &MergeTestCase) {
    bench_merge_case(bencher, case);
}

#[divan::bench(args = upsert_cases())]
fn upsert(bencher: Bencher, case: &MergeTestCase) {
    bench_merge_case(bencher, case);
}
