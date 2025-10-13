use std::path::PathBuf;

use delta_benchmarks::{
    merge_delete, merge_insert, merge_upsert, prepare_source_and_table, MergeOp, MergePerfParams,
};

use divan::{AllocProfiler, Bencher};

fn main() {
    divan::main();
}

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn bench_merge(bencher: Bencher, op: MergeOp, params: &MergePerfParams) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let tmp_dir = tempfile::tempdir().unwrap();
            let parquet_dir = PathBuf::from(
                std::env::var("TPCDS_PARQUET_DIR")
                    .unwrap_or_else(|_| "data/tpcds_parquet".to_string()),
            );
            rt.block_on(async move {
                let (source, table) = prepare_source_and_table(params, &tmp_dir, &parquet_dir)
                    .await
                    .unwrap();
                (source, table, tmp_dir)
            })
        })
        .bench_local_values(|(source, table, tmp_dir)| {
            rt.block_on(async move {
                let _ = divan::black_box(op(source, table).unwrap().await.unwrap());
            });
            drop(tmp_dir);
        });
}

#[divan::bench(args = [
    MergePerfParams {
        sample_matched_rows: 0.05,
        sample_not_matched_rows: 0.0,
    }
])]
fn delete_only(bencher: Bencher, params: &MergePerfParams) {
    bench_merge(bencher, merge_delete, params);
}

#[divan::bench(args = [
    MergePerfParams {
        sample_matched_rows: 0.00,
        sample_not_matched_rows: 0.05,
    },
    MergePerfParams {
        sample_matched_rows: 0.00,
        sample_not_matched_rows: 0.50,
    },
    MergePerfParams {
        sample_matched_rows: 0.00,
        sample_not_matched_rows: 1.0,
    },
])]
fn multiple_insert_only(bencher: Bencher, params: &MergePerfParams) {
    bench_merge(bencher, merge_insert, params);
}

#[divan::bench(args = [
    MergePerfParams {
        sample_matched_rows: 0.01,
        sample_not_matched_rows: 0.1,
    },
    MergePerfParams {
        sample_matched_rows: 0.1,
        sample_not_matched_rows: 0.0,
    },
    MergePerfParams {
        sample_matched_rows: 0.1,
        sample_not_matched_rows: 0.01,
    },
    MergePerfParams {
        sample_matched_rows: 0.5,
        sample_not_matched_rows: 0.001,
    },
    MergePerfParams {
        sample_matched_rows: 0.99,
        sample_not_matched_rows: 0.001,
    },
    MergePerfParams {
        sample_matched_rows: 0.001,
        sample_not_matched_rows: 0.001,
    },
])]
fn upsert_file_matched(bencher: Bencher, params: &MergePerfParams) {
    bench_merge(bencher, merge_upsert, params);
}
