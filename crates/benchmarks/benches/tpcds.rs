use std::path::PathBuf;

use delta_benchmarks::{register_tpcds_tables, tpcds_query, tpcds_query_names};
use divan::{AllocProfiler, Bencher};

fn main() {
    divan::main();
}

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

#[divan::bench(args = tpcds_query_names())]
fn tpcds_query_execution(bencher: Bencher, name: &'static str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let sql = tpcds_query(name)
        .expect("query must exist")
        .split(";")
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>();

    let tmp_dir = tempfile::tempdir().unwrap();
    let parquet_dir = PathBuf::from(
        std::env::var("TPCDS_PARQUET_DIR").unwrap_or_else(|_| "data/tpcds_parquet".to_string()),
    );

    let ctx = rt.block_on(async {
        register_tpcds_tables(&tmp_dir, &parquet_dir)
            .await
            .expect("failed to register TPC-DS tables")
    });

    bencher.bench_local(|| {
        rt.block_on(async {
            for sql in sql.iter() {
                let df = ctx.sql(sql).await.expect("failed to create dataframe");
                divan::black_box(df.collect().await.expect("failed to execute query"));
            }
        });
    });
    drop(tmp_dir);
}
