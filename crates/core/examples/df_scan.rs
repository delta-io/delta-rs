use std::sync::Arc;

use arrow_cast::pretty::print_batches;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect_partitioned;
use datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::engine::DataFusionEngine;
use deltalake_core::kernel::Snapshot;
use deltalake_core::DeltaTableError;
use url::Url;

static CASES: &[&str] = &[
    "./dat/v0.0.3/reader_tests/generated/all_primitive_types/delta/", // 0
    "./dat/v0.0.3/reader_tests/generated/basic_append/delta/",        // 1
    "./dat/v0.0.3/reader_tests/generated/basic_partitioned/delta/",   // 2
    "./dat/v0.0.3/reader_tests/generated/cdf/delta/",                 // 3
    "./dat/v0.0.3/reader_tests/generated/check_constraints/delta/",   // 4
    "./dat/v0.0.3/reader_tests/generated/column_mapping/delta/",      // 5
    "./dat/v0.0.3/reader_tests/generated/deletion_vectors/delta/",    // 6
    "./dat/v0.0.3/reader_tests/generated/generated_columns/delta/",   // 7
    "./dat/v0.0.3/reader_tests/generated/iceberg_compat_v1/delta/",   // 8
    "./dat/v0.0.3/reader_tests/generated/multi_partitioned/delta/",   // 9
    "./dat/v0.0.3/reader_tests/generated/multi_partitioned_2/delta/", // 10
    "./dat/v0.0.3/reader_tests/generated/nested_types/delta/",        // 11
    "./dat/v0.0.3/reader_tests/generated/no_replay/delta/",           // 12
    "./dat/v0.0.3/reader_tests/generated/no_stats/delta/",            // 13
    "./dat/v0.0.3/reader_tests/generated/partitioned_with_null/delta/", // 14
    "./dat/v0.0.3/reader_tests/generated/stats_as_struct/delta/",     // 15
    "./dat/v0.0.3/reader_tests/generated/timestamp_ntz/delta/",       // 16
    "./dat/v0.0.3/reader_tests/generated/with_checkpoint/delta/",     // 17
    "./dat/v0.0.3/reader_tests/generated/with_schema_change/delta/",  // 18
];

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), DeltaTableError> {
    let session = Arc::new(SessionContext::new());
    let engine = DataFusionEngine::new_from_session(&session.state());

    let path = std::fs::canonicalize(CASES[5]).unwrap();
    let table_url = Url::from_directory_path(path).unwrap();
    let snapshot =
        Snapshot::try_new_with_engine(engine.clone(), table_url, Default::default(), None).await?;

    let state = session.state_ref().read().clone();

    let plan = snapshot.scan(&state, None, &[], None).await?;

    let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
        .await?
        .into_iter()
        .flatten()
        .collect();

    print_batches(&batches).unwrap();

    Ok(())
}
