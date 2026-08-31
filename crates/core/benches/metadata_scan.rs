//! Metadata replay benchmark for a 900-Add checkpoint followed by a 100-Add commit.

use std::collections::HashMap;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::bench_support::scan_metadata_file_count;
use deltalake_core::kernel::transaction::CommitBuilder;
use deltalake_core::kernel::{Action, Add, DataType, StructField};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::{DeltaTable, checkpoints};
use tokio::runtime::Runtime;
use url::Url;

const CHECKPOINT_ADD_COUNT: usize = 900;
const COMMIT_ADD_COUNT: usize = 100;
const TOTAL_ADD_COUNT: usize = CHECKPOINT_ADD_COUNT + COMMIT_ADD_COUNT;

fn add_action(index: usize) -> Action {
    Action::Add(Add {
        path: format!("part-{index:05}.parquet"),
        partition_values: HashMap::new(),
        size: 1_024,
        modification_time: index as i64,
        data_change: true,
        stats: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    })
}

fn metadata_scan(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table_dir = tempfile::tempdir().unwrap();
    let table_url = Url::from_directory_path(table_dir.path()).unwrap();
    let table = runtime.block_on(async {
        let table = DeltaTable::try_from_url(table_url.clone())
            .await
            .unwrap()
            .create()
            .with_columns([StructField::nullable("value", DataType::INTEGER)])
            .with_actions((0..CHECKPOINT_ADD_COUNT).map(add_action))
            .await
            .unwrap();
        checkpoints::create_checkpoint(&table, None).await.unwrap();
        CommitBuilder::default()
            .with_actions(
                (CHECKPOINT_ADD_COUNT..TOTAL_ADD_COUNT)
                    .map(add_action)
                    .collect(),
            )
            .build(
                Some(table.snapshot().unwrap()),
                table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await
            .unwrap();
        DeltaTable::try_from_url(table_url).await.unwrap()
    });

    let snapshot = table.snapshot().unwrap().snapshot().clone();
    let session = SessionContext::new().state();
    table.update_datafusion_session(&session).unwrap();

    let mut group = c.benchmark_group("metadata_scan");
    group.throughput(Throughput::Elements(TOTAL_ADD_COUNT as u64));
    group.bench_function("scan_metadata", |b| {
        b.iter(|| {
            let count = runtime
                .block_on(scan_metadata_file_count(&snapshot, &session))
                .unwrap();
            assert_eq!(count, TOTAL_ADD_COUNT);
            black_box(count)
        });
    });
    group.finish();
}

criterion_group!(benches, metadata_scan);
criterion_main!(benches);
