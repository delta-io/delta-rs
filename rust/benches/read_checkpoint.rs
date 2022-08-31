use criterion::{criterion_group, criterion_main, Criterion};
use deltalake::delta::DeltaTableConfig;
use deltalake::table_state::DeltaTableState;
use std::fs::File;
use std::io::Read;

fn read_null_partitions_checkpoint(c: &mut Criterion) {
    let path = "./tests/data/read_null_partitions_from_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet";
    let mut reader = File::open(path).unwrap();
    let mut cp_data = Vec::new();
    reader.read_to_end(&mut cp_data).unwrap();
    let cp_data = bytes::Bytes::from(cp_data);
    let config = DeltaTableConfig {
        require_tombstones: true,
        require_files: true,
    };

    c.bench_function("process checkpoint for table state", |b| {
        b.iter(|| {
            DeltaTableState::with_version(10)
                .process_checkpoint_bytes(cp_data.clone(), &config)
                .unwrap();
        })
    });
}

criterion_group!(benches, read_null_partitions_checkpoint);
criterion_main!(benches);
