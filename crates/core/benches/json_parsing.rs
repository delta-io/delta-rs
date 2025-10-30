use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;
use tokio::runtime::Runtime;

use deltalake_core::logstore::get_actions;

fn generate_commit_log_complex(
    num_actions: usize,
    with_stats: bool,
    with_partition_values: bool,
    with_deletion_vector: bool,
) -> Bytes {
    let mut log_lines = Vec::new();

    log_lines.push(r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.to_string());
    log_lines.push(r#"{"commitInfo":{"timestamp":1234567890}}"#.to_string());

    for i in 0..num_actions {
        let mut add_json = format!(
            r#"{{"path":"part-{:05}.parquet","size":{},"modificationTime":1234567890,"dataChange":true"#,
            i,
            1000 + i * 100
        );

        if with_partition_values {
            add_json.push_str(r#","partitionValues":{"year":"2024","month":"10","day":"09"}"#);
        } else {
            add_json.push_str(r#","partitionValues":{}"#);
        }

        if with_stats {
            add_json.push_str(&format!(
                r#","stats":"{{\"numRecords\":{},\"minValues\":{{\"id\":{},\"name\":\"aaa\",\"value\":{}.5}},\"maxValues\":{{\"id\":{},\"name\":\"zzz\",\"value\":{}.99}},\"nullCount\":{{\"id\":0,\"name\":0,\"value\":{}}}}}""#,
                1000 + i * 10, i, i, i + 1000, i + 1000, i % 10
            ));
        }

        if with_deletion_vector {
            add_json.push_str(r#","deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}"#);
        }

        add_json.push_str("}");
        log_lines.push(format!(r#"{{"add":{}}}"#, add_json));
    }

    Bytes::from(log_lines.join("\n"))
}

fn bench_simple_actions(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("simple_actions_1000");
    group.throughput(Throughput::Elements(1000));
    group.sample_size(150);
    group.measurement_time(Duration::from_secs(10));

    let commit_log = generate_commit_log_complex(1000, false, false, false);

    group.bench_function("get_actions", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = get_actions(0, &commit_log);
                black_box(result.unwrap().len())
            })
        });
    });

    group.finish();
}

fn bench_with_stats(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("with_stats_1000");
    group.throughput(Throughput::Elements(1000));
    group.sample_size(150);
    group.measurement_time(Duration::from_secs(10));

    let commit_log = generate_commit_log_complex(1000, true, false, false);

    group.bench_function("get_actions", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = get_actions(0, &commit_log);
                black_box(result.unwrap().len())
            })
        });
    });

    group.finish();
}

fn bench_full_complexity(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("full_complexity_1000");
    group.throughput(Throughput::Elements(1000));
    group.sample_size(150);
    group.measurement_time(Duration::from_secs(10));

    let commit_log = generate_commit_log_complex(1000, true, true, true);

    group.bench_function("get_actions", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = get_actions(0, &commit_log);
                black_box(result.unwrap().len())
            })
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_actions,
    bench_with_stats,
    bench_full_complexity,
);
criterion_main!(benches);
