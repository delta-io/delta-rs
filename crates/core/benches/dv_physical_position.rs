//! Run each candidate in a separate process with the same lockfile and harness.
//! DV_BENCH_TELEMETRY appends per-sample stage timings and object-store counters.

#[path = "../src/delta_datafusion/table_provider/next/scan/test_support.rs"]
#[allow(dead_code)]
mod fixture;

use async_trait::async_trait;
use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datafusion::{
    physical_plan::{ExecutionPlan, execute_stream},
    prelude::{SessionConfig, SessionContext},
};
use deltalake_core::DeltaTableBuilder;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fmt,
    fs::OpenOptions,
    io::Write,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug)]
struct CountedStore {
    inner: Arc<dyn ObjectStore>,
    requests: AtomicU64,
    bytes: AtomicU64,
}
impl fmt::Display for CountedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "benchmark counted store")
    }
}
#[async_trait]
impl ObjectStore for CountedStore {
    async fn put_opts(
        &self,
        p: &Path,
        v: PutPayload,
        o: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(p, v, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        let head = o.head;
        let result = self.inner.get_opts(p, o).await?;
        self.requests.fetch_add(1, Ordering::Relaxed);
        if !head {
            self.bytes
                .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        }
        Ok(result)
    }
    async fn get_ranges(
        &self,
        p: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let result = self.inner.get_ranges(p, ranges).await?;
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(
            result.iter().map(|bytes| bytes.len() as u64).sum(),
            Ordering::Relaxed,
        );
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, a: &Path, b: &Path, o: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}

fn metrics(plan: &Arc<dyn ExecutionPlan>, output: &mut BTreeMap<String, usize>) {
    use datafusion::physical_plan::metrics::MetricValue;
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            match metric.value() {
                MetricValue::PruningMetrics {
                    name,
                    pruning_metrics,
                } => {
                    *output.entry(format!("{name}.pruned")).or_default() +=
                        pruning_metrics.pruned();
                    *output.entry(format!("{name}.matched")).or_default() +=
                        pruning_metrics.matched();
                }
                value => *output.entry(value.name().to_owned()).or_default() += value.as_usize(),
            }
        }
    }
    for child in plan.children() {
        metrics(child, output);
    }
}

fn benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for (shape, files, rows, groups) in [
        ("large", 1, 262_144, vec![16_384, 32_768, 8192]),
        ("small", 64, 2048, vec![1024, 768, 256]),
    ] {
        for density in ["none", "sparse", "dense"] {
            let fixture = fixture::Fixture::new(
                (0..files)
                    .map(|_| fixture::FileSpec {
                        rows,
                        groups: groups.clone(),
                        log_stats: true,
                        deleted: match density {
                            "none" => None,
                            "sparse" => Some((0..rows as u64).filter(|p| p % 100 == 0).collect()),
                            _ => Some((0..rows as u64).filter(|p| p % 5 != 0).collect()),
                        },
                    })
                    .collect(),
                1024,
            )
            .unwrap();
            let store = Arc::new(CountedStore {
                inner: Arc::new(object_store::local::LocalFileSystem::new()),
                requests: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
            });
            let table = runtime
                .block_on(
                    DeltaTableBuilder::from_url(fixture.url())
                        .unwrap()
                        .with_storage_backend(store.clone(), fixture.url())
                        .load(),
                )
                .unwrap();
            for partitions in [1, 2, 4, 8] {
                let mut config = SessionConfig::new().with_target_partitions(partitions);
                config.options_mut().optimizer.repartition_file_min_size = 0;
                config.options_mut().execution.parquet.pushdown_filters = true;
                let context = SessionContext::new_with_config(config);
                let provider = runtime
                    .block_on(async { table.table_provider().await })
                    .unwrap();
                context.register_table("dv", provider).unwrap();
                for selective in [false, true] {
                    let query = if selective {
                        format!(
                            "SELECT id, value FROM dv WHERE id >= {}",
                            files * rows * 9 / 10
                        )
                    } else {
                        "SELECT id, value FROM dv".into()
                    };
                    let expected_rows = fixture
                        .live_coordinates()
                        .iter()
                        .filter(|row| !selective || row.2 >= (files * rows * 9 / 10) as i64)
                        .count();
                    let prepared = runtime.block_on(async {
                        context
                            .sql(&query)
                            .await
                            .unwrap()
                            .create_physical_plan()
                            .await
                            .unwrap()
                    });
                    let cache = context
                        .runtime_env()
                        .cache_manager
                        .get_file_metadata_cache();
                    for cold in [false, true] {
                        for fresh in [false, true] {
                            let name = format!(
                                "dv_positions/{shape}/{density}/p{partitions}/{}/{}/{}",
                                if selective { "selective" } else { "all" },
                                if fresh { "fresh" } else { "prepared" },
                                if cold {
                                    "cold_metadata"
                                } else {
                                    "warm_metadata"
                                }
                            );
                            c.bench_function(&name, |b| b.iter_custom(|iterations| {
                            let mut elapsed = Duration::ZERO;
                            let mut planning = Duration::ZERO;
                            let mut first_read = Duration::ZERO;
                            let mut streaming = Duration::ZERO;
                            let before_requests = store.requests.load(Ordering::Relaxed);
                            let before_bytes = store.bytes.load(Ordering::Relaxed);
                            let mut last_metrics = BTreeMap::new();
                            let mut active_footer_reference_bytes = 0;
                            for _ in 0..iterations {
                                if cold { cache.clear(); }
                                let start = Instant::now();
                                runtime.block_on(async {
                                    let plan = if fresh { context.sql(&query).await.unwrap().create_physical_plan().await.unwrap() } else { datafusion::physical_plan::execution_plan::reset_plan_states(prepared.clone()).unwrap() };
                                    planning += start.elapsed();
                                    let first_start = Instant::now();
                                    let mut stream = execute_stream(plan.clone(), context.task_ctx()).unwrap();
                                    let first = stream.next().await.transpose().unwrap();
                                    let mut row_count = first.as_ref().map_or(0, |batch| batch.num_rows());
                                    first_read += first_start.elapsed();
                                    let mut first_metrics = BTreeMap::new();
                                    metrics(&plan, &mut first_metrics);
                                    active_footer_reference_bytes = active_footer_reference_bytes.max(first_metrics.get("active_footer_reference_bytes").copied().unwrap_or(0));
                                    let stream_start = Instant::now();
                                    while let Some(batch) = stream.next().await { row_count += batch.unwrap().num_rows(); }
                                    streaming += stream_start.elapsed();
                                    elapsed += start.elapsed();
                                    assert_eq!(black_box(row_count), expected_rows);
                                    last_metrics.clear();
                                    metrics(&plan, &mut last_metrics);
                                });
                            }
                            if let Ok(path) = std::env::var("DV_BENCH_TELEMETRY") {
                                let retention: usize = cache.list_entries().values().map(|entry| entry.size_bytes).sum();
                                let record = json!({"case":name,"iterations":iterations,"elapsed_ns":elapsed.as_nanos(),"planning_ns":planning.as_nanos(),"first_read_ns":first_read.as_nanos(),"stream_ns":streaming.as_nanos(),"object_store_requests":store.requests.load(Ordering::Relaxed)-before_requests,"object_store_bytes":store.bytes.load(Ordering::Relaxed)-before_bytes,"cache_retention_bytes":retention,"sampled_active_footer_reference_bytes":active_footer_reference_bytes,"cache_limit_bytes":cache.cache_limit(),"plan_metrics":last_metrics});
                                writeln!(OpenOptions::new().create(true).append(true).open(path).unwrap(), "{record}").unwrap();
                            }
                            elapsed
                        }));
                        }
                    }
                }
            }
        }
    }
}
criterion_group! { name = benches; config = Criterion::default().sample_size(10).warm_up_time(Duration::from_millis(100)).measurement_time(Duration::from_millis(500)); targets = benchmark }
criterion_main!(benches);
