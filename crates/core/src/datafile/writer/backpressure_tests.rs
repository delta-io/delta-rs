use super::*;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};
use futures::{TryStreamExt, stream::BoxStream};
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::Semaphore;

/// Small rolled files take the single-PUT path. Hold their payloads until the
/// test releases them, without relying on a particular network speed or sleep.
#[derive(Debug)]
struct GatedStore {
    inner: InMemory,
    permits: Semaphore,
    started: Semaphore,
    completed: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    fail_next: AtomicBool,
    multipart_gate: Option<Arc<MultipartGate>>,
}

impl GatedStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemory::new(),
            permits: Semaphore::new(0),
            started: Semaphore::new(0),
            completed: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            fail_next: AtomicBool::new(false),
            multipart_gate: None,
        })
    }
}

impl Display for GatedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.started.add_permits(1);
        self.permits.acquire().await.unwrap().forget();
        let result = if self.fail_next.swap(false, Ordering::SeqCst) {
            Err(object_store::Error::Generic {
                store: "GatedStore",
                source: "injected upload failure".into(),
            })
        } else {
            self.inner.put_opts(location, payload, opts).await
        };
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.completed.fetch_add(1, Ordering::SeqCst);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        match &self.multipart_gate {
            Some(gate) => {
                gate.created.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(GatedMultipartUpload {
                    inner,
                    gate: gate.clone(),
                }))
            }
            None => Ok(inner),
        }
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct MultipartGate {
    permits: Semaphore,
    finishing: Semaphore,
    created: AtomicUsize,
    completed: AtomicUsize,
    aborted: AtomicUsize,
}

#[derive(Debug)]
struct GatedMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    gate: Arc<MultipartGate>,
}

#[async_trait::async_trait]
impl MultipartUpload for GatedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.gate.finishing.add_permits(1);
        self.gate.permits.acquire().await.unwrap().forget();
        let result = self.inner.complete().await;
        self.gate.completed.fetch_add(1, Ordering::SeqCst);
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.gate.aborted.fetch_add(1, Ordering::SeqCst);
        self.inner.abort().await
    }
}

fn batch(rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from_iter_values(0..rows as i32))],
    )
    .unwrap()
}

fn writer(store: Arc<GatedStore>, batch: &RecordBatch) -> PartitionWriter {
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(32))
        .build();
    let config = PartitionWriterConfig::try_new(
        batch.schema(),
        IndexMap::new(),
        Some(props),
        NonZeroU64::new(1),
        Some(32),
        None,
        None,
    )
    .unwrap()
    .with_max_in_flight_uploads(NonZeroUsize::new(2).unwrap());
    PartitionWriter::try_with_config(
        store,
        config,
        DataSkippingNumIndexedCols::NumColumns(32),
        None,
    )
    .unwrap()
}

#[rstest::rstest]
#[case(1)]
#[case(2)]
#[case(4)]
#[tokio::test]
async fn rolled_uploads_backpressure_and_preserve_rows(#[case] limit: usize) {
    let store = GatedStore::new();
    let batch = batch(32 * 20);
    let mut writer = writer(store.clone(), &batch);
    writer.config = writer
        .config
        .with_max_in_flight_uploads(NonZeroUsize::new(limit).unwrap());
    let mut write = Box::pin(writer.write(&batch));

    // A single batch rolls twenty files. With storage blocked, it must yield
    // instead of detaching all twenty payloads and reporting write success.
    let pending = futures::poll!(&mut write).is_pending();
    tokio::task::yield_now().await;
    eprintln!(
        "blocked store: pending={pending}, active uploads={}",
        store.active.load(Ordering::SeqCst)
    );
    assert!(pending);
    tokio::time::timeout(
        Duration::from_secs(5),
        store.started.acquire_many(limit as u32),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    assert_eq!(store.active.load(Ordering::SeqCst), limit);
    store.permits.add_permits(20);
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    let adds = writer.close().await.unwrap();
    assert_eq!(adds.len(), 20);
    assert!(store.peak.load(Ordering::SeqCst) <= limit);
    assert_eq!(store.completed.load(Ordering::SeqCst), 20);
    assert!(adds.windows(2).all(|pair| pair[0].path < pair[1].path));

    let mut actual = Vec::new();
    for add in adds {
        let bytes = store
            .get(&Path::from(add.path))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.len() as i64, add.size);
        for batch in ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            actual.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
            );
        }
    }
    actual.sort_unstable();
    assert_eq!(actual, (0..640).collect::<Vec<i32>>());
}

#[tokio::test]
async fn rolled_upload_failure_stops_writing_and_drains_siblings() {
    let store = GatedStore::new();
    let batch = batch(32 * 20);
    let mut writer = writer(store.clone(), &batch);
    store.fail_next.store(true, Ordering::SeqCst);
    store.permits.add_permits(20);
    let result = writer.write(&batch).await;
    assert!(
        result.is_err(),
        "background failure must surface during write"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected upload failure")
    );
    writer.abort().await.unwrap();
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    assert!(store.completed.load(Ordering::SeqCst) < 20);
}

#[tokio::test]
async fn cancelled_backpressure_wait_can_be_aborted() {
    let store = GatedStore::new();
    let batch = batch(32 * 20);
    let mut writer = writer(store.clone(), &batch);
    let mut write = Box::pin(writer.write(&batch));
    assert!(futures::poll!(&mut write).is_pending());
    tokio::time::timeout(Duration::from_secs(5), store.started.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    drop(write);

    let mut abort = Box::pin(writer.abort());
    assert!(futures::poll!(&mut abort).is_pending());
    store.permits.add_permits(2);
    tokio::time::timeout(Duration::from_secs(5), abort)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    assert_eq!(store.completed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_wait_aborts_active_multipart_upload() {
    use arrow_array::Int64Array;
    let gate = Arc::new(MultipartGate {
        permits: Semaphore::new(0),
        finishing: Semaphore::new(0),
        created: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
        aborted: AtomicUsize::new(0),
    });
    let mut store = GatedStore::new();
    Arc::get_mut(&mut store).unwrap().multipart_gate = Some(gate.clone());
    // Each file exceeds BufWriter's 5 MiB threshold and opens an actual multipart upload.
    let rows = 768 * 1024;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(rows))
        .build();
    let config = PartitionWriterConfig::try_new(
        batch.schema(),
        IndexMap::new(),
        Some(props),
        NonZeroU64::new(1),
        Some(rows),
        Some(1),
        None,
    )
    .unwrap()
    .with_max_in_flight_uploads(NonZeroUsize::new(2).unwrap());
    let mut writer = PartitionWriter::try_with_config(
        store.clone(),
        config,
        DataSkippingNumIndexedCols::NumColumns(32),
        None,
    )
    .unwrap();
    let mut write = Box::pin(async {
        for _ in 0..3 {
            writer.write(&batch).await?;
        }
        Ok::<_, DeltaTableError>(())
    });
    assert!(futures::poll!(&mut write).is_pending());
    tokio::time::timeout(Duration::from_secs(5), gate.finishing.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(gate.created.load(Ordering::SeqCst), 3);
    drop(write);
    gate.permits.add_permits(2);
    tokio::time::timeout(Duration::from_secs(5), writer.abort())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gate.completed.load(Ordering::SeqCst), 2);
    assert_eq!(gate.aborted.load(Ordering::SeqCst), 1);
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(
        files.len(),
        2,
        "the active file must not be finalized by abort"
    );
}

#[tokio::test]
async fn final_partial_file_obeys_backpressure() {
    let store = GatedStore::new();
    let batch = batch(65);
    let mut writer = writer(store.clone(), &batch);
    // Full 32-row groups exceed this target; the last row stays buffered.
    writer.config.target_file_size = NonZeroU64::new(128);
    writer.write(&batch).await.unwrap();
    let mut close = Box::pin(writer.close());
    assert!(futures::poll!(&mut close).is_pending());
    tokio::time::timeout(Duration::from_secs(5), store.started.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    // Yield to any incorrectly spawned third upload as well.
    tokio::task::yield_now().await;
    assert_eq!(store.active.load(Ordering::SeqCst), 2);
    store.permits.add_permits(3);
    let adds = tokio::time::timeout(Duration::from_secs(5), close)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(adds.len(), 3);
    assert!(store.peak.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn close_failure_drains_pending_uploads() {
    let store = GatedStore::new();
    let batch = batch(65);
    let mut writer = writer(store.clone(), &batch);
    writer.config.target_file_size = NonZeroU64::new(128);
    writer.write(&batch).await.unwrap();
    store.fail_next.store(true, Ordering::SeqCst);
    store.permits.add_permits(3);
    let result = tokio::time::timeout(Duration::from_secs(5), writer.close())
        .await
        .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected upload failure")
    );
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    assert_eq!(store.completed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn partitioned_write_preserves_all_files_under_backpressure() {
    let store = GatedStore::new();
    let values = batch(32 * 20);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("partition", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            values.column(0).clone(),
            Arc::new(Int32Array::from_iter_values((0..640).map(|i| i % 2))),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(32))
        .build();
    let config = WriterConfig::new(
        batch.schema(),
        vec!["partition".into()],
        Some(props),
        NonZeroU64::new(1),
        Some(32),
        DataSkippingNumIndexedCols::NumColumns(32),
        None,
    );
    let mut writer = DeltaWriter::new(store.clone(), config);
    let mut write = Box::pin(writer.write(&batch));
    assert!(futures::poll!(&mut write).is_pending());
    store.permits.add_permits(20);
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    let adds = writer.close().await.unwrap();
    assert_eq!(adds.len(), 20);
    assert_eq!(
        adds.iter()
            .filter(|add| add.partition_values["partition"].as_deref() == Some("0"))
            .count(),
        10
    );
    assert_eq!(
        adds.iter()
            .filter(|add| add.partition_values["partition"].as_deref() == Some("1"))
            .count(),
        10
    );
    // The file limit is per partition, so two partitions can have four uploads.
    assert!(store.peak.load(Ordering::SeqCst) <= 4);
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
}
