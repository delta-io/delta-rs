//! Parallel column encoding for a single parquet file.
//!
//! [`ParallelArrowWriter`] keeps one row group open at a time, but
//! gives each leaf column its own async task. Encoding and compression, the
//! expensive part, then run on as many cores as the table has leaf columns.

use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory,
    compute_leaves,
};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::{ArrowSchemaConverter, add_encoded_arrow_schema_to_metadata};
use parquet::column::page_store::PageStoreFactory;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinHandle;

/// One leaf column of the open row group, encoded by its own task.
struct ColumnWorker {
    sender: Option<Sender<ArrowLeafColumn>>,
    handle: JoinHandle<ParquetResult<ArrowColumnChunk>>,
    /// Anticipated encoded size, published by the worker after each write.
    encoded_size: Arc<AtomicUsize>,
    /// Memory the encoder holds, published by the worker after each write.
    memory_size: Arc<AtomicUsize>,
}

/// Arrow-specific settings for writing parquet data files.
#[derive(Debug, Clone)]
pub struct ArrowWriterOptions {
    skip_arrow_metadata_hint: bool,
    page_store_factory: Option<Arc<dyn PageStoreFactory>>,
    enable_parallel_encoding: bool,
}

impl Default for ArrowWriterOptions {
    fn default() -> Self {
        Self {
            skip_arrow_metadata_hint: false,
            page_store_factory: None,
            enable_parallel_encoding: true,
        }
    }
}

impl ArrowWriterOptions {
    /// Creates [`ArrowWriterOptions`] with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Skip writing the serialized arrow schema into the parquet footer (defaults to `false`).
    /// Skip writing the serialized arrow schema into the parquet footer (defaults to `false`).
    pub fn with_skip_arrow_metadata(mut self, skip_arrow_metadata: bool) -> Self {
        self.skip_arrow_metadata_hint = skip_arrow_metadata;
        self
    }
    }

    /// Sets the [`PageStoreFactory`] that buffers completed pages while a row group is open.
    ///
    /// By default pages are held on the heap until the row group is flushed.
    pub fn with_page_store_factory(
        mut self,
        page_store_factory: Arc<dyn PageStoreFactory>,
    ) -> Self {
        self.page_store_factory = Some(page_store_factory);
        self
    }

    /// Encode each column of a row group in its own task (defaults to `true`). When `false`,
    /// arrow-rs's `AsyncArrowWriter` encodes the columns one after another.
    pub fn with_enable_parallel_encoding(mut self, enable_parallel_encoding: bool) -> Self {
        self.enable_parallel_encoding = enable_parallel_encoding;
        self
    }

    pub(crate) fn enable_parallel_encoding(&self) -> bool {
        self.enable_parallel_encoding
    }

    /// The same settings as parquet's own options, for `AsyncArrowWriter`.
    pub(crate) fn to_parquet_options(
        &self,
        props: WriterProperties,
    ) -> parquet::arrow::arrow_writer::ArrowWriterOptions {
        let options = parquet::arrow::arrow_writer::ArrowWriterOptions::new()
            .with_properties(props)
            .with_skip_arrow_metadata(self.skip_arrow_metadata_hint);
        match &self.page_store_factory {
            Some(page_store_factory) => {
                options.with_page_store_factory(Arc::clone(page_store_factory))
            }
            None => options,
        }
    }
}

/// Encodes [`RecordBatch`]es to one parquet file, one column per task.
pub(crate) struct ParallelArrowWriter<W: AsyncFileWriter> {
    /// Underlying parquet writer that writes into buffer
    file_writer: SerializedFileWriter<Vec<u8>>,

    /// Creates new [`ArrowRowGroupWriter`] instances as required
    factory: ArrowRowGroupWriterFactory,

    /// Writer that sinks to storage
    sink_writer: W,

    /// Arrow schema that is used to deduce leaf columns
    schema: SchemaRef,

    /// The maximum number of rows to write to each row group, retrieved from [`WriterProperties`]
    max_rows_in_group_row: usize,

    /// Workers for the open row group; empty until the first write into it.
    workers: Vec<ColumnWorker>,
    buffered_rows: usize,
    next_row_group: usize,
}

impl<W: AsyncFileWriter> ParallelArrowWriter<W> {
    pub(crate) fn try_new(
        sink_writer: W,
        arrow_schema: SchemaRef,
        props: WriterProperties,
        options: Option<ArrowWriterOptions>,
    ) -> ParquetResult<Self> {
        let mut props = props;
        let options = options.unwrap_or_default();

        if !options.skip_arrow_metadata_hint {
            add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut props);
        }

        let props = Arc::new(props);

        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&arrow_schema)?;
        let file_writer =
            SerializedFileWriter::new(Vec::new(), parquet_schema.root_schema_ptr(), props.clone())?;

        let mut row_group_factory =
            ArrowRowGroupWriterFactory::new(&file_writer, arrow_schema.clone());

        if let Some(page_store_factory) = options.page_store_factory {
            row_group_factory = row_group_factory.with_page_store_factory(page_store_factory);
        }

        Ok(Self {
            max_rows_in_group_row: props.max_row_group_row_count().unwrap_or(usize::MAX),
            file_writer,
            factory: row_group_factory,
            sink_writer,
            schema: arrow_schema,
            workers: Vec::new(),
            buffered_rows: 0,
            next_row_group: 0,
        })
    }

    /// Caller is responsible to slice batches to the size of max_rows_in_group_row.
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> ParquetResult<()> {
        if self.workers.is_empty() {
            self.start_row_group()?;
        }

        let schema = self.schema.clone();
        let mut leaf = 0;
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            for column in compute_leaves(field, array)? {
                self.send(leaf, column).await?;
                leaf += 1;
            }
        }

        self.buffered_rows += batch.num_rows();
        if self.buffered_rows >= self.max_rows_in_group_row {
            self.close_row_group().await?;
        }
        Ok(())
    }

    /// Close the file and flush everything to the sink.
    pub(crate) async fn finish(&mut self) -> ParquetResult<ParquetMetaData> {
        if self.buffered_rows > 0 {
            self.close_row_group().await?;
        }
        let metadata = self.file_writer.finish()?;
        self.flush_buffer().await?;
        self.sink_writer.complete().await?;
        Ok(metadata)
    }

    pub(crate) fn into_inner(self) -> W {
        // Callers only take this route to abort, so the open row group is dropped.
        self.sink_writer
    }

    pub(crate) fn bytes_written(&self) -> usize {
        self.file_writer.bytes_written()
    }

    /// Anticipated encoded size of the open row group.
    pub(crate) fn in_progress_size(&self) -> usize {
        self.sum(|worker| &worker.encoded_size)
    }

    /// Memory the open row group's encoders hold.
    pub(crate) fn memory_size(&self) -> usize {
        self.sum(|worker| &worker.memory_size)
    }

    pub(crate) fn in_progress_rows(&self) -> usize {
        self.buffered_rows
    }

    pub(crate) fn flushed_row_groups(&self) -> &[RowGroupMetaData] {
        self.file_writer.flushed_row_groups()
    }

    fn sum(&self, field: impl Fn(&ColumnWorker) -> &Arc<AtomicUsize>) -> usize {
        self.workers
            .iter()
            .map(|worker| field(worker).load(Ordering::Relaxed))
            .sum()
    }

    /// Spawn one task per leaf column of the next row group.
    fn start_row_group(&mut self) -> ParquetResult<()> {
        let column_writers = self.factory.create_column_writers(self.next_row_group)?;
        self.next_row_group += 1;

        self.workers = column_writers
            .into_iter()
            .map(|column_writer| {
                let (sender, receiver) = channel::<ArrowLeafColumn>(2);
                let encoded_size = Arc::new(AtomicUsize::new(0));
                let memory_size = Arc::new(AtomicUsize::new(0));
                let task = encode_column(
                    column_writer,
                    receiver,
                    encoded_size.clone(),
                    memory_size.clone(),
                );
                let handle = tokio::spawn(task);
                ColumnWorker {
                    sender: Some(sender),
                    handle,
                    encoded_size,
                    memory_size,
                }
            })
            .collect();
        Ok(())
    }

    async fn send(&mut self, leaf: usize, column: ArrowLeafColumn) -> ParquetResult<()> {
        let sender = self
            .workers
            .get(leaf)
            .and_then(|worker| worker.sender.clone())
            .ok_or_else(|| ParquetError::General(format!("no column writer for leaf {leaf}")))?;
        sender.send(column).await.map_err(|_| {
            // The worker only goes away when its encode failed; `close_row_group`
            // joins the task and surfaces that error.
            ParquetError::General("column encoder stopped".to_string())
        })
    }

    /// Wait for every column of the open row group, then append it to the file.
    async fn close_row_group(&mut self) -> ParquetResult<()> {
        let mut chunks = Vec::with_capacity(self.workers.len());
        for mut worker in mem::take(&mut self.workers) {
            // Dropping the sender ends the worker's receive loop, which closes
            // its encoder and returns the finished column chunk.
            worker.sender = None;
            chunks.push(
                worker
                    .handle
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e)))??,
            );
        }
        self.buffered_rows = 0;

        let mut row_group = self.file_writer.next_row_group()?;
        for chunk in chunks {
            chunk.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;
        self.flush_buffer().await
    }

    /// Move the bytes the file writer buffered into the sink.
    async fn flush_buffer(&mut self) -> ParquetResult<()> {
        let buffer = mem::take(self.file_writer.inner_mut());
        if buffer.is_empty() {
            return Ok(());
        }
        self.sink_writer.write(Bytes::from(buffer)).await
    }
}

/// Encoding time after which a column task yields its runtime worker.
const YIELD_AFTER: Duration = Duration::from_millis(1);

/// The task behind one [`ColumnWorker`]: encodes each slice as it arrives and
/// returns the finished column chunk once the channel closes.
async fn encode_column(
    mut writer: ArrowColumnWriter,
    mut receiver: Receiver<ArrowLeafColumn>,
    encoded_size: Arc<AtomicUsize>,
    memory_size: Arc<AtomicUsize>,
) -> ParquetResult<ArrowColumnChunk> {
    let mut since_yield = Instant::now();
    while let Some(column) = receiver.recv().await {
        writer.write(&column)?;
        encoded_size.store(writer.get_estimated_total_bytes(), Ordering::Relaxed);
        memory_size.store(writer.memory_size(), Ordering::Relaxed);

        // Encoding never awaits, so we yield every 1 ms so other tasks take the thrread
        if since_yield.elapsed() >= YIELD_AFTER {
            tokio::task::yield_now().await;
            since_yield = Instant::now();
        }
    }
    writer.close()
}
