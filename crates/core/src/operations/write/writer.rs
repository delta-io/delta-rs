//! Abstractions and implementations for writing data to delta tables

use std::collections::HashMap;
use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use futures::{StreamExt, TryStreamExt};
use indexmap::IndexMap;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::task::JoinSet;
use tracing::*;

use crate::crate_version;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Add, PartitionsExt};
use crate::logstore::ObjectStoreRef;
use crate::writer::record_batch::{divide_by_partition_values, PartitionResult};
use crate::writer::stats::create_add;
use crate::writer::utils::{
    arrow_schema_without_partitions, next_data_path, record_batch_without_partitions,
};

use parquet::file::metadata::ParquetMetaData;

// TODO databricks often suggests a file size of 100mb, should we set this default?
const DEFAULT_TARGET_FILE_SIZE: usize = 104_857_600;
const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;
const DEFAULT_UPLOAD_PART_SIZE: usize = 1024 * 1024 * 5;
const DEFAULT_MAX_CONCURRENCY_TASKS: usize = 10;

fn upload_part_size() -> usize {
    static UPLOAD_SIZE: OnceLock<usize> = OnceLock::new();
    *UPLOAD_SIZE.get_or_init(|| {
        std::env::var("DELTARS_UPLOAD_PART_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|size| {
                if size < DEFAULT_UPLOAD_PART_SIZE {
                    // Minimum part size in GCS and S3
                    debug!("DELTARS_UPLOAD_PART_SIZE must be at least 5MB, therefore falling back on default of 5MB.");
                    DEFAULT_UPLOAD_PART_SIZE
                } else if size > 1024 * 1024 * 1024 * 5 {
                    // Maximum part size in GCS and S3
                    debug!("DELTARS_UPLOAD_PART_SIZE must not be higher than 5GB, therefore capping it at 5GB.");
                    1024 * 1024 * 1024 * 5
                } else {
                    size
                }
            })
            .unwrap_or(DEFAULT_UPLOAD_PART_SIZE)
    })
}

fn get_max_concurrency_tasks() -> usize {
    static MAX_CONCURRENCY_TASKS: OnceLock<usize> = OnceLock::new();
    *MAX_CONCURRENCY_TASKS.get_or_init(|| {
        std::env::var("DELTARS_MAX_CONCURRENCY_TASKS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONCURRENCY_TASKS)
    })
}

/// Upload a parquet file to object store and return metadata for creating an Add action
#[instrument(skip(arrow_writer), fields(rows = 0, size = 0))]
async fn upload_parquet_file(
    mut arrow_writer: AsyncArrowWriter<ParquetObjectWriter>,
    path: Path,
) -> DeltaResult<(Path, usize, ParquetMetaData)> {
    let metadata = arrow_writer.finish().await?;
    let file_size = arrow_writer.bytes_written();
    Span::current().record("rows", metadata.file_metadata().num_rows());
    Span::current().record("size", file_size);
    debug!("multipart upload completed successfully");

    Ok((path, file_size, metadata))
}

#[derive(thiserror::Error, Debug)]
enum WriteError {
    #[error("Unexpected Arrow schema: got: {schema}, expected: {expected_schema}")]
    SchemaMismatch {
        schema: ArrowSchemaRef,
        expected_schema: ArrowSchemaRef,
    },

    #[error("Error creating add action: {source}")]
    CreateAdd {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Error handling Arrow data: {source}")]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error("Error partitioning record batch: {0}")]
    Partitioning(String),
}

impl From<WriteError> for DeltaTableError {
    fn from(err: WriteError) -> Self {
        match err {
            WriteError::SchemaMismatch { .. } => DeltaTableError::SchemaMismatch {
                msg: err.to_string(),
            },
            WriteError::Arrow { source } => DeltaTableError::Arrow { source },
            _ => DeltaTableError::GenericError {
                source: Box::new(err),
            },
        }
    }
}

/// Configuration to write data into Delta tables
#[derive(Debug)]
pub struct WriterConfig {
    /// Schema of the delta table
    table_schema: ArrowSchemaRef,
    /// Column names for columns the table is partitioned by
    partition_columns: Vec<String>,
    /// Properties passed to underlying parquet writer
    writer_properties: WriterProperties,
    /// Size above which we will write a buffered parquet file to disk.
    target_file_size: usize,
    /// Row chunks passed to parquet writer. This and the internal parquet writer settings
    /// determine how fine granular we can track / control the size of resulting files.
    write_batch_size: usize,
    /// Num index cols to collect stats for
    num_indexed_cols: DataSkippingNumIndexedCols,
    /// Stats columns, specific columns to collect stats from, takes precedence over num_indexed_cols
    stats_columns: Option<Vec<String>>,
}

impl WriterConfig {
    /// Create a new instance of [WriterConfig].
    pub fn new(
        table_schema: ArrowSchemaRef,
        partition_columns: Vec<String>,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<usize>,
        write_batch_size: Option<usize>,
        num_indexed_cols: DataSkippingNumIndexedCols,
        stats_columns: Option<Vec<String>>,
    ) -> Self {
        let writer_properties = writer_properties.unwrap_or_else(|| {
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build()
        });
        let target_file_size = target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE);
        let write_batch_size = write_batch_size.unwrap_or(DEFAULT_WRITE_BATCH_SIZE);

        Self {
            table_schema,
            partition_columns,
            writer_properties,
            target_file_size,
            write_batch_size,
            num_indexed_cols,
            stats_columns,
        }
    }

    /// Schema of files written to disk
    pub fn file_schema(&self) -> ArrowSchemaRef {
        arrow_schema_without_partitions(&self.table_schema, &self.partition_columns)
    }
}

/// A parquet writer implementation tailored to the needs of writing data to a delta table.
pub struct DeltaWriter {
    /// An object store pointing at Delta table root
    object_store: ObjectStoreRef,
    /// configuration for the writers
    config: WriterConfig,
    /// partition writers for individual partitions
    partition_writers: HashMap<Path, PartitionWriter>,
}

impl DeltaWriter {
    /// Create a new instance of [`DeltaWriter`]
    pub fn new(object_store: ObjectStoreRef, config: WriterConfig) -> Self {
        Self {
            object_store,
            config,
            partition_writers: HashMap::new(),
        }
    }

    /// Apply custom writer_properties to the underlying parquet writer
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.config.writer_properties = writer_properties;
        self
    }

    fn divide_by_partition_values(
        &mut self,
        values: &RecordBatch,
    ) -> DeltaResult<Vec<PartitionResult>> {
        Ok(divide_by_partition_values(
            self.config.file_schema(),
            self.config.partition_columns.clone(),
            values,
        )
        .map_err(|err| WriteError::Partitioning(err.to_string()))?)
    }

    /// Write a batch to the partition induced by the partition_values. The record batch is expected
    /// to be pre-partitioned and only contain rows that belong into the same partition.
    /// However, it should still contain the partition columns.
    pub async fn write_partition(
        &mut self,
        record_batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
    ) -> DeltaResult<()> {
        let partition_key = Path::parse(partition_values.hive_partition_path())?;

        let record_batch =
            record_batch_without_partitions(&record_batch, &self.config.partition_columns)?;

        match self.partition_writers.get_mut(&partition_key) {
            Some(writer) => {
                writer.write(&record_batch).await?;
            }
            None => {
                let config = PartitionWriterConfig::try_new(
                    self.config.file_schema(),
                    partition_values.clone(),
                    Some(self.config.writer_properties.clone()),
                    Some(self.config.target_file_size),
                    Some(self.config.write_batch_size),
                    None,
                )?;
                let mut writer = PartitionWriter::try_with_config(
                    self.object_store.clone(),
                    config,
                    self.config.num_indexed_cols,
                    self.config.stats_columns.clone(),
                )?;
                writer.write(&record_batch).await?;
                let _ = self.partition_writers.insert(partition_key, writer);
            }
        }

        Ok(())
    }

    /// Buffers record batches in-memory per partition up to appx. `target_file_size` for a partition.
    /// Flushes data to storage once a full file can be written.
    ///
    /// The `close` method has to be invoked to write all data still buffered
    /// and get the list of all written files.
    pub async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        for result in self.divide_by_partition_values(batch)? {
            self.write_partition(result.record_batch, &result.partition_values)
                .await?;
        }
        Ok(())
    }

    /// Close the writer and get the new [Add] actions.
    ///
    /// This will flush all remaining data.
    pub async fn close(mut self) -> DeltaResult<Vec<Add>> {
        let writers = std::mem::take(&mut self.partition_writers);
        let actions = futures::stream::iter(writers)
            .map(|(_, writer)| async move {
                let writer_actions = writer.close().await?;
                Ok::<_, DeltaTableError>(writer_actions)
            })
            .buffered(num_cpus::get())
            .try_fold(Vec::new(), |mut acc, actions| {
                acc.extend(actions);
                futures::future::ready(Ok(acc))
            })
            .await?;

        Ok(actions)
    }
}

/// Write configuration for partition writers
#[derive(Debug, Clone)]
pub struct PartitionWriterConfig {
    /// Schema of the data written to disk
    file_schema: ArrowSchemaRef,
    /// Prefix applied to all paths
    prefix: Path,
    /// Values for all partition columns
    partition_values: IndexMap<String, Scalar>,
    /// Properties passed to underlying parquet writer
    writer_properties: WriterProperties,
    /// Size above which we will write a buffered parquet file to disk.
    target_file_size: usize,
    /// Row chunks passed to parquet writer. This and the internal parquet writer settings
    /// determine how fine granular we can track / control the size of resulting files.
    write_batch_size: usize,
    /// Concurrency level for writing to object store
    max_concurrency_tasks: usize,
}

impl PartitionWriterConfig {
    /// Create a new instance of [PartitionWriterConfig]
    pub fn try_new(
        file_schema: ArrowSchemaRef,
        partition_values: IndexMap<String, Scalar>,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<usize>,
        write_batch_size: Option<usize>,
        max_concurrency_tasks: Option<usize>,
    ) -> DeltaResult<Self> {
        let part_path = partition_values.hive_partition_path();
        let prefix = Path::parse(part_path)?;
        let writer_properties = writer_properties.unwrap_or_else(|| {
            WriterProperties::builder()
                .set_created_by(format!("delta-rs version {}", crate_version()))
                .build()
        });
        let target_file_size = target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE);
        let write_batch_size = write_batch_size.unwrap_or(DEFAULT_WRITE_BATCH_SIZE);

        Ok(Self {
            file_schema,
            prefix,
            partition_values,
            writer_properties,
            target_file_size,
            write_batch_size,
            max_concurrency_tasks: max_concurrency_tasks.unwrap_or_else(get_max_concurrency_tasks),
        })
    }
}

enum LazyArrowWriter {
    Initialized(Path, ObjectStoreRef, PartitionWriterConfig),
    Writing(Path, AsyncArrowWriter<ParquetObjectWriter>),
}

impl LazyArrowWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        match self {
            LazyArrowWriter::Initialized(path, object_store, config) => {
                let writer = ParquetObjectWriter::from_buf_writer(
                    BufWriter::with_capacity(
                        object_store.clone(),
                        path.clone(),
                        upload_part_size(),
                    )
                    .with_max_concurrency(config.max_concurrency_tasks),
                );
                let mut arrow_writer = AsyncArrowWriter::try_new(
                    writer,
                    config.file_schema.clone(),
                    Some(config.writer_properties.clone()),
                )?;
                arrow_writer.write(batch).await?;
                *self = LazyArrowWriter::Writing(path.clone(), arrow_writer);
            }
            LazyArrowWriter::Writing(_, arrow_writer) => {
                arrow_writer.write(batch).await?;
            }
        }

        Ok(())
    }

    fn estimated_size(&self) -> usize {
        match self {
            LazyArrowWriter::Initialized(_, _, _) => 0,
            LazyArrowWriter::Writing(_, arrow_writer) => {
                arrow_writer.bytes_written() + arrow_writer.in_progress_size()
            }
        }
    }
}

/// Partition writer implementation
/// This writer takes in table data as RecordBatches and writes it out to partitioned parquet files.
/// It buffers data in memory until it reaches a certain size, then writes it out to optimize file sizes.
/// When you complete writing you get back a list of Add actions that can be used to update the Delta table commit log.
pub struct PartitionWriter {
    object_store: ObjectStoreRef,
    writer_id: uuid::Uuid,
    config: PartitionWriterConfig,
    writer: LazyArrowWriter,
    part_counter: usize,
    /// Num index cols to collect stats for
    num_indexed_cols: DataSkippingNumIndexedCols,
    /// Stats columns, specific columns to collect stats from, takes precedence over num_indexed_cols
    stats_columns: Option<Vec<String>>,
    in_flight_writers: JoinSet<DeltaResult<(Path, usize, ParquetMetaData)>>,
}

impl PartitionWriter {
    /// Create a new instance of [`PartitionWriter`] from [`PartitionWriterConfig`]
    pub fn try_with_config(
        object_store: ObjectStoreRef,
        config: PartitionWriterConfig,
        num_indexed_cols: DataSkippingNumIndexedCols,
        stats_columns: Option<Vec<String>>,
    ) -> DeltaResult<Self> {
        let writer_id = uuid::Uuid::new_v4();
        let first_path = next_data_path(&config.prefix, 0, &writer_id, &config.writer_properties);
        let writer = Self::create_writer(object_store.clone(), first_path.clone(), &config)?;

        Ok(Self {
            object_store,
            writer_id,
            config,
            writer,
            part_counter: 0,
            num_indexed_cols,
            stats_columns,
            in_flight_writers: JoinSet::new(),
        })
    }

    fn create_writer(
        object_store: ObjectStoreRef,
        path: Path,
        config: &PartitionWriterConfig,
    ) -> DeltaResult<LazyArrowWriter> {
        let state = LazyArrowWriter::Initialized(path, object_store.clone(), config.clone());
        Ok(state)
    }

    fn next_data_path(&mut self) -> Path {
        self.part_counter += 1;

        next_data_path(
            &self.config.prefix,
            self.part_counter,
            &self.writer_id,
            &self.config.writer_properties,
        )
    }

    async fn reset_writer(&mut self) -> DeltaResult<()> {
        let next_path = self.next_data_path();
        let new_writer = Self::create_writer(self.object_store.clone(), next_path, &self.config)?;
        let state = std::mem::replace(&mut self.writer, new_writer);

        if let LazyArrowWriter::Writing(path, arrow_writer) = state {
            self.in_flight_writers
                .spawn(upload_parquet_file(arrow_writer, path));
        }
        Ok(())
    }

    /// Buffers record batches in-memory up to appx. `target_file_size`.
    /// Flushes data to storage once a full file can be written.
    ///
    /// The `close` method has to be invoked to write all data still buffered
    /// and get the list of all written files.
    pub async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        if batch.schema() != self.config.file_schema {
            return Err(WriteError::SchemaMismatch {
                schema: batch.schema(),
                expected_schema: self.config.file_schema.clone(),
            }
            .into());
        }

        let max_offset = batch.num_rows();
        for offset in (0..max_offset).step_by(self.config.write_batch_size) {
            let length = usize::min(self.config.write_batch_size, max_offset - offset);
            self.writer
                .write_batch(&batch.slice(offset, length))
                .await?;
            // flush currently buffered data to disk once we meet or exceed the target file size.
            let estimated_size = self.writer.estimated_size();
            if estimated_size >= self.config.target_file_size {
                debug!("Writing file with estimated size {estimated_size:?} in background.");
                self.reset_writer().await?;
            }
        }

        Ok(())
    }

    /// Close the writer and get the new [Add] actions.
    ///
    /// This will flush any remaining data and collect all Add actions from background tasks.
    pub async fn close(mut self) -> DeltaResult<Vec<Add>> {
        if let LazyArrowWriter::Writing(path, arrow_writer) = self.writer {
            self.in_flight_writers
                .spawn(upload_parquet_file(arrow_writer, path));
        }

        let mut results = Vec::new();
        while let Some(result) = self.in_flight_writers.join_next().await {
            match result {
                Ok(Ok(data)) => results.push(data),
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(e) => {
                    return Err(DeltaTableError::GenericError {
                        source: Box::new(e),
                    })
                }
            }
        }

        let adds = results
            .into_iter()
            .map(|(path, file_size, metadata)| {
                create_add(
                    &self.config.partition_values,
                    path.to_string(),
                    file_size as i64,
                    &metadata,
                    self.num_indexed_cols,
                    &self.stats_columns,
                )
                .map_err(|err| WriteError::CreateAdd {
                    source: Box::new(err),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(adds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logstore::tests::flatten_list_stream as list;
    use crate::table::config::DEFAULT_NUM_INDEX_COLS;
    use crate::writer::test_utils::*;
    use crate::DeltaTableBuilder;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn get_delta_writer(
        object_store: ObjectStoreRef,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<usize>,
        write_batch_size: Option<usize>,
    ) -> DeltaWriter {
        let config = WriterConfig::new(
            batch.schema(),
            vec![],
            writer_properties,
            target_file_size,
            write_batch_size,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        );
        DeltaWriter::new(object_store, config)
    }

    fn get_partition_writer(
        object_store: ObjectStoreRef,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<usize>,
        write_batch_size: Option<usize>,
    ) -> PartitionWriter {
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            writer_properties,
            target_file_size,
            write_batch_size,
            None,
        )
        .unwrap();
        PartitionWriter::try_with_config(
            object_store,
            config,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_partition() {
        let log_store = DeltaTableBuilder::from_uri(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap();
        let object_store = log_store.object_store(None);
        let batch = get_record_batch(None, false);

        // write single un-partitioned batch
        let mut writer = get_partition_writer(object_store.clone(), &batch, None, None, None);
        writer.write(&batch).await.unwrap();
        let files = list(object_store.as_ref(), None).await.unwrap();
        assert_eq!(files.len(), 0);
        let adds = writer.close().await.unwrap();
        let files = list(object_store.as_ref(), None).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files.len(), adds.len());
        let head = object_store
            .head(&Path::from(adds[0].path.clone()))
            .await
            .unwrap();
        assert_eq!(head.size, adds[0].size as u64)
    }

    #[tokio::test]
    async fn test_write_partition_with_parts() {
        let base_int = Arc::new(Int32Array::from((0..10000).collect::<Vec<i32>>()));
        let base_str = Arc::new(StringArray::from(vec!["A"; 10000]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![base_str, base_int]).unwrap();

        let object_store = DeltaTableBuilder::from_uri(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store(None);
        let properties = WriterProperties::builder()
            .set_max_row_group_size(1024)
            .build();
        // configure small target file size and and row group size so we can observe multiple files written
        let mut writer =
            get_partition_writer(object_store, &batch, Some(properties), Some(10_000), None);
        writer.write(&batch).await.unwrap();

        // check that we have written more then once file, and no more then 1 is below target size
        let adds = writer.close().await.unwrap();
        assert!(adds.len() > 1);
        let target_file_count = adds
            .iter()
            .fold(0, |acc, add| acc + (add.size > 10_000) as i32);
        assert!(target_file_count >= adds.len() as i32 - 1)
    }

    #[tokio::test]
    async fn test_unflushed_row_group_size() {
        let base_int = Arc::new(Int32Array::from((0..10000).collect::<Vec<i32>>()));
        let base_str = Arc::new(StringArray::from(vec!["A"; 10000]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![base_str, base_int]).unwrap();

        let object_store = DeltaTableBuilder::from_uri(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store(None);
        // configure small target file size so we can observe multiple files written
        let mut writer = get_partition_writer(object_store, &batch, None, Some(10_000), None);
        writer.write(&batch).await.unwrap();

        // check that we have written more then once file, and no more then 1 is below target size
        let adds = writer.close().await.unwrap();
        assert!(adds.len() > 1);
        let target_file_count = adds
            .iter()
            .fold(0, |acc, add| acc + (add.size > 10_000) as i32);
        assert!(target_file_count >= adds.len() as i32 - 1)
    }

    #[tokio::test]
    async fn test_do_not_write_empty_file_on_close() {
        let base_int = Arc::new(Int32Array::from((0..10000_i32).collect::<Vec<i32>>()));
        let base_str = Arc::new(StringArray::from(vec!["A"; 10000]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![base_str, base_int]).unwrap();

        let object_store = DeltaTableBuilder::from_uri(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store(None);
        // configure high batch size and low file size to observe one file written and flushed immediately
        // upon writing batch, then ensures the buffer is empty upon closing writer
        let mut writer = get_partition_writer(object_store, &batch, None, Some(9000), Some(10000));
        writer.write(&batch).await.unwrap();

        let adds = writer.close().await.unwrap();
        assert_eq!(adds.len(), 1);
    }

    #[tokio::test]
    async fn test_write_mismatched_schema() {
        let log_store = DeltaTableBuilder::from_uri(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap();
        let object_store = log_store.object_store(None);
        let batch = get_record_batch(None, false);

        // write single un-partitioned batch
        let mut writer = get_delta_writer(object_store.clone(), &batch, None, None, None);
        writer.write(&batch).await.unwrap();
        // Ensure the write hasn't been flushed
        let files = list(object_store.as_ref(), None).await.unwrap();
        assert_eq!(files.len(), 0);

        // Create a second batch with a different schema
        let second_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let second_batch = RecordBatch::try_new(
            second_schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("will"), Some("robert")])),
            ],
        )
        .unwrap();

        let result = writer.write(&second_batch).await;
        assert!(result.is_err());

        match result {
            Ok(_) => {
                panic!("Should not have successfully written");
            }
            Err(e) => {
                match e {
                    DeltaTableError::SchemaMismatch { .. } => {
                        // this is expected
                    }
                    others => {
                        panic!("Got the wrong error: {others:?}");
                    }
                }
            }
        };
    }
}
