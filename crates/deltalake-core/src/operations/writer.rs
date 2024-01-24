//! Abstractions and implementations for writing data to delta tables

use std::collections::HashMap;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::crate_version;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::protocol::Add;
use crate::storage::ObjectStoreRef;
use crate::writer::record_batch::{divide_by_partition_values, PartitionResult};
use crate::writer::stats::create_add;
use crate::writer::utils::{
    arrow_schema_without_partitions, next_data_path, record_batch_without_partitions,
    PartitionPath, ShareableBuffer,
};

// TODO databricks often suggests a file size of 100mb, should we set this default?
const DEFAULT_TARGET_FILE_SIZE: usize = 104_857_600;
const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;

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

    #[error("Error creating file name from partition info: {source}")]
    FileName {
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
}

impl WriterConfig {
    /// Create a new instance of [WriterConfig].
    pub fn new(
        table_schema: ArrowSchemaRef,
        partition_columns: Vec<String>,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<usize>,
        write_batch_size: Option<usize>,
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
        partition_values: &HashMap<String, Option<String>>,
    ) -> DeltaResult<()> {
        let partition_key =
            PartitionPath::from_hashmap(&self.config.partition_columns, partition_values)?
                .as_ref()
                .into();

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
                    self.config.partition_columns.clone(),
                    Some(self.config.writer_properties.clone()),
                    Some(self.config.target_file_size),
                    Some(self.config.write_batch_size),
                )?;
                let mut writer =
                    PartitionWriter::try_with_config(self.object_store.clone(), config)?;
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
        let mut actions = Vec::new();
        for (_, writer) in writers {
            let writer_actions = writer.close().await?;
            actions.extend(writer_actions);
        }
        Ok(actions)
    }
}

pub(crate) struct PartitionWriterConfig {
    /// Schema of the data written to disk
    file_schema: ArrowSchemaRef,
    /// Prefix applied to all paths
    prefix: Path,
    /// Values for all partition columns
    partition_values: HashMap<String, Option<String>>,
    /// Properties passed to underlying parquet writer
    writer_properties: WriterProperties,
    /// Size above which we will write a buffered parquet file to disk.
    target_file_size: usize,
    /// Row chunks passed to parquet writer. This and the internal parquet writer settings
    /// determine how fine granular we can track / control the size of resulting files.
    write_batch_size: usize,
}

impl PartitionWriterConfig {
    pub fn try_new(
        file_schema: ArrowSchemaRef,
        partition_values: HashMap<String, Option<String>>,
        partition_columns: Vec<String>,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<usize>,
        write_batch_size: Option<usize>,
    ) -> DeltaResult<Self> {
        let part_path = PartitionPath::from_hashmap(&partition_columns, &partition_values)
            .map_err(|err| WriteError::FileName {
                source: Box::new(err),
            })?;
        let prefix = Path::parse(part_path.as_ref())?;
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
        })
    }
}

pub(crate) struct PartitionWriter {
    object_store: ObjectStoreRef,
    writer_id: uuid::Uuid,
    config: PartitionWriterConfig,
    buffer: ShareableBuffer,
    arrow_writer: ArrowWriter<ShareableBuffer>,
    part_counter: usize,
    files_written: Vec<Add>,
}

impl PartitionWriter {
    /// Create a new instance of [`PartitionWriter`] from [`PartitionWriterConfig`]
    pub fn try_with_config(
        object_store: ObjectStoreRef,
        config: PartitionWriterConfig,
    ) -> DeltaResult<Self> {
        let buffer = ShareableBuffer::default();
        let arrow_writer = ArrowWriter::try_new(
            buffer.clone(),
            config.file_schema.clone(),
            Some(config.writer_properties.clone()),
        )?;

        Ok(Self {
            object_store,
            writer_id: uuid::Uuid::new_v4(),
            config,
            buffer,
            arrow_writer,
            part_counter: 0,
            files_written: Vec::new(),
        })
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

    fn reset_writer(&mut self) -> DeltaResult<(ArrowWriter<ShareableBuffer>, ShareableBuffer)> {
        let new_buffer = ShareableBuffer::default();
        let arrow_writer = ArrowWriter::try_new(
            new_buffer.clone(),
            self.config.file_schema.clone(),
            Some(self.config.writer_properties.clone()),
        )?;
        Ok((
            std::mem::replace(&mut self.arrow_writer, arrow_writer),
            std::mem::replace(&mut self.buffer, new_buffer),
        ))
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        Ok(self.arrow_writer.write(batch)?)
    }

    async fn flush_arrow_writer(&mut self) -> DeltaResult<()> {
        // replace counter / buffers and close the current writer
        let (writer, buffer) = self.reset_writer()?;
        let metadata = writer.close()?;
        let buffer = match buffer.into_inner() {
            Some(buffer) => Bytes::from(buffer),
            None => return Ok(()), // Nothing to write
        };

        // collect metadata
        let path = self.next_data_path();
        let file_size = buffer.len() as i64;

        // write file to object store
        self.object_store.put(&path, buffer).await?;
        self.files_written.push(
            create_add(
                &self.config.partition_values,
                path.to_string(),
                file_size,
                &metadata,
            )
            .map_err(|err| WriteError::CreateAdd {
                source: Box::new(err),
            })?,
        );

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
            self.write_batch(&batch.slice(offset, length))?;
            // flush currently buffered data to disk once we meet or exceed the target file size.
            if self.buffer.len() >= self.config.target_file_size {
                log::debug!("Writing file with size {:?} to disk.", self.buffer.len());
                self.flush_arrow_writer().await?;
            }
        }

        Ok(())
    }

    pub async fn close(mut self) -> DeltaResult<Vec<Add>> {
        self.flush_arrow_writer().await?;
        Ok(self.files_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::utils::flatten_list_stream as list;
    use crate::writer::test_utils::get_record_batch;
    use crate::DeltaTableBuilder;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_write_partition() {
        let object_store = DeltaTableBuilder::from_uri("memory://")
            .build_storage()
            .unwrap();
        let batch = get_record_batch(None, false);

        // write single un-partitioned batch
        let mut writer = get_writer(object_store.clone(), &batch, None, None);
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
        assert_eq!(head.size, adds[0].size as usize)
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

        let object_store = DeltaTableBuilder::from_uri("memory://")
            .build_storage()
            .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_size(1024)
            .build();
        // configure small target file size and and row group size so we can observe multiple files written
        let mut writer = get_writer(object_store.clone(), &batch, Some(properties), Some(10_000));
        writer.write(&batch).await.unwrap();

        // check that we have written more then once file, and no more then 1 is below target size
        let adds = writer.close().await.unwrap();
        assert!(adds.len() > 1);
        let target_file_count = adds
            .iter()
            .fold(0, |acc, add| acc + (add.size > 10_000) as i32);
        assert!(target_file_count >= adds.len() as i32 - 1)
    }

    fn get_writer(
        object_store: ObjectStoreRef,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        target_file_size: Option<usize>,
    ) -> PartitionWriter {
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            HashMap::new(),
            Vec::new(),
            writer_properties,
            target_file_size,
            None,
        )
        .unwrap();
        PartitionWriter::try_with_config(object_store, config).unwrap()
    }
}
