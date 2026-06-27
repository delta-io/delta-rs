//! Main writer API to write json messages to delta table
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use tracing::*;
use url::Url;

use super::utils::record_batch_from_message;
use super::{DeltaWriter, DeltaWriterError, WriteMode, ensure_legacy_writer_supports_table};
use crate::DeltaTable;
use crate::datafile::writer::DeltaWriter as DataFileDeltaWriter;
use crate::errors::DeltaTableError;
use crate::kernel::Add;
use crate::parquet_utils::default_writer_properties;
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::TablePropertiesExt as _;

/// Writes messages to a delta lake table.
pub struct JsonWriter {
    table: DeltaTable,
    /// Optional schema to use, otherwise try to rely on the schema from the [DeltaTable]
    schema_ref: Option<ArrowSchemaRef>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    /// Stats-collection config, resolved once at construction (mirrors
    /// `RecordBatchWriter`) so a new sink doesn't re-read the table snapshot.
    num_indexed_cols: DataSkippingNumIndexedCols,
    stats_columns: Option<Vec<String>>,
    /// Streaming sink (created lazily on first write); sealed at flush.
    sink: Option<DataFileDeltaWriter>,
    /// Batches streamed since the last flush (for `buffered_record_batch_count`).
    buffered_batch_count: usize,
    /// Optional target file size; when set, the sink rolls a new file once an
    /// in-progress file reaches it. `None` (default) keeps one file per partition.
    target_file_size: Option<NonZeroU64>,
}

impl std::fmt::Debug for JsonWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonWriter")
    }
}

impl JsonWriter {
    /// Create a new JsonWriter instance
    pub async fn try_new(
        table_url: Url,
        schema_ref: ArrowSchemaRef,
        partition_columns: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self, DeltaTableError> {
        let table = DeltaTableBuilder::from_url(table_url)?
            .with_storage_options(storage_options.unwrap_or_default())
            .load()
            .await?;
        ensure_legacy_writer_supports_table(&table, "JsonWriter")?;

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = default_writer_properties(parquet::basic::Compression::SNAPPY);
        let (num_indexed_cols, stats_columns) = Self::read_stats_config(&table)?;

        Ok(Self {
            table,
            schema_ref: Some(schema_ref),
            writer_properties,
            partition_columns: partition_columns.unwrap_or_default(),
            num_indexed_cols,
            stats_columns,
            sink: None,
            buffered_batch_count: 0,
            target_file_size: None,
        })
    }

    /// Resolve the data-skipping stats configuration from the table snapshot.
    fn read_stats_config(
        table: &DeltaTable,
    ) -> Result<(DataSkippingNumIndexedCols, Option<Vec<String>>), DeltaTableError> {
        let snapshot = table.snapshot()?;
        let table_config = snapshot.table_config();
        Ok((
            table_config.num_indexed_cols(),
            table_config
                .data_skipping_stats_columns
                .as_ref()
                .map(|cols| cols.iter().map(|c| c.to_string()).collect_vec()),
        ))
    }

    /// Creates a JsonWriter to write to the given table
    pub fn for_table(table: &DeltaTable) -> Result<JsonWriter, DeltaTableError> {
        ensure_legacy_writer_supports_table(table, "JsonWriter")?;

        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.snapshot()?.metadata();
        let partition_columns = metadata.partition_columns().into();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = default_writer_properties(parquet::basic::Compression::SNAPPY);
        let (num_indexed_cols, stats_columns) = Self::read_stats_config(table)?;

        Ok(Self {
            table: table.clone(),
            writer_properties,
            partition_columns,
            schema_ref: None,
            num_indexed_cols,
            stats_columns,
            sink: None,
            buffered_batch_count: 0,
            target_file_size: None,
        })
    }

    /// Approximate total encoded (parquet) size of the data buffered across all
    /// in-progress files in the underlying dataset sink (one open file per
    /// partition). May be used by the caller to decide when to finalize the
    /// buffered writes by calling [`flush`](Self::flush).
    pub fn buffer_len(&self) -> usize {
        self.sink
            .as_ref()
            .map_or(0, DataFileDeltaWriter::buffered_size)
    }

    /// Returns the number of record batches streamed since the last flush.
    pub fn buffered_record_batch_count(&self) -> usize {
        self.buffered_batch_count
    }

    /// Resets internal state, discarding any data buffered since the last flush.
    ///
    /// This only drops in-memory buffers; it does not touch object storage. If a
    /// `target_file_size` is set, the underlying sink may already have rolled and
    /// uploaded one or more completed files since the last flush. `reset` does not
    /// delete those files, so they remain in storage unreferenced by the log
    /// (reclaimed only by a later vacuum). Call [`flush`](Self::flush) instead to
    /// commit buffered data.
    pub fn reset(&mut self) {
        self.sink = None;
        self.buffered_batch_count = 0;
    }

    /// Sets a target file size; once an in-progress file reaches it the writer
    /// finalizes it and rolls a new one. Without this the writer emits a single
    /// file per partition per flush (the default).
    ///
    /// A `target_file_size` of `0` means "no limit": size-based rolling is
    /// disabled and the writer keeps one file per partition per flush, exactly
    /// as if this method had not been called.
    pub fn with_target_file_size(mut self, target_file_size: u64) -> Self {
        self.target_file_size = NonZeroU64::new(target_file_size);
        self
    }

    /// Build a fresh streaming sink for the table's current config and the
    /// writer's schema/partitioning/target size.
    fn new_sink(&self) -> Result<DataFileDeltaWriter, DeltaTableError> {
        Ok(super::build_streaming_sink(
            self.table.object_store(),
            self.arrow_schema(),
            self.partition_columns.clone(),
            self.writer_properties.clone(),
            self.target_file_size,
            self.num_indexed_cols,
            self.stats_columns.clone(),
        ))
    }

    /// Returns the user-defined arrow schema representation or the schema defined for the wrapped
    /// table.
    ///
    pub fn arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        if let Some(schema_ref) = self.schema_ref.as_ref() {
            return schema_ref.clone();
        }
        let schema = self
            .table
            .snapshot()
            .expect("Failed to unwrap snapshot for table")
            .schema();
        Arc::new(
            schema
                .as_ref()
                .try_into_arrow()
                .expect("Failed to coerce delta schema to arrow"),
        )
    }
}

#[async_trait::async_trait]
impl DeltaWriter<Vec<Value>> for JsonWriter {
    /// Write a chunk of values into the internal write buffers with the default write mode
    async fn write(&mut self, values: Vec<Value>) -> Result<(), DeltaTableError> {
        self.write_with_mode(values, WriteMode::Default).await
    }

    /// Decode the JSON values into a record batch and stream it into the dataset
    /// writer; partitioning and parquet encoding happen incrementally, and files
    /// are finalized at flush.
    ///
    /// JSON decode and schema-mismatch errors are reported here, per write. A
    /// record that decodes to valid Arrow but only fails when parquet-encoded
    /// surfaces its error later, from [`flush`](JsonWriter::flush), and fails that
    /// whole flush (every batch written since the last flush) rather than being
    /// skipped individually.
    async fn write_with_mode(
        &mut self,
        values: Vec<Value>,
        mode: WriteMode,
    ) -> Result<(), DeltaTableError> {
        if mode != WriteMode::Default {
            warn!(
                "The JsonWriter does not currently support non-default write modes, falling back to default mode"
            );
        }
        // An empty write is a no-op (the JSON decoder yields no batch for `[]`).
        if values.is_empty() {
            return Ok(());
        }
        let arrow_schema = self.arrow_schema();
        let record_batch = record_batch_from_message(arrow_schema.clone(), values.as_slice())?;

        if record_batch.schema() != arrow_schema {
            return Err(DeltaWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: arrow_schema,
            }
            .into());
        }

        if self.sink.is_none() {
            self.sink = Some(self.new_sink()?);
        }
        super::write_into_sink(
            &mut self.sink,
            &mut self.buffered_batch_count,
            &record_batch,
        )
        .await
    }

    /// Finalize all files written since the last flush and return their [`Add`]
    /// actions, resetting internal state to handle another flush window.
    ///
    /// These actions should be committed to the [DeltaTable] for the written data.
    #[instrument(skip(self), fields(batch_count = 0))]
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        Span::current().record("batch_count", self.buffered_batch_count);
        self.buffered_batch_count = 0;
        let actions = match self.sink.take() {
            Some(sink) => sink.close().await?,
            None => Vec::new(),
        };
        debug!(actions_count = actions.len(), "flush completed");
        Ok(actions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::record_batch::RecordBatch;
    use delta_kernel::expressions::Scalar;
    use indexmap::IndexMap;

    use crate::kernel::scalars::ScalarExt;

    use arrow_schema::ArrowError;
    #[cfg(feature = "datafusion")]
    use futures::TryStreamExt;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    use crate::arrow::array::Int32Array;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use crate::operations::create::CreateBuilder;
    use crate::writer::test_utils::get_delta_schema;

    /// Extract partition scalar values from a record batch.
    fn extract_partition_values(
        partition_cols: &[String],
        record_batch: &RecordBatch,
    ) -> Result<IndexMap<String, Scalar>, DeltaWriterError> {
        let mut partition_values = IndexMap::new();

        for col_name in partition_cols.iter() {
            let arrow_schema = record_batch.schema();
            let i = arrow_schema.index_of(col_name)?;
            let col = record_batch.column(i);
            let value = Scalar::from_array(col.as_ref(), 0)
                .ok_or(DeltaWriterError::MissingPartitionColumn(col_name.clone()))?;

            partition_values.insert(col_name.clone(), value);
        }

        Ok(partition_values)
    }

    /// Generate a simple test table which has been pre-created at version 0
    async fn get_test_table(table_dir: &tempfile::TempDir) -> DeltaTable {
        let schema = get_delta_schema();
        let path = table_dir.path().to_str().unwrap().to_string();

        let mut table = CreateBuilder::new()
            .with_location(&path)
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        table.load().await.expect("Failed to load table");
        assert_eq!(table.version(), Some(0));
        table
    }

    #[tokio::test]
    async fn test_json_write_empty_is_noop() {
        let table_dir = tempfile::tempdir().unwrap();
        let table = get_test_table(&table_dir).await;
        let mut writer = JsonWriter::for_table(&table).unwrap();

        // An empty write must be a no-op (not an error), and produce no files.
        writer.write(vec![]).await.unwrap();
        assert_eq!(writer.buffered_record_batch_count(), 0);
        let add_actions = writer.flush().await.unwrap();
        assert!(add_actions.is_empty());
    }

    #[tokio::test]
    async fn test_partition_not_written_to_parquet() {
        let table_dir = tempfile::tempdir().unwrap();
        let table = get_test_table(&table_dir).await;
        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
        .unwrap();

        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );

        writer.write(vec![data]).await.unwrap();
        let add_actions = writer.flush().await.unwrap();
        let add = &add_actions[0];
        let path = table_dir.path().join(&add.path);

        let file = File::open(path.as_path()).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();
        let schema_desc = metadata.file_metadata().schema_descr();

        let columns = schema_desc
            .columns()
            .iter()
            .map(|desc| desc.name().to_string())
            .collect::<Vec<String>>();
        assert_eq!(columns, vec!["id".to_string(), "value".to_string()]);
    }

    #[tokio::test]
    async fn test_json_writer_for_table_defaults_include_delta_rs_created_by() {
        let table_dir = tempfile::tempdir().unwrap();
        let table = get_test_table(&table_dir).await;

        let writer = JsonWriter::for_table(&table).unwrap();

        assert_eq!(
            writer.writer_properties.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
    }

    #[tokio::test]
    async fn test_json_writer_try_new_defaults_include_delta_rs_created_by() {
        let table_dir = tempfile::tempdir().unwrap();
        let table = get_test_table(&table_dir).await;
        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();

        let writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            writer.writer_properties.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
    }

    #[test]
    fn test_extract_partition_values() {
        let record_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("col1", ArrowDataType::Int32, false),
                ArrowField::new("col2", ArrowDataType::Int32, false),
                ArrowField::new("col3", ArrowDataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![2, 1])),
                Arc::new(Int32Array::from(vec![None, None])),
            ],
        )
        .unwrap();

        assert_eq!(
            extract_partition_values(
                &[String::from("col1"), String::from("col2"),],
                &record_batch
            )
            .unwrap(),
            IndexMap::from([
                (String::from("col1"), Scalar::Integer(1)),
                (String::from("col2"), Scalar::Integer(2)),
            ])
        );
        assert_eq!(
            extract_partition_values(&[String::from("col1")], &record_batch).unwrap(),
            IndexMap::from([(String::from("col1"), Scalar::Integer(1)),])
        );
        assert!(extract_partition_values(&[String::from("col4")], &record_batch).is_err())
    }

    #[tokio::test]
    async fn test_parsing_error() {
        let table_dir = tempfile::tempdir().unwrap();
        let table = get_test_table(&table_dir).await;

        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
        .unwrap();

        let data = serde_json::json!(
            {
                "id" : "A",
                "value": "abc",
                "modified": "2021-02-01"
            }
        );

        let res = writer.write(vec![data]).await;
        assert!(matches!(
            res,
            Err(DeltaTableError::Arrow {
                source: ArrowError::JsonError(_)
            })
        ));
    }

    // The following sets of tests are related to #1386 and mergeSchema support
    // <https://github.com/delta-io/delta-rs/issues/1386>
    mod schema_evolution {
        use super::*;

        #[tokio::test]
        async fn test_json_write_mismatched_values() {
            let table_dir = tempfile::tempdir().unwrap();
            let table = get_test_table(&table_dir).await;

            let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
            let mut writer = JsonWriter::try_new(
                Url::from_directory_path(table_dir.path()).unwrap(),
                arrow_schema,
                Some(vec!["modified".to_string()]),
                None,
            )
            .await
            .unwrap();

            let data = serde_json::json!(
                {
                    "id" : "A",
                    "value": 42,
                    "modified": "2021-02-01"
                }
            );

            writer.write(vec![data]).await.unwrap();
            let add_actions = writer.flush().await.unwrap();
            assert_eq!(add_actions.len(), 1);

            let second_data = serde_json::json!(
                {
                    "id" : 1,
                    "name" : "Ion"
                }
            );

            if writer.write(vec![second_data]).await.is_ok() {
                panic!("Should not have successfully written");
            }
        }

        #[cfg(feature = "datafusion")]
        #[tokio::test]
        async fn test_json_write_mismatched_schema() {
            let table_dir = tempfile::tempdir().unwrap();
            let mut table = get_test_table(&table_dir).await;

            let mut writer = JsonWriter::try_new(
                table.table_url().clone(),
                table.snapshot().unwrap().snapshot().arrow_schema(),
                Some(vec!["modified".to_string()]),
                None,
            )
            .await
            .unwrap();

            let data = serde_json::json!(
                {
                    "id" : "A",
                    "value": 42,
                    "modified": "2021-02-01"
                }
            );

            writer.write(vec![data]).await.unwrap();
            let add_actions = writer.flush().await.unwrap();
            assert_eq!(add_actions.len(), 1);

            let second_data = serde_json::json!(
                {
                    "postcode" : 1,
                    "name" : "Ion"
                }
            );

            // TODO This should fail because we haven't asked to evolve the schema
            writer.write(vec![second_data]).await.unwrap();
            writer.flush_and_commit(&mut table).await.unwrap();
            assert_eq!(table.version(), Some(1));
        }
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_json_write_checkpoint() {
        use std::fs;

        let table_dir = tempfile::tempdir().unwrap();
        let schema = get_delta_schema();
        let path = table_dir.path().to_str().unwrap().to_string();
        let config: HashMap<String, Option<String>> = vec![
            (
                "delta.checkpointInterval".to_string(),
                Some("5".to_string()),
            ),
            ("delta.checkpointPolicy".to_string(), Some("v2".to_string())),
        ]
        .into_iter()
        .collect();
        let mut table = CreateBuilder::new()
            .with_location(&path)
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(schema.fields().cloned())
            .with_configuration(config)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let mut writer = JsonWriter::for_table(&table).unwrap();
        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );
        for _ in 1..6 {
            writer.write(vec![data.clone()]).await.unwrap();
            writer.flush_and_commit(&mut table).await.unwrap();
        }
        let dir_path = path + "/_delta_log";

        let target_file = "00000000000000000004.checkpoint.parquet";
        let entries: Vec<_> = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().into_string().unwrap() == target_file)
            .collect();
        assert_eq!(entries.len(), 1);
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_json_write_data_skipping_stats_columns() {
        let table_dir = tempfile::tempdir().unwrap();
        let path = table_dir.path().to_str().unwrap().to_string();
        let config: HashMap<String, Option<String>> = vec![(
            "delta.dataSkippingStatsColumns".to_string(),
            Some("id,value".to_string()),
        )]
        .into_iter()
        .collect();

        let schema = get_delta_schema();
        let mut table = CreateBuilder::new()
            .with_location(&path)
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(schema.fields().cloned())
            .with_configuration(config)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
        .unwrap();
        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );

        writer.write(vec![data]).await.unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();
        assert_eq!(table.version(), Some(1));
        let add_actions: Vec<_> = table
            .snapshot()
            .unwrap()
            .snapshot()
            .file_views(&table.log_store, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(add_actions.len(), 1);
        let expected_stats = "{\"numRecords\":1,\"minValues\":{\"id\":\"A\",\"value\":42},\"maxValues\":{\"id\":\"A\",\"value\":42},\"nullCount\":{\"id\":0,\"value\":0}}";
        assert_eq!(
            expected_stats.parse::<serde_json::Value>().unwrap(),
            add_actions
                .into_iter()
                .next()
                .unwrap()
                .stats()
                .unwrap()
                .parse::<serde_json::Value>()
                .unwrap()
        );
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_json_write_data_skipping_num_indexed_cols() {
        let table_dir = tempfile::tempdir().unwrap();
        let path = table_dir.path().to_str().unwrap().to_string();
        let config: HashMap<String, Option<String>> = vec![(
            "delta.dataSkippingNumIndexedCols".to_string(),
            Some("1".to_string()),
        )]
        .into_iter()
        .collect();

        let schema = get_delta_schema();
        let mut table = CreateBuilder::new()
            .with_location(&path)
            .with_table_name("test-table")
            .with_comment("A table for running tests")
            .with_columns(schema.fields().cloned())
            .with_configuration(config)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let arrow_schema = table.snapshot().unwrap().snapshot().arrow_schema();
        let mut writer = JsonWriter::try_new(
            table.table_url().clone(),
            arrow_schema,
            Some(vec!["modified".to_string()]),
            None,
        )
        .await
        .unwrap();
        let data = serde_json::json!(
            {
                "id" : "A",
                "value": 42,
                "modified": "2021-02-01"
            }
        );

        writer.write(vec![data]).await.unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();
        assert_eq!(table.version(), Some(1));
        let add_actions: Vec<_> = table
            .snapshot()
            .unwrap()
            .snapshot()
            .file_views(&table.log_store, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(add_actions.len(), 1);
        let expected_stats = "{\"numRecords\":1,\"minValues\":{\"id\":\"A\"},\"maxValues\":{\"id\":\"A\"},\"nullCount\":{\"id\":0}}";
        assert_eq!(
            expected_stats.parse::<serde_json::Value>().unwrap(),
            add_actions
                .into_iter()
                .next()
                .unwrap()
                .stats()
                .unwrap()
                .parse::<serde_json::Value>()
                .unwrap()
        );
    }
}
