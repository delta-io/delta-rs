//! Main writer API to write json messages to delta table
#![allow(deprecated)]
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use parquet::errors::ParquetError;
use serde_json::Value;
use tracing::*;
use url::Url;

use super::utils::record_batch_from_message;
use super::{DeltaWriter, WriteMode};
use crate::DeltaTable;
use crate::errors::DeltaTableError;
use crate::kernel::Add;
use crate::table::builder::DeltaTableBuilder;

type BadValue = (Value, ParquetError);

/// Writes messages to a delta lake table.
///
/// # Deprecation
///
/// `JsonWriter` is deprecated. Use [`crate::DeltaTable`] write operations
/// (e.g. [`crate::operations::write::WriteBuilder`]) instead, which support
/// all modern Delta features including encryption.
#[deprecated(
    since = "0.32.0",
    note = "Use DeltaTable write operations instead. \
            See https://delta-io.github.io/delta-rs/usage/writing/index.html"
)]
#[derive(Debug)]
pub struct JsonWriter {
    table: DeltaTable,
    /// Optional schema override; falls back to the table schema when absent.
    schema_ref: Option<ArrowSchemaRef>,
    partition_columns: Vec<String>,
    /// Buffered record batches (full schema, including partition columns).
    batches: Vec<RecordBatch>,
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
        super::ensure_legacy_writer_supports_table(&table, "JsonWriter")?;

        Ok(Self {
            table,
            schema_ref: Some(schema_ref),
            partition_columns: partition_columns.unwrap_or_default(),
            batches: Vec::new(),
        })
    }

    /// Creates a JsonWriter to write to the given table
    pub fn for_table(table: &DeltaTable) -> Result<JsonWriter, DeltaTableError> {
        super::ensure_legacy_writer_supports_table(table, "JsonWriter")?;
        let metadata = table.snapshot()?.metadata();
        let partition_columns = metadata.partition_columns().to_vec();

        Ok(Self {
            table: table.clone(),
            partition_columns,
            schema_ref: None,
            batches: Vec::new(),
        })
    }

    /// Returns the current byte length of the in memory buffer (approximate).
    pub fn buffer_len(&self) -> usize {
        self.batches
            .iter()
            .map(|b| b.num_rows() * b.num_columns() * 8)
            .sum()
    }

    /// Returns the number of records held in the current buffer.
    pub fn buffered_record_batch_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Resets internal state.
    pub fn reset(&mut self) {
        self.batches.clear();
    }

    /// Returns the user-defined arrow schema or the schema defined for the wrapped table.
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
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
    async fn write(&mut self, values: Vec<Value>) -> Result<(), DeltaTableError> {
        self.write_with_mode(values, WriteMode::Default).await
    }

    async fn write_with_mode(
        &mut self,
        values: Vec<Value>,
        mode: WriteMode,
    ) -> Result<(), DeltaTableError> {
        if mode != WriteMode::Default {
            warn!(
                "The JsonWriter does not currently support non-default write modes, \
                 falling back to default mode"
            );
        }
        let arrow_schema = self.arrow_schema();
        let record_batch = record_batch_from_message(arrow_schema, values.as_slice())?;
        self.batches.push(record_batch);
        Ok(())
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another
    /// file.
    ///
    /// This function returns the [`Add`] actions which should be committed to the [`DeltaTable`]
    /// for the written data files.
    async fn flush(&mut self) -> Result<Vec<Add>, DeltaTableError> {
        flush_batches_to_store(self).await
    }

    /// Flush the internal write buffers to files in the delta table folder structure
    /// and commit the changes to the Delta log, creating a new table version.
    async fn flush_and_commit(
        &mut self,
        table: &mut DeltaTable,
    ) -> Result<crate::kernel::Version, DeltaTableError> {
        flush_and_commit_via_exec(self, table).await
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DataFusion-backed write helpers (cfg-gated)
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "datafusion")]
async fn flush_batches_to_store(writer: &mut JsonWriter) -> Result<Vec<Add>, DeltaTableError> {
    use std::sync::Arc;

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::SessionConfig;

    use crate::kernel::Action;
    use crate::operations::write::configs::WriterStatsConfig;
    use crate::operations::write::execution::{default_writer_properties, write_execution_plan};

    let batches = std::mem::take(&mut writer.batches);
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = writer.arrow_schema();
    let ctx = SessionContext::new_with_config(SessionConfig::new());
    let mem_table = MemTable::try_new(schema, vec![batches])
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
    let exec = ctx
        .read_table(Arc::new(mem_table))
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?
        .create_physical_plan()
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

    let state = writer.table.snapshot()?;
    let table_config = state.snapshot().table_configuration();
    let stats_config = WriterStatsConfig::from_config(table_config);

    let actions = write_execution_plan(
        None,
        &ctx.state(),
        exec,
        writer.partition_columns.clone(),
        writer.table.object_store(),
        None,
        None,
        Some(default_writer_properties()),
        stats_config,
    )
    .await?;

    Ok(actions
        .into_iter()
        .filter_map(|a| match a {
            Action::Add(add) => Some(add),
            _ => None,
        })
        .collect())
}

#[cfg(not(feature = "datafusion"))]
async fn flush_batches_to_store(writer: &mut JsonWriter) -> Result<Vec<Add>, DeltaTableError> {
    use bytes::Bytes;
    use indexmap::IndexMap;
    use object_store::path::Path;
    use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
    use uuid::Uuid;

    use super::stats::create_add;
    use super::utils::{next_data_path, ShareableBuffer};
    use crate::logstore::ObjectStoreRetryExt;
    use crate::table::config::TablePropertiesExt as _;

    let batches = std::mem::take(&mut writer.batches);
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = writer.arrow_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(format!("delta-rs version {}", crate::crate_version()))
        .build();

    let buffer = ShareableBuffer::default();
    let mut arrow_writer = ArrowWriter::try_new(buffer.clone(), schema, Some(props.clone()))
        .map_err(|e| DeltaTableError::Parquet { source: e })?;
    for batch in &batches {
        arrow_writer
            .write(batch)
            .map_err(|e| DeltaTableError::Parquet { source: e })?;
    }
    let metadata = arrow_writer
        .close()
        .map_err(|e| DeltaTableError::Parquet { source: e })?;

    let obj_bytes = Bytes::from(buffer.to_vec());
    let file_size = obj_bytes.len() as i64;
    let prefix = Path::parse("")?;
    let uuid = Uuid::new_v4();
    let path = next_data_path(&prefix, 0, &uuid, &props);

    writer
        .table
        .object_store()
        .put_with_retries(&path, obj_bytes.into(), 15)
        .await?;

    let table_config = writer.table.snapshot()?.table_config();
    let add = create_add(
        &IndexMap::new(),
        path.to_string(),
        file_size,
        &metadata,
        table_config.num_indexed_cols(),
        &table_config
            .data_skipping_stats_columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.to_string()).collect::<Vec<_>>()),
    )?;
    Ok(vec![add])
}

#[cfg(feature = "datafusion")]
async fn flush_and_commit_via_exec(
    writer: &mut JsonWriter,
    table: &mut DeltaTable,
) -> Result<crate::kernel::Version, DeltaTableError> {
    use std::sync::Arc;

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::SessionConfig;

    use crate::kernel::Action;
    use crate::operations::write::execution::write_exec_plan;

    let batches = std::mem::take(&mut writer.batches);
    if batches.is_empty() {
        return Ok(table.version().unwrap_or(0));
    }

    let schema = writer.arrow_schema();
    let ctx = SessionContext::new_with_config(SessionConfig::new());
    let mem_table = MemTable::try_new(schema, vec![batches])
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
    let exec = ctx
        .read_table(Arc::new(mem_table))
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?
        .create_physical_plan()
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

    let table_config = table.snapshot()?.snapshot().table_configuration();
    let (actions, _) = write_exec_plan(
        &ctx.state(),
        table.log_store().as_ref(),
        &table_config,
        exec,
        None,
        None,
        false,
    )
    .await?;

    let adds: Vec<_> = actions
        .into_iter()
        .filter_map(|a| match a {
            Action::Add(add) => Some(Action::Add(add)),
            _ => None,
        })
        .collect();

    super::flush_and_commit(adds, table, None).await
}

#[cfg(not(feature = "datafusion"))]
async fn flush_and_commit_via_exec(
    writer: &mut JsonWriter,
    table: &mut DeltaTable,
) -> Result<crate::kernel::Version, DeltaTableError> {
    let adds: Vec<_> = writer
        .flush()
        .await?
        .into_iter()
        .map(crate::kernel::Action::Add)
        .collect();
    super::flush_and_commit(adds, table, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[cfg(feature = "datafusion")]
    use futures::TryStreamExt;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    use crate::operations::create::CreateBuilder;
    use crate::writer::test_utils::get_delta_schema;

    #[allow(deprecated)]
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

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    #[allow(deprecated)]
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
    #[allow(deprecated)]
    async fn test_json_writer_for_table_defaults_include_delta_rs_created_by() {
        let table_dir = tempfile::tempdir().unwrap();
        let table = get_test_table(&table_dir).await;

        let mut writer = JsonWriter::for_table(&table).unwrap();
        let data = serde_json::json!({"id": "A", "value": 1, "modified": "2021-02-01"});
        writer.write(vec![data]).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 1);

        let path = table_dir.path().join(&adds[0].path);
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let created_by = reader
            .metadata()
            .file_metadata()
            .created_by()
            .unwrap_or("");
        assert!(
            created_by.starts_with("delta-rs version"),
            "Expected 'delta-rs version ...' but got '{created_by}'"
        );
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_parsing_error() {
        use arrow_schema::ArrowError;

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

    mod schema_evolution {
        use super::*;

        #[cfg(feature = "datafusion")]
        #[tokio::test]
        #[allow(deprecated)]
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
    #[allow(deprecated)]
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
    #[allow(deprecated)]
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
    #[allow(deprecated)]
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
