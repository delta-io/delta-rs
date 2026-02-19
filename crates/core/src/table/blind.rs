//! Blind append-only Delta table implementation
//!
//! This module provides [BlindDeltaTable], a lightweight table representation
//! optimized for append-only write operations. Unlike [`DeltaTable`], it skips loading
//! file statistics during initialization, significantly reducing load time for large tables.
//!
//! # Example
//!
//! ```ignore
//! use deltalake_core::BlindDeltaTable;
//! use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
//!
//! let mut table = BlindDeltaTable::try_new("s3://bucket/table").await?;
//! let mut writer = RecordBatchWriter::for_blind_appends(&table)?;
//! writer.write(batch).await?;
//!
//! let adds = writer.flush().await?;
//! table.commit(adds).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::transaction::CommitResult;
use url::Url;

use super::builder::{DeltaTableConfig, ensure_table_uri};
use super::config::TablePropertiesExt;
use crate::DeltaResult;
use crate::kernel::{Add, Metadata, Protocol, Snapshot};
use crate::logstore::{LogStoreRef, ObjectStoreRef, StorageConfig};

/// Maximum number of commit retries for blind append operations.
const BLIND_COMMIT_MAX_RETRIES: usize = 15;

/// A Delta table optimized for blind append-only write operations.
///
/// `BlindDeltaTable` loads only the table metadata (protocol, schema, properties)
/// without scanning file statistics. This makes it ideal for:
///
/// - Large tables with many files where stats parsing is expensive
/// - Append-only workloads that don't need to read existing data
/// - High-throughput write scenarios
///
/// # Type Safety
///
/// This type intentionally does not expose methods like `files()` or `log_data()`
/// that would require loading file statistics. This prevents accidental use of
/// operations like merge or delete that are incompatible with append-only tables.
///
/// # Kernel Transaction API
///
/// This implementation uses the Kernel Transaction API directly for commits,
/// bypassing the `CommitBuilder` used by the standard `DeltaTable`. This provides
/// a more lightweight commit path optimized for append-only workloads.
#[derive(Debug, Clone)]
pub struct BlindDeltaTable {
    log_store: LogStoreRef,
    snapshot: Snapshot,
}

impl BlindDeltaTable {
    /// Create a new [`BlindDeltaTable`] from a table URI.
    ///
    /// This loads only the table metadata without parsing file statistics.
    ///
    /// # Arguments
    ///
    /// * `table_uri` - The URI of the Delta table (e.g., "s3://bucket/table", "/path/to/table")
    ///
    /// # Example
    ///
    /// ```ignore
    /// let table = BlindDeltaTable::try_new("s3://bucket/my-table").await?;
    /// ```
    pub async fn try_new(table_uri: impl AsRef<str>) -> DeltaResult<Self> {
        Self::try_new_with_options(table_uri, HashMap::new()).await
    }

    /// Create a new [`BlindDeltaTable`] with storage options.
    ///
    /// # Arguments
    ///
    /// * `table_uri` - The URI of the Delta table
    /// * `storage_options` - Backend-specific storage options
    pub async fn try_new_with_options(
        table_uri: impl AsRef<str>,
        storage_options: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let table_url = ensure_table_uri(table_uri)?;
        let storage_config = StorageConfig::parse_options(storage_options)?;
        let log_store = crate::logstore::logstore_for(&table_url, storage_config)?;

        Self::try_new_with_log_store(log_store).await
    }

    /// Create a new [`BlindDeltaTable`] from an existing log store.
    ///
    /// # Arguments
    ///
    /// * `log_store` - The log store to use
    pub async fn try_new_with_log_store(log_store: LogStoreRef) -> DeltaResult<Self> {
        let config = DeltaTableConfig {
            require_files: false,
            ..Default::default()
        };
        let snapshot = Snapshot::try_new(log_store.as_ref(), config, None).await?;

        Ok(Self {
            log_store,
            snapshot,
        })
    }

    /// Get a reference to the underlying log store.
    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }

    /// Get a reference to the object store.
    pub fn object_store(&self) -> ObjectStoreRef {
        self.log_store.object_store(None)
    }

    /// Get the table URL.
    pub fn table_url(&self) -> &Url {
        self.log_store.root_url()
    }

    /// Get the table version.
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the table metadata.
    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    /// Get the table protocol.
    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    /// Get the table schema (kernel format).
    pub fn schema(&self) -> KernelSchemaRef {
        self.snapshot.schema()
    }

    /// Get the table schema (Arrow format).
    pub fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        Ok(Arc::new(self.snapshot.schema().as_ref().try_into_arrow()?))
    }

    /// Get the table properties.
    pub fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }

    /// Get a reference to the underlying snapshot.
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Check if this table has the `appendOnly` property set to true.
    pub fn is_append_only(&self) -> bool {
        self.table_properties().append_only()
    }

    /// Commit add actions to the table.
    ///
    /// This commits the provided Add actions as an append operation to the Delta table
    /// using the Kernel Transaction API directly.
    ///
    /// # Arguments
    ///
    /// * `adds` - The Add file actions to commit
    ///
    /// # Returns
    ///
    /// The new version number after the commit.
    pub async fn commit(&mut self, adds: Vec<Add>) -> DeltaResult<i64> {
        use crate::DeltaTableError;
        use crate::kernel::spawn_blocking_with_span;

        let add_files_batch = adds_to_record_batch(&adds)?;

        let kernel_snapshot = self.snapshot.inner.clone();
        let engine = self.log_store.engine(None);

        let commit_version = spawn_blocking_with_span(move || {
            let committer = Box::new(FileSystemCommitter::new());
            let mut txn = kernel_snapshot
                .transaction(committer)
                .map_err(|e| DeltaTableError::from(e))?
                .with_operation("WRITE".to_string())
                .with_engine_info(format!("delta-rs:{}", env!("CARGO_PKG_VERSION")))
                .with_data_change(true);

            txn.add_files(Box::new(ArrowEngineData::new(add_files_batch)));

            let mut retries = 0;
            loop {
                match txn.commit(engine.as_ref()) {
                    Ok(CommitResult::CommittedTransaction(committed)) => {
                        return Ok(committed.commit_version() as i64);
                    }
                    Ok(CommitResult::ConflictedTransaction(conflict)) => {
                        return Err(DeltaTableError::VersionAlreadyExists(
                            conflict.conflict_version() as i64,
                        ));
                    }
                    Ok(CommitResult::RetryableTransaction(retryable)) => {
                        retries += 1;
                        if retries >= BLIND_COMMIT_MAX_RETRIES {
                            return Err(DeltaTableError::Generic(format!(
                                "Max commit attempts ({}) exceeded",
                                BLIND_COMMIT_MAX_RETRIES
                            )));
                        }
                        txn = retryable.transaction;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        let config = DeltaTableConfig {
            require_files: false,
            ..Default::default()
        };

        self.snapshot =
            Snapshot::try_new(self.log_store().as_ref(), config, Some(commit_version)).await?;
        Ok(commit_version)
    }
}

/// Convert a vector of Add actions to a RecordBatch compatible with Kernel's add_files_schema.
///
/// The schema expected by Kernel is:
/// - path: STRING (not null)
/// - partitionValues: MAP<STRING, STRING> (not null)
/// - size: LONG (not null)
/// - modificationTime: LONG (not null)
/// - stats: STRUCT { numRecords: LONG } (nullable)
fn adds_to_record_batch(adds: &[Add]) -> DeltaResult<RecordBatch> {
    use crate::DeltaTableError;
    use arrow::array::{
        ArrayRef, Int64Array, Int64Builder, MapBuilder, StringBuilder, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema};

    let num_rows = adds.len();

    let paths: Vec<&str> = adds.iter().map(|a| a.path.as_str()).collect();
    let path_array: ArrayRef = Arc::new(arrow::array::StringArray::from(paths));

    let mut map_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for add in adds {
        for (key, value) in &add.partition_values {
            map_builder.keys().append_value(key);
            match value {
                Some(v) => map_builder.values().append_value(v),
                None => map_builder.values().append_null(),
            }
        }
        map_builder.append(true).map_err(|e| {
            DeltaTableError::Generic(format!("Failed to build partition values map: {}", e))
        })?;
    }
    let partition_values_array: ArrayRef = Arc::new(map_builder.finish());

    let sizes: Vec<i64> = adds.iter().map(|a| a.size).collect();
    let size_array: ArrayRef = Arc::new(Int64Array::from(sizes));

    let mod_times: Vec<i64> = adds.iter().map(|a| a.modification_time).collect();
    let mod_time_array: ArrayRef = Arc::new(Int64Array::from(mod_times));

    let mut num_records_builder = Int64Builder::with_capacity(num_rows);
    for add in adds {
        if let Some(stats_json) = &add.stats {
            if let Ok(stats) = serde_json::from_str::<serde_json::Value>(stats_json) {
                if let Some(num_records) = stats.get("numRecords").and_then(|v| v.as_i64()) {
                    num_records_builder.append_value(num_records);
                } else {
                    num_records_builder.append_null();
                }
            } else {
                num_records_builder.append_null();
            }
        } else {
            num_records_builder.append_null();
        }
    }
    let num_records_array: ArrayRef = Arc::new(num_records_builder.finish());

    let stats_fields = Fields::from(vec![Field::new("numRecords", DataType::Int64, true)]);
    let stats_array: ArrayRef = Arc::new(StructArray::new(
        stats_fields,
        vec![num_records_array],
        None,
    ));

    // Build the schema - MapBuilder uses "keys" and "values" as default field names
    let schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new(
            "partitionValues",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            false,
        ),
        Field::new("size", DataType::Int64, false),
        Field::new("modificationTime", DataType::Int64, false),
        Field::new(
            "stats",
            DataType::Struct(Fields::from(vec![Field::new(
                "numRecords",
                DataType::Int64,
                true,
            )])),
            true,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            path_array,
            partition_values_array,
            size_array,
            mod_time_array,
            stats_array,
        ],
    )
    .map_err(|e| DeltaTableError::Generic(format!("Failed to create record batch: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::operations::create::CreateBuilder;

    #[tokio::test]
    async fn test_blind_table_creation() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("test-table")
            .with_columns(vec![StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();

        let blind = BlindDeltaTable::try_new(table_path).await.unwrap();
        assert_eq!(blind.version(), 0);
        assert!(blind.metadata().name().is_some());
    }

    #[tokio::test]
    async fn test_blind_table_with_append_only_property() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("append-only-table")
            .with_columns(vec![StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .with_configuration_property(crate::TableProperty::AppendOnly, Some("true"))
            .await
            .unwrap();

        let blind = BlindDeltaTable::try_new(table_path).await.unwrap();
        assert!(blind.is_append_only());
    }

    #[tokio::test]
    async fn test_blind_table_schema() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("test-table")
            .with_columns(vec![
                StructField::new(
                    "id".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    true,
                ),
                StructField::new(
                    "name".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .await
            .unwrap();

        let blind = BlindDeltaTable::try_new(table_path).await.unwrap();
        let arrow_schema = blind.arrow_schema().unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_blind_table_write_and_commit() {
        use crate::writer::{DeltaWriter, RecordBatchWriter};
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("write-test-table")
            .with_columns(vec![StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();

        let mut blind = BlindDeltaTable::try_new(table_path).await.unwrap();
        assert_eq!(blind.version(), 0);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut writer = RecordBatchWriter::for_blind_appends(&blind).unwrap();
        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();

        assert_eq!(adds.len(), 1);
        let new_version = blind.commit(adds).await.unwrap();
        assert_eq!(new_version, 1);
        assert_eq!(blind.version(), 1);

        // Verify data by reading with DeltaTable
        let table = crate::DeltaTable::try_from_url(
            crate::table::builder::ensure_table_uri(table_path).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(table.version(), Some(1));
    }

    #[test]
    fn test_adds_to_record_batch() {
        let adds = vec![
            Add {
                path: "part-00000.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 1024,
                modification_time: 1234567890,
                data_change: true,
                stats: Some(r#"{"numRecords": 100}"#.to_string()),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
            Add {
                path: "part-00001.parquet".to_string(),
                partition_values: {
                    let mut m = HashMap::new();
                    m.insert("date".to_string(), Some("2024-01-01".to_string()));
                    m
                },
                size: 2048,
                modification_time: 1234567891,
                data_change: true,
                stats: None,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
        ];

        let batch = adds_to_record_batch(&adds).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);

        let path_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "part-00000.parquet");
        assert_eq!(path_col.value(1), "part-00001.parquet");

        let size_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(size_col.value(0), 1024);
        assert_eq!(size_col.value(1), 2048);
    }

    #[tokio::test]
    async fn test_consecutive_commits() {
        use crate::writer::{DeltaWriter, RecordBatchWriter};
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("consecutive-test")
            .with_columns(vec![StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();

        let mut blind = BlindDeltaTable::try_new(table_path).await.unwrap();
        assert_eq!(blind.version(), 0);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            true,
        )]));

        for i in 1..=3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1]))],
            )
            .unwrap();

            let mut writer = RecordBatchWriter::for_blind_appends(&blind).unwrap();
            writer.write(batch).await.unwrap();
            let adds = writer.flush().await.unwrap();

            let new_version = blind.commit(adds).await.unwrap();
            assert_eq!(new_version, i as i64);
            assert_eq!(blind.version(), i as i64);
        }
    }

    #[tokio::test]
    async fn test_partitioned_table_write() {
        use crate::writer::{DeltaWriter, RecordBatchWriter};
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("partitioned-test")
            .with_columns(vec![
                StructField::new(
                    "id".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    true,
                ),
                StructField::new(
                    "category".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .with_partition_columns(vec!["category"])
            .await
            .unwrap();

        let mut blind = BlindDeltaTable::try_new(table_path).await.unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, true),
            Field::new("category", arrow::datatypes::DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["A", "B", "A"])),
            ],
        )
        .unwrap();

        let mut writer = RecordBatchWriter::for_blind_appends(&blind).unwrap();
        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();

        // Two partitions (A, B) should create at least 2 files
        assert!(
            adds.len() >= 2,
            "Expected at least 2 files, got {}",
            adds.len()
        );

        // Verify partition values are set
        for add in &adds {
            assert!(
                add.partition_values.contains_key("category"),
                "Missing partition key 'category'"
            );
        }

        let new_version = blind.commit(adds).await.unwrap();
        assert_eq!(new_version, 1);
    }

    #[tokio::test]
    async fn test_empty_commit() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("empty-commit-test")
            .with_columns(vec![StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();

        let mut blind = BlindDeltaTable::try_new(table_path).await.unwrap();
        let initial_version = blind.version();
        let result = blind.commit(vec![]).await;

        // Empty commit creates a new version with no file changes
        assert!(result.is_ok());
        assert_eq!(blind.version(), initial_version + 1);
    }

    #[tokio::test]
    async fn test_large_batch_commit() {
        use crate::writer::{DeltaWriter, RecordBatchWriter};
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("large-batch-test")
            .with_columns(vec![StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();

        let mut blind = BlindDeltaTable::try_new(table_path).await.unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            true,
        )]));

        // Create a batch with 10000 rows
        let row_count = 10000;
        let large_data: Vec<i32> = (0..row_count).collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(large_data))])
                .unwrap();

        let mut writer = RecordBatchWriter::for_blind_appends(&blind).unwrap();
        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();

        // Verify stats contain correct numRecords
        let total_records: i64 = adds
            .iter()
            .filter_map(|add| {
                add.stats
                    .as_ref()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                    .and_then(|v| v.get("numRecords").and_then(|n| n.as_i64()))
            })
            .sum();
        assert_eq!(total_records, row_count as i64);

        let new_version = blind.commit(adds).await.unwrap();
        assert_eq!(new_version, 1);

        // Verify with DeltaTable
        let table = crate::DeltaTable::try_from_url(
            crate::table::builder::ensure_table_uri(table_path).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(table.version(), Some(1));
    }

    #[tokio::test]
    async fn test_nonexistent_table_error() {
        use crate::DeltaTableError;

        // Use a path that exists but is not a delta table
        let table_dir = tempfile::tempdir().unwrap();
        let nonexistent_path = table_dir.path().join("not_a_table");
        std::fs::create_dir(&nonexistent_path).unwrap();

        let result = BlindDeltaTable::try_new(nonexistent_path.to_str().unwrap()).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            matches!(err, DeltaTableError::NotATable(_)),
            "Expected NotATable error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_stats_num_records_extraction() {
        use arrow::array::{Array, Int64Array, StructArray};

        let adds = vec![
            Add {
                path: "file1.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 1000,
                modification_time: 123456,
                data_change: true,
                stats: Some(r#"{"numRecords": 500}"#.to_string()),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
            Add {
                path: "file2.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 2000,
                modification_time: 123457,
                data_change: true,
                stats: Some(
                    r#"{"numRecords": 1000, "minValues": {}, "maxValues": {}}"#.to_string(),
                ),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
            Add {
                path: "file3.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 500,
                modification_time: 123458,
                data_change: true,
                stats: None, // No stats
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
            Add {
                path: "file4.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 800,
                modification_time: 123459,
                data_change: true,
                stats: Some(r#"{"invalid": "json"}"#.to_string()), // No numRecords field
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
        ];

        let batch = adds_to_record_batch(&adds).unwrap();

        // Get the stats column (index 4)
        let stats_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Get numRecords from the struct
        let num_records = stats_col
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(num_records.value(0), 500);
        assert_eq!(num_records.value(1), 1000);
        assert!(num_records.is_null(2)); // No stats
        assert!(num_records.is_null(3)); // No numRecords field
    }

    #[test]
    fn test_multiple_partition_values() {
        use arrow::array::{Array, MapArray};

        let adds = vec![Add {
            path: "part-00000.parquet".to_string(),
            partition_values: {
                let mut m = HashMap::new();
                m.insert("year".to_string(), Some("2024".to_string()));
                m.insert("month".to_string(), Some("01".to_string()));
                m.insert("day".to_string(), Some("15".to_string()));
                m
            },
            size: 1024,
            modification_time: 1234567890,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }];

        let batch = adds_to_record_batch(&adds).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Verify partition values map was created with 3 entries
        let partition_col = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!partition_col.is_null(0));
        // Map should have 3 key-value pairs
        assert_eq!(partition_col.value(0).len(), 3);
    }

    #[test]
    fn test_null_partition_value() {
        use arrow::array::{Array, MapArray, StringArray};

        let adds = vec![Add {
            path: "part-00000.parquet".to_string(),
            partition_values: {
                let mut m = HashMap::new();
                m.insert("category".to_string(), None); // null partition value
                m
            },
            size: 1024,
            modification_time: 1234567890,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }];

        let batch = adds_to_record_batch(&adds).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Verify partition values map has 1 entry with null value
        let partition_col = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
        let map_entries = partition_col.value(0);
        assert_eq!(map_entries.len(), 1);

        // Value should be null
        let values = map_entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(values.is_null(0));
    }

    #[tokio::test]
    async fn test_schema_mismatch_error() {
        use crate::writer::{DeltaWriter, RecordBatchWriter};
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        // Create table with Integer column
        let _table = CreateBuilder::new()
            .with_location(table_path)
            .with_table_name("schema-mismatch-test")
            .with_columns(vec![StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();

        let blind = BlindDeltaTable::try_new(table_path).await.unwrap();

        // Try to write String data to Integer column
        let wrong_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Utf8, // Wrong type
            true,
        )]));
        let batch = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap();

        let mut writer = RecordBatchWriter::for_blind_appends(&blind).unwrap();
        let result = writer.write(batch).await;

        // Should fail due to schema mismatch
        assert!(result.is_err(), "Expected schema mismatch error");
    }
}
