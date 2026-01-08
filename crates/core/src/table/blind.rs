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
//! use deltalake_core::kernel::Action;
//!
//! let mut table = BlindDeltaTable::try_new("s3://bucket/table").await?;
//! let mut writer = RecordBatchWriter::for_appendable(&table)?;
//! writer.write(batch).await?;
//!
//! let adds = writer.flush().await?;
//! let actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();
//! table.flush_and_commit(actions, None).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::table_properties::TableProperties;
use url::Url;

use super::builder::{DeltaTableConfig, ensure_table_uri};
use super::config::TablePropertiesExt;
use crate::DeltaResult;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{Action, EagerSnapshot, Metadata, Protocol};
use crate::logstore::{LogStoreRef, ObjectStoreRef, StorageConfig};
use crate::protocol::{DeltaOperation, SaveMode};

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
#[derive(Debug, Clone)]
pub struct BlindDeltaTable {
    log_store: LogStoreRef,
    snapshot: EagerSnapshot,
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

        // EagerSnapshot with require_files=false creates empty files vec (no stats parsing)
        let snapshot = EagerSnapshot::try_new(log_store.as_ref(), config, None).await?;

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
    pub fn snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    /// Check if this table has the `appendOnly` property set to true.
    pub fn is_append_only(&self) -> bool {
        self.table_properties().append_only()
    }

    /// Flush pending writes and commit them to the table.
    ///
    /// This commits the provided actions as an append operation to the Delta table.
    ///
    /// # Arguments
    ///
    /// * `actions` - The actions (Add files) to commit
    /// * `commit_properties` - Optional commit properties
    ///
    /// # Returns
    ///
    /// The new version number after the commit.
    pub async fn flush_and_commit(
        &mut self,
        actions: Vec<Action>,
        commit_properties: Option<CommitProperties>,
    ) -> DeltaResult<i64> {
        let partition_cols = self.metadata().partition_columns().clone();
        let partition_by = if !partition_cols.is_empty() {
            Some(partition_cols)
        } else {
            None
        };

        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by,
            predicate: None,
        };

        let finalized = CommitBuilder::from(commit_properties.unwrap_or_default())
            .with_actions(actions)
            .build(Some(&self.snapshot), self.log_store.clone(), operation)
            .await?;

        self.snapshot = finalized.snapshot().snapshot().clone();
        Ok(finalized.version())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::operations::create::CreateBuilder;

    #[tokio::test]
    async fn test_appendable_table_creation() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        // Create a table first
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

        // Open as BlindDeltaTable
        let appendable = BlindDeltaTable::try_new(table_path).await.unwrap();

        assert_eq!(appendable.version(), 0);
        assert!(appendable.metadata().name().is_some());
    }

    #[tokio::test]
    async fn test_appendable_table_with_append_only_property() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        // Create a table with appendOnly=true
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

        let appendable = BlindDeltaTable::try_new(table_path).await.unwrap();

        assert!(appendable.is_append_only());
    }

    #[tokio::test]
    async fn test_appendable_table_schema() {
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

        let appendable = BlindDeltaTable::try_new(table_path).await.unwrap();
        let arrow_schema = appendable.arrow_schema().unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_appendable_table_write_and_commit() {
        use crate::writer::{DeltaWriter, RecordBatchWriter};
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        // Create a table first
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

        // Open as BlindDeltaTable
        let mut appendable = BlindDeltaTable::try_new(table_path).await.unwrap();
        assert_eq!(appendable.version(), 0);

        // Create a RecordBatch
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

        // Write using RecordBatchWriter
        let mut writer = RecordBatchWriter::for_blind_appends(&appendable).unwrap();
        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();

        // Convert Add to Action
        let actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();

        // Commit
        let new_version = appendable.flush_and_commit(actions, None).await.unwrap();
        assert_eq!(new_version, 1);
        assert_eq!(appendable.version(), 1);
    }
}
