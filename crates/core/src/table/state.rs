//! The module for delta table state.

use std::sync::Arc;

use chrono::Utc;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::expressions::column_expr_ref;
use delta_kernel::schema::StructField;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{EvaluationHandler, Expression};
use futures::stream::BoxStream;
use futures::{future::ready, StreamExt as _, TryStreamExt as _};
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use super::DeltaTableConfig;
use crate::kernel::arrow::engine_ext::{ExpressionEvaluatorExt, SnapshotExt};
#[cfg(test)]
use crate::kernel::Action;
use crate::kernel::{
    Add, DataType, EagerSnapshot, LogDataHandler, LogicalFileView, Metadata, Protocol, StructType,
    TombstoneView, ARROW_HANDLER,
};
use crate::logstore::LogStore;
use crate::partitions::PartitionFilter;
use crate::table::config::TablePropertiesExt;
use crate::{DeltaResult, DeltaTableError};

/// State snapshot currently held by the Delta Table instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaTableState {
    pub(crate) snapshot: EagerSnapshot,
}

impl DeltaTableState {
    pub fn new(snapshot: EagerSnapshot) -> Self {
        Self { snapshot }
    }

    /// Create a new DeltaTableState
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        log_store.refresh().await?;
        // TODO: pass through predictae
        let snapshot = EagerSnapshot::try_new(log_store, config, version).await?;
        Ok(Self { snapshot })
    }

    /// Return table version
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// The most recent protocol of the table.
    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    /// The most recent metadata of the table.
    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    /// The table schema
    pub fn schema(&self) -> &StructType {
        self.snapshot.schema()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
    }

    /// Well known table configuration
    pub fn table_config(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }

    /// Get the timestamp when a version commit was created.
    /// This is the timestamp of the commit file.
    /// If the commit file is not present, None is returned.
    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        self.snapshot.version_timestamp(version)
    }

    /// Construct a delta table state object from a list of actions
    #[cfg(test)]
    pub async fn from_actions(actions: Vec<Action>) -> DeltaResult<Self> {
        use crate::kernel::transaction::CommitData;
        use crate::protocol::{DeltaOperation, SaveMode};
        use std::collections::HashMap;

        let metadata = actions
            .iter()
            .find_map(|a| match a {
                Action::Metadata(m) => Some(m.clone()),
                _ => None,
            })
            .ok_or(DeltaTableError::NotInitialized)?;
        let protocol = actions
            .iter()
            .find_map(|a| match a {
                Action::Protocol(p) => Some(p.clone()),
                _ => None,
            })
            .ok_or(DeltaTableError::NotInitialized)?;

        let commit_data = [CommitData::new(
            actions,
            DeltaOperation::Create {
                mode: SaveMode::Append,
                location: Path::default().to_string(),
                protocol: protocol.clone(),
                metadata: metadata.clone(),
            },
            HashMap::new(),
            Vec::new(),
        )];

        let snapshot = EagerSnapshot::new_test(&commit_data).await.unwrap();

        Ok(Self { snapshot })
    }

    /// Returns a semantic accessor to the currently loaded log data.
    pub fn log_data(&self) -> LogDataHandler<'_> {
        self.snapshot.log_data()
    }

    /// Full list of tombstones (remove actions) representing files removed from table state).
    pub fn all_tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> BoxStream<'_, DeltaResult<TombstoneView>> {
        self.snapshot.snapshot().tombstones(log_store)
    }

    /// List of unexpired tombstones (remove actions) representing files removed from table state.
    /// The retention period is set by `deletedFileRetentionDuration` with default value of 1 week.
    #[deprecated(
        since = "0.30.0",
        note = "Use `all_tombstones` instead and filter by retention timestamp."
    )]
    pub fn unexpired_tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> BoxStream<'_, DeltaResult<TombstoneView>> {
        let retention_timestamp = Utc::now().timestamp_millis()
            - self
                .table_config()
                .deleted_file_retention_duration()
                .as_millis() as i64;
        self.all_tombstones(log_store)
            .try_filter(move |t| ready(t.deletion_timestamp().unwrap_or(0) > retention_timestamp))
            .boxed()
    }

    /// Full list of add actions representing all parquet files that are part of the current
    /// delta table state.
    pub async fn file_actions(&self, log_store: &dyn LogStore) -> DeltaResult<Vec<Add>> {
        self.file_actions_iter(log_store).try_collect().await
    }

    /// Full list of add actions representing all parquet files that are part of the current
    /// delta table state.
    pub fn file_actions_iter(&self, log_store: &dyn LogStore) -> BoxStream<'_, DeltaResult<Add>> {
        self.snapshot
            .files(log_store, None)
            .map_ok(|v| v.add_action())
            .boxed()
    }

    /// Returns an iterator of file names present in the loaded state
    #[inline]
    pub fn file_paths_iter(&self) -> impl Iterator<Item = Path> + '_ {
        self.log_data().iter().map(|add| add.object_store_path())
    }

    /// Get the transaction version for the given application ID.
    ///
    /// Returns `None` if the application ID is not found.
    pub async fn transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: impl ToString,
    ) -> DeltaResult<Option<i64>> {
        self.snapshot.transaction_version(log_store, app_id).await
    }

    /// Obtain the Eager snapshot of the state
    pub fn snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    /// Update the state of the table to the given version.
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        log_store.refresh().await?;
        self.snapshot
            .update(log_store, version.map(|v| v as u64))
            .await?;
        Ok(())
    }

    /// Obtain a stream of logical file views that match the partition filters
    ///
    /// ## Arguments
    ///
    /// * `log_store` - The log store to use for reading the table's log.
    /// * `filters` - The partition filters to apply to the file views.
    ///
    /// ## Returns
    ///
    /// A stream of logical file views that match the partition filters.
    #[deprecated(
        since = "0.30.0",
        note = "Use `.snapshot().files(log_store, predicate)` with a kernel predicate instead."
    )]
    pub fn get_active_add_actions_by_partitions(
        &self,
        log_store: &dyn LogStore,
        filters: &[PartitionFilter],
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        self.snapshot().file_views_by_partitions(log_store, filters)
    }

    /// Get an [arrow::record_batch::RecordBatch] containing add action data.
    ///
    /// # Arguments
    ///
    /// * `flatten` - whether to flatten the schema. Partition values columns are
    ///   given the prefix `partition.`, statistics (null_count, min, and max) are
    ///   given the prefix `null_count.`, `min.`, and `max.`, and tags the
    ///   prefix `tags.`. Nested field names are concatenated with `.`.
    ///
    /// # Data schema
    ///
    /// Each row represents a file that is a part of the selected tables state.
    ///
    /// * `path` (String): relative or absolute to a file.
    /// * `size_bytes` (Int64): size of file in bytes.
    /// * `modification_time` (Millisecond Timestamp): time the file was created.
    /// * `null_count.{col_name}` (Int64): number of null values for column in
    ///   this file.
    /// * `num_records.{col_name}` (Int64): number of records for column in
    ///   this file.
    /// * `min.{col_name}` (matches column type): minimum value of column in file
    ///   (if available).
    /// * `max.{col_name}` (matches column type): maximum value of column in file
    ///   (if available).
    /// * `partition.{partition column name}` (matches column type): value of
    ///   partition the file corresponds to.
    pub fn add_actions_table(
        &self,
        flatten: bool,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        self.snapshot.add_actions_table(flatten)
    }
}

impl EagerSnapshot {
    /// Get an [arrow::record_batch::RecordBatch] containing add action data.
    ///
    /// # Arguments
    ///
    /// * `flatten` - whether to flatten the schema. Partition values columns are
    ///   given the prefix `partition.`, statistics (null_count, min, and max) are
    ///   given the prefix `null_count.`, `min.`, and `max.`, and tags the
    ///   prefix `tags.`. Nested field names are concatenated with `.`.
    ///
    /// # Data schema
    ///
    /// Each row represents a file that is a part of the selected tables state.
    ///
    /// * `path` (String): relative or absolute to a file.
    /// * `size_bytes` (Int64): size of file in bytes.
    /// * `modification_time` (Millisecond Timestamp): time the file was created.
    /// * `null_count.{col_name}` (Int64): number of null values for column in
    ///   this file.
    /// * `num_records.{col_name}` (Int64): number of records for column in
    ///   this file.
    /// * `min.{col_name}` (matches column type): minimum value of column in file
    ///   (if available).
    /// * `max.{col_name}` (matches column type): maximum value of column in file
    ///   (if available).
    /// * `partition.{partition column name}` (matches column type): value of
    ///   partition the file corresponds to.
    pub fn add_actions_table(
        &self,
        flatten: bool,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let mut expressions = vec![
            column_expr_ref!("path"),
            column_expr_ref!("size"),
            column_expr_ref!("modificationTime"),
        ];
        let mut fields = vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size_bytes", DataType::LONG),
            StructField::not_null("modification_time", DataType::LONG),
        ];

        let stats_schema = self.snapshot().inner.stats_schema()?;
        let num_records_field = stats_schema
            .field("numRecords")
            .ok_or_else(|| DeltaTableError::SchemaMismatch {
                msg: "numRecords field not found".to_string(),
            })?
            .with_name("num_records");

        expressions.push(column_expr_ref!("stats_parsed.numRecords"));
        fields.push(num_records_field);

        if let Some(null_count_field) = stats_schema.field("nullCount") {
            let null_count_field = null_count_field.with_name("null_count");
            expressions.push(column_expr_ref!("stats_parsed.nullCount"));
            fields.push(null_count_field);
        }

        if let Some(min_values_field) = stats_schema.field("minValues") {
            let min_values_field = min_values_field.with_name("min");
            expressions.push(column_expr_ref!("stats_parsed.minValues"));
            fields.push(min_values_field);
        }

        if let Some(max_values_field) = stats_schema.field("maxValues") {
            let max_values_field = max_values_field.with_name("max");
            expressions.push(column_expr_ref!("stats_parsed.maxValues"));
            fields.push(max_values_field);
        }

        if let Some(partition_schema) = self.snapshot().inner.partitions_schema()? {
            fields.push(StructField::nullable(
                "partition",
                DataType::try_struct_type(partition_schema.fields().cloned())?,
            ));
            expressions.push(column_expr_ref!("partitionValues_parsed"));
        }

        let expression = Expression::Struct(expressions);
        let table_schema = DataType::try_struct_type(fields)?;

        let input_schema = self.files.schema();
        let input_schema = Arc::new(input_schema.as_ref().try_into_kernel()?);
        let actions = self.files.clone();

        let evaluator =
            ARROW_HANDLER.new_expression_evaluator(input_schema, expression.into(), table_schema);
        let result = evaluator.evaluate_arrow(actions)?;

        if flatten {
            Ok(result.normalize(".", None)?)
        } else {
            Ok(result)
        }
    }
}
