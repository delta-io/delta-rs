//! The module for delta table state.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use arrow_select::coalesce::BatchCoalescer;
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::column_expr_ref;
use delta_kernel::schema::{SchemaRef as KernelSchemaRef, StructField};
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{EvaluationHandler, Expression};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use super::DeltaTableConfig;
#[cfg(test)]
use crate::kernel::Action;
use crate::kernel::arrow::engine_ext::{ExpressionEvaluatorExt, SnapshotExt};
use crate::kernel::{
    ARROW_HANDLER, DataType, EagerSnapshot, LogDataHandler, Metadata, Protocol, TombstoneView,
};
use crate::logstore::LogStore;
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
    pub fn schema(&self) -> KernelSchemaRef {
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
                location: url::Url::parse("memory:///example")
                    .expect("Failed to parse a hard-coded URL, that's magical isn't it"),
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

    /// Get add action data as a list of [arrow::record_batch::RecordBatch]
    /// without concatenating them into a single batch.
    pub fn add_actions_batches(
        &self,
        flatten: bool,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DeltaTableError> {
        self.snapshot.add_actions_batches(flatten)
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
        let (expression, table_schema) = self.add_actions_expr_and_schema()?;
        let batches =
            self.add_actions_batches_with_schema(flatten, expression, table_schema.clone())?;
        if batches.is_empty() {
            // Return an empty batch with the correct schema
            return self.add_actions_batches_empty(flatten, &table_schema);
        }
        Ok(concat_batches(batches[0].schema_ref(), &batches)?)
    }

    /// Get add action data as a list of [arrow::record_batch::RecordBatch] without
    /// concatenating them into a single batch. This avoids the 2GB Arrow offset
    /// limit for tables with a very large number of files.
    ///
    /// See [Self::add_actions_table] for schema details.
    pub fn add_actions_batches(
        &self,
        flatten: bool,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DeltaTableError> {
        let (expression, table_schema) = self.add_actions_expr_and_schema()?;
        self.add_actions_batches_with_schema(flatten, expression, table_schema)
    }

    /// Get add action metadata batches containing only path and partition columns.
    ///
    /// This is a fast-path for partition-only planning and file matching.
    pub(crate) fn add_actions_partition_batches(
        &self,
    ) -> Result<Vec<RecordBatch>, DeltaTableError> {
        let mut expressions = vec![column_expr_ref!("path")];
        let mut fields = vec![StructField::not_null("path", DataType::STRING)];

        if let Some(partition_schema) = self.snapshot().inner.partitions_schema()? {
            fields.push(StructField::nullable(
                "partition",
                DataType::try_struct_type(partition_schema.fields().cloned())?,
            ));
            expressions.push(column_expr_ref!("partitionValues_parsed"));
        }

        let expression = Expression::Struct(expressions);
        let table_schema = DataType::try_struct_type(fields)?;
        self.add_actions_batches_with_schema(true, expression, table_schema)
    }

    fn add_actions_batches_with_schema(
        &self,
        flatten: bool,
        expression: Expression,
        table_schema: DataType,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, DeltaTableError> {
        let files = self.files()?;
        if files.is_empty() {
            return Ok(vec![]);
        }

        let input_schema = self.snapshot().inner.scan_row_parsed_schema_arrow()?;
        let input_schema = Arc::new(input_schema.as_ref().try_into_kernel()?);
        let evaluator = ARROW_HANDLER.new_expression_evaluator(
            input_schema,
            expression.into(),
            table_schema,
        )?;

        let evaluated_batches = files.iter().map(|file| {
            let batch = evaluator.evaluate_arrow(file.clone())?;
            if flatten {
                Ok(batch.normalize(".", None)?)
            } else {
                Ok(batch)
            }
        });

        // Coalesce small batches into larger ones for efficiency while streaming
        // evaluator output instead of materializing all batches first.
        coalesce_batches(evaluated_batches)
    }

    /// Build the expression and output schema used by add_actions_table / add_actions_batches.
    fn add_actions_expr_and_schema(&self) -> Result<(Expression, DataType), DeltaTableError> {
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
        Ok((expression, table_schema))
    }

    /// Return an empty RecordBatch with the correct add-actions schema.
    fn add_actions_batches_empty(
        &self,
        flatten: bool,
        table_schema: &DataType,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let DataType::Struct(struct_type) = table_schema else {
            return Err(DeltaTableError::Generic(
                "Expected Struct type for table schema".to_string(),
            ));
        };
        let arrow_schema: ArrowSchema = struct_type.as_ref().try_into_arrow()?;
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema));

        if flatten {
            Ok(empty_batch.normalize(".", None)?)
        } else {
            Ok(empty_batch)
        }
    }
}

/// Target number of rows per coalesced batch. Matches DataFusion's default batch size.
const COALESCE_TARGET_BATCH_SIZE: usize = 8192;

/// Coalesce many small [RecordBatch]es into fewer, larger ones.
///
/// tables with many small commits can produce thousands of
/// single-row batches from the kernel evaluator. This function merges them
/// into batches of approximately [`COALESCE_TARGET_BATCH_SIZE`] rows using
/// Arrow's [`BatchCoalescer`], which is more memory-efficient than
/// [`concat_batches`].
fn coalesce_batches<I>(input: I) -> Result<Vec<RecordBatch>, DeltaTableError>
where
    I: IntoIterator<Item = Result<RecordBatch, DeltaTableError>>,
{
    let mut coalescer = None;
    let mut output = Vec::new();

    for batch in input {
        let batch = batch?;
        let current = coalescer
            .get_or_insert_with(|| BatchCoalescer::new(batch.schema(), COALESCE_TARGET_BATCH_SIZE));
        current.push_batch(batch)?;
        while let Some(done) = current.next_completed_batch() {
            output.push(done);
        }
    }

    let Some(mut coalescer) = coalescer else {
        return Ok(vec![]);
    };

    coalescer.finish_buffered_batch()?;
    while let Some(done) = coalescer.next_completed_batch() {
        output.push(done);
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "datafusion")]
    use crate::protocol::SaveMode;
    #[cfg(feature = "datafusion")]
    use crate::test_utils::multibatch_add_actions_for_partition;
    use crate::writer::test_utils::get_delta_schema;
    #[cfg(feature = "datafusion")]
    use crate::writer::test_utils::get_record_batch;
    use crate::{DeltaResult, DeltaTable};
    use arrow_array::Int32Array;

    /// <https://github.com/delta-io/delta-rs/issues/3918>
    #[tokio::test]
    async fn test_add_actions_empty() -> DeltaResult<()> {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_column(
                "id",
                DataType::Primitive(delta_kernel::schema::PrimitiveType::Long),
                true,
                None,
            )
            .await?;
        let _actions = table.snapshot()?.add_actions_table(false)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_add_actions_batches_empty() -> DeltaResult<()> {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .await?;

        let snapshot = table.snapshot()?.snapshot();
        assert!(snapshot.add_actions_batches(false)?.is_empty());
        assert!(snapshot.add_actions_batches(true)?.is_empty());
        Ok(())
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_add_actions_batches_non_empty_flatten_has_partition_columns() -> DeltaResult<()> {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified"])
            .await?;
        let table = table
            .write(vec![get_record_batch(None, false)])
            .with_save_mode(SaveMode::Append)
            .await?;

        let snapshot = table.snapshot()?.snapshot();
        let flattened_batches = snapshot.add_actions_batches(true)?;
        assert!(!flattened_batches.is_empty());
        assert!(
            flattened_batches[0]
                .schema()
                .field_with_name("partition.modified")
                .is_ok()
        );

        let concatenated = concat_batches(flattened_batches[0].schema_ref(), &flattened_batches)?;
        let single = snapshot.add_actions_table(true)?;
        assert_eq!(concatenated, single);
        Ok(())
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_add_actions_batches_non_empty_non_flatten_has_partition_struct() -> DeltaResult<()>
    {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified"])
            .await?;
        let table = table
            .write(vec![get_record_batch(None, false)])
            .with_save_mode(SaveMode::Append)
            .await?;

        let snapshot = table.snapshot()?.snapshot();

        // Non-flattened batches should have a nested "partition" struct column
        let batches = snapshot.add_actions_batches(false)?;
        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        assert!(
            schema.field_with_name("partition").is_ok(),
            "non-flattened schema should contain a nested 'partition' field"
        );
        assert!(
            schema.field_with_name("partition.modified").is_err(),
            "non-flattened schema should NOT contain flattened 'partition.modified'"
        );

        // Concatenated batches should equal the single-batch add_actions_table output
        let concatenated = concat_batches(batches[0].schema_ref(), &batches)?;
        let single = snapshot.add_actions_table(false)?;
        assert_eq!(concatenated, single);
        Ok(())
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_add_actions_batches_flatten_multibatch_stress() -> DeltaResult<()> {
        let action_count = 9000;
        let actions = multibatch_add_actions_for_partition(
            action_count,
            "modified",
            "2021-02-02",
            "2021-02-03",
        );

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified"])
            .with_actions(actions)
            .await?;

        let snapshot = table.snapshot()?.snapshot();
        let batches = snapshot.add_actions_batches(true)?;
        assert!(batches.len() > 1, "expected multi-batch add-actions output");

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, action_count);

        let concatenated = concat_batches(batches[0].schema_ref(), &batches)?;
        let single = snapshot.add_actions_table(true)?;
        assert_eq!(concatenated.num_rows(), action_count);
        assert_eq!(concatenated, single);
        Ok(())
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_add_actions_partition_batches_only_path_and_partitions() -> DeltaResult<()> {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified"])
            .await?;
        let table = table
            .write(vec![get_record_batch(None, false)])
            .with_save_mode(SaveMode::Append)
            .await?;

        let snapshot = table.snapshot()?.snapshot();
        let batches = snapshot.add_actions_partition_batches()?;
        assert!(!batches.is_empty());

        let schema = batches[0].schema();
        assert!(schema.field_with_name("path").is_ok());
        assert!(schema.field_with_name("partition.modified").is_ok());
        assert!(schema.field_with_name("size_bytes").is_err());
        assert!(schema.field_with_name("modification_time").is_err());
        assert!(schema.field_with_name("num_records").is_err());
        Ok(())
    }

    #[test]
    fn test_coalesce_batches_merges_small_batches() -> DeltaResult<()> {
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            false,
        )]));

        let input_batches = vec![
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])?,
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![2]))])?,
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![3]))])?,
        ];

        let output_batches = coalesce_batches(
            input_batches
                .into_iter()
                .map(Ok::<RecordBatch, DeltaTableError>),
        )?;
        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].num_rows(), 3);

        let value_array = output_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array column");
        assert_eq!(
            value_array.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );

        Ok(())
    }
}
