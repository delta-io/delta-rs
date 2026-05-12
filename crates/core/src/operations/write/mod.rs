//!
//! New Table Semantics
//!  - The schema of the [Plan] is used to initialize the table.
//!  - The partition columns will be used to partition the table.
//!
//! Existing Table Semantics
//!  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
//!  - Conflicting columns (i.e. a INT, and a STRING)
//!    will result in an exception.
//!    Partition columns, if present, are validated against the existing metadata.
//!    When omitted, the table partitioning is respected.
//!    Full table overwrite with `SchemaMode::Overwrite` and no replaceWhere predicate may
//!    replace the partition columns.
//!
//! In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
//! replace data that matches a predicate.
//!
//! # Example
//! ```rust ignore
//! let id_field = arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false);
//! let schema = Arc::new(arrow::datatypes::Schema::new(vec![id_field]));
//! let ids = arrow::array::Int32Array::from(vec![1, 2, 3, 4, 5]);
//! let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)])?;
//! let ops = DeltaOps::try_from_url("../path/to/empty/dir").await?;
//! let table = ops.write(vec![batch]).await?;
//! ````

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use std::vec;

use arrow::array::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, UNNAMED_TABLE};
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::table_features::ColumnMappingMode;
use futures::future::BoxFuture;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use url::Url;

pub use self::configs::WriterStatsConfig;
use self::execution::write_execution_plan_v2;
use self::metrics::{SOURCE_COUNT_ID, SOURCE_COUNT_METRIC};
use super::{CreateBuilder, CustomExecuteHandler, Operation};
use crate::DeltaTable;
use crate::delta_datafusion::Expression;
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::physical::{find_metric_node, get_metric};
use crate::delta_datafusion::{
    DeltaSessionExt, SessionFallbackPolicy, SessionResolveContext, create_session,
    resolve_session_state, update_datafusion_session,
};
use crate::errors::{DeltaResult, DeltaTableError, unsupported_column_mapping_write};
use crate::kernel::schema::cast::normalize_for_delta;
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL, TableReference};
use crate::kernel::{Action, EagerSnapshot, StructType};
use crate::logstore::LogStoreRef;
use crate::protocol::{DeltaOperation, SaveMode};

pub mod configs;
pub(crate) mod execution;
pub(crate) mod generated_columns;
pub(crate) mod metrics;
mod plan;
pub(crate) mod schema_evolution;
pub mod writer;

#[derive(thiserror::Error, Debug)]
pub(crate) enum WriteError {
    #[error("No data source supplied to write command.")]
    MissingData,

    #[error("A table already exists at: {0}")]
    AlreadyExists(Url),

    #[error(
        "Specified table partitioning does not match table partitioning: expected: {expected:?}, got: {got:?}. To change partition columns, use full table overwrite with schema overwrite and no replaceWhere predicate."
    )]
    PartitionColumnMismatch {
        expected: Vec<String>,
        got: Vec<String>,
    },

    #[error("Partition column(s) not found in write schema: {}", columns.join(", "))]
    MissingPartitionColumns { columns: Vec<String> },
}

impl From<WriteError> for DeltaTableError {
    fn from(err: WriteError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}

///Specifies how to handle schema drifts
#[derive(PartialEq, Clone, Copy)]
pub enum SchemaMode {
    /// Overwrite the schema with the new schema
    Overwrite,
    /// Append the new schema to the existing schema
    Merge,
}

impl FromStr for SchemaMode {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "overwrite" => Ok(SchemaMode::Overwrite),
            "merge" => Ok(SchemaMode::Merge),
            _ => Err(DeltaTableError::Generic(format!(
                "Invalid schema write mode provided: {s}, only these are supported: ['overwrite', 'merge']"
            ))),
        }
    }
}

/// Write data into a DeltaTable
pub struct WriteBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// The input plan
    input: Option<LogicalPlan>,
    /// Datafusion session state relevant for executing the input plan
    session: Option<Arc<dyn Session>>,
    session_fallback_policy: SessionFallbackPolicy,
    /// SaveMode defines how to treat data already written to table location
    mode: SaveMode,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    /// When using `Overwrite` mode, replace data that matches a predicate
    predicate: Option<Expression>,
    /// Size above which we will write a buffered parquet file to disk.
    /// If None, the writer will not create a new file until the writer is closed.
    target_file_size: Option<Option<NonZeroU64>>,
    /// Number of records to be written in single batch to underlying writer
    write_batch_size: Option<usize>,
    /// whether to overwrite the schema or to merge it. None means to fail on schmema drift
    schema_mode: Option<SchemaMode>,
    /// how to handle cast failures, either return NULL (safe=true) or return ERR (safe=false)
    safe_cast: bool,
    /// Parquet writer properties
    writer_properties: Option<WriterProperties>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// Name of the table, only used when table doesn't exist yet
    name: Option<String>,
    /// Description of the table, only used when table doesn't exist yet
    description: Option<String>,
    /// Configurations of the delta table, only used when table doesn't exist
    configuration: HashMap<String, Option<String>>,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
/// Metrics for the Write Operation
pub struct WriteMetrics {
    /// Number of files added
    pub num_added_files: usize,
    /// Number of files removed
    pub num_removed_files: usize,
    /// Number of partitions
    pub num_partitions: usize,
    /// Number of rows added
    pub num_added_rows: usize,
    /// Time taken to execute the entire operation
    pub execution_time_ms: u64,
}

impl super::Operation for WriteBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl WriteBuilder {
    /// Create a new [`WriteBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            input: None,
            session: None,
            session_fallback_policy: SessionFallbackPolicy::default(),
            mode: SaveMode::Append,
            partition_columns: None,
            predicate: None,
            target_file_size: None,
            write_batch_size: None,
            safe_cast: false,
            schema_mode: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            name: None,
            description: None,
            configuration: Default::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the behavior when a table exists at location
    pub fn with_save_mode(mut self, save_mode: SaveMode) -> Self {
        self.mode = save_mode;
        self
    }

    /// Add Schema Write Mode
    pub fn with_schema_mode(mut self, schema_mode: SchemaMode) -> Self {
        self.schema_mode = Some(schema_mode);
        self
    }

    /// When using `Overwrite` mode, replace data that matches a predicate
    pub fn with_replace_where(mut self, predicate: impl Into<Expression>) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// (Optional) Specify table partitioning. For existing tables this must match the
    /// current partitioning, except full table overwrite with schema overwrite and
    /// no replaceWhere predicate may replace the partitioning. For new tables, the
    /// partitioning is applied.
    pub fn with_partition_columns(
        mut self,
        partition_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_columns = Some(partition_columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Logical execution plan that produces the data to be written to the delta table
    #[deprecated(since = "0.31.0", note = "Use `with_input_plan` instead")]
    pub fn with_input_execution_plan(self, plan: Arc<LogicalPlan>) -> Self {
        self.with_input_plan(plan.as_ref().clone())
    }

    /// Logical plan that produces the data to be written to the delta table
    pub fn with_input_plan(mut self, plan: LogicalPlan) -> Self {
        self.input = Some(plan);
        self
    }

    /// Set the DataFusion session used for planning and execution.
    ///
    /// The provided `session` should wrap a concrete `datafusion::execution::context::SessionState`.
    ///
    /// If `session` is not a `SessionState`, the default policy is to log a warning and fall back to
    /// internal defaults. To make this strict (error instead), set
    /// `with_session_fallback_policy(SessionFallbackPolicy::RequireSessionState)`.
    ///
    /// Example: `Arc::new(create_session().state())`.
    pub fn with_session_state(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Control how delta-rs resolves the provided session when it is not a concrete `SessionState`.
    ///
    /// Defaults to `SessionFallbackPolicy::InternalDefaults` to preserve existing behavior.
    pub fn with_session_fallback_policy(mut self, policy: SessionFallbackPolicy) -> Self {
        self.session_fallback_policy = policy;
        self
    }

    /// Specify the target file size for data files written to the delta table.
    pub fn with_target_file_size(mut self, target_file_size: Option<NonZeroU64>) -> Self {
        self.target_file_size = Some(target_file_size);
        self
    }

    /// Specify the target batch size for row groups written to parquet files.
    pub fn with_write_batch_size(mut self, write_batch_size: usize) -> Self {
        self.write_batch_size = Some(write_batch_size);
        self
    }

    /// Specify the safety of the casting operation
    /// how to handle cast failures, either return NULL (safe=true) or return ERR (safe=false)
    pub fn with_cast_safety(mut self, safe: bool) -> Self {
        self.safe_cast = safe;
        self
    }

    /// Specify the writer properties to use when writing a parquet file
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Specify the table name. Optionally qualified with
    /// a database name [database_name.] table_name.
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Comment to describe the table.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    /// Set configuration on created table
    pub fn with_configuration(
        mut self,
        configuration: impl IntoIterator<Item = (impl Into<String>, Option<impl Into<String>>)>,
    ) -> Self {
        self.configuration = configuration
            .into_iter()
            .map(|(k, v)| (k.into(), v.map(|s| s.into())))
            .collect();
        self
    }

    /// Execution plan that produces the data to be written to the delta table
    pub fn with_input_batches(mut self, batches: impl IntoIterator<Item = RecordBatch>) -> Self {
        let batches: Vec<RecordBatch> = batches.into_iter().collect();
        if !batches.is_empty() {
            let table_provider: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(batches[0].schema(), vec![batches]).unwrap());
            let source_plan =
                LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(table_provider), None)
                    .unwrap()
                    .build()
                    .unwrap();
            self.input = Some(source_plan);
        }
        self
    }

    /// Partition layout changes require a full table rewrite. Predicate overwrites
    /// replace only a table subset and must keep the existing layout.
    fn can_overwrite_partition_columns(&self) -> bool {
        self.mode == SaveMode::Overwrite
            && self.schema_mode == Some(SchemaMode::Overwrite)
            && self.predicate.is_none()
    }

    fn get_partition_columns(&self) -> Result<Vec<String>, WriteError> {
        // validate partition columns
        let active_partitions = self
            .snapshot
            .as_ref()
            .map(|s| s.metadata().partition_columns().to_vec());

        if let Some(active_part) = active_partitions {
            if let Some(ref partition_columns) = self.partition_columns {
                if &active_part != partition_columns {
                    if self.can_overwrite_partition_columns() {
                        Ok(partition_columns.clone())
                    } else {
                        Err(WriteError::PartitionColumnMismatch {
                            expected: active_part,
                            got: partition_columns.to_vec(),
                        })
                    }
                } else {
                    Ok(partition_columns.clone())
                }
            } else {
                Ok(active_part)
            }
        } else {
            Ok(self.partition_columns.clone().unwrap_or_default().to_vec())
        }
    }

    async fn check_preconditions(&self) -> DeltaResult<Vec<Action>> {
        if self.schema_mode == Some(SchemaMode::Overwrite) && self.mode != SaveMode::Overwrite {
            return Err(DeltaTableError::Generic(
                "Schema overwrite not supported for Append".to_string(),
            ));
        }

        let input = self
            .input
            .as_ref()
            .ok_or::<DeltaTableError>(WriteError::MissingData.into())?;
        let normalized_arrow = normalize_for_delta(input.schema().inner());
        let schema: StructType = normalized_arrow.try_into_kernel()?;

        match &self.snapshot {
            Some(snapshot) => {
                if snapshot.table_configuration().column_mapping_mode() != ColumnMappingMode::None {
                    return Err(unsupported_column_mapping_write("WRITE"));
                }

                if self.mode == SaveMode::Overwrite {
                    PROTOCOL.check_append_only(snapshot)?;
                    if !snapshot.load_config().require_files {
                        return Err(DeltaTableError::NotInitializedWithFiles("WRITE".into()));
                    }
                }

                PROTOCOL.can_write_to(snapshot)?;

                if self.schema_mode.is_none() {
                    PROTOCOL.check_can_write_timestamp_ntz(snapshot, &schema)?;
                    #[cfg(feature = "nanosecond-timestamps")]
                    PROTOCOL.check_can_write_timestamp_nanos(snapshot, &schema)?;
                }
                match self.mode {
                    SaveMode::ErrorIfExists => {
                        Err(WriteError::AlreadyExists(self.log_store.root_url().clone()).into())
                    }
                    _ => Ok(vec![]),
                }
            }
            None => {
                let mut builder = CreateBuilder::new()
                    .with_log_store(self.log_store.clone())
                    .with_columns(schema.fields().cloned())
                    .with_configuration(self.configuration.clone());
                if let Some(partition_columns) = self.partition_columns.as_ref() {
                    builder = builder.with_partition_columns(partition_columns.clone())
                }

                if let Some(name) = self.name.as_ref() {
                    builder = builder.with_table_name(name.clone());
                };

                if let Some(desc) = self.description.as_ref() {
                    builder = builder.with_comment(desc.clone());
                };

                let (_, actions, _, _) = builder.into_table_and_actions().await?;
                Ok(actions)
            }
        }
    }
}

impl std::future::IntoFuture for WriteBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;
        let table_uri = this.log_store.root_url().clone();
        let mode = this.mode;

        Box::pin(
            async move {
                // Runs pre execution handler.
                let operation_id = this.get_operation_id();
                this.pre_execute(operation_id).await?;

                let mut metrics = WriteMetrics::default();
                let exec_start = Instant::now();

                // Create table actions to initialize table in case it does not yet exist
                // and should be created
                let mut actions = this.check_preconditions().await?;

                let partition_columns = this.get_partition_columns()?;

                let Some(source) = this.input.take() else {
                    return Err(WriteError::MissingData.into());
                };

                let (session, _) = resolve_session_state(
                    this.session.as_deref(),
                    this.session_fallback_policy,
                    || create_session().state(),
                    SessionResolveContext {
                        operation: "write",
                        table_uri: Some(this.log_store.root_url()),
                        cdc: false,
                    },
                )?;

                update_datafusion_session(&session, &this.log_store, Some(operation_id))?;
                session.ensure_log_store_registered(this.log_store.as_ref())?;

                let prepared_write = plan::prepare_write(plan::WritePreparationInput {
                    snapshot: this.snapshot.as_ref(),
                    session: &session,
                    source,
                    mode: this.mode,
                    schema_mode: this.schema_mode,
                    safe_cast: this.safe_cast,
                    partition_columns: partition_columns.clone(),
                    predicate: this.predicate,
                    target_file_size: this.target_file_size,
                    write_batch_size: this.write_batch_size,
                    writer_properties: this.writer_properties.clone(),
                    configuration: &this.configuration,
                })?;

                let overwrite_plan = plan::plan_overwrite_rewrite(
                    this.snapshot.as_ref(),
                    &this.log_store,
                    &session,
                    this.mode,
                    &prepared_write,
                    operation_id,
                )
                .await?;

                if overwrite_plan.diagnostics.dropped_pruning_term_count > 0 {
                    tracing::warn!(
                        rewrite_kind = ?overwrite_plan.kind,
                        matched_file_count = overwrite_plan.diagnostics.matched_file_count,
                        translated_pruning_term_count =
                            overwrite_plan.diagnostics.translated_pruning_term_count,
                        dropped_pruning_term_count =
                            overwrite_plan.diagnostics.dropped_pruning_term_count,
                        "overwrite rewrite predicate was only partially translated for pruning; exact validation remains enabled"
                    );
                }

                let plan::PreparedWrite {
                    schema_delta,
                    exact_validation,
                    exec_options,
                    ..
                } = prepared_write;
                actions.extend(schema_delta.into_actions());

                metrics.num_removed_files = overwrite_plan.num_removed_files();

                let plan::WriteExecOptions {
                    partition_columns,
                    target_file_size,
                    write_batch_size,
                    writer_properties,
                    writer_stats_config,
                } = exec_options;
                let predicate_sql = exact_validation.as_ref().map(fmt_expr_to_sql).transpose()?;
                let (sink_plan, contains_cdc, insert_marker_column) =
                    overwrite_plan.build_sink_plan()?;
                let source_plan = session.create_physical_plan(&sink_plan).await?;

                // Here we need to validate if the new data conforms to a predicate if one is provided
                let (add_actions, _) = write_execution_plan_v2(
                    this.snapshot.as_ref(),
                    &session,
                    source_plan.clone(),
                    partition_columns.clone(),
                    this.log_store.object_store(Some(operation_id)).clone(),
                    target_file_size,
                    write_batch_size,
                    writer_properties,
                    writer_stats_config,
                    exact_validation,
                    contains_cdc,
                    insert_marker_column,
                )
                .await?;

                actions.extend(
                    overwrite_plan
                        .matched_existing
                        .into_actions(overwrite_plan.deletion_timestamp)?,
                );

                let source_count =
                    find_metric_node(SOURCE_COUNT_ID, &source_plan).ok_or_else(|| {
                        DeltaTableError::Generic("Unable to locate expected metric node".into())
                    })?;
                let source_count_metrics = source_count.metrics().unwrap();
                let num_added_rows = get_metric(&source_count_metrics, SOURCE_COUNT_METRIC);
                metrics.num_added_rows = num_added_rows;

                metrics.num_added_files = add_actions.len();
                actions.extend(add_actions);

                metrics.execution_time_ms =
                    Instant::now().duration_since(exec_start).as_millis() as u64;

                let operation = DeltaOperation::Write {
                    mode: this.mode,
                    partition_by: if !partition_columns.is_empty() {
                        Some(partition_columns)
                    } else {
                        None
                    },
                    predicate: predicate_sql,
                };

                let mut commit_properties = this.commit_properties.clone();
                commit_properties.app_metadata.insert(
                    "operationMetrics".to_owned(),
                    serde_json::to_value(&metrics)?,
                );

                let commit = CommitBuilder::from(commit_properties)
                    .with_actions(actions)
                    .with_post_commit_hook_handler(this.custom_execute_handler.clone())
                    .with_operation_id(operation_id)
                    .build(
                        this.snapshot.as_ref().map(|f| f as &dyn TableReference),
                        this.log_store.clone(),
                        operation.clone(),
                    )
                    .await?;

                if let Some(handler) = this.custom_execute_handler {
                    handler.post_execute(&this.log_store, operation_id).await?;
                }

                Ok(DeltaTable::new_with_state(this.log_store, commit.snapshot))
            }
            .instrument(tracing::info_span!(
                "write_operation",
                operation = "write",
                mode = ?mode,
                table_uri = %table_uri
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TableProperty;
    use crate::ensure_table_uri;
    use crate::kernel::CommitInfo;
    use crate::logstore::get_actions;
    use crate::operations::collect_sendable_stream;
    use crate::protocol::SaveMode;
    use crate::test_utils::{TestResult, TestSchemas};
    use crate::writer::test_utils::datafusion::{get_data, get_data_sorted, write_batch};
    use crate::writer::test_utils::{
        get_arrow_schema, get_delta_schema, get_delta_schema_with_nested_struct, get_record_batch,
        get_record_batch_with_nested_struct, setup_table_with_configuration,
    };
    use arrow_array::{
        Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::*;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::schema::MetadataValue;
    use futures::TryStreamExt;
    use itertools::Itertools;
    use serde_json::{Value, json};

    async fn get_write_metrics(table: &DeltaTable) -> WriteMetrics {
        let mut commit_info: Vec<_> = table.history(Some(1)).await.unwrap().collect();
        let metrics = commit_info
            .first_mut()
            .unwrap()
            .info
            .remove("operationMetrics")
            .unwrap();
        serde_json::from_value(metrics).unwrap()
    }

    async fn query_table(table: &DeltaTable, sql: &str) -> TestResult<Vec<RecordBatch>> {
        let table = DeltaTable::new_with_state(
            table.log_store.clone(),
            table.state.as_ref().unwrap().clone(),
        );
        let ctx = SessionContext::new();
        table.update_datafusion_session(&ctx.state()).unwrap();
        ctx.register_table("test", table.table_provider().await.unwrap())
            .unwrap();

        Ok(ctx.sql(sql).await?.collect().await?)
    }

    async fn query_single_i64_row(table: &DeltaTable, sql: &str) -> TestResult<Vec<i64>> {
        let batches = query_table(table, sql).await?;
        let batch = batches
            .first()
            .expect("expected aggregate query to return a single batch");

        Ok(batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("expected Int64 aggregate column")
                    .value(0)
            })
            .collect())
    }

    async fn query_i32_rows(table: &DeltaTable, sql: &str, column: &str) -> TestResult<Vec<i32>> {
        let mut values = Vec::new();
        for batch in query_table(table, sql).await? {
            let array = batch
                .column_by_name(column)
                .expect("expected query column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("expected Int32 query column");
            values.extend(
                array
                    .iter()
                    .map(|value| value.expect("expected non-null Int32 value")),
            );
        }
        Ok(values)
    }

    async fn open_copied_table_fixture(
        fixture_source: &std::path::Path,
        table_dir_name: &str,
    ) -> TestResult<(tempfile::TempDir, DeltaTable)> {
        let temp_dir = tempfile::tempdir()?;
        fs_extra::dir::copy(fixture_source, temp_dir.path(), &Default::default())?;
        let table_url =
            url::Url::from_directory_path(temp_dir.path().join(table_dir_name).canonicalize()?)
                .unwrap();
        Ok((temp_dir, crate::open_table(table_url).await?))
    }

    async fn latest_remove_actions(table: &DeltaTable) -> TestResult<Vec<crate::kernel::Remove>> {
        let version = table
            .version()
            .expect("expected committed version for latest remove actions");
        let snapshot_bytes = table
            .log_store
            .read_commit_entry(version)
            .await?
            .expect("failed to get snapshot bytes");
        Ok(get_actions(version, &snapshot_bytes)?
            .into_iter()
            .filter_map(|action| match action {
                Action::Remove(remove) => Some(remove),
                _ => None,
            })
            .collect())
    }

    async fn modified_partitioned_table(batch: &RecordBatch) -> TestResult<DeltaTable> {
        Ok(DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .with_partition_columns(["modified"])
            .await?)
    }

    fn expect_write_error(err: &DeltaTableError) -> &WriteError {
        let DeltaTableError::GenericError { source } = err else {
            panic!("expected WriteError, got {err:?}");
        };
        source
            .downcast_ref::<WriteError>()
            .expect("expected WriteError source")
    }

    fn assert_common_write_metrics(write_metrics: WriteMetrics) {
        // assert!(write_metrics.execution_time_ms > 0);
        assert!(write_metrics.num_added_files > 0);
    }

    #[tokio::test]
    async fn test_write_when_delta_table_is_append_only() {
        let table = setup_table_with_configuration(TableProperty::AppendOnly, Some("true")).await;
        let batch = get_record_batch(None, false);
        // Append
        let table = write_batch(table, batch.clone()).await;
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert_eq!(write_metrics.num_removed_files, 0);
        assert_common_write_metrics(write_metrics);

        // Overwrite
        let _err = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .await
            .expect_err("Remove action is included when Delta table is append-only. Should error");
    }

    #[tokio::test]
    async fn test_create_write() {
        let table_schema = get_delta_schema();
        let batch = get_record_batch(None, false);

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        // write some data
        let metadata = HashMap::from_iter(vec![("k1".to_string(), json!("v1.1"))]);
        let mut table = table
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(CommitProperties::default().with_metadata(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert_eq!(
            write_metrics.num_added_files,
            table.snapshot().unwrap().log_data().num_files()
        );
        assert_common_write_metrics(write_metrics);

        table.load().await.unwrap();
        let history: Vec<CommitInfo> = table.history(None).await.unwrap().collect();
        assert_eq!(history.len(), 2);
        assert_eq!(
            history[0]
                .info
                .clone()
                .into_iter()
                .filter(|(k, _)| k == "k1")
                .collect::<HashMap<String, Value>>(),
            metadata
        );

        // append some data
        let metadata: HashMap<String, Value> =
            HashMap::from_iter(vec![("k1".to_string(), json!("v1.2"))]);
        let mut table = table
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(CommitProperties::default().with_metadata(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));
        assert_eq!(table.snapshot().unwrap().log_data().num_files(), 2);
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert_eq!(write_metrics.num_added_files, 1);
        assert_common_write_metrics(write_metrics);

        table.load().await.unwrap();
        let history: Vec<CommitInfo> = table.history(None).await.unwrap().collect();
        assert_eq!(history.len(), 3);
        assert_eq!(
            history[0]
                .info
                .clone()
                .into_iter()
                .filter(|(k, _)| k == "k1")
                .collect::<HashMap<String, Value>>(),
            metadata
        );

        // overwrite table
        let metadata: HashMap<String, Value> =
            HashMap::from_iter(vec![("k2".to_string(), json!("v2.1"))]);
        let mut table = table
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Overwrite)
            .with_commit_properties(CommitProperties::default().with_metadata(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(3));
        assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert!(write_metrics.num_removed_files > 0);
        assert_common_write_metrics(write_metrics);

        table.load().await.unwrap();
        let history: Vec<CommitInfo> = table.history(None).await.unwrap().collect();
        assert_eq!(history.len(), 4);
        assert_eq!(
            history[0]
                .info
                .clone()
                .into_iter()
                .filter(|(k, _)| k == "k2")
                .collect::<HashMap<String, Value>>(),
            metadata
        );
    }

    #[tokio::test]
    async fn test_write_different_types() {
        // Ensure write data is casted when data of a different type from the table is provided.

        // Validate String -> Int is err
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(0), None]))],
        )
        .unwrap();
        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap();
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 2);
        assert_common_write_metrics(write_metrics);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("Test123".to_owned()),
                Some("123".to_owned()),
                None,
            ]))],
        )
        .unwrap();

        // Test cast options
        let table = table
            .write(vec![batch.clone()])
            .with_cast_safety(true)
            .await
            .unwrap();

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert_common_write_metrics(write_metrics);

        let expected = [
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "|       |",
            "| 123   |",
            "| 0     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        let res = table.write(vec![batch]).await;
        assert!(res.is_err());

        // Validate the datetime -> string behavior
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![Some(
                "2023-06-03 15:35:00".to_owned(),
            )]))],
        )
        .unwrap();
        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap();

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 1);
        assert_common_write_metrics(write_metrics);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string().into())),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![Some(10000)]).with_timezone("UTC"),
            )],
        )
        .unwrap();

        let _res = table.write(vec![batch]).await.unwrap();
        let expected = [
            "+--------------------------+",
            "| value                    |",
            "+--------------------------+",
            "| 1970-01-01T00:00:00.010Z |",
            "| 2023-06-03 15:35:00      |",
            "+--------------------------+",
        ];
        let actual = get_data(&_res).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_write_nonexistent() {
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_write_partitioned() {
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified"])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.snapshot().unwrap().log_data().num_files(), 2);
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_files, 2);
        assert_common_write_metrics(write_metrics);

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.snapshot().unwrap().log_data().num_files(), 4);

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_files, 4);
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_write_partitioned_parallel_writers() {
        let batch = get_record_batch(None, false);

        let multi_stream_input: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                batch.schema(),
                vec![
                    vec![batch.clone()],
                    vec![batch.clone()],
                    vec![batch.clone()],
                ],
            )
            .unwrap(),
        );
        let multi_stream_plan =
            LogicalPlanBuilder::scan("source", provider_as_source(multi_stream_input), None)
                .unwrap()
                .build()
                .unwrap();

        let parallel_table = DeltaTable::new_in_memory()
            .write(vec![])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_input_plan(multi_stream_plan)
            .with_partition_columns(["modified"])
            .await
            .unwrap();

        let single_writer_table = DeltaTable::new_in_memory()
            .write(vec![batch.clone(), batch.clone(), batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified"])
            .await
            .unwrap();

        let parallel_data = get_data_sorted(&parallel_table, "modified, id, value").await;
        let single_writer_data = get_data_sorted(&single_writer_table, "modified, id, value").await;
        assert_eq!(parallel_data, single_writer_data);

        let parallel_files = parallel_table.snapshot().unwrap().log_data().num_files();
        let single_writer_files = single_writer_table
            .snapshot()
            .unwrap()
            .log_data()
            .num_files();
        assert_eq!(parallel_files, single_writer_files);
        assert_eq!(parallel_files, 2);

        let parallel_write_metrics: WriteMetrics = get_write_metrics(&parallel_table).await;
        let single_writer_metrics: WriteMetrics = get_write_metrics(&single_writer_table).await;
        assert_eq!(
            parallel_write_metrics.num_added_files,
            single_writer_metrics.num_added_files
        );
        assert_eq!(parallel_write_metrics.num_added_files, 2);
    }

    #[tokio::test]
    async fn test_write_partitioned_parallel_writers_error_propagation() {
        let batch = get_record_batch(None, false);

        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": true, "metadata": {}},
                {"name": "value", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"value < 6\"} }"
                }},
                {"name": "modified", "type": "string", "nullable": true, "metadata": {}},
            ]
        }))
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(schema.fields().cloned())
            .with_partition_columns(["modified"])
            .await
            .unwrap();

        let multi_stream_input: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                batch.schema(),
                vec![
                    vec![batch.clone()],
                    vec![batch.clone()],
                    vec![batch.clone()],
                ],
            )
            .unwrap(),
        );
        let multi_stream_plan =
            LogicalPlanBuilder::scan("source", provider_as_source(multi_stream_input), None)
                .unwrap()
                .build()
                .unwrap();

        let result = table
            .write(vec![])
            .with_save_mode(SaveMode::Append)
            .with_input_plan(multi_stream_plan)
            .await;

        assert!(
            result.is_err(),
            "write should fail when invariant is violated in parallel writers"
        );
    }

    #[tokio::test]
    async fn test_merge_schema() {
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        let mut new_schema_builder = arrow_schema::SchemaBuilder::new();
        for field in batch.schema().fields() {
            if field.name() != "modified" {
                new_schema_builder.push(field.clone());
            }
        }
        new_schema_builder.push(Field::new("inserted_by", DataType::Utf8, true));
        let new_schema = new_schema_builder.finish();
        let new_fields = new_schema.fields();
        let new_names = new_fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(new_names, vec!["id", "value", "inserted_by"]);
        let inserted_by = StringArray::from(vec![
            Some("A1"),
            Some("B1"),
            None,
            Some("B2"),
            Some("A3"),
            Some("A4"),
            None,
            None,
            Some("B4"),
            Some("A5"),
            Some("A7"),
        ]);
        let new_batch = RecordBatch::try_new(
            Arc::new(new_schema),
            vec![
                Arc::new(batch.column_by_name("id").unwrap().clone()),
                Arc::new(batch.column_by_name("value").unwrap().clone()),
                Arc::new(inserted_by),
            ],
        )
        .unwrap();

        let mut table = table
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Merge)
            .await
            .unwrap();
        table.load().await.unwrap();
        assert_eq!(table.version(), Some(1));
        let new_schema = table.snapshot().unwrap().metadata().parse_schema().unwrap();
        let fields = new_schema.fields();
        let names = fields.map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(names, vec!["id", "value", "modified", "inserted_by"]);

        // <https://github.com/delta-io/delta-rs/issues/2925>
        let metadata = table
            .snapshot()
            .expect("Failed to retrieve updated snapshot")
            .metadata();
        assert_ne!(
            None,
            metadata.created_time(),
            "Created time should be the milliseconds since epoch of when the action was created"
        );

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_merge_schema_with_partitions() {
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .with_partition_columns(vec!["id", "value"])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        let mut new_schema_builder = arrow_schema::SchemaBuilder::new();
        for field in batch.schema().fields() {
            if field.name() != "modified" {
                new_schema_builder.push(field.clone());
            }
        }
        new_schema_builder.push(Field::new("inserted_by", DataType::Utf8, true));
        let new_schema = new_schema_builder.finish();
        let new_fields = new_schema.fields();
        let new_names = new_fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(new_names, vec!["id", "value", "inserted_by"]);
        let inserted_by = StringArray::from(vec![
            Some("A1"),
            Some("B1"),
            None,
            Some("B2"),
            Some("A3"),
            Some("A4"),
            None,
            None,
            Some("B4"),
            Some("A5"),
            Some("A7"),
        ]);
        let new_batch = RecordBatch::try_new(
            Arc::new(new_schema),
            vec![
                Arc::new(batch.column_by_name("id").unwrap().clone()),
                Arc::new(batch.column_by_name("value").unwrap().clone()),
                Arc::new(inserted_by),
            ],
        )
        .unwrap();
        let table = table
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Merge)
            .await
            .unwrap();

        assert_eq!(table.version(), Some(1));
        let new_schema = table.snapshot().unwrap().metadata().parse_schema().unwrap();
        let fields = new_schema.fields();
        let mut names = fields.map(|f| f.name()).collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, vec!["id", "inserted_by", "modified", "value"]);
        let part_cols = table.snapshot().unwrap().metadata().partition_columns();
        assert_eq!(part_cols, ["id".to_string(), "value".to_string()]); // we want to preserve partitions

        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_merge_schema_with_partitions_allows_source_missing_partition_column() -> TestResult
    {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let evolved_schema = Arc::new(ArrowSchema::new(vec![
            batch.schema().field(0).as_ref().clone(),
            batch.schema().field(1).as_ref().clone(),
            Field::new("inserted_by", DataType::Utf8, true),
        ]));
        let evolved_batch = RecordBatch::try_new(
            evolved_schema,
            vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                Arc::new(StringArray::from(vec![
                    Some("A1"),
                    Some("B1"),
                    None,
                    Some("B2"),
                    Some("A3"),
                    Some("A4"),
                    None,
                    None,
                    Some("B4"),
                    Some("A5"),
                    Some("A7"),
                ])),
            ],
        )?;

        let table = table
            .write(vec![evolved_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Merge)
            .await?;

        assert_eq!(table.version(), Some(1));
        let schema = table.snapshot().unwrap().metadata().parse_schema()?;
        let names = schema
            .fields()
            .map(|field| field.name())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["id", "value", "modified", "inserted_by"]);
        assert_eq!(
            table.snapshot().unwrap().metadata().partition_columns(),
            &vec!["modified".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_schema_preserves_existing_field_metadata() {
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": true, "metadata": {}},
                {"name": "value", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"value < 12\"} }",
                    "delta.userMetadata": "preserve-me"
                }},
                {"name": "modified", "type": "string", "nullable": true, "metadata": {}},
            ]
        }))
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(schema.fields().cloned())
            .await
            .unwrap()
            .write(vec![get_record_batch(None, false)])
            .await
            .unwrap();

        let batch = get_record_batch(None, false);
        let evolved_schema = Arc::new(ArrowSchema::new(vec![
            batch.schema().field(0).as_ref().clone(),
            batch.schema().field(1).as_ref().clone(),
            batch.schema().field(2).as_ref().clone(),
            Field::new("inserted_by", DataType::Utf8, true),
        ]));
        let evolved_batch = RecordBatch::try_new(
            evolved_schema,
            vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                batch.column(2).clone(),
                Arc::new(StringArray::from(vec![
                    Some("A1"),
                    Some("B1"),
                    None,
                    Some("B2"),
                    Some("A3"),
                    Some("A4"),
                    None,
                    None,
                    Some("B4"),
                    Some("A5"),
                    Some("A7"),
                ])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![evolved_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Merge)
            .await
            .unwrap();

        let schema = table.snapshot().unwrap().metadata().parse_schema().unwrap();
        let value = schema.field("value").unwrap();
        assert_eq!(
            value.metadata.get("delta.invariants"),
            Some(&MetadataValue::String(
                "{\"expression\": { \"expression\": \"value < 12\"} }".to_string()
            ))
        );
        assert_eq!(
            value.metadata.get("delta.userMetadata"),
            Some(&MetadataValue::String("preserve-me".to_string()))
        );
    }

    #[tokio::test]
    async fn test_overwrite_schema() {
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);
        let mut new_schema_builder = arrow_schema::SchemaBuilder::new();
        for field in batch.schema().fields() {
            if field.name() != "modified" {
                new_schema_builder.push(field.clone());
            }
        }
        new_schema_builder.push(Field::new("inserted_by", DataType::Utf8, true));
        let new_schema = new_schema_builder.finish();
        let new_fields = new_schema.fields();
        let new_names = new_fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(new_names, vec!["id", "value", "inserted_by"]);
        let inserted_by = StringArray::from(vec![
            Some("A1"),
            Some("B1"),
            None,
            Some("B2"),
            Some("A3"),
            Some("A4"),
            None,
            None,
            Some("B4"),
            Some("A5"),
            Some("A7"),
        ]);
        let new_batch = RecordBatch::try_new(
            Arc::new(new_schema),
            vec![
                Arc::new(batch.column_by_name("id").unwrap().clone()),
                Arc::new(batch.column_by_name("value").unwrap().clone()),
                Arc::new(inserted_by),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Overwrite)
            .await;
        assert!(table.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_schema_can_change_partition_columns_without_schema_change() -> TestResult
    {
        let batch = get_record_batch(None, false);

        let table = modified_partitioned_table(&batch).await?;

        assert_eq!(
            table.snapshot().unwrap().metadata().partition_columns(),
            &vec!["modified".to_string()]
        );
        let initial_num_files = table.snapshot().unwrap().log_data().num_files();

        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .with_partition_columns(["id"])
            .await?;

        assert_eq!(table.version(), Some(1));
        assert_eq!(
            table.snapshot().unwrap().metadata().partition_columns(),
            &vec!["id".to_string()]
        );

        let add_paths = table
            .snapshot()
            .unwrap()
            .log_data()
            .iter()
            .map(|add| add.path().into_owned())
            .collect::<Vec<_>>();
        assert!(!add_paths.is_empty());
        assert!(add_paths.iter().all(|path| path.contains("id=")));

        let remove_actions = latest_remove_actions(&table).await?;
        assert_eq!(remove_actions.len(), initial_num_files);
        assert!(
            remove_actions
                .iter()
                .all(|remove| remove.deletion_timestamp.is_some())
        );

        let commit_info: Vec<_> = table.history(Some(1)).await?.collect();
        let operation_parameters = commit_info[0].operation_parameters.as_ref().unwrap();
        assert_eq!(operation_parameters["partitionBy"], json!("[\"id\"]"));

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_schema_can_change_schema_and_partition_columns() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let mut new_schema_builder = arrow_schema::SchemaBuilder::new();
        for field in batch.schema().fields() {
            if field.name() != "modified" {
                new_schema_builder.push(field.clone());
            }
        }
        new_schema_builder.push(Field::new("inserted_by", DataType::Utf8, true));
        let new_schema = new_schema_builder.finish();
        let inserted_by = StringArray::from(vec![
            Some("A1"),
            Some("B1"),
            None,
            Some("B2"),
            Some("A3"),
            Some("A4"),
            None,
            None,
            Some("B4"),
            Some("A5"),
            Some("A7"),
        ]);
        let new_batch = RecordBatch::try_new(
            Arc::new(new_schema),
            vec![
                Arc::new(batch.column_by_name("id").unwrap().clone()),
                Arc::new(batch.column_by_name("value").unwrap().clone()),
                Arc::new(inserted_by),
            ],
        )?;

        let table = table
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .with_partition_columns(["inserted_by"])
            .await?;

        let schema = table.snapshot().unwrap().metadata().parse_schema()?;
        let names = schema
            .fields()
            .map(|field| field.name())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["id", "value", "inserted_by"]);
        assert_eq!(
            table.snapshot().unwrap().metadata().partition_columns(),
            &vec!["inserted_by".to_string()]
        );

        let add_paths = table
            .snapshot()
            .unwrap()
            .log_data()
            .iter()
            .map(|add| add.path().into_owned())
            .collect::<Vec<_>>();
        assert!(!add_paths.is_empty());
        assert!(add_paths.iter().all(|path| path.contains("inserted_by=")));

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_schema_partition_change_preserves_table_metadata() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .with_partition_columns(["modified"])
            .with_table_name("preserve_name")
            .with_description("preserve_description")
            .await?;

        let initial_metadata = table.snapshot().unwrap().metadata().clone();
        let initial_created_time = initial_metadata.created_time();

        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .with_partition_columns(["id"])
            .await?;

        let metadata = table.snapshot().unwrap().metadata();
        assert_eq!(metadata.id(), initial_metadata.id());
        assert_eq!(metadata.name(), Some("preserve_name"));
        assert_eq!(metadata.description(), Some("preserve_description"));
        assert_eq!(metadata.created_time(), initial_created_time);
        assert_eq!(metadata.partition_columns(), &vec!["id".to_string()]);

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_schema_can_remove_partition_columns() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .with_partition_columns(std::iter::empty::<&str>())
            .await?;

        assert_eq!(table.version(), Some(1));
        assert!(
            table
                .snapshot()
                .unwrap()
                .metadata()
                .partition_columns()
                .is_empty()
        );

        let add_paths = table
            .snapshot()
            .unwrap()
            .log_data()
            .iter()
            .map(|add| add.path().into_owned())
            .collect::<Vec<_>>();
        assert!(!add_paths.is_empty());
        assert!(add_paths.iter().all(|path| !path.contains('/')));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_rejects_partition_column_change() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let result = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_partition_columns(["id"])
            .await;

        let err = result.expect_err("append should reject partition column change");
        match expect_write_error(&err) {
            WriteError::PartitionColumnMismatch { expected, got } => {
                assert_eq!(expected, &vec!["modified".to_string()]);
                assert_eq!(got, &vec!["id".to_string()]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_without_schema_overwrite_rejects_partition_column_change() -> TestResult
    {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let result = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_partition_columns(["id"])
            .await;

        assert!(matches!(result, Err(DeltaTableError::GenericError { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_where_rejects_partition_column_change() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let result = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .with_partition_columns(["id"])
            .with_replace_where(col("id").eq(lit("A")))
            .await;

        assert!(matches!(result, Err(DeltaTableError::GenericError { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_schema_preserves_partition_columns_when_omitted() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .await?;

        assert_eq!(
            table.snapshot().unwrap().metadata().partition_columns(),
            &vec!["modified".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_schema_rejects_missing_new_partition_column() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = modified_partitioned_table(&batch).await?;

        let result = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .with_partition_columns(["missing_partition"])
            .await;

        let err = result.expect_err("missing partition column should fail");
        match expect_write_error(&err) {
            WriteError::MissingPartitionColumns { columns } => {
                assert_eq!(columns, &vec!["missing_partition".to_string()]);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_check() {
        // If you do not pass a schema mode, we want to check the schema
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        let mut new_schema_builder = arrow_schema::SchemaBuilder::new();

        new_schema_builder.push(Field::new("inserted_by", DataType::Utf8, true));
        let new_schema = new_schema_builder.finish();
        let new_fields = new_schema.fields();
        let new_names = new_fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(new_names, vec!["inserted_by"]);
        let inserted_by = StringArray::from(vec![
            Some("A1"),
            Some("B1"),
            None,
            Some("B2"),
            Some("A3"),
            Some("A4"),
            None,
            None,
            Some("B4"),
            Some("A5"),
            Some("A7"),
        ]);
        let new_batch =
            RecordBatch::try_new(Arc::new(new_schema), vec![Arc::new(inserted_by)]).unwrap();

        let table = table
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Append)
            .await;
        assert!(table.is_err());
    }

    #[tokio::test]
    async fn test_check_invariants() {
        let batch = get_record_batch(None, false);
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": true, "metadata": {}},
                {"name": "value", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"value < 12\"} }"
                }},
                {"name": "modified", "type": "string", "nullable": true, "metadata": {}},
            ]
        }))
        .unwrap();
        let table = DeltaTable::new_in_memory()
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let table = table.write(vec![batch.clone()]).await.unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": true, "metadata": {}},
                {"name": "value", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"value < 6\"} }"
                }},
                {"name": "modified", "type": "string", "nullable": true, "metadata": {}},
            ]
        }))
        .unwrap();
        let table = DeltaTable::new_in_memory()
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let table = table.write(vec![batch.clone()]).await;
        assert!(table.is_err());
    }

    #[tokio::test]
    async fn test_nested_struct() {
        let table_schema = get_delta_schema_with_nested_struct();
        let batch = get_record_batch_with_nested_struct();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let table = table
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        let actual = get_data(&table).await;
        let expected = DataType::Struct(Fields::from(vec![Field::new(
            "count",
            DataType::Int32,
            true,
        )]));
        assert_eq!(
            actual[0].column_by_name("nested").unwrap().data_type(),
            &expected
        );
    }

    #[tokio::test]
    async fn test_special_characters_write_read() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("string", DataType::Utf8, true),
            Field::new("data", DataType::Utf8, true),
        ]));

        let str_values = StringArray::from(vec![r#"$%&/()=^"[]#*?._- {=}|`<>~/\r\n+"#]);
        let data_values = StringArray::from(vec!["test"]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(str_values), Arc::new(data_values)])
            .unwrap();

        let ops = DeltaTable::try_from_url(
            ensure_table_uri(tmp_path.as_os_str().to_str().unwrap()).unwrap(),
        )
        .await
        .unwrap();

        let table = ops
            .write([batch.clone()])
            .with_partition_columns(["string"])
            .await
            .unwrap();
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        let table_uri = url::Url::from_directory_path(&tmp_path).unwrap();
        let table = crate::open_table(table_uri).await.unwrap();
        let (_table, stream) = table.scan_table().await.unwrap();
        let data: Vec<RecordBatch> = collect_sendable_stream(stream).await.unwrap();

        let expected = vec![
            r#"+----------------------------------+------+"#,
            r#"| string                           | data |"#,
            r#"+----------------------------------+------+"#,
            r#"| $%&/()=^"[]#*?._- {=}|`<>~/\r\n+ | test |"#,
            r#"+----------------------------------+------+"#,
        ];

        assert_batches_eq!(&expected, &data);
    }

    #[tokio::test]
    async fn test_replace_where() {
        let schema = get_arrow_schema(&None);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-04",
                ])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 4);
        assert_common_write_metrics(write_metrics);

        let batch_add = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["C"])),
                Arc::new(arrow::array::Int32Array::from(vec![50])),
                Arc::new(arrow::array::StringArray::from(vec!["2023-01-01"])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch_add])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(col("id").eq(lit("C")))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 1);
        assert_common_write_metrics(write_metrics);

        let expected = [
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| B  | 20    | 2021-02-03 |",
            "| C  | 50    | 2023-01-01 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_replace_where_fail_not_matching_predicate() {
        let schema = get_arrow_schema(&None);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-04",
                ])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        // Take clones of these before an operation resulting in error, otherwise it will
        // be impossible to refer to an in-memory table
        let table_logstore = table.log_store.clone();
        let table_state = table.state.clone().unwrap();

        // An attempt to write records non conforming to predicate should fail
        let batch_fail = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["D"])),
                Arc::new(arrow::array::Int32Array::from(vec![1000])),
                Arc::new(arrow::array::StringArray::from(vec!["2023-01-01"])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch_fail])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(col("id").eq(lit("C")))
            .await;
        assert!(table.is_err());

        // Verify that table state hasn't changed
        let table = DeltaTable::new_with_state(table_logstore, table_state);
        assert_eq!(table.get_latest_version().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_replace_where_no_matching_files_still_validates_input() {
        let schema = get_arrow_schema(&None);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let table_logstore = table.log_store();
        let table_state = table.state.clone().unwrap();

        let batch_fail = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["D"])),
                Arc::new(arrow::array::Int32Array::from(vec![1000])),
                Arc::new(arrow::array::StringArray::from(vec!["2023-01-01"])),
            ],
        )
        .unwrap();

        let result = table
            .write(vec![batch_fail])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(col("id").eq(lit("Z")))
            .await;
        assert!(result.is_err());

        let table = DeltaTable::new_with_state(table_logstore, table_state);
        assert_eq!(table.get_latest_version().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_write_preserves_user_insert_marker_column_outside_rewrite() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
            Field::new(
                super::plan::WRITE_INSERT_MARKER_COLUMN,
                DataType::Boolean,
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("C")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("2021-02-02"),
                    Some("2021-02-03"),
                    Some("2021-02-04"),
                ])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(true),
                    Some(false),
                ])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let actual = get_data_sorted(
            &table,
            format!(
                "id,value,modified,{}",
                super::plan::WRITE_INSERT_MARKER_COLUMN
            )
            .as_str(),
        )
        .await;
        assert_batches_sorted_eq!(
            &[
                "+----+-------+------------+-------------------------+",
                "| id | value | modified   | __delta_rs_write_insert |",
                "+----+-------+------------+-------------------------+",
                "| A  | 1     | 2021-02-02 | false                   |",
                "| B  | 2     | 2021-02-03 | true                    |",
                "| C  | 3     | 2021-02-04 | false                   |",
                "+----+-------+------------+-------------------------+",
            ],
            &actual
        );
    }

    #[tokio::test]
    async fn test_replace_where_preserves_user_insert_marker_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
            Field::new(
                super::plan::WRITE_INSERT_MARKER_COLUMN,
                DataType::Boolean,
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("C")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("2021-02-02"),
                    Some("2021-02-03"),
                    Some("2021-02-04"),
                ])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(true),
                ])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let replacement_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("C")])),
                Arc::new(Int32Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec![Some("2023-01-01")])),
                Arc::new(arrow::array::BooleanArray::from(vec![Some(false)])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(col("value").eq(lit(3)))
            .await
            .expect("replaceWhere should preserve user columns named like internal markers");

        let actual = get_data_sorted(
            &table,
            format!(
                "id,value,modified,{}",
                super::plan::WRITE_INSERT_MARKER_COLUMN
            )
            .as_str(),
        )
        .await;
        assert_batches_sorted_eq!(
            &[
                "+----+-------+------------+-------------------------+",
                "| id | value | modified   | __delta_rs_write_insert |",
                "+----+-------+------------+-------------------------+",
                "| A  | 1     | 2021-02-02 | false                   |",
                "| B  | 2     | 2021-02-03 | false                   |",
                "| C  | 3     | 2023-01-01 | false                   |",
                "+----+-------+------------+-------------------------+",
            ],
            &actual
        );
    }

    #[tokio::test]
    async fn test_replace_where_merge_schema_rescues_existing_rows() -> TestResult {
        let base_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
        ]));
        let base_batch = RecordBatch::try_new(
            Arc::clone(&base_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("C")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("2021-02-02"),
                    Some("2021-02-03"),
                    Some("2021-02-04"),
                ])),
            ],
        )?;

        let table = DeltaTable::new_in_memory()
            .write(vec![base_batch])
            .with_save_mode(SaveMode::Append)
            .await?;

        let merge_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
            Field::new("inserted_by", DataType::Utf8, true),
        ]));
        let replacement_batch = RecordBatch::try_new(
            merge_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("C")])),
                Arc::new(Int32Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec![Some("2023-01-01")])),
                Arc::new(StringArray::from(vec![Some("rewrite")])),
            ],
        )?;

        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Merge)
            .with_replace_where(col("value").eq(lit(3)))
            .await?;

        let actual = get_data_sorted(&table, "id,value,modified,inserted_by").await;
        assert_batches_sorted_eq!(
            &[
                "+----+-------+------------+-------------+",
                "| id | value | modified   | inserted_by |",
                "+----+-------+------------+-------------+",
                "| A  | 1     | 2021-02-02 |             |",
                "| B  | 2     | 2021-02-03 |             |",
                "| C  | 3     | 2023-01-01 | rewrite     |",
                "+----+-------+------------+-------------+",
            ],
            &actual
        );

        Ok(())
    }

    fn mixed_case_replace_where_batches() -> TestResult<(Arc<ArrowSchema>, RecordBatch, RecordBatch)>
    {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("utcDate", DataType::Utf8, true),
            Field::new("homeTeam", DataType::Utf8, true),
            Field::new("score", DataType::Utf8, true),
        ]));
        let base_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("2008-08-16T15:00:00Z"),
                    Some("2009-05-16T15:00:00Z"),
                ])),
                Arc::new(StringArray::from(vec![Some("Everton"), Some("Everton")])),
                Arc::new(StringArray::from(vec![Some("0-1"), Some("3-1")])),
            ],
        )?;
        let replacement_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("2010-01-01T15:00:00Z")])),
                Arc::new(StringArray::from(vec![Some("Everton")])),
                Arc::new(StringArray::from(vec![Some("0-1")])),
            ],
        )?;

        Ok((schema, base_batch, replacement_batch))
    }

    #[tokio::test]
    async fn test_replace_where_preserves_mixed_case_columns_when_rescuing_rows() -> TestResult {
        let (_, base_batch, replacement_batch) = mixed_case_replace_where_batches()?;

        let table = DeltaTable::new_in_memory()
            .write(vec![base_batch])
            .with_save_mode(SaveMode::Append)
            .await?;

        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_schema_mode(SchemaMode::Overwrite)
            .with_replace_where(col("score").eq(lit("0-1")))
            .await?;

        let actual = get_data_sorted(&table, r#""utcDate","homeTeam",score"#).await;
        assert_batches_sorted_eq!(
            &[
                "+----------------------+----------+-------+",
                "| utcDate              | homeTeam | score |",
                "+----------------------+----------+-------+",
                "| 2009-05-16T15:00:00Z | Everton  | 3-1   |",
                "| 2010-01-01T15:00:00Z | Everton  | 0-1   |",
                "+----------------------+----------+-------+",
            ],
            &actual
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_where_preserves_live_rows_with_deletion_vectors() -> TestResult {
        let (_temp_dir, table) = open_copied_table_fixture(
            &crate::test_utils::TestTables::WithDvSmall.as_path(),
            "table-with-dv-small",
        )
        .await?;

        let source_files = table
            .get_active_add_actions_by_partitions(&[])
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(source_files.len(), 1);
        let source_path = source_files[0].path().to_string();
        let source_deletion_vector = source_files[0].deletion_vector_descriptor();
        assert!(
            source_deletion_vector.is_some(),
            "expected DV-backed source file"
        );
        assert_eq!(
            query_i32_rows(&table, "SELECT value FROM test ORDER BY value", "value").await?,
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );

        let replacement_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![Some(50)]))],
        )?;

        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where("value = 5 OR value = 50")
            .await?;
        assert_eq!(table.version(), Some(2));

        assert_eq!(
            query_i32_rows(&table, "SELECT value FROM test ORDER BY value", "value").await?,
            vec![1, 2, 3, 4, 6, 7, 8, 50]
        );

        let remove_actions = latest_remove_actions(&table).await?;

        assert_eq!(remove_actions.len(), 1);
        let remove = &remove_actions[0];
        assert_eq!(remove.path, source_path);
        assert_eq!(remove.deletion_vector, source_deletion_vector);

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_where_rewrites_multiple_files_with_deletion_vectors() -> TestResult {
        let (_temp_dir, table) = open_copied_table_fixture(
            &crate::test_utils::TestTables::WithDvSmall.as_path(),
            "table-with-dv-small",
        )
        .await?;

        let source_files = table
            .get_active_add_actions_by_partitions(&[])
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(source_files.len(), 1);
        let dv_source = source_files
            .into_iter()
            .next()
            .expect("expected DV-backed source file");
        assert!(
            dv_source.deletion_vector_descriptor().is_some(),
            "expected DV-backed source file"
        );

        let append_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![Some(0), Some(9)]))],
        )?;

        let table = table
            .write(vec![append_batch])
            .with_save_mode(SaveMode::Append)
            .await?;

        let source_files = table
            .get_active_add_actions_by_partitions(&[])
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(source_files.len(), 2);
        let appended_source = source_files
            .iter()
            .find(|file| file.path() != dv_source.path())
            .expect("expected appended source file");
        assert!(
            appended_source.deletion_vector_descriptor().is_none(),
            "expected appended source without DV metadata"
        );

        let replacement_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![Some(50)]))],
        )?;

        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where("value >= 5")
            .await?;
        assert_eq!(table.version(), Some(3));

        assert_eq!(
            query_i32_rows(&table, "SELECT value FROM test ORDER BY value", "value").await?,
            vec![0, 1, 2, 3, 4, 50]
        );

        let remove_actions = latest_remove_actions(&table).await?;

        assert_eq!(remove_actions.len(), 2);
        assert!(
            remove_actions.iter().any(|remove| {
                remove.path == dv_source.path()
                    && remove.deletion_vector == dv_source.deletion_vector_descriptor()
            }),
            "expected tombstone for DV-backed source file"
        );
        assert!(
            remove_actions.iter().any(|remove| {
                remove.path == appended_source.path() && remove.deletion_vector.is_none()
            }),
            "expected tombstone for appended non-DV source file"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_where_real_world_deletion_logs_preserve_live_rows() -> TestResult {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../test/tests/data/table_with_deletion_logs");
        let (_temp_dir, table) =
            open_copied_table_fixture(&fixture_path, "table_with_deletion_logs").await?;

        let source_files = table
            .get_active_add_actions_by_partitions(&[])
            .try_collect::<Vec<_>>()
            .await?;
        let dv_sources = source_files
            .iter()
            .filter_map(|file| {
                file.deletion_vector_descriptor()
                    .map(|descriptor| (file.path().to_string(), descriptor))
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert!(
            !dv_sources.is_empty(),
            "expected at least one active DV-backed source file"
        );

        let initial_counts = query_single_i64_row(
            &table,
            "SELECT \
                SUM(CASE WHEN id < 100 THEN 1 ELSE 0 END) AS matching_rows, \
                SUM(CASE WHEN id >= 100 THEN 1 ELSE 0 END) AS preserved_rows \
             FROM test",
        )
        .await?;
        let matching_rows = initial_counts[0];
        let preserved_rows = initial_counts[1];
        assert!(matching_rows > 0, "expected fixture rows matching id < 100");
        assert!(preserved_rows > 0, "expected fixture rows with id >= 100");

        let replacement_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("address", DataType::Utf8, true),
                Field::new("age", DataType::Float64, true),
                Field::new("company", DataType::Utf8, true),
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("nbr", DataType::Int64, true),
                Field::new("phone_number", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("Replacement Ave")])),
                Arc::new(Float64Array::from(vec![Some(42.0)])),
                Arc::new(StringArray::from(vec![Some("delta-rs")])),
                Arc::new(Int64Array::from(vec![Some(42)])),
                Arc::new(StringArray::from(vec![Some("replacement")])),
                Arc::new(Int64Array::from(vec![Some(4242)])),
                Arc::new(StringArray::from(vec![Some("555-4242")])),
            ],
        )?;

        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where("id < 100")
            .await?;

        let final_counts = query_single_i64_row(
            &table,
            "SELECT \
                COUNT(*) AS total_rows, \
                SUM(CASE WHEN id < 100 THEN 1 ELSE 0 END) AS matching_rows, \
                SUM(CASE WHEN id >= 100 THEN 1 ELSE 0 END) AS preserved_rows, \
                SUM(CASE WHEN id = 42 AND name = 'replacement' THEN 1 ELSE 0 END) AS replacement_rows \
             FROM test",
        )
        .await?;

        assert_eq!(final_counts[0], preserved_rows + 1);
        assert_eq!(final_counts[1], 1);
        assert_eq!(final_counts[2], preserved_rows);
        assert_eq!(final_counts[3], 1);

        let remove_actions = latest_remove_actions(&table).await?;

        assert!(
            remove_actions.iter().any(|remove| {
                dv_sources
                    .get(&remove.path)
                    .is_some_and(|descriptor| remove.deletion_vector.as_ref() == Some(descriptor))
            }),
            "expected at least one DV-backed tombstone preserving its deletion vector"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_without_files_is_rejected() -> TestResult {
        let temp_dir = tempfile::tempdir()?;
        let table_path = temp_dir.path().join("without_files_overwrite");
        std::fs::create_dir(&table_path)?;
        let table_uri = ensure_table_uri(table_path.to_str().unwrap())?;

        DeltaTable::try_from_url(table_uri.clone())
            .await?
            .write(vec![get_record_batch(None, false)])
            .await?;

        let table = crate::DeltaTableBuilder::from_url(table_uri)?
            .without_files()
            .load()
            .await?;

        assert_eq!(table.version(), Some(0));

        // Phase 3 now routes overwrite planning through matched-file discovery, so this guard
        // stays covered here to ensure we still fail before any rewrite planning starts.
        let err = table
            .write(vec![get_record_batch(None, false)])
            .with_save_mode(SaveMode::Overwrite)
            .await
            .expect_err("overwrite should fail when table was loaded without files");

        assert!(matches!(
            err,
            DeltaTableError::NotInitializedWithFiles(operation) if operation == "WRITE"
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_where_partitioned() {
        let schema = get_arrow_schema(&None);

        let batch = get_record_batch(None, false);

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_partition_columns(["id", "value"])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_common_write_metrics(write_metrics);

        let batch_add = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![11, 13, 15])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2024-02-02",
                    "2024-02-02",
                    "2024-02-01",
                ])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch_add])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(col("id").eq(lit("A")))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert_common_write_metrics(write_metrics);

        let expected = [
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 11    | 2024-02-02 |",
            "| A  | 13    | 2024-02-02 |",
            "| A  | 15    | 2024-02-01 |",
            "| B  | 2     | 2021-02-02 |",
            "| B  | 4     | 2021-02-01 |",
            "| B  | 8     | 2021-02-01 |",
            "| B  | 9     | 2021-02-01 |",
            "+----+-------+------------+",
        ];
        let actual = get_data_sorted(&table, "id,value,modified").await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_dont_write_cdc_with_overwrite() -> TestResult {
        let delta_schema = TestSchemas::simple();
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(["id"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let schema: Arc<ArrowSchema> = Arc::new(delta_schema.try_into_arrow()?);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("1"), Some("2"), Some("3")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("yes"),
                    Some("yes"),
                    Some("no"),
                ])),
            ],
        )
        .unwrap();

        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("3")])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("yes")])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert_common_write_metrics(write_metrics);

        let table = table
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 1);
        assert!(write_metrics.num_removed_files > 0);
        assert_common_write_metrics(write_metrics);

        let snapshot_bytes = table
            .log_store
            .read_commit_entry(2)
            .await?
            .expect("failed to get snapshot bytes");
        let version_actions = get_actions(2, &snapshot_bytes)?;

        let cdc_actions = version_actions
            .iter()
            .filter(|action| matches!(action, &&Action::Cdc(_)))
            .collect_vec();
        assert!(cdc_actions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_dont_write_cdc_with_overwrite_predicate_partitioned() -> TestResult {
        let delta_schema = TestSchemas::simple();
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(["id"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let schema: Arc<ArrowSchema> = Arc::new(delta_schema.try_into_arrow()?);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("1"), Some("2"), Some("3")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("yes"),
                    Some("yes"),
                    Some("no"),
                ])),
            ],
        )
        .unwrap();

        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("3")])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("yes")])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert_common_write_metrics(write_metrics);

        let table = table
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("id='3'")
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));
        let write_metrics: WriteMetrics = get_write_metrics(&table).await;
        assert_eq!(write_metrics.num_added_rows, 1);
        assert!(write_metrics.num_removed_files > 0);
        assert_common_write_metrics(write_metrics);

        let snapshot_bytes = table
            .log_store
            .read_commit_entry(2)
            .await?
            .expect("failed to get snapshot bytes");
        let version_actions = get_actions(2, &snapshot_bytes)?;

        let cdc_actions = version_actions
            .iter()
            .filter(|action| matches!(action, &&Action::Cdc(_)))
            .collect_vec();
        assert!(cdc_actions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_dont_write_cdc_with_overwrite_predicate_unpartitioned() -> TestResult {
        let delta_schema = TestSchemas::simple();
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(["id"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let schema: Arc<ArrowSchema> = Arc::new(delta_schema.try_into_arrow()?);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("1"), Some("2"), Some("3")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("yes"),
                    Some("yes"),
                    Some("no"),
                ])),
            ],
        )
        .unwrap();

        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("3")])),
                Arc::new(Int32Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec![Some("yes")])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let table = table
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("value=3")
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));

        let ctx = SessionContext::new();
        let cdf_scan = table
            .clone()
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");

        let mut batches = collect(cdf_scan, ctx.state().task_ctx())
            .await
            .expect("Failed to collect batches");

        // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(5)).collect();

        assert_batches_sorted_eq! {[
            "+-------+----------+----+--------------+-----------------+",
            "| value | modified | id | _change_type | _commit_version |",
            "+-------+----------+----+--------------+-----------------+",
            "| 1     | yes      | 1  | insert       | 1               |",
            "| 2     | yes      | 2  | insert       | 1               |",
            "| 3     | no       | 3  | delete       | 2               |",
            "| 3     | no       | 3  | insert       | 1               |",
            "| 3     | yes      | 3  | insert       | 2               |",
            "+-------+----------+----+--------------+-----------------+",
        ], &batches }

        let snapshot_bytes = table
            .log_store
            .read_commit_entry(2)
            .await?
            .expect("failed to get snapshot bytes");
        let version_actions = get_actions(2, &snapshot_bytes)?;

        let cdc_actions = version_actions
            .iter()
            .filter(|action| matches!(action, &&Action::Cdc(_)))
            .collect_vec();
        assert!(!cdc_actions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_write_cdc_with_replace_where_preserves_mixed_case_columns() -> TestResult {
        let (schema, base_batch, replacement_batch) = mixed_case_replace_where_batches()?;
        let delta_schema: StructType = Arc::clone(&schema).try_into_kernel()?;
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await?;
        assert_eq!(table.version(), Some(0));

        let table = table.write(vec![base_batch]).await?;
        assert_eq!(table.version(), Some(1));

        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("score='0-1'")
            .await?;
        assert_eq!(table.version(), Some(2));

        let ctx = SessionContext::new();
        let cdf_scan = table
            .clone()
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");
        let mut batches = collect(cdf_scan, ctx.state().task_ctx())
            .await
            .expect("Failed to collect CDF batches");

        let commit_timestamp_index = batches
            .first()
            .expect("expected CDF batches")
            .schema()
            .index_of("_commit_timestamp")
            .expect("expected CDF commit timestamp column");
        let _: Vec<_> = batches
            .iter_mut()
            .map(|batch| batch.remove_column(commit_timestamp_index))
            .collect();

        assert_batches_sorted_eq! {[
            "+----------------------+----------+-------+--------------+-----------------+",
            "| utcDate              | homeTeam | score | _change_type | _commit_version |",
            "+----------------------+----------+-------+--------------+-----------------+",
            "| 2008-08-16T15:00:00Z | Everton  | 0-1   | delete       | 2               |",
            "| 2008-08-16T15:00:00Z | Everton  | 0-1   | insert       | 1               |",
            "| 2009-05-16T15:00:00Z | Everton  | 3-1   | insert       | 1               |",
            "| 2010-01-01T15:00:00Z | Everton  | 0-1   | insert       | 2               |",
            "+----------------------+----------+-------+--------------+-----------------+",
        ], &batches }

        Ok(())
    }

    #[tokio::test]
    async fn test_write_cdc_with_overwrite_predicate_partitioned_parallel_input() -> TestResult {
        let delta_schema = TestSchemas::simple();
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(["id"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let schema: Arc<ArrowSchema> = Arc::new(delta_schema.try_into_arrow()?);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("1"), Some("2"), Some("3")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("yes"),
                    Some("yes"),
                    Some("no"),
                ])),
            ],
        )
        .unwrap();

        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("3")])),
                Arc::new(Int32Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec![Some("updated")])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let multi_stream_input: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                second_batch.schema(),
                vec![
                    vec![second_batch.clone()],
                    vec![second_batch.clone()],
                    vec![second_batch.clone()],
                ],
            )
            .unwrap(),
        );
        let multi_stream_plan =
            LogicalPlanBuilder::scan("source", provider_as_source(multi_stream_input), None)?
                .build()?;

        let table = table
            .write(vec![])
            .with_input_plan(multi_stream_plan)
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("value=3")
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));

        let snapshot_bytes = table
            .log_store
            .read_commit_entry(2)
            .await?
            .expect("failed to get snapshot bytes");
        let version_actions = get_actions(2, &snapshot_bytes)?;

        let cdc_actions = version_actions
            .iter()
            .filter(|action| matches!(action, &&Action::Cdc(_)))
            .collect_vec();
        assert!(!cdc_actions.is_empty());

        let ctx = SessionContext::new();
        let cdf_scan = table
            .clone()
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");
        let mut batches = collect(cdf_scan, ctx.state().task_ctx())
            .await
            .expect("Failed to collect CDF batches");

        // _commit_timestamp is dynamic, drop it for stable assertions.
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(5)).collect();

        assert_batches_sorted_eq! {[
            "+-------+----------+----+--------------+-----------------+",
            "| value | modified | id | _change_type | _commit_version |",
            "+-------+----------+----+--------------+-----------------+",
            "| 1     | yes      | 1  | insert       | 1               |",
            "| 2     | yes      | 2  | insert       | 1               |",
            "| 3     | no       | 3  | delete       | 2               |",
            "| 3     | no       | 3  | insert       | 1               |",
            "| 3     | updated  | 3  | insert       | 2               |",
            "| 3     | updated  | 3  | insert       | 2               |",
            "| 3     | updated  | 3  | insert       | 2               |",
            "+-------+----------+----+--------------+-----------------+",
        ], &batches }

        let expected_table = [
            "+-------+----------+----+",
            "| value | modified | id |",
            "+-------+----------+----+",
            "| 1     | yes      | 1  |",
            "| 2     | yes      | 2  |",
            "| 3     | updated  | 3  |",
            "| 3     | updated  | 3  |",
            "| 3     | updated  | 3  |",
            "+-------+----------+----+",
        ];
        let actual_table = get_data_sorted(&table, "value, modified, id").await;
        assert_batches_sorted_eq!(&expected_table, &actual_table);
        Ok(())
    }

    /// SMall module to collect test cases which validate the [WriteBuilder]'s
    /// check_preconditions() function
    mod check_preconditions_test {
        use super::*;

        #[tokio::test]
        async fn test_schema_overwrite_on_append() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let batch = get_record_batch(None, false);
            let table = DeltaTable::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = table
                .write(vec![batch])
                .with_schema_mode(SchemaMode::Overwrite)
                .with_save_mode(SaveMode::Append);

            let check = writer.check_preconditions().await;
            assert!(check.is_err());
            Ok(())
        }

        #[tokio::test]
        async fn test_savemode_overwrite_on_append_table() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let batch = get_record_batch(None, false);
            let table = DeltaTable::new_in_memory()
                .create()
                .with_configuration_property(TableProperty::AppendOnly, Some("true".to_string()))
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = table.write(vec![batch]).with_save_mode(SaveMode::Overwrite);

            let check = writer.check_preconditions().await;
            assert!(check.is_err());
            Ok(())
        }

        #[tokio::test]
        async fn test_empty_set_of_batches() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let table = DeltaTable::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = table.write(vec![]);

            match writer.check_preconditions().await {
                Ok(_) => panic!("Expected check_preconditions to fail!"),
                Err(DeltaTableError::GenericError { .. }) => {}
                Err(e) => panic!("Unexpected error returned: {e:#?}"),
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_errorifexists() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let batch = get_record_batch(None, false);
            let table = DeltaTable::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = table
                .write(vec![batch])
                .with_save_mode(SaveMode::ErrorIfExists);

            match writer.check_preconditions().await {
                Ok(_) => panic!("Expected check_preconditions to fail!"),
                Err(DeltaTableError::GenericError { .. }) => {}
                Err(e) => panic!("Unexpected error returned: {e:#?}"),
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_allow_empty_batches_with_input_plan() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let table = DeltaTable::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;

            let ctx = SessionContext::new();
            let plan = ctx
                .sql("SELECT 1 as id")
                .await
                .unwrap()
                .logical_plan()
                .clone();
            let writer =
                WriteBuilder::new(table.log_store.clone(), table.state.map(|f| f.snapshot))
                    .with_input_plan(plan)
                    .with_save_mode(SaveMode::Overwrite);

            let _ = writer.check_preconditions().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_no_snapshot_create_actions() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let table = DeltaTable::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let batch = get_record_batch(None, false);
            let writer =
                WriteBuilder::new(table.log_store.clone(), None).with_input_batches(vec![batch]);

            let actions = writer.check_preconditions().await?;
            assert_eq!(
                actions.len(),
                2,
                "Expecting a Protocol and a Metadata action in {actions:?}"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_no_snapshot_err_no_batches_check() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let table = DeltaTable::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer =
                WriteBuilder::new(table.log_store.clone(), None).with_input_batches(vec![]);

            match writer.check_preconditions().await {
                Ok(_) => panic!("Expected check_preconditions to fail!"),
                Err(DeltaTableError::GenericError { .. }) => {}
                Err(e) => panic!("Unexpected error returned: {e:#?}"),
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_preserve_nullability_on_overwrite() -> TestResult {
        // Test that nullability constraints are preserved when overwriting with mode=overwrite, schema_mode=None
        use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        // Create initial table with non-nullable columns
        let initial_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false), // non-nullable
            Field::new("name", DataType::Utf8, true), // nullable
            Field::new("active", DataType::Boolean, false), // non-nullable
            Field::new("count", DataType::Int32, false), // non-nullable
        ]));

        let initial_batch = RecordBatch::try_new(
            initial_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )?;

        // Create initial table
        let table = DeltaTable::new_in_memory()
            .write(vec![initial_batch])
            .with_save_mode(SaveMode::Overwrite)
            .await?;

        // Verify initial schema has correct nullability
        let schema = table.snapshot().unwrap().schema();
        let schema_fields: Vec<_> = schema.fields().collect();
        assert!(!schema_fields[0].is_nullable(), "id should be non-nullable");
        assert!(schema_fields[1].is_nullable(), "name should be nullable");
        assert!(
            !schema_fields[2].is_nullable(),
            "active should be non-nullable"
        );
        assert!(
            !schema_fields[3].is_nullable(),
            "count should be non-nullable"
        );

        // Create new data with all nullable fields (simulating data from sources like Pandas)
        let new_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true), // nullable in new data
            Field::new("name", DataType::Utf8, true), // nullable in new data
            Field::new("active", DataType::Boolean, true), // nullable in new data
            Field::new("count", DataType::Int32, true), // nullable in new data
        ]));

        let new_batch = RecordBatch::try_new(
            new_schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(4), Some(5), Some(6)])),
                Arc::new(StringArray::from(vec![
                    Some("David"),
                    Some("Eve"),
                    Some("Frank"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    Some(true),
                    Some(false),
                ])),
                Arc::new(Int32Array::from(vec![Some(40), Some(50), Some(60)])),
            ],
        )?;

        // Overwrite with schema_mode=None (default) - should preserve nullability
        let table = table
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Overwrite)
            // schema_mode is None by default
            .await?;

        // Verify that nullability constraints are preserved
        let schema = table.snapshot().unwrap().schema();
        let final_fields: Vec<_> = schema.fields().collect();
        assert!(
            !final_fields[0].is_nullable(),
            "id should remain non-nullable after overwrite"
        );
        assert!(
            final_fields[1].is_nullable(),
            "name should remain nullable after overwrite"
        );
        assert!(
            !final_fields[2].is_nullable(),
            "active should remain non-nullable after overwrite"
        );
        assert!(
            !final_fields[3].is_nullable(),
            "count should remain non-nullable after overwrite"
        );

        // Verify the data was actually overwritten by checking version increased
        assert_eq!(table.version(), Some(1)); // Version should be 1 after overwrite

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_mode_none_enforces_constraints_on_overwrite() -> TestResult {
        // Test that schema_mode=None with mode=overwrite:
        // 1. Does NOT update/overwrite the schema
        // 2. ENFORCES existing constraints (e.g., non-nullable fields)
        use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        // Create initial table with strict constraints
        let initial_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false), // NON-NULLABLE
            Field::new("name", DataType::Utf8, true), // nullable
            Field::new("active", DataType::Boolean, false), // NON-NULLABLE
            Field::new("count", DataType::Int32, false), // NON-NULLABLE
        ]));

        let initial_batch = RecordBatch::try_new(
            initial_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )?;

        // Create initial table
        let table = DeltaTable::new_in_memory()
            .write(vec![initial_batch])
            .with_save_mode(SaveMode::Overwrite)
            .await?;

        // Capture initial schema for comparison
        let initial_schema_fields: Vec<_> = table
            .snapshot()
            .unwrap()
            .schema()
            .fields()
            .cloned()
            .collect();

        // Test 1: Verify schema is NOT changed even when incoming data has different nullability
        let new_schema_all_nullable = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true), // nullable in new data
            Field::new("name", DataType::Utf8, true), // nullable in new data
            Field::new("active", DataType::Boolean, true), // nullable in new data
            Field::new("count", DataType::Int32, true), // nullable in new data
        ]));

        let valid_batch = RecordBatch::try_new(
            new_schema_all_nullable.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(4), Some(5), Some(6)])),
                Arc::new(StringArray::from(vec![
                    Some("David"),
                    Some("Eve"),
                    Some("Frank"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    Some(true),
                    Some(false),
                ])),
                Arc::new(Int32Array::from(vec![Some(40), Some(50), Some(60)])),
            ],
        )?;

        // This should succeed - data is valid even though schema differs
        let table = table
            .write(vec![valid_batch])
            .with_save_mode(SaveMode::Overwrite)
            // schema_mode is None by default
            .await?;

        // Verify schema was NOT updated
        let schema = table.snapshot().unwrap().schema();
        let after_overwrite_fields: Vec<_> = schema.fields().collect();

        // Schema should be EXACTLY the same as before
        assert_eq!(
            after_overwrite_fields.len(),
            initial_schema_fields.len(),
            "Schema should have same number of fields"
        );

        for (i, field) in after_overwrite_fields.iter().enumerate() {
            assert_eq!(
                field.is_nullable(),
                initial_schema_fields[i].is_nullable(),
                "Field '{}' nullability should not change",
                field.name()
            );
        }

        // Specifically verify non-nullable fields are still non-nullable
        assert!(
            !after_overwrite_fields[0].is_nullable(),
            "id must remain non-nullable"
        );
        assert!(
            !after_overwrite_fields[2].is_nullable(),
            "active must remain non-nullable"
        );
        assert!(
            !after_overwrite_fields[3].is_nullable(),
            "count must remain non-nullable"
        );

        // Test 2: Verify constraints are ENFORCED - attempt to write NULL to non-nullable field
        // This should FAIL because we're trying to violate the non-nullable constraint

        // Create data with NULL in a non-nullable field
        let invalid_batch = RecordBatch::try_new(
            new_schema_all_nullable.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])), // NULL in non-nullable id!
                Arc::new(StringArray::from(vec![
                    Some("George"),
                    Some("Helen"),
                    Some("Ivan"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                ])),
                Arc::new(Int32Array::from(vec![Some(70), Some(80), Some(90)])),
            ],
        )?;

        // This should fail because id is non-nullable in the table schema
        let result = table
            .clone()
            .write(vec![invalid_batch])
            .with_save_mode(SaveMode::Overwrite)
            .await;

        // The write should fail due to constraint violation
        assert!(
            result.is_err(),
            "Writing NULL to non-nullable field should fail"
        );

        // Test 3: Also test with NULL in another non-nullable field (active)
        let invalid_batch_2 = RecordBatch::try_new(
            new_schema_all_nullable.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])),
                Arc::new(StringArray::from(vec![
                    Some("Jane"),
                    Some("Karl"),
                    Some("Lisa"),
                ])),
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])), // NULL in non-nullable active!
                Arc::new(Int32Array::from(vec![Some(100), Some(110), Some(120)])),
            ],
        )?;

        let result2 = table
            .clone()
            .write(vec![invalid_batch_2])
            .with_save_mode(SaveMode::Overwrite)
            .await;

        // This should also fail
        assert!(
            result2.is_err(),
            "Writing NULL to non-nullable 'active' field should fail"
        );

        // Verify the table data and schema remain unchanged after failed writes
        let schema = table.snapshot().unwrap().schema();
        let final_fields: Vec<_> = schema.fields().collect();

        // Schema should still be the original schema
        assert!(
            !final_fields[0].is_nullable(),
            "id still non-nullable after failed writes"
        );
        assert!(
            !final_fields[2].is_nullable(),
            "active still non-nullable after failed writes"
        );
        assert!(
            !final_fields[3].is_nullable(),
            "count still non-nullable after failed writes"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_preserved_with_replace_where() -> TestResult {
        // Test that schema is preserved when using overwrite with predicate (replaceWhere)
        use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        // Create initial table with mixed nullability
        let initial_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false), // non-nullable
            Field::new("name", DataType::Utf8, true), // nullable
            Field::new("active", DataType::Boolean, false), // non-nullable
            Field::new("count", DataType::Int32, false), // non-nullable
        ]));

        let initial_batch = RecordBatch::try_new(
            initial_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("Alice"),
                    Some("Bob"),
                    None,
                    Some("David"),
                    Some("Eve"),
                ])),
                Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )?;

        let table = DeltaTable::new_in_memory()
            .write(vec![initial_batch])
            .with_save_mode(SaveMode::Overwrite)
            .await?;

        // Capture initial schema
        let initial_fields: Vec<_> = table
            .snapshot()
            .unwrap()
            .schema()
            .fields()
            .cloned()
            .collect();

        // Create new data with all nullable fields (typical from Pandas)
        let new_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true), // nullable in new data
            Field::new("name", DataType::Utf8, true), // nullable
            Field::new("active", DataType::Boolean, true), // nullable
            Field::new("count", DataType::Int32, true), // nullable
        ]));

        let replacement_batch = RecordBatch::try_new(
            new_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(2), Some(4)])), // Replace ids 2 and 4
                Arc::new(StringArray::from(vec![Some("Bob2"), Some("David2")])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(true)])),
                Arc::new(Int32Array::from(vec![Some(200), Some(400)])),
            ],
        )?;

        // Use replaceWhere to selectively overwrite
        let table = table
            .write(vec![replacement_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where("id = 2 OR id = 4")
            .await?;

        // Verify schema is preserved
        let schema = table.snapshot().unwrap().schema();
        let final_fields: Vec<_> = schema.fields().collect();

        for (i, field) in final_fields.iter().enumerate() {
            assert_eq!(
                field.is_nullable(),
                initial_fields[i].is_nullable(),
                "Field '{}' nullability should be preserved with replaceWhere",
                field.name()
            );
        }

        // Now test that constraints are still enforced with replaceWhere
        let invalid_batch = RecordBatch::try_new(
            new_schema,
            vec![
                Arc::new(Int64Array::from(vec![None, Some(3)])), // NULL in non-nullable id!
                Arc::new(StringArray::from(vec![Some("Invalid"), Some("Valid")])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(false)])),
                Arc::new(Int32Array::from(vec![Some(999), Some(333)])),
            ],
        )?;

        let result = table
            .write(vec![invalid_batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where("id = 1 OR id = 3")
            .await;

        assert!(
            result.is_err(),
            "replaceWhere should still enforce non-nullable constraints"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_date64_normalizes_to_date32() {
        use arrow_array::Date64Array;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("sales_date", DataType::Date64, true),
        ]));
        let millis = 1760918400000i64; // 2025 10 20 in ms since epoch
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Date64Array::from(vec![millis])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap();

        let table_schema = table.snapshot().unwrap().schema();
        let date_field = table_schema.field("sales_date").unwrap();
        assert_eq!(date_field.data_type(), &crate::kernel::DataType::DATE);

        let batches = get_data(&table).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
        assert_eq!(
            batches[0]
                .schema()
                .field_with_name("sales_date")
                .unwrap()
                .data_type(),
            &DataType::Date32,
        );
    }

    #[cfg(not(feature = "nanosecond-timestamps"))]
    #[tokio::test]
    async fn test_write_timestamp_ns_normalizes_to_us() {
        test_write_timestamp_ns_maybe_normalization(TimeUnit::Microsecond).await;
    }

    #[cfg(feature = "nanosecond-timestamps")]
    #[tokio::test]
    async fn test_write_timestamp_ns_stays_ns() {
        test_write_timestamp_ns_maybe_normalization(TimeUnit::Nanosecond).await;
    }

    async fn test_write_timestamp_ns_maybe_normalization(unit: TimeUnit) {
        use arrow_array::TimestampNanosecondArray;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]));
        let nanos = 1_760_961_600_123_456_789_i64;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(TimestampNanosecondArray::from(vec![nanos]).with_timezone("UTC")),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap();

        let batches = get_data(&table).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let schema = batches[0].schema();
        let result_field = schema.field_with_name("ts").unwrap();
        assert_eq!(
            result_field.data_type(),
            &DataType::Timestamp(unit, Some("UTC".into())),
        );
    }

    #[tokio::test]
    async fn test_write_large_utf8_normalizes_to_utf8() {
        use arrow_array::LargeStringArray;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::LargeUtf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(LargeStringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap();

        let table_schema = table.snapshot().unwrap().schema();
        assert_eq!(
            table_schema.field("name").unwrap().data_type(),
            &crate::kernel::DataType::STRING,
        );

        let batches = get_data(&table).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }

    #[tokio::test]
    async fn test_append_date64_to_existing_date32_table() {
        use arrow_array::{Date32Array, Date64Array};

        let schema32 = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("d", DataType::Date32, true),
        ]));
        let batch32 = RecordBatch::try_new(
            schema32,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Date32Array::from(vec![19650])),
            ],
        )
        .unwrap();

        let table = DeltaTable::new_in_memory()
            .write(vec![batch32])
            .await
            .unwrap();

        let schema64 = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("d", DataType::Date64, true),
        ]));
        let batch64 = RecordBatch::try_new(
            schema64,
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(Date64Array::from(vec![1760918400000i64])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch64])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let batches = get_data(&table).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
