//!
//! New Table Semantics
//!  - The schema of the [Plan] is used to initialize the table.
//!  - The partition columns will be used to partition the table.
//!
//! Existing Table Semantics
//!  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
//!  - Conflicting columns (i.e. a INT, and a STRING)
//!    will result in an exception.
//!  - The partition columns, if present, are validated against the existing metadata. If not
//!    present, then the partitioning of the table is respected.
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
//! let ops = DeltaOps::try_from_uri("../path/to/empty/dir").await?;
//! let table = ops.write(vec![batch]).await?;
//! ````

pub(crate) mod async_utils;
pub mod configs;
pub(crate) mod execution;
pub(crate) mod generated_columns;
pub(crate) mod metrics;
pub(crate) mod schema_evolution;
pub mod writer;

use arrow_schema::Schema;
pub use configs::WriterStatsConfig;
use datafusion::execution::SessionStateBuilder;
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use generated_columns::{able_to_gc, add_generated_columns, add_missing_generated_columns};
use metrics::{WriteMetricExtensionPlanner, SOURCE_COUNT_ID, SOURCE_COUNT_METRIC};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::vec;

use arrow_array::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::{Column, DFSchema, Result, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::logical_expr::{cast, lit, try_cast, Expr, Extension, LogicalPlan};
use datafusion::prelude::DataFrame;
use execution::{prepare_predicate_actions, write_execution_plan_v2};
use futures::future::BoxFuture;
use parquet::file::properties::WriterProperties;
use schema_evolution::try_cast_schema;
use serde::{Deserialize, Serialize};
use tracing::log::*;

use super::cdc::CDC_COLUMN_NAME;
use super::datafusion_utils::Expression;
use super::{CreateBuilder, CustomExecuteHandler, Operation};
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::logical::MetricObserver;
use crate::delta_datafusion::physical::{find_metric_node, get_metric};
use crate::delta_datafusion::planner::DeltaPlanner;
use crate::delta_datafusion::register_store;
use crate::delta_datafusion::DataFusionMixins;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::schema::cast::merge_arrow_schema;
use crate::kernel::transaction::{CommitBuilder, CommitProperties, TableReference, PROTOCOL};
use crate::kernel::{
    new_metadata, Action, ActionType, MetadataExt as _, ProtocolExt as _, StructType, StructTypeExt,
};
use crate::logstore::LogStoreRef;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::table::state::DeltaTableState;
use crate::DeltaTable;

#[derive(thiserror::Error, Debug)]
pub(crate) enum WriteError {
    #[error("No data source supplied to write command.")]
    MissingData,

    #[error("Failed to execute write task: {source}")]
    WriteTask { source: tokio::task::JoinError },

    #[error("A table already exists at: {0}")]
    AlreadyExists(String),

    #[error(
        "Specified table partitioning does not match table partitioning: expected: {expected:?}, got: {got:?}",
    )]
    PartitionColumnMismatch {
        expected: Vec<String>,
        got: Vec<String>,
    },
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
            _ => Err(DeltaTableError::Generic(format!("Invalid schema write mode provided: {s}, only these are supported: ['overwrite', 'merge']"))),
        }
    }
}

/// Write data into a DeltaTable
pub struct WriteBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: Option<DeltaTableState>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// The input plan
    input: Option<Arc<LogicalPlan>>,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// SaveMode defines how to treat data already written to table location
    mode: SaveMode,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    /// When using `Overwrite` mode, replace data that matches a predicate
    predicate: Option<Expression>,
    /// Size above which we will write a buffered parquet file to disk.
    target_file_size: Option<usize>,
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

impl super::Operation<()> for WriteBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl WriteBuilder {
    /// Create a new [`WriteBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: Option<DeltaTableState>) -> Self {
        Self {
            snapshot,
            log_store,
            input: None,
            state: None,
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

    /// (Optional) Specify table partitioning. If specified, the partitioning is validated,
    /// if the table already exists. In case a new table is created, the partitioning is applied.
    pub fn with_partition_columns(
        mut self,
        partition_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_columns = Some(partition_columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Logical execution plan that produces the data to be written to the delta table
    pub fn with_input_execution_plan(mut self, plan: Arc<LogicalPlan>) -> Self {
        self.input = Some(plan);
        self
    }

    /// A session state accompanying a given input plan, containing e.g. registered object stores
    pub fn with_input_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }

    /// Specify the target file size for data files written to the delta table.
    pub fn with_target_file_size(mut self, target_file_size: usize) -> Self {
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
        let ctx = SessionContext::new();
        let batches: Vec<RecordBatch> = batches.into_iter().collect();
        if !batches.is_empty() {
            let table_provider: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(batches[0].schema(), vec![batches]).unwrap());
            let df = ctx.read_table(table_provider).unwrap();
            self.input = Some(Arc::new(df.logical_plan().clone()));
        }
        self
    }

    fn get_partition_columns(&self) -> Result<Vec<String>, WriteError> {
        // validate partition columns
        let active_partitions = self
            .snapshot
            .as_ref()
            .map(|s| s.metadata().partition_columns().clone());

        if let Some(active_part) = active_partitions {
            if let Some(ref partition_columns) = self.partition_columns {
                if &active_part != partition_columns {
                    Err(WriteError::PartitionColumnMismatch {
                        expected: active_part,
                        got: partition_columns.to_vec(),
                    })
                } else {
                    Ok(partition_columns.clone())
                }
            } else {
                Ok(active_part)
            }
        } else {
            Ok(self.partition_columns.clone().unwrap_or_default())
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
            .clone()
            .ok_or::<DeltaTableError>(WriteError::MissingData.into())?;
        let schema: StructType = input.schema().as_arrow().try_into_kernel()?;

        match &self.snapshot {
            Some(snapshot) => {
                if self.mode == SaveMode::Overwrite {
                    PROTOCOL.check_append_only(&snapshot.snapshot)?;
                    if !snapshot.load_config().require_files {
                        return Err(DeltaTableError::NotInitializedWithFiles("WRITE".into()));
                    }
                }

                PROTOCOL.can_write_to(snapshot)?;

                if self.schema_mode.is_none() {
                    PROTOCOL.check_can_write_timestamp_ntz(snapshot, &schema)?;
                }
                match self.mode {
                    SaveMode::ErrorIfExists => {
                        Err(WriteError::AlreadyExists(self.log_store.root_uri()).into())
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
        let this = self;

        Box::pin(async move {
            // Runs pre execution handler.
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let mut metrics = WriteMetrics::default();
            let exec_start = Instant::now();

            let write_planner = DeltaPlanner::<WriteMetricExtensionPlanner> {
                extension_planner: WriteMetricExtensionPlanner {},
            };

            // Create table actions to initialize table in case it does not yet exist
            // and should be created
            let mut actions = this.check_preconditions().await?;

            let partition_columns = this.get_partition_columns()?;

            let state = match this.state {
                Some(state) => SessionStateBuilder::new_from_existing(state.clone())
                    .with_query_planner(Arc::new(write_planner))
                    .build(),
                None => {
                    let state = SessionStateBuilder::new()
                        .with_default_features()
                        .with_query_planner(Arc::new(write_planner))
                        .build();
                    register_store(this.log_store.clone(), state.runtime_env().clone());
                    state
                }
            };
            let mut schema_drift = false;
            let mut generated_col_exp = None;
            let mut missing_gen_col = None;
            let mut source = DataFrame::new(state.clone(), this.input.unwrap().as_ref().clone());
            if let Some(snapshot) = &this.snapshot {
                if able_to_gc(snapshot)? {
                    let generated_col_expressions = snapshot.schema().get_generated_columns()?;
                    // Add missing generated columns to source_df
                    let (source_with_gc, missing_generated_columns) =
                        add_missing_generated_columns(source, &generated_col_expressions)?;
                    source = source_with_gc;
                    missing_gen_col = Some(missing_generated_columns);
                    generated_col_exp = Some(generated_col_expressions);
                }
            }

            let source_schema: Arc<Schema> = Arc::new(source.schema().as_arrow().clone());

            // Schema merging code should be aware of columns that can be generated during write
            // so they might be empty in the batch, but the will exist in the input_schema()
            // in this case we have to insert the generated column and it's type in the schema of the batch
            let mut new_schema = None;
            if let Some(snapshot) = &this.snapshot {
                let table_schema = snapshot.input_schema()?;

                if let Err(schema_err) =
                    try_cast_schema(source_schema.fields(), table_schema.fields())
                {
                    schema_drift = true;
                    if this.mode == SaveMode::Overwrite
                        && this.schema_mode == Some(SchemaMode::Overwrite)
                    {
                        new_schema = None // we overwrite anyway, so no need to cast
                    } else if this.schema_mode == Some(SchemaMode::Merge) {
                        new_schema = Some(merge_arrow_schema(
                            table_schema.clone(),
                            source_schema.clone(),
                            schema_drift,
                        )?);
                    } else {
                        return Err(schema_err.into());
                    }
                } else if this.mode == SaveMode::Overwrite
                    && this.schema_mode == Some(SchemaMode::Overwrite)
                {
                    new_schema = None // we overwrite anyway, so no need to cast
                } else {
                    // Schema needs to be merged so that utf8/binary/list types are preserved from the batch side if both table
                    // and batch contains such type. Other types are preserved from the table side.
                    // At this stage it will never introduce more fields since try_cast_batch passed correctly.
                    new_schema = Some(merge_arrow_schema(
                        table_schema.clone(),
                        source_schema.clone(),
                        schema_drift,
                    )?);
                }
            }
            if let Some(new_schema) = new_schema {
                let mut schema_evolution_projection = Vec::new();
                for field in new_schema.fields() {
                    // If field exist in source data, we cast to new datatype
                    if source_schema.index_of(field.name()).is_ok() {
                        let cast_fn = if this.safe_cast { try_cast } else { cast };
                        let cast_expr = cast_fn(
                            Expr::Column(Column::from_name(field.name())),
                            // col(field.name()),
                            field.data_type().clone(),
                        )
                        .alias(field.name());
                        schema_evolution_projection.push(cast_expr)
                    // If field doesn't exist in source data, we insert the column
                    // with null values
                    } else {
                        schema_evolution_projection.push(
                            cast(
                                lit(ScalarValue::Null).alias(field.name()),
                                field.data_type().clone(),
                            )
                            .alias(field.name()),
                        );
                    }
                }
                source = source.select(schema_evolution_projection)?;
            }

            if let Some(generated_columns_exp) = generated_col_exp {
                if let Some(missing_generated_col) = missing_gen_col {
                    source = add_generated_columns(
                        source,
                        &generated_columns_exp,
                        &missing_generated_col,
                        &state,
                    )?;
                }
            }

            let source = LogicalPlan::Extension(Extension {
                node: Arc::new(MetricObserver {
                    id: "write_source_count".into(),
                    input: source.logical_plan().clone(),
                    enable_pushdown: false,
                }),
            });

            let mut source = DataFrame::new(state.clone(), source);

            let schema = Arc::new(source.schema().as_arrow().clone());

            // Maybe create schema action
            if this.schema_mode == Some(SchemaMode::Merge) && schema_drift {
                if let Some(snapshot) = &this.snapshot {
                    let schema_struct: StructType = schema.clone().try_into_kernel()?;
                    // Verify if delta schema changed
                    if &schema_struct != snapshot.schema() {
                        let current_protocol = snapshot.protocol();
                        let configuration = snapshot.metadata().configuration().clone();
                        let new_protocol = current_protocol
                            .clone()
                            .apply_column_metadata_to_protocol(&schema_struct)?
                            .move_table_properties_into_features(&configuration);

                        let mut metadata =
                            new_metadata(&schema_struct, &partition_columns, configuration)?;
                        let existing_metadata_id = snapshot.metadata().id().to_string();

                        if !existing_metadata_id.is_empty() {
                            metadata = metadata.with_table_id(existing_metadata_id)?;
                        }
                        let schema_action = Action::Metadata(metadata);
                        actions.push(schema_action);
                        if current_protocol != &new_protocol {
                            actions.push(new_protocol.into())
                        }
                    }
                }
            }

            let (predicate_str, predicate) = match this.predicate {
                Some(predicate) => {
                    let pred = match predicate {
                        Expression::DataFusion(expr) => expr,
                        Expression::String(s) => {
                            let df_schema = DFSchema::try_from(schema.as_ref().to_owned())?;
                            parse_predicate_expression(&df_schema, s, &state)?
                        }
                    };
                    (Some(fmt_expr_to_sql(&pred)?), Some(pred))
                }
                _ => (None, None),
            };

            let config: Option<crate::table::config::TableConfig<'_>> = this
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.table_config());

            let target_file_size = this.target_file_size.or_else(|| {
                Some(super::get_target_file_size(&config, &this.configuration) as usize)
            });
            let (num_indexed_cols, stats_columns) =
                super::get_num_idx_cols_and_stats_columns(config, this.configuration);

            let writer_stats_config = WriterStatsConfig {
                num_indexed_cols,
                stats_columns,
            };

            let mut contains_cdc = false;

            // Collect remove actions if we are overwriting the table
            if let Some(snapshot) = &this.snapshot {
                if matches!(this.mode, SaveMode::Overwrite) {
                    let delta_schema: StructType = schema.as_ref().try_into_kernel()?;
                    // Update metadata with new schema if there is a change
                    if &delta_schema != snapshot.schema() {
                        let mut metadata = snapshot.metadata().clone();

                        metadata = metadata.with_schema(&delta_schema)?;
                        actions.push(Action::Metadata(metadata));

                        let configuration = snapshot.metadata().configuration().clone();
                        let current_protocol = snapshot.protocol();
                        let new_protocol = current_protocol
                            .clone()
                            .apply_column_metadata_to_protocol(&delta_schema)?
                            .move_table_properties_into_features(&configuration);

                        if current_protocol != &new_protocol {
                            actions.push(new_protocol.into())
                        }
                    }

                    let deletion_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    match &predicate {
                        Some(pred) => {
                            let (predicate_actions, cdf_df) = prepare_predicate_actions(
                                pred.clone(),
                                this.log_store.clone(),
                                snapshot,
                                state.clone(),
                                partition_columns.clone(),
                                this.writer_properties.clone(),
                                deletion_timestamp,
                                writer_stats_config.clone(),
                                operation_id,
                            )
                            .await?;

                            if let Some(cdf_df) = cdf_df {
                                contains_cdc = true;
                                source = source
                                    .with_column(CDC_COLUMN_NAME, lit("insert"))?
                                    .union(cdf_df)?;
                            }

                            if !predicate_actions.is_empty() {
                                actions.extend(predicate_actions);
                            }
                        }
                        _ => {
                            let remove_actions = snapshot
                                .log_data()
                                .into_iter()
                                .map(|p| p.remove_action(true).into());
                            actions.extend(remove_actions);
                        }
                    };
                }
                metrics.num_removed_files = actions
                    .iter()
                    .filter(|a| a.action_type() == ActionType::Remove)
                    .count();
            }

            let source_plan = source.clone().create_physical_plan().await?;

            // Here we need to validate if the new data conforms to a predicate if one is provided
            let add_actions = write_execution_plan_v2(
                this.snapshot.as_ref(),
                state.clone(),
                source_plan.clone(),
                partition_columns.clone(),
                this.log_store.object_store(Some(operation_id)).clone(),
                target_file_size,
                this.write_batch_size,
                this.writer_properties,
                writer_stats_config.clone(),
                predicate.clone(),
                contains_cdc,
            )
            .await?;

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
                predicate: predicate_str,
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logstore::get_actions;
    use crate::operations::load_cdf::collect_batches;
    use crate::operations::{collect_sendable_stream, DeltaOps};
    use crate::protocol::SaveMode;
    use crate::test_utils::{TestResult, TestSchemas};
    use crate::writer::test_utils::datafusion::{get_data, get_data_sorted, write_batch};
    use crate::writer::test_utils::{
        get_arrow_schema, get_delta_schema, get_delta_schema_with_nested_struct, get_record_batch,
        get_record_batch_with_nested_struct, setup_table_with_configuration,
    };
    use crate::TableProperty;
    use arrow_array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
    use datafusion::prelude::*;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use itertools::Itertools;
    use serde_json::{json, Value};

    async fn get_write_metrics(table: DeltaTable) -> WriteMetrics {
        let mut commit_info = table.history(Some(1)).await.unwrap();
        let metrics = commit_info
            .first_mut()
            .unwrap()
            .info
            .remove("operationMetrics")
            .unwrap();
        serde_json::from_value(metrics).unwrap()
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
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert_eq!(write_metrics.num_removed_files, 0);
        assert_common_write_metrics(write_metrics);

        // Overwrite
        let _err = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .await
            .expect_err("Remove action is included when Delta table is append-only. Should error");
    }

    #[tokio::test]
    async fn test_create_write() {
        let table_schema = get_delta_schema();
        let batch = get_record_batch(None, false);

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.history(None).await.unwrap().len(), 1);

        // write some data
        let metadata = HashMap::from_iter(vec![("k1".to_string(), json!("v1.1"))]);
        let mut table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(CommitProperties::default().with_metadata(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        assert_eq!(table.get_files_count(), 1);

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert_eq!(write_metrics.num_added_files, table.get_files_count());
        assert_common_write_metrics(write_metrics);

        table.load().await.unwrap();
        assert_eq!(table.history(None).await.unwrap().len(), 2);
        assert_eq!(
            table.history(None).await.unwrap()[0]
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
        let mut table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(CommitProperties::default().with_metadata(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));
        assert_eq!(table.get_files_count(), 2);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert_eq!(write_metrics.num_added_files, 1);
        assert_common_write_metrics(write_metrics);

        table.load().await.unwrap();
        assert_eq!(table.history(None).await.unwrap().len(), 3);
        assert_eq!(
            table.history(None).await.unwrap()[0]
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
        let mut table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Overwrite)
            .with_commit_properties(CommitProperties::default().with_metadata(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(3));
        assert_eq!(table.get_files_count(), 1);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, batch.num_rows());
        assert!(write_metrics.num_removed_files > 0);
        assert_common_write_metrics(write_metrics);

        table.load().await.unwrap();
        assert_eq!(table.history(None).await.unwrap().len(), 4);
        assert_eq!(
            table.history(None).await.unwrap()[0]
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
        let table = DeltaOps::new_in_memory().write(vec![batch]).await.unwrap();
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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
        let table = DeltaOps::from(table)
            .write(vec![batch.clone()])
            .with_cast_safety(true)
            .await
            .unwrap();

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let res = DeltaOps::from(table).write(vec![batch]).await;
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
        let table = DeltaOps::new_in_memory().write(vec![batch]).await.unwrap();

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let _res = DeltaOps::from(table).write(vec![batch]).await.unwrap();
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
        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.get_files_count(), 1);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_write_partitioned() {
        let batch = get_record_batch(None, false);
        let table = DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified"])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.get_files_count(), 2);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_files, 2);
        assert_common_write_metrics(write_metrics);

        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.get_files_count(), 4);

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_files, 4);
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_merge_schema() {
        let batch = get_record_batch(None, false);
        let table = DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let mut table = DeltaOps(table)
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Merge)
            .await
            .unwrap();
        table.load().await.unwrap();
        assert_eq!(table.version(), Some(1));
        let new_schema = table.metadata().unwrap().parse_schema().unwrap();
        let fields = new_schema.fields();
        let names = fields.map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(names, vec!["id", "value", "modified", "inserted_by"]);

        // <https://github.com/delta-io/delta-rs/issues/2925>
        let metadata = table
            .metadata()
            .expect("Failed to retrieve updated metadata");
        assert_ne!(
            None,
            metadata.created_time(),
            "Created time should be the milliseconds since epoch of when the action was created"
        );

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_merge_schema_with_partitions() {
        let batch = get_record_batch(None, false);
        let table = DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_partition_columns(vec!["id", "value"])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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
        let table = DeltaOps(table)
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Merge)
            .await
            .unwrap();

        assert_eq!(table.version(), Some(1));
        let new_schema = table.metadata().unwrap().parse_schema().unwrap();
        let fields = new_schema.fields();
        let mut names = fields.map(|f| f.name()).collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, vec!["id", "inserted_by", "modified", "value"]);
        let part_cols = table.metadata().unwrap().partition_columns().clone();
        assert_eq!(part_cols, vec!["id", "value"]); // we want to preserve partitions

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_common_write_metrics(write_metrics);
    }

    #[tokio::test]
    async fn test_overwrite_schema() {
        let batch = get_record_batch(None, false);
        let table = DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let table = DeltaOps(table)
            .write(vec![new_batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Overwrite)
            .await;
        assert!(table.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_check() {
        // If you do not pass a schema mode, we want to check the schema
        let batch = get_record_batch(None, false);
        let table = DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let table = DeltaOps(table)
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
        let table = DeltaOps::new_in_memory()
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let table = DeltaOps(table).write(vec![batch.clone()]).await.unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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
        let table = DeltaOps::new_in_memory()
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let table = DeltaOps(table).write(vec![batch.clone()]).await;
        assert!(table.is_err());
    }

    #[tokio::test]
    async fn test_nested_struct() {
        let table_schema = get_delta_schema_with_nested_struct();
        let batch = get_record_batch_with_nested_struct();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let ops = DeltaOps::try_from_uri(tmp_path.as_os_str().to_str().unwrap())
            .await
            .unwrap();

        let _table = ops
            .write([batch.clone()])
            .with_partition_columns(["string"])
            .await
            .unwrap();
        let write_metrics: WriteMetrics = get_write_metrics(_table.clone()).await;
        assert_common_write_metrics(write_metrics);

        let table = crate::open_table(tmp_path.as_os_str().to_str().unwrap())
            .await
            .unwrap();
        let (_table, stream) = DeltaOps(table).load().await.unwrap();
        let data: Vec<RecordBatch> = collect_sendable_stream(stream).await.unwrap();

        let expected = vec![
            "+------+----------------------------------+",
            "| data | string                           |",
            "+------+----------------------------------+",
            "| test | $%&/()=^\"[]#*?._- {=}|`<>~/\\r\\n+ |",
            "+------+----------------------------------+",
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

        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let table = DeltaOps(table)
            .write(vec![batch_add])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(col("id").eq(lit("C")))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let table = DeltaOps(table)
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
    async fn test_replace_where_partitioned() {
        let schema = get_arrow_schema(&None);

        let batch = get_record_batch(None, false);

        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_partition_columns(["id", "value"])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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

        let table = DeltaOps(table)
            .write(vec![batch_add])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(col("id").eq(lit("A")))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
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
        let table: DeltaTable = DeltaOps::new_in_memory()
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

        let table = DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert_common_write_metrics(write_metrics);

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, 1);
        assert!(write_metrics.num_removed_files > 0);
        assert_common_write_metrics(write_metrics);

        let snapshot_bytes = table
            .log_store
            .read_commit_entry(2)
            .await?
            .expect("failed to get snapshot bytes");
        let version_actions = get_actions(2, snapshot_bytes).await?;

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
        let table: DeltaTable = DeltaOps::new_in_memory()
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

        let table = DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert_common_write_metrics(write_metrics);

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("id='3'")
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, 1);
        assert!(write_metrics.num_removed_files > 0);
        assert_common_write_metrics(write_metrics);

        let snapshot_bytes = table
            .log_store
            .read_commit_entry(2)
            .await?
            .expect("failed to get snapshot bytes");
        let version_actions = get_actions(2, snapshot_bytes).await?;

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
        let table: DeltaTable = DeltaOps::new_in_memory()
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

        let table = DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("value=3")
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));

        let ctx = SessionContext::new();
        let cdf_scan = DeltaOps(table.clone())
            .load_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");

        let mut batches = collect_batches(
            cdf_scan
                .properties()
                .output_partitioning()
                .partition_count(),
            cdf_scan,
            ctx,
        )
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
        let version_actions = get_actions(2, snapshot_bytes).await?;

        let cdc_actions = version_actions
            .iter()
            .filter(|action| matches!(action, &&Action::Cdc(_)))
            .collect_vec();
        assert!(!cdc_actions.is_empty());
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
            let table = DeltaOps::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = DeltaOps(table)
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
            let table = DeltaOps::new_in_memory()
                .create()
                .with_configuration_property(TableProperty::AppendOnly, Some("true".to_string()))
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = DeltaOps(table)
                .write(vec![batch])
                .with_save_mode(SaveMode::Overwrite);

            let check = writer.check_preconditions().await;
            assert!(check.is_err());
            Ok(())
        }

        #[tokio::test]
        async fn test_empty_set_of_batches() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let table = DeltaOps::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = DeltaOps(table).write(vec![]);

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
            let table = DeltaOps::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;
            let writer = DeltaOps(table)
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
            let table = DeltaOps::new_in_memory()
                .create()
                .with_columns(table_schema.fields().cloned())
                .await?;

            let ctx = SessionContext::new();
            let plan = Arc::new(
                ctx.sql("SELECT 1 as id")
                    .await
                    .unwrap()
                    .logical_plan()
                    .clone(),
            );
            let writer = WriteBuilder::new(table.log_store.clone(), table.state)
                .with_input_execution_plan(plan)
                .with_save_mode(SaveMode::Overwrite);

            let _ = writer.check_preconditions().await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_no_snapshot_create_actions() -> DeltaResult<()> {
            let table_schema = get_delta_schema();
            let table = DeltaOps::new_in_memory()
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
            let table = DeltaOps::new_in_memory()
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
}
