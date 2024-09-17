//!
//! New Table Semantics
//!  - The schema of the [RecordBatch] is used to initialize the table.
//!  - The partition columns will be used to partition the table.
//!
//! Existing Table Semantics
//!  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
//!  - (NOT YET IMPLEMENTED) The schema of the RecordBatch will be checked and if there are new columns present
//!    they will be added to the tables schema. Conflicting columns (i.e. a INT, and a STRING)
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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::vec;

use arrow_array::RecordBatch;
use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, DataType, Fields, SchemaRef as ArrowSchemaRef};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion_common::DFSchema;
use datafusion_expr::{lit, Expr};
use datafusion_physical_expr::expressions::{self};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{memory::MemoryExec, ExecutionPlan};
use futures::future::BoxFuture;
use futures::StreamExt;
use object_store::prefix::PrefixStore;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use tracing::log::*;

use super::cdc::should_write_cdc;
use super::datafusion_utils::Expression;
use super::transaction::{CommitBuilder, CommitProperties, TableReference, PROTOCOL};
use super::writer::{DeltaWriter, WriterConfig};
use super::CreateBuilder;
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::{
    find_files, register_store, DeltaScanBuilder, DeltaScanConfigBuilder,
};
use crate::delta_datafusion::{DataFusionMixins, DeltaDataChecker};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{
    Action, ActionType, Add, AddCDCFile, Metadata, PartitionsExt, Remove, StructType,
};
use crate::logstore::LogStoreRef;
use crate::operations::cast::{cast_record_batch, merge_schema::merge_arrow_schema};
use crate::protocol::{DeltaOperation, SaveMode};
use crate::storage::ObjectStoreRef;
use crate::table::state::DeltaTableState;
use crate::table::Constraint as DeltaConstraint;
use crate::writer::record_batch::divide_by_partition_values;
use crate::DeltaTable;

use tokio::sync::mpsc::Sender;

#[derive(thiserror::Error, Debug)]
enum WriteError {
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
            _ => Err(DeltaTableError::Generic(format!(
                "Invalid schema write mode provided: {}, only these are supported: ['overwrite', 'merge']",
                s
            ))),
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
    input: Option<Arc<dyn ExecutionPlan>>,
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
    /// RecordBatches to be written into the table
    batches: Option<Vec<RecordBatch>>,
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

impl super::Operation<()> for WriteBuilder {}

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
            batches: None,
            safe_cast: false,
            schema_mode: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            name: None,
            description: None,
            configuration: Default::default(),
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

    /// Execution plan that produces the data to be written to the delta table
    pub fn with_input_execution_plan(mut self, plan: Arc<dyn ExecutionPlan>) -> Self {
        self.input = Some(plan);
        self
    }

    /// A session state accompanying a given input plan, containing e.g. registered object stores
    pub fn with_input_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }

    /// Execution plan that produces the data to be written to the delta table
    pub fn with_input_batches(mut self, batches: impl IntoIterator<Item = RecordBatch>) -> Self {
        self.batches = Some(batches.into_iter().collect());
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

    async fn check_preconditions(&self) -> DeltaResult<Vec<Action>> {
        match &self.snapshot {
            Some(snapshot) => {
                PROTOCOL.can_write_to(snapshot)?;

                let schema: StructType = if let Some(plan) = &self.input {
                    (plan.schema()).try_into()?
                } else if let Some(batches) = &self.batches {
                    if batches.is_empty() {
                        return Err(WriteError::MissingData.into());
                    }
                    (batches[0].schema()).try_into()?
                } else {
                    return Err(WriteError::MissingData.into());
                };

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
                let schema: StructType = if let Some(plan) = &self.input {
                    Ok(plan.schema().try_into()?)
                } else if let Some(batches) = &self.batches {
                    if batches.is_empty() {
                        return Err(WriteError::MissingData.into());
                    }
                    Ok(batches[0].schema().try_into()?)
                } else {
                    Err(WriteError::MissingData)
                }?;
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

                let (_, actions, _) = builder.into_table_and_actions()?;
                Ok(actions)
            }
        }
    }
}
/// Configuration for the writer on how to collect stats
#[derive(Clone)]
pub struct WriterStatsConfig {
    /// Number of columns to collect stats for, idx based
    num_indexed_cols: i32,
    /// Optional list of columns which to collect stats for, takes precedende over num_index_cols
    stats_columns: Option<Vec<String>>,
}

impl WriterStatsConfig {
    /// Create new writer stats config
    pub fn new(num_indexed_cols: i32, stats_columns: Option<Vec<String>>) -> Self {
        Self {
            num_indexed_cols,
            stats_columns,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_execution_plan_with_predicate(
    predicate: Option<Expr>,
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    sender: Option<Sender<RecordBatch>>,
) -> DeltaResult<Vec<Action>> {
    // We always take the plan Schema since the data may contain Large/View arrow types,
    // the schema and batches were prior constructed with this in mind.
    let schema: ArrowSchemaRef = plan.schema();
    let checker = if let Some(snapshot) = snapshot {
        DeltaDataChecker::new(snapshot)
    } else {
        DeltaDataChecker::empty()
    };
    let checker = match predicate {
        Some(pred) => {
            // TODO: get the name of the outer-most column? `*` will also work but would it be slower?
            let chk = DeltaConstraint::new("*", &fmt_expr_to_sql(&pred)?);
            checker.with_extra_constraints(vec![chk])
        }
        _ => checker,
    };
    // Write data to disk
    let mut tasks = vec![];
    for i in 0..plan.properties().output_partitioning().partition_count() {
        let inner_plan = plan.clone();
        let inner_schema = schema.clone();
        let task_ctx = Arc::new(TaskContext::from(&state));
        let config = WriterConfig::new(
            inner_schema.clone(),
            partition_columns.clone(),
            writer_properties.clone(),
            target_file_size,
            write_batch_size,
            writer_stats_config.num_indexed_cols,
            writer_stats_config.stats_columns.clone(),
        );
        let mut writer = DeltaWriter::new(object_store.clone(), config);
        let checker_stream = checker.clone();
        let sender_stream = sender.clone();
        let mut stream = inner_plan.execute(i, task_ctx)?;

        let handle: tokio::task::JoinHandle<DeltaResult<Vec<Action>>> = tokio::task::spawn(
            async move {
                let sendable = sender_stream.clone();
                while let Some(maybe_batch) = stream.next().await {
                    let batch = maybe_batch?;

                    checker_stream.check_batch(&batch).await?;

                    if let Some(s) = sendable.as_ref() {
                        if let Err(e) = s.send(batch.clone()).await {
                            error!("Failed to send data to observer: {e:#?}");
                        }
                    } else {
                        debug!("write_execution_plan_with_predicate did not send any batches, no sender.");
                    }
                    writer.write(&batch).await?;
                }
                let add_actions = writer.close().await;
                match add_actions {
                    Ok(actions) => Ok(actions.into_iter().map(Action::Add).collect::<Vec<_>>()),
                    Err(err) => Err(err),
                }
            },
        );

        tasks.push(handle);
    }
    let actions = futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| WriteError::WriteTask { source: err })?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .concat()
        .into_iter()
        .collect::<Vec<_>>();
    // Collect add actions to add to commit
    Ok(actions)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_cdc(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    sender: Option<Sender<RecordBatch>>,
) -> DeltaResult<Vec<Action>> {
    let cdc_store = Arc::new(PrefixStore::new(object_store, "_change_data"));

    Ok(write_execution_plan(
        snapshot,
        state,
        plan,
        partition_columns,
        cdc_store,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config,
        sender,
    )
    .await?
    .into_iter()
    .map(|add| {
        // Modify add actions into CDC actions
        match add {
            Action::Add(add) => {
                Action::Cdc(AddCDCFile {
                    // This is a gnarly hack, but the action needs the nested path, not the
                    // path isnide the prefixed store
                    path: format!("_change_data/{}", add.path),
                    size: add.size,
                    partition_values: add.partition_values,
                    data_change: false,
                    tags: add.tags,
                })
            }
            _ => panic!("Expected Add action"),
        }
    })
    .collect::<Vec<_>>())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    sender: Option<Sender<RecordBatch>>,
) -> DeltaResult<Vec<Action>> {
    write_execution_plan_with_predicate(
        None,
        snapshot,
        state,
        plan,
        partition_columns,
        object_store,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config,
        sender,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_non_empty_expr(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    partition_columns: Vec<String>,
    expression: &Expr,
    rewrite: &[Add],
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    partition_scan: bool,
    insert_plan: Arc<dyn ExecutionPlan>,
) -> DeltaResult<Vec<Action>> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.
    let mut actions: Vec<Action> = Vec::new();

    // Take the insert plan schema since it might have been schema evolved, if its not
    // it is simply the table schema
    let df_schema = insert_plan.schema();
    let input_dfschema: DFSchema = df_schema.as_ref().clone().try_into()?;

    let scan_config = DeltaScanConfigBuilder::new()
        .with_schema(snapshot.input_schema()?)
        .build(snapshot)?;

    let scan = DeltaScanBuilder::new(snapshot, log_store.clone(), &state)
        .with_files(rewrite)
        // Use input schema which doesn't wrap partition values, otherwise divide_by_partition_value won't work on UTF8 partitions
        // Since it can't fetch a scalar from a dictionary type
        .with_scan_config(scan_config)
        .build()
        .await?;
    let scan = Arc::new(scan);

    // We don't want to verify the predicate against existing data
    if !partition_scan {
        // Apply the negation of the filter and rewrite files
        let negated_expression = Expr::Not(Box::new(Expr::IsTrue(Box::new(expression.clone()))));

        let predicate_expr = state.create_physical_expr(negated_expression, &input_dfschema)?;
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);

        let add_actions: Vec<Action> = write_execution_plan(
            Some(snapshot),
            state.clone(),
            filter,
            partition_columns.clone(),
            log_store.object_store(),
            Some(snapshot.table_config().target_file_size() as usize),
            None,
            writer_properties.clone(),
            writer_stats_config.clone(),
            None,
        )
        .await?;

        actions.extend(add_actions);
    }

    // CDC logic, simply filters data with predicate and adds the _change_type="delete" as literal column
    // Only write when CDC actions when it was not a partition scan, load_cdf can deduce the deletes in that case
    // based on the remove actions if a partition got deleted
    if !partition_scan {
        // We only write deletions when it was not a partition scan
        if let Some(cdc_actions) = execute_non_empty_expr_cdc(
            snapshot,
            log_store,
            state.clone(),
            scan,
            input_dfschema,
            expression,
            partition_columns,
            writer_properties,
            writer_stats_config,
            insert_plan,
        )
        .await?
        {
            actions.extend(cdc_actions)
        }
    }
    Ok(actions)
}

/// If CDC is enabled it writes all the deletions based on predicate into _change_data directory
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_non_empty_expr_cdc(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    scan: Arc<crate::delta_datafusion::DeltaScan>,
    input_dfschema: DFSchema,
    expression: &Expr,
    table_partition_cols: Vec<String>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    insert_plan: Arc<dyn ExecutionPlan>,
) -> DeltaResult<Option<Vec<Action>>> {
    match should_write_cdc(snapshot) {
        // Create CDC scan
        Ok(true) => {
            let cdc_predicate_expr =
                state.create_physical_expr(expression.clone(), &input_dfschema)?;
            let cdc_scan: Arc<dyn ExecutionPlan> =
                Arc::new(FilterExec::try_new(cdc_predicate_expr, scan.clone())?);

            // Add literal column "_change_type"
            let delete_change_type_expr =
                state.create_physical_expr(lit("delete"), &input_dfschema)?;

            let insert_change_type_expr =
                state.create_physical_expr(lit("insert"), &input_dfschema)?;

            // Project columns and lit
            let mut delete_project_expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = scan
                .schema()
                .fields()
                .into_iter()
                .enumerate()
                .map(|(idx, field)| -> (Arc<dyn PhysicalExpr>, String) {
                    (
                        Arc::new(expressions::Column::new(field.name(), idx)),
                        field.name().to_owned(),
                    )
                })
                .collect();

            let mut insert_project_expressions = delete_project_expressions.clone();
            delete_project_expressions.insert(
                delete_project_expressions.len(),
                (delete_change_type_expr, "_change_type".to_owned()),
            );
            insert_project_expressions.insert(
                insert_project_expressions.len(),
                (insert_change_type_expr, "_change_type".to_owned()),
            );

            let delete_plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                delete_project_expressions,
                cdc_scan.clone(),
            )?);

            let insert_plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                insert_project_expressions,
                insert_plan.clone(),
            )?);

            let cdc_plan: Arc<dyn ExecutionPlan> =
                Arc::new(UnionExec::new(vec![delete_plan, insert_plan]));

            let cdc_actions = write_execution_plan_cdc(
                Some(snapshot),
                state.clone(),
                cdc_plan.clone(),
                table_partition_cols.clone(),
                log_store.object_store(),
                Some(snapshot.table_config().target_file_size() as usize),
                None,
                writer_properties,
                writer_stats_config,
                None,
            )
            .await?;
            Ok(Some(cdc_actions))
        }
        _ => Ok(None),
    }
}

// This should only be called wth a valid predicate
#[allow(clippy::too_many_arguments)]
async fn prepare_predicate_actions(
    predicate: Expr,
    log_store: LogStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    partition_columns: Vec<String>,
    writer_properties: Option<WriterProperties>,
    deletion_timestamp: i64,
    writer_stats_config: WriterStatsConfig,
    insert_plan: Arc<dyn ExecutionPlan>,
) -> DeltaResult<Vec<Action>> {
    let candidates =
        find_files(snapshot, log_store.clone(), &state, Some(predicate.clone())).await?;

    let mut actions = execute_non_empty_expr(
        snapshot,
        log_store,
        state,
        partition_columns,
        &predicate,
        &candidates.candidates,
        writer_properties,
        writer_stats_config,
        candidates.partition_scan,
        insert_plan,
    )
    .await?;

    let remove = candidates.candidates;

    for action in remove {
        actions.push(Action::Remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            deletion_vector: action.deletion_vector,
            tags: None,
            base_row_id: action.base_row_id,
            default_row_commit_version: action.default_row_commit_version,
        }))
    }
    Ok(actions)
}

impl std::future::IntoFuture for WriteBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let mut metrics = WriteMetrics::default();
            let exec_start = Instant::now();

            if this.mode == SaveMode::Overwrite {
                if let Some(snapshot) = &this.snapshot {
                    PROTOCOL.check_append_only(&snapshot.snapshot)?;
                    if !snapshot.load_config().require_files {
                        return Err(DeltaTableError::NotInitializedWithFiles("WRITE".into()));
                    }
                }
            }
            if this.schema_mode == Some(SchemaMode::Overwrite) && this.mode != SaveMode::Overwrite {
                return Err(DeltaTableError::Generic(
                    "Schema overwrite not supported for Append".to_string(),
                ));
            }

            // Create table actions to initialize table in case it does not yet exist and should be created
            let mut actions = this.check_preconditions().await?;

            let active_partitions = this
                .snapshot
                .as_ref()
                .map(|s| s.metadata().partition_columns.clone());

            // validate partition columns
            let partition_columns = if let Some(active_part) = active_partitions {
                if let Some(ref partition_columns) = this.partition_columns {
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
                Ok(this.partition_columns.unwrap_or_default())
            }?;
            let mut schema_drift = false;
            let plan = if let Some(plan) = this.input {
                if this.schema_mode == Some(SchemaMode::Merge) {
                    return Err(DeltaTableError::Generic(
                        "Schema merge not supported yet for Datafusion".to_string(),
                    ));
                }
                Ok(plan)
            } else if let Some(batches) = this.batches {
                if batches.is_empty() {
                    Err(WriteError::MissingData)
                } else {
                    let schema = batches[0].schema();

                    let mut new_schema = None;
                    if let Some(snapshot) = &this.snapshot {
                        let table_schema = snapshot.input_schema()?;
                        if let Err(schema_err) =
                            try_cast_batch(schema.fields(), table_schema.fields())
                        {
                            schema_drift = true;
                            if this.mode == SaveMode::Overwrite
                                && this.schema_mode == Some(SchemaMode::Overwrite)
                            {
                                new_schema = None // we overwrite anyway, so no need to cast
                            } else if this.schema_mode == Some(SchemaMode::Merge) {
                                new_schema = Some(merge_arrow_schema(
                                    table_schema.clone(),
                                    schema.clone(),
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
                                schema.clone(),
                                schema_drift,
                            )?);
                        }
                    }
                    let data = if !partition_columns.is_empty() {
                        // TODO partitioning should probably happen in its own plan ...
                        let mut partitions: HashMap<String, Vec<RecordBatch>> = HashMap::new();
                        let mut num_partitions = 0;
                        let mut num_added_rows = 0;
                        for batch in batches {
                            let real_batch = match new_schema.clone() {
                                Some(new_schema) => cast_record_batch(
                                    &batch,
                                    new_schema,
                                    this.safe_cast,
                                    schema_drift, // Schema drifted so we have to add the missing columns/structfields.
                                )?,
                                None => batch,
                            };

                            let divided = divide_by_partition_values(
                                new_schema.clone().unwrap_or(schema.clone()),
                                partition_columns.clone(),
                                &real_batch,
                            )?;
                            num_partitions += divided.len();
                            for part in divided {
                                num_added_rows += part.record_batch.num_rows();
                                let key = part.partition_values.hive_partition_path();
                                match partitions.get_mut(&key) {
                                    Some(part_batches) => {
                                        part_batches.push(part.record_batch);
                                    }
                                    None => {
                                        partitions.insert(key, vec![part.record_batch]);
                                    }
                                }
                            }
                        }
                        metrics.num_partitions = num_partitions;
                        metrics.num_added_rows = num_added_rows;
                        partitions.into_values().collect::<Vec<_>>()
                    } else {
                        match new_schema {
                            Some(ref new_schema) => {
                                let mut new_batches = vec![];
                                let mut num_added_rows = 0;
                                for batch in batches {
                                    new_batches.push(cast_record_batch(
                                        &batch,
                                        new_schema.clone(),
                                        this.safe_cast,
                                        schema_drift, // Schema drifted so we have to add the missing columns/structfields.
                                    )?);
                                    num_added_rows += batch.num_rows();
                                }
                                metrics.num_added_rows = num_added_rows;
                                vec![new_batches]
                            }
                            None => {
                                metrics.num_added_rows = batches.iter().map(|b| b.num_rows()).sum();
                                vec![batches]
                            }
                        }
                    };

                    Ok(Arc::new(MemoryExec::try_new(
                        &data,
                        new_schema.unwrap_or(schema).clone(),
                        None,
                    )?) as Arc<dyn ExecutionPlan>)
                }
            } else {
                Err(WriteError::MissingData)
            }?;
            let schema = plan.schema();
            if this.schema_mode == Some(SchemaMode::Merge) && schema_drift {
                if let Some(snapshot) = &this.snapshot {
                    let schema_struct: StructType = schema.clone().try_into()?;
                    let current_protocol = snapshot.protocol();
                    let configuration = snapshot.metadata().configuration.clone();
                    let maybe_new_protocol = if PROTOCOL
                        .contains_timestampntz(schema_struct.fields())
                        && !current_protocol
                            .reader_features
                            .clone()
                            .unwrap_or_default()
                            .contains(&crate::kernel::ReaderFeatures::TimestampWithoutTimezone)
                    // We can check only reader features, as reader and writer timestampNtz
                    // should be always enabled together
                    {
                        let new_protocol = current_protocol.clone().enable_timestamp_ntz();
                        if !(current_protocol.min_reader_version == 3
                            && current_protocol.min_writer_version == 7)
                        {
                            Some(new_protocol.move_table_properties_into_features(&configuration))
                        } else {
                            Some(new_protocol)
                        }
                    } else {
                        None
                    };
                    let schema_action = Action::Metadata(Metadata::try_new(
                        schema_struct,
                        partition_columns.clone(),
                        configuration,
                    )?);
                    actions.push(schema_action);
                    if let Some(new_protocol) = maybe_new_protocol {
                        actions.push(new_protocol.into())
                    }
                }
            }
            let state = match this.state {
                Some(state) => state,
                None => {
                    let ctx = SessionContext::new();
                    register_store(this.log_store.clone(), ctx.runtime_env());
                    ctx.state()
                }
            };

            let (predicate_str, predicate) = match this.predicate {
                Some(predicate) => {
                    let pred = match predicate {
                        Expression::DataFusion(expr) => expr,
                        Expression::String(s) => {
                            let df_schema = DFSchema::try_from(schema.as_ref().to_owned())?;
                            parse_predicate_expression(&df_schema, s, &state)?
                            // this.snapshot.unwrap().parse_predicate_expression(s, &state)?
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

            // Here we need to validate if the new data conforms to a predicate if one is provided
            let add_actions = write_execution_plan_with_predicate(
                predicate.clone(),
                this.snapshot.as_ref(),
                state.clone(),
                plan.clone(),
                partition_columns.clone(),
                this.log_store.object_store().clone(),
                target_file_size,
                this.write_batch_size,
                this.writer_properties.clone(),
                writer_stats_config.clone(),
                None,
            )
            .await?;
            metrics.num_added_files = add_actions.len();
            actions.extend(add_actions);

            // Collect remove actions if we are overwriting the table
            if let Some(snapshot) = &this.snapshot {
                if matches!(this.mode, SaveMode::Overwrite) {
                    // Update metadata with new schema
                    let table_schema = snapshot.input_schema()?;

                    let configuration = snapshot.metadata().configuration.clone();
                    let current_protocol = snapshot.protocol();
                    let maybe_new_protocol = if PROTOCOL.contains_timestampntz(
                        TryInto::<StructType>::try_into(schema.clone())?.fields(),
                    ) && !current_protocol
                        .reader_features
                        .clone()
                        .unwrap_or_default()
                        .contains(&crate::kernel::ReaderFeatures::TimestampWithoutTimezone)
                    // We can check only reader features, as reader and writer timestampNtz
                    // should be always enabled together
                    {
                        let new_protocol = current_protocol.clone().enable_timestamp_ntz();
                        if !(current_protocol.min_reader_version == 3
                            && current_protocol.min_writer_version == 7)
                        {
                            Some(new_protocol.move_table_properties_into_features(&configuration))
                        } else {
                            Some(new_protocol)
                        }
                    } else {
                        None
                    };

                    if let Some(protocol) = maybe_new_protocol {
                        actions.push(protocol.into())
                    }

                    if schema != table_schema {
                        let mut metadata = snapshot.metadata().clone();
                        let delta_schema: StructType = schema.as_ref().try_into()?;
                        metadata.schema_string = serde_json::to_string(&delta_schema)?;
                        actions.push(Action::Metadata(metadata));
                    }

                    let deletion_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    match predicate {
                        Some(pred) => {
                            let predicate_actions = prepare_predicate_actions(
                                pred,
                                this.log_store.clone(),
                                snapshot,
                                state,
                                partition_columns.clone(),
                                this.writer_properties,
                                deletion_timestamp,
                                writer_stats_config,
                                plan,
                            )
                            .await?;
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
                .build(
                    this.snapshot.as_ref().map(|f| f as &dyn TableReference),
                    this.log_store.clone(),
                    operation.clone(),
                )
                .await?;

            Ok(DeltaTable::new_with_state(this.log_store, commit.snapshot))
        })
    }
}

fn try_cast_batch(from_fields: &Fields, to_fields: &Fields) -> Result<(), ArrowError> {
    if from_fields.len() != to_fields.len() {
        return Err(ArrowError::SchemaError(format!(
            "Cannot cast schema, number of fields does not match: {} vs {}",
            from_fields.len(),
            to_fields.len()
        )));
    }

    from_fields
        .iter()
        .map(|f| {
            if let Some((_, target_field)) = to_fields.find(f.name()) {
                if let (DataType::Struct(fields0), DataType::Struct(fields1)) =
                    (f.data_type(), target_field.data_type())
                {
                    try_cast_batch(fields0, fields1)
                } else {
                    match (f.data_type(), target_field.data_type()) {
                        (
                            DataType::Decimal128(left_precision, left_scale) | DataType::Decimal256(left_precision, left_scale),
                            DataType::Decimal128(right_precision, right_scale)
                        ) => {
                            if left_precision <= right_precision && left_scale <= right_scale {
                                Ok(())
                            } else {
                                Err(ArrowError::SchemaError(format!(
                                    "Cannot cast field {} from {} to {}",
                                    f.name(),
                                    f.data_type(),
                                    target_field.data_type()
                                )))
                            }
                        },
                        (
                            _,
                            DataType::Decimal256(_, _),
                        ) => {
                            unreachable!("Target field can never be Decimal 256. According to the protocol: 'The precision and scale can be up to 38.'")
                        },
                        (left, right) => {
                            if !can_cast_types(left, right) {
                                Err(ArrowError::SchemaError(format!(
                                    "Cannot cast field {} from {} to {}",
                                    f.name(),
                                    f.data_type(),
                                    target_field.data_type()
                                )))
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
            } else {
                Err(ArrowError::SchemaError(format!(
                    "Field {} not found in schema",
                    f.name()
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(())
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
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use datafusion::prelude::*;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
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
        assert!(write_metrics.execution_time_ms > 0);
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
        assert_eq!(table.version(), 0);
        assert_eq!(table.history(None).await.unwrap().len(), 1);

        // write some data
        let metadata = HashMap::from_iter(vec![("k1".to_string(), json!("v1.1"))]);
        let mut table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_commit_properties(CommitProperties::default().with_metadata(metadata.clone()))
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
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
        assert_eq!(table.version(), 2);
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
        assert_eq!(table.version(), 3);
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
        assert_eq!(table.version(), 0);
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
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 2);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert!(write_metrics.num_partitions > 0);
        assert_eq!(write_metrics.num_added_files, 2);
        assert_common_write_metrics(write_metrics);

        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 4);

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert!(write_metrics.num_partitions > 0);
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
        assert_eq!(table.version(), 0);

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
        assert_eq!(table.version(), 1);
        let new_schema = table.metadata().unwrap().schema().unwrap();
        let fields = new_schema.fields();
        let names = fields.map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(names, vec!["id", "value", "modified", "inserted_by"]);

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
        assert_eq!(table.version(), 0);

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert!(write_metrics.num_partitions > 0);
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

        assert_eq!(table.version(), 1);
        let new_schema = table.metadata().unwrap().schema().unwrap();
        let fields = new_schema.fields();
        let mut names = fields.map(|f| f.name()).collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, vec!["id", "inserted_by", "modified", "value"]);
        let part_cols = table.metadata().unwrap().partition_columns.clone();
        assert_eq!(part_cols, vec!["id", "value"]); // we want to preserve partitions

        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert!(write_metrics.num_partitions > 0);
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
        assert_eq!(table.version(), 0);
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
        assert_eq!(table.version(), 0);
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
        assert_eq!(table.version(), 0);

        let table = DeltaOps(table).write(vec![batch.clone()]).await.unwrap();
        assert_eq!(table.version(), 1);
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
        assert_eq!(table.version(), 0);

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
        assert_eq!(table.version(), 0);

        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
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
        assert_eq!(table.version(), 0);
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
        assert_eq!(table.version(), 1);
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
        assert_eq!(table.version(), 0);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_common_write_metrics(write_metrics);

        // Take clones of these before an operation resulting in error, otherwise it will
        // be impossible to refer to an in-memory table
        let table_logstore = table.log_store.clone();
        let table_state = table.state.clone().unwrap();

        // An attempt to write records non comforming to predicate should fail
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
        assert_eq!(table.version(), 0);
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
        assert_eq!(table.version(), 1);
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
        assert_eq!(table.version(), 0);

        let schema = Arc::new(ArrowSchema::try_from(delta_schema)?);

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
        assert_eq!(table.version(), 1);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert_common_write_metrics(write_metrics);

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
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
        assert_eq!(table.version(), 0);

        let schema = Arc::new(ArrowSchema::try_from(delta_schema)?);

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
        assert_eq!(table.version(), 1);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, 3);
        assert!(write_metrics.num_partitions > 0);
        assert_common_write_metrics(write_metrics);

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("id='3'")
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        let write_metrics: WriteMetrics = get_write_metrics(table.clone()).await;
        assert_eq!(write_metrics.num_added_rows, 1);
        assert!(write_metrics.num_partitions > 0);
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
        assert_eq!(table.version(), 0);

        let schema = Arc::new(ArrowSchema::try_from(delta_schema)?);

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
        assert_eq!(table.version(), 1);

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .with_replace_where("value=3")
            .await
            .unwrap();
        assert_eq!(table.version(), 2);

        let ctx = SessionContext::new();
        let cdf_scan = DeltaOps(table.clone())
            .load_cdf()
            .with_session_ctx(ctx.clone())
            .with_starting_version(0)
            .build()
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
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(4)).collect();

        assert_batches_sorted_eq! {[
        "+-------+----------+--------------+-----------------+----+",
        "| value | modified | _change_type | _commit_version | id |",
        "+-------+----------+--------------+-----------------+----+",
        "| 1     | yes      | insert       | 1               | 1  |",
        "| 2     | yes      | insert       | 1               | 2  |",
        "| 3     | no       | delete       | 2               | 3  |",
        "| 3     | no       | insert       | 1               | 3  |",
        "| 3     | yes      | insert       | 2               | 3  |",
        "+-------+----------+--------------+-----------------+----+",
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
}
