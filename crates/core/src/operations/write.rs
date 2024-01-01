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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_cast::can_cast_types;
use arrow_schema::{DataType, Fields, SchemaRef as ArrowSchemaRef};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use futures::future::BoxFuture;
use futures::StreamExt;
use parquet::file::properties::WriterProperties;

use super::datafusion_utils::Expression;
use super::transaction::PROTOCOL;
use super::writer::{DeltaWriter, WriterConfig};
use super::{transaction::commit, CreateBuilder};
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::DeltaDataChecker;
use crate::delta_datafusion::{find_files, register_store, DeltaScanBuilder};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Action, Add, PartitionsExt, Remove, StructType};
use crate::logstore::LogStoreRef;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::storage::ObjectStoreRef;
use crate::table::state::DeltaTableState;
use crate::table::Constraint as DeltaConstraint;
use crate::writer::record_batch::divide_by_partition_values;
use crate::DeltaTable;

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
    /// whether to overwrite the schema
    overwrite_schema: bool,
    /// how to handle cast failures, either return NULL (safe=true) or return ERR (safe=false)
    safe_cast: bool,
    /// Parquet writer properties
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
    /// Name of the table, only used when table doesn't exist yet
    name: Option<String>,
    /// Description of the table, only used when table doesn't exist yet
    description: Option<String>,
    /// Configurations of the delta table, only used when table doesn't exist
    configuration: HashMap<String, Option<String>>,
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
            batches: None,
            safe_cast: false,
            overwrite_schema: false,
            writer_properties: None,
            app_metadata: None,
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

    /// Add overwrite_schema
    pub fn with_overwrite_schema(mut self, overwrite_schema: bool) -> Self {
        self.overwrite_schema = overwrite_schema;
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
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.app_metadata = Some(HashMap::from_iter(metadata));
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
                    .with_columns(schema.fields().clone())
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
    safe_cast: bool,
    overwrite_schema: bool,
) -> DeltaResult<Vec<Add>> {
    // Use input schema to prevent wrapping partitions columns into a dictionary.
    let schema: ArrowSchemaRef = if overwrite_schema {
        plan.schema()
    } else {
        snapshot
            .and_then(|s| s.input_schema().ok())
            .unwrap_or(plan.schema())
    };

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
    for i in 0..plan.output_partitioning().partition_count() {
        let inner_plan = plan.clone();
        let inner_schema = schema.clone();
        let task_ctx = Arc::new(TaskContext::from(&state));
        let config = WriterConfig::new(
            inner_schema.clone(),
            partition_columns.clone(),
            writer_properties.clone(),
            target_file_size,
            write_batch_size,
        );
        let mut writer = DeltaWriter::new(object_store.clone(), config);
        let checker_stream = checker.clone();
        let mut stream = inner_plan.execute(i, task_ctx)?;
        let handle: tokio::task::JoinHandle<DeltaResult<Vec<Add>>> =
            tokio::task::spawn(async move {
                while let Some(maybe_batch) = stream.next().await {
                    let batch = maybe_batch?;
                    checker_stream.check_batch(&batch).await?;
                    let arr =
                        super::cast::cast_record_batch(&batch, inner_schema.clone(), safe_cast)?;
                    writer.write(&arr).await?;
                }
                writer.close().await
            });

        tasks.push(handle);
    }

    // Collect add actions to add to commit
    Ok(futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| WriteError::WriteTask { source: err })?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .concat()
        .into_iter()
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
    safe_cast: bool,
    overwrite_schema: bool,
) -> DeltaResult<Vec<Add>> {
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
        safe_cast,
        overwrite_schema,
    )
    .await
}

async fn execute_non_empty_expr(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    partition_columns: Vec<String>,
    expression: &Expr,
    rewrite: &[Add],
    writer_properties: Option<WriterProperties>,
) -> DeltaResult<Vec<Add>> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.

    let input_schema = snapshot.input_schema()?;
    let input_dfschema: DFSchema = input_schema.clone().as_ref().clone().try_into()?;

    let scan = DeltaScanBuilder::new(snapshot, log_store.clone(), &state)
        .with_files(rewrite)
        .build()
        .await?;
    let scan = Arc::new(scan);

    // Apply the negation of the filter and rewrite files
    let negated_expression = Expr::Not(Box::new(Expr::IsTrue(Box::new(expression.clone()))));

    let predicate_expr = create_physical_expr(
        &negated_expression,
        &input_dfschema,
        state.execution_props(),
    )?;
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);

    // We don't want to verify the predicate against existing data
    let add_actions = write_execution_plan(
        Some(snapshot),
        state,
        filter,
        partition_columns,
        log_store.object_store(),
        Some(snapshot.table_config().target_file_size() as usize),
        None,
        writer_properties,
        false,
        false,
    )
    .await?;

    Ok(add_actions)
}

// This should only be called wth a valid predicate
async fn prepare_predicate_actions(
    predicate: Expr,
    log_store: LogStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    partition_columns: Vec<String>,
    writer_properties: Option<WriterProperties>,
    deletion_timestamp: i64,
) -> DeltaResult<Vec<Action>> {
    let candidates =
        find_files(snapshot, log_store.clone(), &state, Some(predicate.clone())).await?;

    let add = if candidates.partition_scan {
        Vec::new()
    } else {
        execute_non_empty_expr(
            snapshot,
            log_store,
            state,
            partition_columns,
            &predicate,
            &candidates.candidates,
            writer_properties,
        )
        .await?
    };
    let remove = candidates.candidates;

    let mut actions: Vec<Action> = add.into_iter().map(Action::Add).collect();

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
            if this.mode == SaveMode::Overwrite {
                if let Some(snapshot) = &this.snapshot {
                    PROTOCOL.check_append_only(snapshot)?;
                }
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

            let plan = if let Some(plan) = this.input {
                Ok(plan)
            } else if let Some(batches) = this.batches {
                if batches.is_empty() {
                    Err(WriteError::MissingData)
                } else {
                    let schema = batches[0].schema();

                    if let Some(snapshot) = &this.snapshot {
                        let table_schema = snapshot
                            .physical_arrow_schema(this.log_store.object_store().clone())
                            .await
                            .or_else(|_| snapshot.arrow_schema())
                            .unwrap_or(schema.clone());

                        if !can_cast_batch(schema.fields(), table_schema.fields())
                            && !(this.overwrite_schema && matches!(this.mode, SaveMode::Overwrite))
                        {
                            return Err(DeltaTableError::Generic(
                                "Schema of data does not match table schema".to_string(),
                            ));
                        };
                    }

                    let data = if !partition_columns.is_empty() {
                        // TODO partitioning should probably happen in its own plan ...
                        let mut partitions: HashMap<String, Vec<RecordBatch>> = HashMap::new();
                        for batch in batches {
                            let divided = divide_by_partition_values(
                                schema.clone(),
                                partition_columns.clone(),
                                &batch,
                            )?;
                            for part in divided {
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
                        partitions.into_values().collect::<Vec<_>>()
                    } else {
                        vec![batches]
                    };

                    Ok(Arc::new(MemoryExec::try_new(&data, schema.clone(), None)?)
                        as Arc<dyn ExecutionPlan>)
                }
            } else {
                Err(WriteError::MissingData)
            }?;
            let schema = plan.schema();

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

            // Here we need to validate if the new data conforms to a predicate if one is provided
            let add_actions = write_execution_plan_with_predicate(
                predicate.clone(),
                this.snapshot.as_ref(),
                state.clone(),
                plan,
                partition_columns.clone(),
                this.log_store.object_store().clone(),
                this.target_file_size,
                this.write_batch_size,
                this.writer_properties.clone(),
                this.safe_cast,
                this.overwrite_schema,
            )
            .await?;
            actions.extend(add_actions.into_iter().map(Action::Add));

            // Collect remove actions if we are overwriting the table
            if let Some(snapshot) = &this.snapshot {
                if matches!(this.mode, SaveMode::Overwrite) {
                    // Update metadata with new schema
                    let table_schema = snapshot
                        .physical_arrow_schema(this.log_store.object_store().clone())
                        .await
                        .or_else(|_| snapshot.arrow_schema())
                        .unwrap_or(schema.clone());

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
            }

            let operation = DeltaOperation::Write {
                mode: this.mode,
                partition_by: if !partition_columns.is_empty() {
                    Some(partition_columns)
                } else {
                    None
                },
                predicate: predicate_str,
            };

            let version = commit(
                this.log_store.as_ref(),
                &actions,
                operation.clone(),
                this.snapshot.as_ref(),
                this.app_metadata,
            )
            .await?;

            // TODO we do not have the table config available, but since we are merging only our newly
            // created actions, it may be safe to assume, that we want to include all actions.
            // then again, having only some tombstones may be misleading.
            if let Some(mut snapshot) = this.snapshot {
                snapshot.merge(actions, &operation, version)?;
                Ok(DeltaTable::new_with_state(this.log_store, snapshot))
            } else {
                let mut table = DeltaTable::new(this.log_store, Default::default());
                table.update().await?;
                Ok(table)
            }
        })
    }
}

fn can_cast_batch(from_fields: &Fields, to_fields: &Fields) -> bool {
    if from_fields.len() != to_fields.len() {
        return false;
    }

    from_fields.iter().all(|f| {
        if let Some((_, target_field)) = to_fields.find(f.name()) {
            if let (DataType::Struct(fields0), DataType::Struct(fields1)) =
                (f.data_type(), target_field.data_type())
            {
                can_cast_batch(fields0, fields1)
            } else {
                can_cast_types(f.data_type(), target_field.data_type())
            }
        } else {
            false
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::{collect_sendable_stream, DeltaOps};
    use crate::protocol::SaveMode;
    use crate::writer::test_utils::datafusion::write_batch;
    use crate::writer::test_utils::datafusion::{get_data, get_data_sorted};
    use crate::writer::test_utils::{
        get_arrow_schema, get_delta_schema, get_delta_schema_with_nested_struct, get_record_batch,
        get_record_batch_with_nested_struct, setup_table_with_configuration,
    };
    use crate::DeltaConfigKey;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, TimeUnit};
    use datafusion::prelude::*;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use serde_json::{json, Value};

    #[tokio::test]
    async fn test_write_when_delta_table_is_append_only() {
        let table = setup_table_with_configuration(DeltaConfigKey::AppendOnly, Some("true")).await;
        let batch = get_record_batch(None, false);
        // Append
        let table = write_batch(table, batch.clone()).await;
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
            .with_columns(table_schema.fields().clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.history(None).await.unwrap().len(), 1);

        // write some data
        let metadata = HashMap::from_iter(vec![("k1".to_string(), json!("v1.1"))]);
        let mut table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_metadata(metadata.clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);
        table.load().await.unwrap();
        assert_eq!(table.history(None).await.unwrap().len(), 2);
        assert_eq!(
            table.history(None).await.unwrap()[0]
                .info
                .clone()
                .into_iter()
                .filter(|(k, _)| k != "clientVersion")
                .collect::<HashMap<String, Value>>(),
            metadata
        );

        // append some data
        let metadata: HashMap<String, Value> =
            HashMap::from_iter(vec![("k1".to_string(), json!("v1.2"))]);
        let mut table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_metadata(metadata.clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 2);
        table.load().await.unwrap();
        assert_eq!(table.history(None).await.unwrap().len(), 3);
        assert_eq!(
            table.history(None).await.unwrap()[0]
                .info
                .clone()
                .into_iter()
                .filter(|(k, _)| k != "clientVersion")
                .collect::<HashMap<String, Value>>(),
            metadata
        );

        // overwrite table
        let metadata: HashMap<String, Value> =
            HashMap::from_iter(vec![("k2".to_string(), json!("v2.1"))]);
        let mut table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .with_metadata(metadata.clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_files_count(), 1);
        table.load().await.unwrap();
        assert_eq!(table.history(None).await.unwrap().len(), 4);
        assert_eq!(
            table.history(None).await.unwrap()[0]
                .info
                .clone()
                .into_iter()
                .filter(|(k, _)| k != "clientVersion")
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

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![Some(10000)]))],
        )
        .unwrap();

        let _res = DeltaOps::from(table).write(vec![batch]).await.unwrap();
        let expected = [
            "+-------------------------+",
            "| value                   |",
            "+-------------------------+",
            "| 1970-01-01T00:00:00.010 |",
            "| 2023-06-03 15:35:00     |",
            "+-------------------------+",
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
        assert_eq!(table.get_files_count(), 1)
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

        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 4)
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
            .with_columns(schema.fields().clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let table = DeltaOps(table).write(vec![batch.clone()]).await.unwrap();
        assert_eq!(table.version(), 1);

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
            .with_columns(schema.fields().clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let table = DeltaOps(table).write(vec![batch.clone()]).await;
        assert!(table.is_err())
    }

    #[tokio::test]
    async fn test_nested_struct() {
        let table_schema = get_delta_schema_with_nested_struct();
        let batch = get_record_batch_with_nested_struct();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);

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
}
