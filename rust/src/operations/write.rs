//! Used to write [RecordBatch]es into a delta table.
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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::writer::{DeltaWriter, WriterConfig};
use super::MAX_SUPPORTED_WRITER_VERSION;
use super::{transaction::commit, CreateBuilder};
use crate::action::{Action, Add, DeltaOperation, Remove, SaveMode};
use crate::delta::{DeltaResult, DeltaTable, DeltaTableError};
use crate::delta_datafusion::DeltaDataChecker;
use crate::schema::Schema;
use crate::storage::DeltaObjectStore;
use crate::table_state::DeltaTableState;
use crate::writer::record_batch::divide_by_partition_values;
use crate::writer::utils::PartitionPath;

use arrow_array::RecordBatch;
use arrow_cast::{can_cast_types, cast};
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan};
use futures::future::BoxFuture;
use futures::StreamExt;

#[derive(thiserror::Error, Debug)]
enum WriteError {
    #[error("No data source supplied to write command.")]
    MissingData,

    #[error("Failed to execute write task: {source}")]
    WriteTask { source: tokio::task::JoinError },

    #[error("Delta-rs does not support writer version requirement: {0}")]
    UnsupportedWriterVersion(i32),

    #[error("A table already exists at: {0}")]
    AlreadyExists(String),

    #[error(
        "Specified table partitioning does not match table partitioning: expected: {:?}, got: {:?}",
        expected,
        got
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
#[derive(Debug, Clone)]
pub struct WriteBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    store: Arc<DeltaObjectStore>,
    /// The input plan
    input: Option<Arc<dyn ExecutionPlan>>,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// SaveMode defines how to treat data already written to table location
    mode: SaveMode,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    /// When using `Overwrite` mode, replace data that matches a predicate
    predicate: Option<String>,
    /// Size above which we will write a buffered parquet file to disk.
    target_file_size: Option<usize>,
    /// Number of records to be written in single batch to underlying writer
    write_batch_size: Option<usize>,
    /// RecordBatches to be written into the table
    batches: Option<Vec<RecordBatch>>,
}

impl WriteBuilder {
    /// Create a new [`WriteBuilder`]
    pub fn new(store: Arc<DeltaObjectStore>, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            store,
            input: None,
            state: None,
            mode: SaveMode::Append,
            partition_columns: None,
            predicate: None,
            target_file_size: None,
            write_batch_size: None,
            batches: None,
        }
    }

    /// Specify the behavior when a table exists at location
    pub fn with_save_mode(mut self, save_mode: SaveMode) -> Self {
        self.mode = save_mode;
        self
    }

    /// When using `Overwrite` mode, replace data that matches a predicate
    pub fn with_replace_where(mut self, predicate: impl Into<String>) -> Self {
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

    async fn check_preconditions(&self) -> DeltaResult<Vec<Action>> {
        match self.store.is_delta_table_location().await? {
            true => {
                let min_writer = self.snapshot.min_writer_version();
                if min_writer > MAX_SUPPORTED_WRITER_VERSION {
                    Err(WriteError::UnsupportedWriterVersion(min_writer).into())
                } else {
                    match self.mode {
                        SaveMode::ErrorIfExists => {
                            Err(WriteError::AlreadyExists(self.store.root_uri()).into())
                        }
                        _ => Ok(vec![]),
                    }
                }
            }
            false => {
                let schema: Schema = if let Some(plan) = &self.input {
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
                    .with_object_store(self.store.clone())
                    .with_columns(schema.get_fields().clone());
                if let Some(partition_columns) = self.partition_columns.as_ref() {
                    builder = builder.with_partition_columns(partition_columns.clone())
                }
                let (_, actions, _) = builder.into_table_and_actions()?;
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

        Box::pin(async move {
            // Create table actions to initialize table in case it does not yet exist and should be created
            let mut actions = this.check_preconditions().await?;

            let active_partitions = this
                .snapshot
                .current_metadata()
                .map(|meta| meta.partition_columns.clone());

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
                    let table_schema = this
                        .snapshot
                        .physical_arrow_schema(this.store.clone())
                        .await
                        .or_else(|_| this.snapshot.arrow_schema())
                        .unwrap_or(schema.clone());

                    if !can_cast_batch(schema.as_ref(), table_schema.as_ref()) {
                        return Err(DeltaTableError::Generic(
                            "Updating table schema not yet implemented".to_string(),
                        ));
                    };

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
                                let key = PartitionPath::from_hashmap(
                                    &partition_columns,
                                    &part.partition_values,
                                )
                                .map_err(DeltaTableError::from)?
                                .into();
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

                    Ok(Arc::new(MemoryExec::try_new(&data, schema, None)?)
                        as Arc<dyn ExecutionPlan>)
                }
            } else {
                Err(WriteError::MissingData)
            }?;

            let invariants = this
                .snapshot
                .current_metadata()
                .and_then(|meta| meta.schema.get_invariants().ok())
                .unwrap_or_default();
            let checker = DeltaDataChecker::new(invariants);

            // Write data to disk
            let mut tasks = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                let inner_plan = plan.clone();
                let task_ctx = Arc::from(if let Some(state) = this.state.clone() {
                    TaskContext::from(&state)
                } else {
                    let ctx = SessionContext::new();
                    TaskContext::from(&ctx)
                });
                let config = WriterConfig::new(
                    inner_plan.schema(),
                    partition_columns.clone(),
                    None,
                    this.target_file_size,
                    this.write_batch_size,
                );
                let mut writer = DeltaWriter::new(this.store.clone(), config);
                let checker_stream = checker.clone();
                let schema = inner_plan.schema().clone();
                let mut stream = inner_plan.execute(i, task_ctx)?;
                let handle: tokio::task::JoinHandle<DeltaResult<Vec<Add>>> =
                    tokio::task::spawn(async move {
                        while let Some(maybe_batch) = stream.next().await {
                            let batch = maybe_batch?;
                            checker_stream.check_batch(&batch).await?;
                            let arr = cast_record_batch(&batch, schema.clone())?;
                            writer.write(&arr).await?;
                        }
                        writer.close().await
                    });

                tasks.push(handle);
            }

            // Collect add actions to add to commit
            let add_actions = futures::future::join_all(tasks)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| WriteError::WriteTask { source: err })?
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .concat()
                .into_iter()
                .map(Action::add)
                .collect::<Vec<_>>();

            actions.extend(add_actions);

            // Collect remove actions if we are overwriting the table
            if matches!(this.mode, SaveMode::Overwrite) {
                // This should never error, since now() will always be larger than UNIX_EPOCH
                let deletion_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let to_remove_action = |add: &Add| {
                    Action::remove(Remove {
                        path: add.path.clone(),
                        deletion_timestamp: Some(deletion_timestamp),
                        data_change: true,
                        extended_file_metadata: Some(false),
                        partition_values: Some(add.partition_values.clone()),
                        size: Some(add.size),
                        // TODO add file metadata to remove action (tags missing)
                        tags: None,
                    })
                };

                match this.predicate {
                    Some(_pred) => {
                        todo!("Overwriting data based on predicate is not yet implemented")
                    }
                    _ => {
                        let remove_actions = this
                            .snapshot
                            .files()
                            .iter()
                            .map(to_remove_action)
                            .collect::<Vec<_>>();
                        actions.extend(remove_actions);
                    }
                }
            };

            let version = commit(
                this.store.as_ref(),
                &actions,
                DeltaOperation::Write {
                    mode: this.mode,
                    partition_by: if !partition_columns.is_empty() {
                        Some(partition_columns)
                    } else {
                        None
                    },
                    predicate: this.predicate,
                },
                &this.snapshot,
                // TODO pass through metadata
                None,
            )
            .await?;

            // TODO we do not have the table config available, but since we are merging only our newly
            // created actions, it may be safe to assume, that we want to include all actions.
            // then again, having only some tombstones may be misleading.
            this.snapshot
                .merge(DeltaTableState::from_actions(actions, version)?, true, true);

            // TODO should we build checkpoints based on config?

            Ok(DeltaTable::new_with_state(this.store, this.snapshot))
        })
    }
}

fn can_cast_batch(from_schema: &ArrowSchema, to_schema: &ArrowSchema) -> bool {
    if from_schema.fields.len() != to_schema.fields.len() {
        return false;
    }
    from_schema.all_fields().iter().all(|f| {
        if let Ok(target_field) = to_schema.field_with_name(f.name()) {
            can_cast_types(f.data_type(), target_field.data_type())
        } else {
            false
        }
    })
}

fn cast_record_batch(
    batch: &RecordBatch,
    target_schema: ArrowSchemaRef,
) -> DeltaResult<RecordBatch> {
    let columns = target_schema
        .all_fields()
        .iter()
        .map(|f| {
            let col = batch.column_by_name(f.name()).unwrap();
            if !col.data_type().equals_datatype(f.data_type()) {
                cast(col, f.data_type())
            } else {
                Ok(col.clone())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(target_schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::{get_delta_schema, get_record_batch};
    use serde_json::json;

    #[tokio::test]
    async fn test_create_write() {
        let table_schema = get_delta_schema();
        let batch = get_record_batch(None, false);

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.get_fields().clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);

        // append some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 2);

        // overwrite table
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_file_uris().count(), 1)
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
        assert_eq!(table.get_file_uris().count(), 1)
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
        assert_eq!(table.get_file_uris().count(), 2);

        let table = DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_file_uris().count(), 4)
    }

    #[tokio::test]
    async fn test_check_invariants() {
        let batch = get_record_batch(None, false);
        let schema: Schema = serde_json::from_value(json!({
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
            .with_columns(schema.get_fields().clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let table = DeltaOps(table).write(vec![batch.clone()]).await.unwrap();
        assert_eq!(table.version(), 1);

        let schema: Schema = serde_json::from_value(json!({
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
            .with_columns(schema.get_fields().clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let table = DeltaOps(table).write(vec![batch.clone()]).await;
        assert!(table.is_err())
    }
}
