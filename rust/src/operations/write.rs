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

use arrow_array::RecordBatch;
use arrow_cast::{can_cast_types, cast_with_options, CastOptions};
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan};
use futures::future::BoxFuture;
use futures::StreamExt;
use parquet::file::properties::WriterProperties;

use super::writer::{DeltaWriter, WriterConfig};
use super::MAX_SUPPORTED_WRITER_VERSION;
use super::{transaction::commit, CreateBuilder};
use crate::action::{Action, Add, DeltaOperation, Remove, SaveMode};
use crate::delta::DeltaTable;
use crate::delta_datafusion::DeltaDataChecker;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::schema::Schema;
use crate::storage::{DeltaObjectStore, ObjectStoreRef};
use crate::table_state::DeltaTableState;
use crate::writer::record_batch::divide_by_partition_values;
use crate::writer::utils::PartitionPath;

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
    /// CastOptions determines how data types that do not match the underlying table are handled
    /// By default an error is returned
    cast_options: CastOptions,
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
            cast_options: CastOptions { safe: false },
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

    /// Specify the cast options to use when casting columns that do not match the table's schema.
    pub fn with_cast_options(mut self, cast_options: CastOptions) -> Self {
        self.cast_options = cast_options;
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan(
    snapshot: &DeltaTableState,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    cast_options: &CastOptions,
) -> DeltaResult<Vec<Add>> {
    let invariants = snapshot
        .current_metadata()
        .and_then(|meta| meta.schema.get_invariants().ok())
        .unwrap_or_default();

    // Use input schema to prevent wrapping partitions columns into a dictionary.
    let schema = snapshot.input_schema().unwrap_or(plan.schema());

    let checker = DeltaDataChecker::new(invariants);

    // Write data to disk
    let mut tasks = vec![];
    for i in 0..plan.output_partitioning().partition_count() {
        let inner_plan = plan.clone();
        let inner_schema = schema.clone();
        let task_ctx = Arc::new(TaskContext::from(&state));
        let inner_cast = cast_options.clone();
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
                    let arr = cast_record_batch(&batch, inner_schema.clone(), &inner_cast)?;
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

            let state = match this.state {
                Some(state) => state,
                None => {
                    let ctx = SessionContext::new();
                    ctx.state()
                }
            };

            let add_actions = write_execution_plan(
                &this.snapshot,
                state,
                plan,
                partition_columns.clone(),
                this.store.clone(),
                this.target_file_size,
                this.write_batch_size,
                None,
                &this.cast_options,
            )
            .await?;
            actions.extend(add_actions.into_iter().map(Action::add));

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
    cast_options: &CastOptions,
) -> DeltaResult<RecordBatch> {
    //let cast_options = CastOptions { safe: false };

    let columns = target_schema
        .all_fields()
        .iter()
        .map(|f| {
            let col = batch.column_by_name(f.name()).unwrap();
            if !col.data_type().equals_datatype(f.data_type()) {
                cast_with_options(col, f.data_type(), cast_options)
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
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::{get_delta_schema, get_record_batch};
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, TimeUnit};
    use datafusion::assert_batches_sorted_eq;
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
            .with_cast_options(CastOptions { safe: true })
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
