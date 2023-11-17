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

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::{can_cast_types, cast_with_options, CastOptions};
use arrow_schema::{DataType, Fields, SchemaRef as ArrowSchemaRef};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan};
use futures::future::BoxFuture;
use futures::StreamExt;
use parquet::file::properties::WriterProperties;

use super::transaction::PROTOCOL;
use super::writer::{DeltaWriter, WriterConfig};
use super::{transaction::commit, CreateBuilder};
use crate::delta_datafusion::DeltaDataChecker;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Action, Add, Remove, StructType};
use crate::logstore::LogStoreRef;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::storage::ObjectStoreRef;
use crate::table::state::DeltaTableState;
use crate::writer::record_batch::divide_by_partition_values;
use crate::writer::utils::PartitionPath;
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
#[derive(Debug, Clone)]
pub struct WriteBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: DeltaTableState,
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
    predicate: Option<String>,
    /// Size above which we will write a buffered parquet file to disk.
    target_file_size: Option<usize>,
    /// Number of records to be written in single batch to underlying writer
    write_batch_size: Option<usize>,
    /// RecordBatches to be written into the table
    batches: Option<Vec<RecordBatch>>,
    /// how to handle cast failures, either return NULL (safe=true) or return ERR (safe=false)
    safe_cast: bool,
    /// Parquet writer properties
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
}

impl WriteBuilder {
    /// Create a new [`WriteBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
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
            writer_properties: None,
            app_metadata: None,
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

    async fn check_preconditions(&self) -> DeltaResult<Vec<Action>> {
        match self.log_store.is_delta_table_location().await? {
            true => {
                PROTOCOL.can_write_to(&self.snapshot)?;
                match self.mode {
                    SaveMode::ErrorIfExists => {
                        Err(WriteError::AlreadyExists(self.log_store.root_uri()).into())
                    }
                    _ => Ok(vec![]),
                }
            }
            false => {
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
                    .with_columns(schema.fields().clone());
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
    safe_cast: bool,
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
                    let arr = cast_record_batch(&batch, inner_schema.clone(), safe_cast)?;
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
                        .physical_arrow_schema(this.log_store.object_store().clone())
                        .await
                        .or_else(|_| this.snapshot.arrow_schema())
                        .unwrap_or(schema.clone());

                    if !can_cast_batch(schema.fields(), table_schema.fields()) {
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
                this.log_store.object_store().clone(),
                this.target_file_size,
                this.write_batch_size,
                this.writer_properties,
                this.safe_cast,
            )
            .await?;
            actions.extend(add_actions.into_iter().map(Action::Add));

            // Collect remove actions if we are overwriting the table
            if matches!(this.mode, SaveMode::Overwrite) {
                // This should never error, since now() will always be larger than UNIX_EPOCH
                let deletion_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let to_remove_action = |add: &Add| {
                    Action::Remove(Remove {
                        path: add.path.clone(),
                        deletion_timestamp: Some(deletion_timestamp),
                        data_change: true,
                        extended_file_metadata: Some(false),
                        partition_values: Some(add.partition_values.clone()),
                        size: Some(add.size),
                        // TODO add file metadata to remove action (tags missing)
                        tags: None,
                        deletion_vector: add.deletion_vector.clone(),
                        base_row_id: add.base_row_id,
                        default_row_commit_version: add.default_row_commit_version,
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
                this.log_store.as_ref(),
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
                this.app_metadata,
            )
            .await?;

            // TODO we do not have the table config available, but since we are merging only our newly
            // created actions, it may be safe to assume, that we want to include all actions.
            // then again, having only some tombstones may be misleading.
            this.snapshot
                .merge(DeltaTableState::from_actions(actions, version)?, true, true);

            // TODO should we build checkpoints based on config?

            Ok(DeltaTable::new_with_state(this.log_store, this.snapshot))
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

fn cast_record_batch_columns(
    batch: &RecordBatch,
    fields: &Fields,
    cast_options: &CastOptions,
) -> Result<Vec<Arc<(dyn Array)>>, arrow_schema::ArrowError> {
    fields
        .iter()
        .map(|f| {
            let col = batch.column_by_name(f.name()).unwrap();
            if let (DataType::Struct(_), DataType::Struct(child_fields)) =
                (col.data_type(), f.data_type())
            {
                let child_batch = RecordBatch::from(StructArray::from(col.into_data()));
                let child_columns =
                    cast_record_batch_columns(&child_batch, child_fields, cast_options)?;
                Ok(Arc::new(StructArray::new(
                    child_fields.clone(),
                    child_columns.clone(),
                    None,
                )) as ArrayRef)
            } else if !col.data_type().equals_datatype(f.data_type()) {
                cast_with_options(col, f.data_type(), cast_options)
            } else {
                Ok(col.clone())
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

fn cast_record_batch(
    batch: &RecordBatch,
    target_schema: ArrowSchemaRef,
    safe: bool,
) -> DeltaResult<RecordBatch> {
    let cast_options = CastOptions {
        safe,
        ..Default::default()
    };

    let columns = cast_record_batch_columns(batch, target_schema.fields(), &cast_options)?;
    Ok(RecordBatch::try_new(target_schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::{collect_sendable_stream, DeltaOps};
    use crate::protocol::SaveMode;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::datafusion::write_batch;
    use crate::writer::test_utils::{
        get_delta_schema, get_delta_schema_with_nested_struct, get_record_batch,
        get_record_batch_with_nested_struct, setup_table_with_configuration,
    };
    use crate::DeltaConfigKey;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, TimeUnit};
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
        assert_eq!(table.state.commit_infos().len(), 1);

        // write some data
        let metadata = HashMap::from_iter(vec![("k1".to_string(), json!("v1.1"))]);
        let mut table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .with_metadata(metadata.clone())
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);
        table.load().await.unwrap();
        assert_eq!(table.state.commit_infos().len(), 2);
        assert_eq!(
            table.state.commit_infos()[1]
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
        assert_eq!(table.get_file_uris().count(), 2);
        table.load().await.unwrap();
        assert_eq!(table.state.commit_infos().len(), 3);
        assert_eq!(
            table.state.commit_infos()[2]
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
        assert_eq!(table.get_file_uris().count(), 1);
        table.load().await.unwrap();
        assert_eq!(table.state.commit_infos().len(), 4);
        assert_eq!(
            table.state.commit_infos()[3]
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
        let tmp_dir = tempdir::TempDir::new("test").unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("string", DataType::Utf8, true),
            Field::new("data", DataType::Utf8, true),
        ]));

        let str_values = StringArray::from(vec![r#"$%&/()=^"[]#*?.:_- {=}|`<>~/\r\n+"#]);
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
            "+------+-----------------------------------+",
            "| data | string                            |",
            "+------+-----------------------------------+",
            r#"| test | $%&/()=^"[]#*?.:_- {=}|`<>~/\r\n+ |"#,
            "+------+-----------------------------------+",
        ];

        assert_batches_eq!(&expected, &data);
    }
}
