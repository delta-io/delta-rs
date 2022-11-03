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

// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::writer::{DeltaWriter, WriterConfig};
use super::MAX_SUPPORTED_WRITER_VERSION;
use super::{transaction::commit, CreateBuilder};
use crate::action::{Action, Add, DeltaOperation, Remove, SaveMode};
use crate::builder::DeltaTableBuilder;
use crate::delta::{DeltaResult, DeltaTable, DeltaTableError};
use crate::schema::Schema;
use crate::storage::DeltaObjectStore;
use crate::writer::record_batch::divide_by_partition_values;
use crate::writer::utils::PartitionPath;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{SessionContext, TaskContext};
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
    /// The input plan
    input: Option<Arc<dyn ExecutionPlan>>,
    /// Location where the table is stored
    location: Option<String>,
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
    /// An object store to be used as backend for delta table
    object_store: Option<Arc<DeltaObjectStore>>,
    /// Storage options used to create a new storage backend
    storage_options: Option<HashMap<String, String>>,
    /// RecordBatches to be written into the table
    batches: Option<Vec<RecordBatch>>,
}

impl Default for WriteBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBuilder {
    /// Create a new [`WriteBuilder`]
    pub fn new() -> Self {
        Self {
            input: None,
            location: None,
            mode: SaveMode::Append,
            partition_columns: None,
            predicate: None,
            storage_options: None,
            target_file_size: None,
            write_batch_size: None,
            object_store: None,
            batches: None,
        }
    }

    /// Specify the path to the location where table data is stored,
    /// which could be a path on distributed storage.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
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

    /// Execution plan that produces the data to be written to the delta table
    pub fn with_input_batches(mut self, batches: impl IntoIterator<Item = RecordBatch>) -> Self {
        self.batches = Some(batches.into_iter().collect());
        self
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables.
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    /// Provide a [`DeltaObjectStore`] instance, that points at table location
    pub fn with_object_store(mut self, object_store: Arc<DeltaObjectStore>) -> Self {
        self.object_store = Some(object_store);
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
}

impl std::future::IntoFuture for WriteBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let object_store = if let Some(store) = this.object_store {
                Ok(store)
            } else {
                DeltaTableBuilder::from_uri(&this.location.unwrap())
                    .with_storage_options(this.storage_options.unwrap_or_default())
                    .build_storage()
            }?;

            // TODO we can find a more optimized config. Of course we want to pass in the state anyhow..
            let mut table = DeltaTable::new(object_store.clone(), Default::default());
            let mut actions = match table.load().await {
                Err(DeltaTableError::NotATable(_)) => {
                    let schema: Schema = if let Some(plan) = &this.input {
                        Ok(plan.schema().try_into()?)
                    } else if let Some(batches) = &this.batches {
                        if batches.is_empty() {
                            return Err(WriteError::MissingData.into());
                        }
                        Ok(batches[0].schema().try_into()?)
                    } else {
                        Err(WriteError::MissingData)
                    }?;
                    let mut builder = CreateBuilder::new()
                        .with_object_store(table.object_store())
                        .with_columns(schema.get_fields().clone());
                    if let Some(partition_columns) = this.partition_columns.as_ref() {
                        builder = builder.with_partition_columns(partition_columns.clone())
                    }
                    let (_, actions, _) = builder.into_table_and_actions()?;
                    Ok(actions)
                }
                Ok(_) => {
                    if table.get_min_writer_version() > MAX_SUPPORTED_WRITER_VERSION {
                        Err(
                            WriteError::UnsupportedWriterVersion(table.get_min_writer_version())
                                .into(),
                        )
                    } else {
                        match this.mode {
                            SaveMode::ErrorIfExists => {
                                Err(WriteError::AlreadyExists(table.table_uri()).into())
                            }
                            _ => Ok(vec![]),
                        }
                    }
                }
                Err(err) => Err(err),
            }?;

            // validate partition columns
            let partition_columns = if let Ok(meta) = table.get_metadata() {
                if let Some(ref partition_columns) = this.partition_columns {
                    if &meta.partition_columns != partition_columns {
                        Err(WriteError::PartitionColumnMismatch {
                            expected: table.get_metadata()?.partition_columns.clone(),
                            got: partition_columns.to_vec(),
                        })
                    } else {
                        Ok(partition_columns.clone())
                    }
                } else {
                    Ok(meta.partition_columns.clone())
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

                    if let Ok(meta) = table.get_metadata() {
                        let curr_schema: ArrowSchemaRef = Arc::new((&meta.schema).try_into()?);
                        if schema != curr_schema {
                            return Err(DeltaTableError::Generic(
                                "Updating table schema not yet implemented".to_string(),
                            ));
                        }
                    };

                    let data = if !partition_columns.is_empty() {
                        // TODO partitioning should probably happen in its own plan ...
                        let mut partitions: HashMap<String, Vec<RecordBatch>> = HashMap::new();
                        for batch in batches {
                            let divided = divide_by_partition_values(
                                schema.clone(),
                                partition_columns.clone(),
                                &batch,
                            )
                            .unwrap();
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

            // Write data to disk
            let mut tasks = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                let inner_plan = plan.clone();
                let state = SessionContext::new();
                let task_ctx = Arc::new(TaskContext::from(&state));
                let config = WriterConfig::new(
                    inner_plan.schema(),
                    partition_columns.clone(),
                    None,
                    this.target_file_size,
                    this.write_batch_size,
                );
                let mut writer = DeltaWriter::new(object_store.clone(), config);

                let mut stream = inner_plan.execute(i, task_ctx)?;
                let handle: tokio::task::JoinHandle<DeltaResult<Vec<Add>>> =
                    tokio::task::spawn(async move {
                        while let Some(batch) = stream.next().await {
                            writer.write(&batch?).await?;
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
                        let remove_actions = table
                            .get_state()
                            .files()
                            .iter()
                            .map(to_remove_action)
                            .collect::<Vec<_>>();
                        actions.extend(remove_actions);
                    }
                }
            };

            // Finally, commit ...
            let operation = DeltaOperation::Write {
                mode: this.mode,
                partition_by: if !partition_columns.is_empty() {
                    Some(partition_columns)
                } else {
                    None
                },
                predicate: this.predicate,
            };
            let _version = commit(
                &table.storage,
                table.version() + 1,
                actions,
                operation,
                None,
            )
            .await?;
            table.update().await?;

            // TODO should we build checkpoints based on config?

            Ok(table)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::{get_delta_schema, get_record_batch};

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
}
