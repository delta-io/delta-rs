//! Used to write a [RecordBatch] into a delta table.
//!
//! New Table Semantics
//!  - The schema of the [RecordBatch] is used to initialize the table.
//!  - The partition columns will be used to partition the table.
//!
//! Existing Table Semantics
//!  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
//!  - The schema of the RecordBatch will be checked and if there are new columns present
//!    they will be added to the tables schema. Conflicting columns (i.e. a INT, and a STRING)
//!    will result in an exception.
//!  - The partition columns, if present, are validated against the existing metadata. If not
//!    present, then the partitioning of the table is respected.
//!
//! In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
//! replace data that matches a predicate.

// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala
use super::{
    create::CreateCommand,
    transaction::{serialize_actions, OPERATION_SCHEMA},
    *,
};
use crate::{
    action::{Action, Add, Remove, SaveMode},
    writer::{DeltaWriter, RecordBatchWriter},
    Schema,
};
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use core::any::Any;
use datafusion::{
    execution::context::TaskContext,
    physical_plan::{
        common::{
            collect as collect_batch, compute_record_batch_statistics, SizedRecordBatchStream,
        },
        expressions::PhysicalSortExpr,
        metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics},
        stream::RecordBatchStreamAdapter,
        Distribution, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_common::Result as DataFusionResult;
use futures::{TryFutureExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_SUPPORTED_WRITER_VERSION: i32 = 1;

/// Command for writing data into Delta table
#[derive(Debug)]
pub struct WriteCommand {
    table_uri: String,
    /// The save mode used in operation
    mode: SaveMode,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    /// When using `Overwrite` mode, replace data that matches a predicate
    predicate: Option<String>,
    /// Schema of data to be written to disk
    schema: ArrowSchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl WriteCommand {
    /// Create a new write command
    pub fn try_new<T>(
        table_uri: T,
        operation: DeltaOperation,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self, DeltaCommandError>
    where
        T: Into<String> + Clone,
    {
        match operation {
            DeltaOperation::Write {
                mode,
                partition_by,
                predicate,
            } => {
                let uri = table_uri.into();
                let plan = Arc::new(WritePartitionCommand::new(
                    uri.clone(),
                    partition_by.clone(),
                    input.clone(),
                ));
                Ok(Self {
                    table_uri: uri,
                    mode,
                    partition_columns: partition_by,
                    predicate,
                    schema: input.schema(),
                    input: plan,
                })
            }
            _ => Err(DeltaCommandError::UnsupportedCommand(
                "WriteCommand only implemented for write operation".to_string(),
            )),
        }
    }
}

impl ExecutionPlan for WriteCommand {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::new(OPERATION_SCHEMA.clone())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                do_write(
                    input,
                    self.table_uri.clone(),
                    partition,
                    self.partition_columns.clone(),
                    self.schema.clone(),
                    self.predicate.clone(),
                    self.mode.clone(),
                    context,
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn do_write(
    input: SendableRecordBatchStream,
    table_uri: String,
    partition_id: usize,
    partition_columns: Option<Vec<String>>,
    schema: ArrowSchemaRef,
    predicate: Option<String>,
    mode: SaveMode,
    context: Arc<TaskContext>,
) -> DataFusionResult<SendableRecordBatchStream> {
    let mut table = DeltaTableBuilder::from_uri(&table_uri)
        .build()
        .map_err(to_datafusion_err)?;
    let metrics = ExecutionPlanMetricsSet::new();
    let tracking_metrics = MemTrackingMetrics::new(&metrics, partition_id);

    let mut actions = match table.load().await {
        // Table is not yet created
        Err(_) => {
            let schema = Schema::try_from(schema.clone()).map_err(to_datafusion_err)?;
            let metadata = DeltaTableMetaData::new(
                None,
                None,
                None,
                schema,
                partition_columns.unwrap_or_default(),
                HashMap::new(),
            );
            let op = DeltaOperation::Create {
                location: table_uri.clone(),
                metadata: metadata.clone(),
                mode: SaveMode::ErrorIfExists,
                // TODO get the protocol from somewhere central
                protocol: Protocol {
                    min_reader_version: 1,
                    min_writer_version: 1,
                },
            };
            let plan =
                Arc::new(CreateCommand::try_new(table_uri.clone(), op).map_err(to_datafusion_err)?);

            let create_actions = collect(plan.clone(), context.clone()).await?;

            Ok(create_actions)
        }
        Ok(_) => {
            if table.get_min_writer_version() > MAX_SUPPORTED_WRITER_VERSION {
                Err(DeltaCommandError::UnsupportedWriterVersion(
                    table.get_min_writer_version(),
                ))
            } else {
                match mode {
                    SaveMode::ErrorIfExists => {
                        Err(DeltaCommandError::TableAlreadyExists(table_uri.clone()))
                    }
                    SaveMode::Ignore => {
                        return Ok(Box::pin(SizedRecordBatchStream::new(
                            schema,
                            vec![],
                            tracking_metrics,
                        )))
                    }
                    _ => Ok(vec![]),
                }
            }
        }
    }
    .map_err(to_datafusion_err)?;

    actions.append(&mut collect_batch(input).await?);

    if let SaveMode::Overwrite = mode {
        // This should never error, since SystemTime::now() will always be larger than UNIX_EPOCH
        let deletion_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let deletion_timestamp = deletion_timestamp.as_millis() as i64;

        let to_remove_action = |add: &Add| {
            Action::remove(Remove {
                path: add.path.clone(),
                deletion_timestamp: Some(deletion_timestamp),
                data_change: true,
                // TODO add file metadata to remove action (tags missing)
                extended_file_metadata: Some(false),
                partition_values: Some(add.partition_values.clone()),
                size: Some(add.size),
                tags: None,
            })
        };

        match predicate {
            Some(_) => todo!("Overwriting data based on predicate is not yet implemented"),
            _ => {
                let remove_actions = table
                    .get_state()
                    .files()
                    .iter()
                    .map(to_remove_action)
                    .collect::<Vec<_>>();
                actions.push(serialize_actions(remove_actions)?);
            }
        }
    };

    let stream = SizedRecordBatchStream::new(
        schema,
        actions
            .iter()
            .map(|b| Arc::new(b.to_owned()))
            .collect::<Vec<_>>(),
        tracking_metrics,
    );
    Ok(Box::pin(stream))
}

#[derive(Debug)]
/// Writes the partitioned input data into separate batches
/// and forwards the add actions as record batches
struct WritePartitionCommand {
    table_uri: String,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    // TODO When using `Overwrite` mode, replace data that matches a predicate
    // predicate: Option<String>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl WritePartitionCommand {
    pub fn new<T>(
        table_uri: T,
        partition_columns: Option<Vec<String>>,
        // predicate: Option<String>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self
    where
        T: Into<String>,
    {
        Self {
            table_uri: table_uri.into(),
            partition_columns,
            // predicate,
            input,
        }
    }
}

impl ExecutionPlan for WritePartitionCommand {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::new(OPERATION_SCHEMA.clone())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                do_write_partition(
                    input,
                    self.table_uri.clone(),
                    partition,
                    self.partition_columns.clone(),
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

async fn do_write_partition(
    input: SendableRecordBatchStream,
    table_uri: String,
    partition_id: usize,
    partition_columns: Option<Vec<String>>,
) -> DataFusionResult<SendableRecordBatchStream> {
    let schema = input.schema().clone();
    let data = collect_batch(input).await?;
    if data.is_empty() {
        let stream = EmptyRecordBatchStream::new(Arc::new(OPERATION_SCHEMA.clone()));
        return Ok(Box::pin(stream));
    }
    let mut writer =
        RecordBatchWriter::try_new(table_uri.clone(), schema, partition_columns.clone(), None)
            .map_err(to_datafusion_err)?;

    for batch in data {
        // TODO we should have an API that allows us to circumvent internal partitioning
        writer.write(batch).await.map_err(to_datafusion_err)?;
    }
    let actions = writer
        .flush()
        .await
        .map_err(to_datafusion_err)?
        .iter()
        .map(|e| Action::add(e.clone()))
        .collect::<Vec<_>>();

    let serialized_actions = serialize_actions(actions)?;
    let metrics = ExecutionPlanMetricsSet::new();
    let tracking_metrics = MemTrackingMetrics::new(&metrics, partition_id);
    let stream = SizedRecordBatchStream::new(
        serialized_actions.schema(),
        vec![Arc::new(serialized_actions)],
        tracking_metrics,
    );

    Ok(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        open_table,
        writer::test_utils::{create_initialized_table, get_record_batch},
    };

    #[tokio::test]
    async fn test_append_data() {
        let partition_cols = vec!["modified".to_string()];
        let mut table = create_initialized_table(&partition_cols).await;
        assert_eq!(table.version(), 0);

        let transaction = get_transaction(table.table_uri(), 0, SaveMode::Append);
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let _ = collect(transaction.clone(), task_ctx.clone())
            .await
            .unwrap();
        table.update().await.unwrap();
        assert_eq!(table.get_file_uris().collect::<Vec<_>>().len(), 2);
        assert_eq!(table.version(), 1);

        let transaction = get_transaction(table.table_uri(), 1, SaveMode::Append);
        let _ = collect(transaction.clone(), task_ctx).await.unwrap();
        table.update().await.unwrap();
        assert_eq!(table.get_file_uris().collect::<Vec<_>>().len(), 4);
        assert_eq!(table.version(), 2);
    }

    #[tokio::test]
    async fn test_overwrite_data() {
        let partition_cols = vec!["modified".to_string()];
        let mut table = create_initialized_table(&partition_cols).await;
        assert_eq!(table.version(), 0);

        let transaction = get_transaction(table.table_uri(), 0, SaveMode::Overwrite);
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let _ = collect(transaction.clone(), task_ctx.clone())
            .await
            .unwrap();
        table.update().await.unwrap();
        assert_eq!(table.get_file_uris().collect::<Vec<_>>().len(), 2);
        assert_eq!(table.version(), 1);

        let transaction = get_transaction(table.table_uri(), 1, SaveMode::Overwrite);
        let _ = collect(transaction.clone(), task_ctx).await.unwrap();
        table.update().await.unwrap();
        assert_eq!(table.get_file_uris().collect::<Vec<_>>().len(), 2);
        assert_eq!(table.version(), 2);
    }

    #[tokio::test]
    async fn test_write_non_existent() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();

        let transaction = get_transaction(
            table_path.to_str().unwrap().to_string(),
            -1,
            SaveMode::Overwrite,
        );
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let _ = collect(transaction.clone(), task_ctx.clone())
            .await
            .unwrap();

        // THe table should be created on write and thus have version 0
        let table = open_table(table_path.to_str().unwrap()).await.unwrap();
        assert_eq!(table.get_file_uris().collect::<Vec<_>>().len(), 2);
        assert_eq!(table.version(), 0);
    }

    fn get_transaction(
        table_uri: String,
        table_version: i64,
        mode: SaveMode,
    ) -> Arc<DeltaTransactionPlan> {
        let batch = get_record_batch(None, false);
        let schema = batch.schema();
        let data_plan = Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap());
        let op = DeltaOperation::Write {
            partition_by: Some(vec!["modified".to_string()]),
            mode,
            predicate: None,
        };
        let command = WriteCommand::try_new(&table_uri, op.clone(), data_plan).unwrap();

        Arc::new(DeltaTransactionPlan::new(
            table_uri,
            table_version,
            Arc::new(command),
            op,
            None,
        ))
    }
}
