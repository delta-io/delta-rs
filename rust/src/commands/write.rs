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
//!    will result in an exception
//!  - The partition columns, if present are validated against the existing metadata. If not
//!    present, then the partitioning of the table is respected.
//!
//! In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
//! replace data that matches a predicate.

// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala
use super::*;
use crate::{action::Action, open_table, write::DeltaWriter};
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::{
        array::{Array, StringArray},
        datatypes::{
            DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
        },
    },
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{
        collect,
        common::{
            collect as collect_batch, compute_record_batch_statistics, SizedRecordBatchStream,
        },
        empty::EmptyExec,
        Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
/// The write mode when writing data to delta table.
pub enum PartitionWriteMode {
    /// write input partitions in single operations
    Single(Arc<dyn ExecutionPlan>),
    /// write input partitions in separate operations
    Distributed(Arc<dyn ExecutionPlan>),
}

#[derive(Debug)]
/// The write mode when writing data to delta table.
pub enum WriteMode {
    /// append data to existing table
    Append,
    /// overwrite table with new data
    Overwrite,
}

/// Write command
#[derive(Debug)]
pub struct WriteCommand {
    table_uri: String,
    mode: WriteMode,
    /// Defines behavior for writing individual partitions
    partition_write_mode: PartitionWriteMode,
}

impl WriteCommand {
    /// Create new [`WriteCommand`] instance
    pub fn new(
        table_uri: String,
        mode: WriteMode,
        partition_write_mode: PartitionWriteMode,
    ) -> Self {
        let exec_plan = match partition_write_mode {
            PartitionWriteMode::Distributed(plan) => {
                println!("Setting up distributed mode");
                let new_plan = Arc::new(WritePartitionCommand {
                    table_uri: table_uri.clone(),
                    input: plan.clone(),
                });
                PartitionWriteMode::Distributed(new_plan)
            }
            other => other,
        };
        Self {
            table_uri,
            mode,
            partition_write_mode: exec_plan,
        }
    }
}

#[async_trait]
impl ExecutionPlan for WriteCommand {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![]))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        match &self.partition_write_mode {
            PartitionWriteMode::Single(plan) => {
                vec![plan.clone()]
            }
            PartitionWriteMode::Distributed(plan) => {
                vec![plan.clone()]
            }
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // match children.len() {
        //     1 => Ok(Arc::new(SortExec::try_new(
        //         self.expr.clone(),
        //         children[0].clone(),
        //     )?)),
        //     _ => Err(DataFusionError::Internal(
        //         "SortExec wrong number of children".to_string(),
        //     )),
        // }
        todo!()
    }

    async fn execute(&self, partition: usize) -> DataFusionResult<SendableRecordBatchStream> {
        println!("WriteCommand -> partition {:?}", partition);

        let mut table = open_table(&self.table_uri)
            .await
            .map_err(to_datafusion_err)?;
        let table_exists = check_table_exists(&table)
            .await
            .map_err(to_datafusion_err)?;

        if !table_exists {
            println!("{:?}", self.mode);
            todo!()
        }

        let mut txn = table.create_transaction(None);

        match &self.partition_write_mode {
            PartitionWriteMode::Single(plan) => {
                let data = collect(plan.clone()).await?;
                txn.write_files(data).await.map_err(to_datafusion_err)?;
            }
            PartitionWriteMode::Distributed(plan) => {
                // add actions
                let mut actions = Vec::new();
                let data = collect(plan.clone()).await?;
                for batch in data {
                    // TODO we assume that all children send a single column record batch with serialized actions
                    let serialized_actions = arrow::array::as_string_array(batch.column(0));
                    let mut new_actions = (0..serialized_actions.len())
                        .map(|idx| serde_json::from_str::<Action>(serialized_actions.value(idx)))
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(to_datafusion_err)?;
                    actions.append(&mut new_actions);
                }
                txn.add_actions(actions);
            }
        };

        let _ = txn.commit(None).await.map_err(to_datafusion_err)?;

        // TODO send back some useful statistics
        let empty_plan = EmptyExec::new(false, self.schema());
        Ok(empty_plan.execute(0).await?)
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

#[derive(Debug)]
/// Writes the partitioned input data into separate batches
/// and forwards the add actions as record batches
struct WritePartitionCommand {
    table_uri: String,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl ExecutionPlan for WritePartitionCommand {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "serialized",
            DataType::Utf8,
            true,
        )]))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(&self, partition: usize) -> DataFusionResult<SendableRecordBatchStream> {
        println!("WritePartitionCommand -> partition {:?}", partition);

        let table = open_table(&self.table_uri)
            .await
            .map_err(to_datafusion_err)?;

        let data = collect_batch(self.input.execute(partition).await?).await?;
        if data.is_empty() {
            return Err(DataFusionError::Plan(
                DeltaCommandError::EmptyPartition("no data".to_string()).to_string(),
            ));
        }

        let mut writer =
            DeltaWriter::for_table(&table, HashMap::new()).map_err(to_datafusion_err)?;
        for batch in data {
            // TODO we should have an API that allows us to circumvent internal partitioning
            writer.write(&batch).map_err(to_datafusion_err)?;
        }
        let json_adds = writer
            .flush()
            .await
            .map_err(to_datafusion_err)?
            .iter()
            .map(|e| serde_json::to_value(Action::add(e.clone())).unwrap())
            .collect::<Vec<_>>();

        let serialized = StringArray::from(
            json_adds
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()
                .map_err(to_datafusion_err)?,
        );
        let serialized_batch = RecordBatch::try_new(self.schema(), vec![Arc::new(serialized)])?;

        let stream = SizedRecordBatchStream::new(self.schema(), vec![Arc::new(serialized_batch)]);

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

fn to_datafusion_err(e: impl std::error::Error) -> DataFusionError {
    DataFusionError::Plan(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::{
        divide_by_partition_values,
        test_utils::{create_initialized_table, get_record_batch},
    };
    use datafusion::physical_plan::{collect, memory::MemoryExec};

    #[tokio::test]
    async fn test_write_partition_writer() {
        let mut table = create_initialized_table(&vec!["modified".to_string()]).await;
        assert_eq!(table.version, 0);

        let batch = get_record_batch(None, false);
        let schema = batch.schema();
        let divided =
            divide_by_partition_values(schema.clone(), vec!["modified".to_string()], &batch)
                .unwrap();
        let mut partitions = Vec::new();
        for part in divided {
            partitions.push(vec![part.record_batch]);
        }

        let plan = MemoryExec::try_new(partitions.as_slice(), schema.clone(), None).unwrap();
        let command = Arc::new(WriteCommand::new(
            table.table_uri.to_string(),
            WriteMode::Append,
            PartitionWriteMode::Distributed(Arc::new(plan)),
        ));

        let _ = collect(command).await.unwrap();

        table.update().await.unwrap();
        assert_eq!(table.version, 1);

        let files = table.get_file_uris();
        assert_eq!(files.len(), 2);
    }
}
