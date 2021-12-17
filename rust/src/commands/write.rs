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
use super::{create::CreateCommand, transaction::serialize_actions, *};
use crate::{
    action::{Action, SaveMode},
    write::DeltaWriter,
    Schema,
};
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::datatypes::{
        DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
    },
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{
        common::{
            collect as collect_batch, compute_record_batch_statistics, SizedRecordBatchStream,
        },
        Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use std::collections::HashMap;
use std::sync::Arc;

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
    pub fn new(
        table_uri: String,
        mode: SaveMode,
        partition_columns: Option<Vec<String>>,
        predicate: Option<String>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let plan = Arc::new(WritePartitionCommand::new(
            table_uri.clone(),
            mode.clone(),
            partition_columns.clone(),
            None,
            input.clone(),
        ));
        Self {
            table_uri,
            mode,
            partition_columns,
            predicate,
            schema: input.schema(),
            input: plan,
        }
    }
}

#[async_trait]
impl ExecutionPlan for WriteCommand {
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
        Partitioning::UnknownPartitioning(1)
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

    async fn execute(&self, _partition: usize) -> DataFusionResult<SendableRecordBatchStream> {
        let mut table =
            get_table_from_uri_without_update(self.table_uri.clone()).map_err(to_datafusion_err)?;

        let mut actions = match table.load().await {
            // Table is not yet created
            Err(_) => {
                let schema = Schema::try_from(self.schema.clone()).map_err(to_datafusion_err)?;
                let metadata = DeltaTableMetaData::new(
                    None,
                    None,
                    None,
                    schema,
                    self.partition_columns.clone().unwrap_or_default(),
                    HashMap::new(),
                );
                // TODO get the protocol from somewhere central
                let protocol = Protocol {
                    min_reader_version: 1,
                    min_writer_version: 2,
                };
                let plan = Arc::new(CreateCommand::new(
                    self.table_uri.clone(),
                    SaveMode::ErrorIfExists,
                    metadata.clone(),
                    protocol,
                ));

                let create_actions = collect(plan.clone()).await?;

                Ok(create_actions)
            }
            Ok(_) => match self.mode {
                SaveMode::ErrorIfExists => Err(DeltaCommandError::TableAlreadyExists(
                    self.table_uri.clone(),
                )),
                SaveMode::Ignore => {
                    return Ok(Box::pin(SizedRecordBatchStream::new(self.schema(), vec![])))
                }
                _ => Ok(vec![]),
            },
        }
        .map_err(to_datafusion_err)?;

        actions.append(&mut collect(self.input.clone()).await?);
        println!("{:?}", self.predicate);

        let stream = SizedRecordBatchStream::new(
            self.schema(),
            actions
                .iter()
                .map(|b| Arc::new(b.to_owned()))
                .collect::<Vec<_>>(),
        );
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

#[derive(Debug)]
/// Writes the partitioned input data into separate batches
/// and forwards the add actions as record batches
pub(crate) struct WritePartitionCommand {
    table_uri: String,
    /// The save mode used in operation
    mode: SaveMode,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    /// When using `Overwrite` mode, replace data that matches a predicate
    predicate: Option<String>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl WritePartitionCommand {
    pub fn new(
        table_uri: String,
        mode: SaveMode,
        partition_columns: Option<Vec<String>>,
        predicate: Option<String>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            table_uri,
            mode,
            partition_columns,
            predicate,
            input,
        }
    }
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
        println!(
            "{:?} {:?} {:?}",
            self.mode, self.partition_columns, self.predicate
        );
        // let table = open_table(&self.table_uri)
        //     .await
        //     .map_err(to_datafusion_err)?;

        let data = collect_batch(self.input.execute(partition).await?).await?;
        if data.is_empty() {
            return Err(DataFusionError::Plan(
                DeltaCommandError::EmptyPartition("no data".to_string()).to_string(),
            ));
        }
        let mut writer = DeltaWriter::try_new(
            self.table_uri.clone(),
            self.input.schema(),
            self.partition_columns.clone(),
            None,
        )
        .map_err(to_datafusion_err)?;

        for batch in data {
            // TODO we should have an API that allows us to circumvent internal partitioning
            writer.write(&batch).map_err(to_datafusion_err)?;
        }
        let json_adds = writer
            .flush()
            .await
            .map_err(to_datafusion_err)?
            .iter()
            .map(|e| Action::add(e.clone()))
            .collect::<Vec<_>>();

        let serialized_batch = serialize_actions(json_adds)?;
        let stream = SizedRecordBatchStream::new(
            serialized_batch.schema(),
            vec![Arc::new(serialized_batch)],
        );

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

// #[cfg(test)]
// mod tests {
//     #[tokio::test]
//     async fn test_append_data() {
//         todo!()
//     }
// }
