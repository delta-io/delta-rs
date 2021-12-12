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
use crate::open_table;
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{
        collect, empty::EmptyExec, Distribution, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
};
use std::sync::Arc;

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
    // mode: WriteMode,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl ExecutionPlan for WriteCommand {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // if self.preserve_partitioning {
        //     self.input.output_partitioning()
        // } else {
        //     Partitioning::UnknownPartitioning(1)
        // }
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        // if self.preserve_partitioning {
        //     Distribution::UnspecifiedDistribution
        // } else {
        //     Distribution::SinglePartition
        // }
        Distribution::SinglePartition
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

    async fn execute(&self, _partition: usize) -> DataFusionResult<SendableRecordBatchStream> {
        let mut table = open_table(&self.table_uri)
            .await
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
        let table_exists = check_table_exists(&table)
            .await
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        if !table_exists {
            todo!()
            // let delta_schema = Schema::try_from(self.inputs[0].schema())?;
            //
            // // TODO make meta data configurable and get partitions from somewhere
            // let metadata = DeltaTableMetaData::new(
            //     None,
            //     None,
            //     None,
            //     delta_schema,
            //     self.partition_columns.clone().unwrap_or_else(|| vec![]),
            //     HashMap::new(),
            // );
            //
            // let protocol = Protocol {
            //     min_reader_version: 1,
            //     min_writer_version: 2,
            // };
            //
            // let command = CreateCommand::new(metadata, protocol);
            // command.execute(table).await?;
        }

        let data = collect(self.input.clone()).await.unwrap();
        let schema = data[0].schema();
        let mut txn = table.create_transaction(None);

        txn.write_files(data).await.unwrap();

        let _ = txn
            .commit(None)
            .await
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        // TODO 
        let empty_plan = EmptyExec::new(false, schema);
        Ok(empty_plan
            .execute(0)
            .await
            .map_err(|e| DataFusionError::Plan(e.to_string()))?)
    }

    fn statistics(&self) -> Statistics {
        // TODO
        self.input.statistics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test_utils::get_record_batch;
    use datafusion::physical_plan::{collect, memory::MemoryExec};

    #[tokio::test]
    async fn write_and_create_table() {
        let batch = get_record_batch(None, false);
        let schema = batch.schema();
        let plan = MemoryExec::try_new(vec![vec![batch]].as_slice(), schema, None).unwrap();

        // let table = create_initialized_table(&vec!["modified".to_string()]).await;
        let command = Arc::new(WriteCommand {
            table_uri: "/home/robstar/github/delta-rs/data".to_string(),
            input: Arc::new(plan),
            // mode: WriteMode::Append,
        });

        let _results = collect(command).await.unwrap();
    }
}
