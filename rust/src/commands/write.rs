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
use super::{transaction::serialize_actions, *};
use crate::{action::Action, write::DeltaWriter};
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

#[derive(Debug)]
/// Writes the partitioned input data into separate batches
/// and forwards the add actions as record batches
pub(crate) struct WritePartitionCommand {
    table_uri: String,
    /// The save mode used in operation
    mode: action::SaveMode,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl WritePartitionCommand {
    pub fn new(table_uri: String, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            table_uri,
            mode: action::SaveMode::Append,
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
        println!("{:?}", self.mode);
        let table = open_table(&self.table_uri).await.map_err(to_datafusion_err)?;

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
