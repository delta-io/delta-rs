//! Wrapper Execution plan to handle distributed operations
use super::*;
use crate::action::Action;
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::{
        array::Array,
        datatypes::{
            DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
        },
    },
    error::Result as DataFusionResult,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, common::compute_record_batch_statistics,
        empty::EmptyExec, Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    /// Schema expected form plans wrapped by transaction
    pub static ref OPERATION_SCHEMA: ArrowSchema =
        ArrowSchema::new(vec![ArrowField::new("serialized", DataType::Utf8, true,)]);
}

/// Write command
#[derive(Debug)]
pub struct DeltaTransactionPlan {
    table_uri: String,
    input: Arc<dyn ExecutionPlan>,
}

impl DeltaTransactionPlan {
    /// Wrap partitioned delta operations in a DeltaTransaction
    pub fn new(table_uri: String, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            table_uri,
            input: Arc::new(CoalescePartitionsExec::new(input)),
        }
    }

    /// Arrow schema expected to be produced by wrapped operations
    pub fn input_operation_schema() -> ArrowSchemaRef {
        Arc::new(OPERATION_SCHEMA.clone())
    }
}

#[async_trait]
impl ExecutionPlan for DeltaTransactionPlan {
    /// Return a reference to Any that can be used for downcasting
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
        let mut table = open_table(&self.table_uri)
            .await
            .map_err(to_datafusion_err)?;
        let table_exists = check_table_exists(&table)
            .await
            .map_err(to_datafusion_err)?;

        if !table_exists {
            todo!()
        }

        let mut txn = table.create_transaction(None);

        let mut actions = Vec::new();
        let data = collect(self.input.clone()).await?;
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
        txn.commit(None).await.map_err(to_datafusion_err)?;

        let empty_plan = EmptyExec::new(false, self.schema());
        Ok(empty_plan.execute(0).await?)
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}
