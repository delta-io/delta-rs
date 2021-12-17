//! Wrapper Execution plan to handle distributed operations
use super::*;
use crate::action::Action;
// use crate::commands::create::CreateCommand;
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::{
        array::{Array, StringArray},
        datatypes::{
            DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
        },
        record_batch::RecordBatch,
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

pub(crate) fn serialize_actions(actions: Vec<Action>) -> DataFusionResult<RecordBatch> {
    let serialized = StringArray::from(
        actions
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_datafusion_err)?,
    );
    Ok(RecordBatch::try_new(
        Arc::new(OPERATION_SCHEMA.clone()),
        vec![Arc::new(serialized)],
    )?)
}

/// Write command
#[derive(Debug)]
pub struct DeltaTransactionPlan {
    table_uri: String,
    input: Arc<dyn ExecutionPlan>,
    operation: DeltaOperation,
    app_metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

impl DeltaTransactionPlan {
    /// Wrap partitioned delta operations in a DeltaTransaction
    pub fn new(
        table_uri: String,
        input: Arc<dyn ExecutionPlan>,
        operation: DeltaOperation,
        app_metadata: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        Self {
            table_uri,
            input: Arc::new(CoalescePartitionsExec::new(input)),
            operation,
            app_metadata,
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
        let mut table =
            get_table_from_uri_without_update(self.table_uri.clone()).map_err(to_datafusion_err)?;

        let mut actions = Vec::new();
        let data = collect(self.input.clone()).await?;
        for batch in data {
            // TODO we assume that all children send a single column record batch with serialized actions
            let mut new_actions = deserialize_actions(&batch)?;
            actions.append(&mut new_actions);
        }

        if actions.is_empty() {
            let empty_plan = EmptyExec::new(false, self.schema());
            return empty_plan.execute(0).await;
        }

        let _new_version = match table.update().await {
            Err(_) => {
                let mut txn = table.create_transaction(None);
                txn.add_actions(actions);
                let prepared_commit = txn
                    .prepare_commit_with_info(
                        Some(self.operation.clone()),
                        self.app_metadata.clone(),
                    )
                    .await
                    .map_err(to_datafusion_err)?;
                let committed_version = table
                    .try_commit_transaction(&prepared_commit, 0)
                    .await
                    .map_err(to_datafusion_err)?;
                committed_version
            }
            _ => {
                let mut txn = table.create_transaction(None);
                txn.add_actions(actions);
                let committed_version = txn
                    .commit_with_info(Some(self.operation.clone()), self.app_metadata.clone())
                    .await
                    .map_err(to_datafusion_err)?;
                committed_version
            }
        };

        // TODO report some helpful data - at least current version
        let empty_plan = EmptyExec::new(false, self.schema());
        empty_plan.execute(0).await
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

fn deserialize_actions(data: &RecordBatch) -> DataFusionResult<Vec<Action>> {
    let serialized_actions = arrow::array::as_string_array(data.column(0));
    (0..serialized_actions.len())
        .map(|idx| serde_json::from_str::<Action>(serialized_actions.value(idx)))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_datafusion_err)
}
