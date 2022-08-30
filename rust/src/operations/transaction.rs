//! Wrapper Execution plan to handle distributed operations
use super::*;
use crate::action::Action;
use crate::schema::DeltaDataTypeVersion;
use core::any::Any;
use datafusion::{
    arrow::{
        array::StringArray,
        datatypes::{
            DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
        },
        record_batch::RecordBatch,
    },
    error::Result as DataFusionResult,
    execution::context::TaskContext,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, common::compute_record_batch_statistics,
        empty::EmptyExec, expressions::PhysicalSortExpr, stream::RecordBatchStreamAdapter,
        Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use futures::{TryFutureExt, TryStreamExt};
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    /// Schema expected for plans wrapped by transaction
    pub static ref OPERATION_SCHEMA: ArrowSchema =
        ArrowSchema::new(vec![ArrowField::new("serialized", DataType::Utf8, false,)]);
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
    table_version: DeltaDataTypeVersion,
    input: Arc<dyn ExecutionPlan>,
    operation: DeltaOperation,
    app_metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

impl DeltaTransactionPlan {
    /// Wrap partitioned delta operations in a DeltaTransaction
    pub fn new<T>(
        table_uri: T,
        table_version: DeltaDataTypeVersion,
        input: Arc<dyn ExecutionPlan>,
        operation: DeltaOperation,
        app_metadata: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Self
    where
        T: Into<String>,
    {
        Self {
            table_uri: table_uri.into(),
            table_version,
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                do_transaction(
                    self.input.clone(),
                    self.table_uri.clone(),
                    self.table_version,
                    self.operation.clone(),
                    self.app_metadata.clone(),
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

async fn do_transaction(
    input: Arc<dyn ExecutionPlan>,
    table_uri: String,
    table_version: i64,
    operation: DeltaOperation,
    app_metadata: Option<serde_json::Map<String, serde_json::Value>>,
    context: Arc<TaskContext>,
) -> DataFusionResult<SendableRecordBatchStream> {
    let mut table = DeltaTableBuilder::from_uri(table_uri)
        .build()
        .map_err(to_datafusion_err)?;
    let schema = input.schema().clone();

    let data = collect(input, context.clone()).await?;
    // TODO we assume that all children send a single column record batch with serialized actions
    let actions = data
        .iter()
        .flat_map(|batch| match deserialize_actions(batch) {
            Ok(vec) => vec.into_iter().map(Ok).collect(),
            Err(er) => vec![Err(er)],
        })
        .collect::<Result<Vec<_>, _>>()?;

    if actions.is_empty() {
        let empty_plan = EmptyExec::new(false, schema);
        return empty_plan.execute(0, context);
    }

    let mut txn = table.create_transaction(None);
    txn.add_actions(actions);
    let prepared_commit = txn
        .prepare_commit(Some(operation.clone()), app_metadata.clone())
        .await
        .map_err(to_datafusion_err)?;
    let _committed_version = table
        .try_commit_transaction(&prepared_commit, table_version + 1)
        .await
        .map_err(to_datafusion_err)?;

    // TODO report some helpful data - at least current version
    let empty_plan = EmptyExec::new(false, schema);
    empty_plan.execute(0, context)
}

fn deserialize_actions(data: &RecordBatch) -> DataFusionResult<Vec<Action>> {
    let serialized_actions = arrow::array::as_string_array(data.column(0));
    serialized_actions
        .iter()
        .map(|val| serde_json::from_str::<Action>(val.unwrap_or("")))
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_datafusion_err)
}
