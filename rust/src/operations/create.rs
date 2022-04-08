//! Command for creating a new delta table
// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/CreateDeltaTableCommand.scala
use super::{
    get_table_from_uri_without_update, to_datafusion_err, transaction::serialize_actions,
    DeltaCommandError,
};
use crate::{
    action::{Action, DeltaOperation, MetaData, Protocol, SaveMode},
    DeltaTableMetaData, Schema,
};
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::TaskContext,
    physical_plan::{
        common::{compute_record_batch_statistics, SizedRecordBatchStream},
        expressions::PhysicalSortExpr,
        metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics},
        Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use std::sync::Arc;

/// Command for creating new delta table
pub struct CreateCommand {
    table_uri: String,
    mode: SaveMode,
    metadata: DeltaTableMetaData,
    protocol: Protocol,
}

impl CreateCommand {
    /// Create new CreateCommand
    pub fn try_new<T>(table_uri: T, operation: DeltaOperation) -> Result<Self, DeltaCommandError>
    where
        T: Into<String>,
    {
        match operation {
            DeltaOperation::Create {
                metadata,
                mode,
                protocol,
                ..
            } => Ok(Self {
                table_uri: table_uri.into(),
                mode,
                metadata,
                protocol,
            }),
            _ => Err(DeltaCommandError::UnsupportedCommand(
                "WriteCommand only implemented for write operation".to_string(),
            )),
        }
    }
}

impl std::fmt::Debug for CreateCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateCommand")
    }
}

#[async_trait]
impl ExecutionPlan for CreateCommand {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(
            <ArrowSchema as TryFrom<&Schema>>::try_from(&self.metadata.schema.clone())
                .expect("SChema must be valid"),
        )
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_child_distribution(&self) -> Distribution {
        // TODO
        Distribution::SinglePartition
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "SortExec wrong number of children".to_string(),
        ))
    }

    async fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut table =
            get_table_from_uri_without_update(self.table_uri.clone()).map_err(to_datafusion_err)?;

        let actions = match table.load_version(0).await {
            Err(_) => Ok(vec![
                Action::protocol(self.protocol.clone()),
                Action::metaData(MetaData::try_from(self.metadata.clone()).unwrap()),
            ]),
            Ok(_) => match self.mode {
                SaveMode::Ignore => Ok(Vec::new()),
                SaveMode::ErrorIfExists => Err(DeltaCommandError::TableAlreadyExists(
                    self.table_uri.clone(),
                )),
                _ => todo!("Write mode not implemented {:?}", self.mode),
            },
        }
        .map_err(to_datafusion_err)?;

        let serialized_batch = serialize_actions(actions)?;
        let metrics = ExecutionPlanMetricsSet::new();
        let tracking_metrics = MemTrackingMetrics::new(&metrics, partition);
        let stream = SizedRecordBatchStream::new(
            serialized_batch.schema(),
            vec![Arc::new(serialized_batch)],
            tracking_metrics,
        );

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&[], &self.schema(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        action::{DeltaOperation, Protocol},
        open_table,
        operations::transaction::DeltaTransactionPlan,
        DeltaTableMetaData,
    };
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    use std::collections::HashMap;
    use std::process::Command;

    #[tokio::test]
    async fn create_table_without_partitions() {
        let table_schema = crate::writer::test_utils::get_delta_schema();
        let metadata =
            DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let table_uri = table_path.to_str().unwrap().to_string();

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let transaction = get_transaction(table_uri.clone(), metadata.clone(), SaveMode::Ignore);
        let _ = collect(transaction.clone(), task_ctx.clone())
            .await
            .unwrap();

        let table_path = std::path::Path::new(&table_uri);
        let log_path = table_path.join("_delta_log/00000000000000000000.json");
        assert!(log_path.exists());

        let mut table = open_table(&table_uri).await.unwrap();
        assert_eq!(table.version, 0);

        // Check we can create an existing table with ignore
        let ts1 = table.get_version_timestamp(0).await.unwrap();
        let mut child = Command::new("sleep").arg("1").spawn().unwrap();
        let _result = child.wait().unwrap();
        let _ = collect(transaction, task_ctx.clone()).await.unwrap();
        let mut table = open_table(&table_uri).await.unwrap();
        assert_eq!(table.version, 0);
        let ts2 = table.get_version_timestamp(0).await.unwrap();
        assert_eq!(ts1, ts2);

        // Check error for ErrorIfExists mode
        let transaction = get_transaction(table_uri, metadata, SaveMode::ErrorIfExists);
        let result = collect(transaction.clone(), task_ctx).await;
        assert!(result.is_err())
    }

    fn get_transaction(
        table_uri: String,
        metadata: DeltaTableMetaData,
        mode: SaveMode,
    ) -> Arc<DeltaTransactionPlan> {
        let op = DeltaOperation::Create {
            location: table_uri.clone(),
            metadata: metadata.clone(),
            mode: mode.clone(),
            protocol: Protocol {
                min_reader_version: 1,
                min_writer_version: 2,
            },
        };

        let transaction = Arc::new(DeltaTransactionPlan::new(
            table_uri.clone(),
            Arc::new(CreateCommand::try_new(table_uri.clone(), op.clone()).unwrap()),
            op,
            None,
        ));

        transaction
    }
}
