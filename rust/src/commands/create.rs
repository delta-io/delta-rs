//! Command for creating a new delta table
// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/CreateDeltaTableCommand.scala
use super::{
    get_table_from_uri_without_update, to_datafusion_err, transaction::serialize_actions,
    DeltaCommandError,
};
use crate::{
    action::{Action, MetaData, Protocol, SaveMode},
    DeltaTableMetaData, Schema,
};
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{
        common::{compute_record_batch_statistics, SizedRecordBatchStream},
        Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use std::sync::Arc;

/// Command for creating ne delta table
pub struct CreateCommand {
    table_uri: String,
    mode: SaveMode,
    metadata: DeltaTableMetaData,
    protocol: Protocol,
}

impl CreateCommand {
    /// Create new CreateCommand
    pub fn new(
        table_uri: String,
        mode: SaveMode,
        metadata: DeltaTableMetaData,
        protocol: Protocol,
    ) -> Self {
        Self {
            table_uri,
            mode,
            metadata,
            protocol,
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

    async fn execute(&self, _partition: usize) -> DataFusionResult<SendableRecordBatchStream> {
        let mut table =
            get_table_from_uri_without_update(self.table_uri.clone()).map_err(to_datafusion_err)?;

        let actions = match table.load_version(0).await {
            Err(_) => Ok(vec![
                Action::protocol(self.protocol.clone()),
                Action::metaData(MetaData::try_from(self.metadata.clone()).unwrap()),
            ]),
            Ok(_) => match self.mode {
                SaveMode::Ignore => Ok(Vec::new()),
                _ => Err(DeltaCommandError::TableAlreadyExists(self.table_uri.clone())),
            },
        }
        .map_err(to_datafusion_err)?;

        let serialized_batch = serialize_actions(actions)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        action::{DeltaOperation, Protocol},
        commands::transaction::DeltaTransactionPlan,
        open_table, DeltaTableMetaData,
    };
    use datafusion::physical_plan::collect;
    use std::collections::HashMap;

    #[tokio::test]
    async fn create_table_without_partitions() {
        let table_schema = crate::write::test_utils::get_delta_schema();
        let metadata =
            DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let table_uri = table_path.to_str().unwrap().to_string();

        let op = DeltaOperation::Create {
            location: table_uri.clone(),
            metadata: metadata.clone(),
        };

        let transaction = Arc::new(DeltaTransactionPlan::new(
            table_uri.clone(),
            Arc::new(CreateCommand::new(
                table_uri.clone(),
                SaveMode::Ignore,
                metadata,
                protocol,
            )),
            op,
            None,
        ));
        let _ = collect(transaction).await.unwrap();

        assert!(table_path.exists());
        let log_path = table_path.join("_delta_log/00000000000000000000.json");
        assert!(log_path.exists());
        assert!(log_path.is_file());

        let table = open_table(&table_uri).await.unwrap();
        assert_eq!(table.version, 0);
    }
}
