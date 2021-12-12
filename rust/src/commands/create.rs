//! Command for creating a new delta table
// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/CreateDeltaTableCommand.scala
use crate::{
    action::Protocol, get_backend_for_uri_with_options, DeltaTable, DeltaTableConfig,
    DeltaTableMetaData, Schema,
};
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use core::any::Any;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{
        common::compute_record_batch_statistics, empty::EmptyExec, Distribution, ExecutionPlan,
        Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use serde_json::{Map as JsonMap, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// The save mode when creating new table.
pub enum TableCreationMode {
    /// Create a new table that does not exist
    Create,
    /// Replace an existing table
    Replace,
    /// Create a new table replacing an existing one
    CrateOrReplace,
}

/// Command for creating ne delta table
pub struct CreateCommand {
    table_uri: String,
    // mode: TableCreationMode,
    metadata: DeltaTableMetaData,
    protocol: Protocol,
    commit_info: JsonMap<String, Value>,
}

impl CreateCommand {
    /// Create new CreateCommand
    pub fn new(table_uri: String, metadata: DeltaTableMetaData, protocol: Protocol) -> Self {
        Self {
            table_uri,
            metadata,
            protocol,
            commit_info: JsonMap::<String, Value>::new(),
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
        let backend = get_backend_for_uri_with_options(&self.table_uri, HashMap::new())
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
        let mut table = DeltaTable::new(&self.table_uri, backend, DeltaTableConfig::default())
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        let mut commit_info = self.commit_info.clone();
        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String("CREATE TABLE".to_string()),
        );
        table
            .create(
                self.metadata.clone(),
                self.protocol.clone(),
                Some(commit_info),
            )
            .await
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        let arrow_schema =
            <ArrowSchema as TryFrom<&Schema>>::try_from(&self.metadata.schema.clone())?;
        let empty_plan = EmptyExec::new(false, Arc::new(arrow_schema));
        Ok(empty_plan
            .execute(0)
            .await
            .map_err(|e| DataFusionError::Plan(e.to_string()))?)
    }

    fn statistics(&self) -> Statistics {
        compute_record_batch_statistics(&vec![], &self.schema(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{action::Protocol, DeltaTableMetaData, open_table};
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

        let table_path = tempfile::tempdir().unwrap();
        let table_uri = table_path.path().to_str().unwrap().to_string();

        let command = Arc::new(CreateCommand::new(table_uri.clone(), metadata, protocol));
        let _ = collect(command).await.unwrap();

        assert!(table_path.path().exists());
        let log_path = table_path
            .path()
            .join("_delta_log/00000000000000000000.json");
        assert!(log_path.exists());
        assert!(log_path.is_file());

        let table = open_table(&table_uri).await.unwrap();
        assert_eq!(table.version, 0);
    }
}
