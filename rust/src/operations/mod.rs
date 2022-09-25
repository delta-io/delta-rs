//! High level delta commands that can be executed against a delta table
// TODO
// - rename to delta operations
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use crate::{
    action::{DeltaOperation, Protocol, SaveMode},
    builder::DeltaTableBuilder,
    open_table,
    operations::{create::CreateCommand, transaction::DeltaTransactionPlan, write::WriteCommand},
    writer::{record_batch::divide_by_partition_values, utils::PartitionPath},
    DeltaTable, DeltaTableError, DeltaTableMetaData,
};

use arrow::{datatypes::SchemaRef as ArrowSchemaRef, error::ArrowError, record_batch::RecordBatch};
use datafusion::{
    physical_plan::{collect, memory::MemoryExec, ExecutionPlan},
    prelude::SessionContext,
};
use datafusion_common::DataFusionError;

pub mod create;
pub mod transaction;
pub mod write;

type DeltaCommandResult<T> = Result<T, DeltaCommandError>;

/// Enum representing an error when calling [DeltaCommands.execute].
#[derive(thiserror::Error, Debug)]
pub enum DeltaCommandError {
    /// Error returned when some data is expected but only an empty dataset is provided.
    #[error("Received empty data partition {0}")]
    EmptyPartition(String),

    /// Error returned when the provided command is not supported
    #[error("Command not available {0}")]
    UnsupportedCommand(String),

    /// Error returned when the table requires an unsupported writer version
    #[error("Delta-rs does not support writer version {0}")]
    UnsupportedWriterVersion(i32),

    /// Error returned when the table to be created already exists
    #[error("Table: '{0}' already exists")]
    TableAlreadyExists(String),

    /// Error returned when errors occur in underlying delta table instance
    #[error("DeltaTable error: {} ({:?})", source, source)]
    DeltaTable {
        /// Raw internal DeltaTableError
        #[from]
        source: DeltaTableError,
    },

    /// Error returned when errors occur in Arrow
    #[error("Arrow error: {} ({:?})", source, source)]
    Arrow {
        /// Raw internal ArrowError
        #[from]
        source: ArrowError,
    },

    /// Error returned for errors internal to Datafusion
    #[error("Datafusion error: {} ({:?})", source, source)]
    DataFusion {
        /// Raw internal DataFusionError
        source: DataFusionError,
    },

    /// Error returned for errors internal to Datafusion
    #[error("ObjectStore error: {} ({:?})", source, source)]
    ObjectStore {
        /// Raw internal DataFusionError
        #[from]
        source: object_store::Error,
    },
}

impl From<DataFusionError> for DeltaCommandError {
    fn from(err: DataFusionError) -> Self {
        match err {
            DataFusionError::ArrowError(source) => DeltaCommandError::Arrow { source },
            DataFusionError::ObjectStore(source) => DeltaCommandError::ObjectStore { source },
            source => DeltaCommandError::DataFusion { source },
        }
    }
}

fn to_datafusion_err(e: impl std::error::Error) -> DataFusionError {
    DataFusionError::Plan(e.to_string())
}

/// High level interface for executing commands against a DeltaTable
pub struct DeltaCommands {
    table: DeltaTable,
}

impl DeltaCommands {
    /// load table from uri
    pub async fn try_from_uri<T>(uri: T) -> DeltaCommandResult<Self>
    where
        T: Into<String>,
    {
        let table_uri: String = uri.into();
        let table = if let Ok(tbl) = open_table(&table_uri).await {
            Ok(tbl)
        } else {
            DeltaTableBuilder::from_uri(table_uri).build()
        }?;
        Ok(Self { table })
    }

    /// Get a reference to the underlying table
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    async fn execute(
        &mut self,
        operation: DeltaOperation,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DeltaCommandResult<()> {
        let transaction = Arc::new(DeltaTransactionPlan::new(
            self.table.table_uri(),
            self.table.version(),
            plan,
            operation,
            None,
        ));

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let _ = collect(transaction, task_ctx).await?;
        self.table.update().await?;

        Ok(())
    }

    /// Create a new Delta table
    pub async fn create(
        &mut self,
        metadata: DeltaTableMetaData,
        mode: SaveMode,
    ) -> DeltaCommandResult<()> {
        let operation = DeltaOperation::Create {
            mode,
            metadata: metadata.clone(),
            location: self.table.table_uri(),
            // TODO get the protocol from somewhere central
            protocol: Protocol {
                min_reader_version: 1,
                min_writer_version: 1,
            },
        };
        let plan = Arc::new(CreateCommand::try_new(
            self.table.table_uri(),
            operation.clone(),
        )?);

        self.execute(operation, plan).await
    }

    /// Write data to Delta table
    pub async fn write(
        &mut self,
        data: Vec<RecordBatch>,
        mode: SaveMode,
        partition_columns: Option<Vec<String>>,
    ) -> DeltaCommandResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let schema = data[0].schema();
        if let Ok(meta) = self.table.get_metadata() {
            let curr_schema: ArrowSchemaRef = Arc::new((&meta.schema).try_into()?);
            if schema != curr_schema {
                return Err(DeltaCommandError::UnsupportedCommand(
                    "Updating table schema not yet implemented".to_string(),
                ));
            }
            if let Some(cols) = partition_columns.as_ref() {
                if cols != &meta.partition_columns {
                    return Err(DeltaCommandError::UnsupportedCommand(
                        "Updating table partitions not yet implemented".to_string(),
                    ));
                }
            };
        };
        let data = if let Some(cols) = partition_columns.as_ref() {
            // TODO partitioning should probably happen in its own plan ...
            let mut partitions: HashMap<String, Vec<RecordBatch>> = HashMap::new();
            for batch in data {
                let divided =
                    divide_by_partition_values(schema.clone(), cols.clone(), &batch).unwrap();
                for part in divided {
                    let key = PartitionPath::from_hashmap(cols, &part.partition_values)
                        .map_err(DeltaTableError::from)?
                        .into();
                    match partitions.get_mut(&key) {
                        Some(batches) => {
                            batches.push(part.record_batch);
                        }
                        None => {
                            partitions.insert(key, vec![part.record_batch]);
                        }
                    }
                }
            }
            partitions.into_values().collect::<Vec<_>>()
        } else {
            vec![data]
        };

        let operation = DeltaOperation::Write {
            mode,
            partition_by: partition_columns.clone(),
            predicate: None,
        };
        let data_plan = Arc::new(MemoryExec::try_new(&data, schema, None)?);
        let plan = Arc::new(WriteCommand::try_new(
            self.table.table_uri(),
            operation.clone(),
            data_plan,
        )?);
        self.execute(operation, plan).await
    }
}

impl From<DeltaTable> for DeltaCommands {
    fn from(table: DeltaTable) -> Self {
        Self { table }
    }
}

impl From<DeltaCommands> for DeltaTable {
    fn from(comm: DeltaCommands) -> Self {
        comm.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        open_table,
        writer::test_utils::{create_initialized_table, get_delta_schema, get_record_batch},
    };

    #[tokio::test]
    async fn test_create_command() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let table_uri = table_path.to_str().unwrap().to_string();

        let mut commands = DeltaCommands::try_from_uri(table_uri.clone())
            .await
            .unwrap();

        let table_schema = get_delta_schema();
        let metadata =
            DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());

        commands
            .create(metadata.clone(), SaveMode::Ignore)
            .await
            .unwrap();

        let table = open_table(&table_uri).await.unwrap();
        assert_eq!(table.version(), 0);

        let res = commands.create(metadata, SaveMode::ErrorIfExists).await;
        assert!(res.is_err())
    }

    #[tokio::test]
    async fn test_write_command() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let mut table = create_initialized_table(&partition_cols).await;
        assert_eq!(table.version(), 0);

        let mut commands = DeltaCommands::try_from_uri(table.table_uri())
            .await
            .unwrap();

        commands
            .write(
                vec![batch],
                SaveMode::Append,
                Some(vec!["modified".to_string()]),
            )
            .await
            .unwrap();

        table.update().await.unwrap();
        assert_eq!(table.version(), 1);

        let files = table.get_file_uris();
        assert_eq!(files.count(), 2)
    }

    #[tokio::test]
    async fn test_create_and_write_command() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let table_uri = table_path.to_str().unwrap().to_string();

        let mut commands = DeltaCommands::try_from_uri(&table_uri).await.unwrap();

        let batch = get_record_batch(None, false);
        commands
            .write(
                vec![batch],
                SaveMode::Append,
                Some(vec!["modified".to_string()]),
            )
            .await
            .unwrap();

        let table = open_table(&table_uri).await.unwrap();
        assert_eq!(table.version(), 0);

        let files = table.get_file_uris();
        assert_eq!(files.count(), 2)
    }
}
