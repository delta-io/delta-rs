//! High level delta commands that can be executed against a delta table
use crate::{
    action::{self, DeltaOperation, Protocol},
    commands::{
        create::CreateCommand, transaction::DeltaTransactionPlan, write::WritePartitionCommand,
    },
    get_backend_for_uri_with_options, open_table,
    storage::StorageError,
    write::{divide_by_partition_values, DeltaWriter, DeltaWriterError},
    DeltaTable, DeltaTableConfig, DeltaTableError, DeltaTableMetaData,
};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use datafusion::{
    error::DataFusionError,
    physical_plan::{collect, memory::MemoryExec, ExecutionPlan},
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub mod create;
pub mod transaction;
pub mod write;

type DeltaCommandResult<T> = Result<T, DeltaCommandError>;

/// Enum representing an error when calling [`DeltaCommandExec`].
#[derive(thiserror::Error, Debug)]
pub enum DeltaCommandError {
    /// Error returned when the table to be created already exists
    #[error("Received empty data partition {0}")]
    EmptyPartition(String),

    /// Error returned when the table to be created already exists
    #[error("Command not available {0}")]
    UnsupportedCommand(String),

    /// Error returned when the table to be created already exists
    #[error("Table: '{0}' already exists")]
    TableAlreadyExists(String),

    /// Error returned when errors occur in underlying delta table instance
    #[error("Error in underlying DeltaTable")]
    DeltaTableError {
        /// Raw internal DeltaTableError
        #[from]
        source: DeltaTableError,
    },

    /// Errors occurring inside the DeltaWriter modules
    #[error("Error in underlying storage backend")]
    DeltaWriterError {
        /// Raw internal StorageError
        #[from]
        source: DeltaWriterError,
    },

    /// Error returned when errors occur in underlying storage instance
    #[error("Error in underlying storage backend")]
    StorageError {
        /// Raw internal StorageError
        #[from]
        source: StorageError,
    },

    /// Error returned when errors occur in underlying storage instance
    #[error("Error handling arrow data")]
    ArrowError {
        /// Raw internal StorageError
        #[from]
        source: ArrowError,
    },

    /// Error returned for errors internal to Datafusion
    #[error("Error handling arrow data")]
    DataFusionError {
        /// Raw internal DataFusionError
        #[from]
        source: DataFusionError,
    },
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
    pub async fn try_from_uri(uri: String) -> DeltaCommandResult<Self> {
        let table = get_table_from_uri_without_update(uri)?;
        Ok(Self { table })
    }

    async fn execute(
        &mut self,
        operation: DeltaOperation,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DeltaCommandResult<()> {
        let transaction = Arc::new(DeltaTransactionPlan::new(
            self.table.table_uri.clone(),
            plan,
            operation,
            None,
        ));

        let _ = collect(transaction).await?;
        self.table.update().await?;

        Ok(())
    }

    /// Create a new Delta table
    pub async fn create(
        &mut self,
        table_uri: String,
        metadata: DeltaTableMetaData,
        mode: Option<action::SaveMode>,
    ) -> DeltaCommandResult<()> {
        let operation = DeltaOperation::Create {
            mode: mode.clone().unwrap_or(action::SaveMode::Ignore),
            metadata: metadata.clone(),
            location: table_uri.clone(),
        };
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };
        let plan = Arc::new(CreateCommand::new(
            table_uri.clone(),
            mode.unwrap_or(action::SaveMode::Ignore),
            metadata.clone(),
            protocol,
        ));

        self.execute(operation, plan).await
    }

    /// Write data to Delta table
    pub async fn write(&mut self, data: Vec<RecordBatch>) -> DeltaCommandResult<()> {
        let schema = data[0].schema();
        let partition_columns = self.table.get_metadata()?.partition_columns.clone();

        let mut partitions: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        for batch in data {
            let divided =
                divide_by_partition_values(schema.clone(), partition_columns.clone(), &batch)
                    .unwrap();
            for part in divided {
                let key =
                    DeltaWriter::get_partition_key(&partition_columns, &part.partition_values)?;
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

        let operation = DeltaOperation::Write {
            mode: action::SaveMode::Append,
            partition_by: Some(partition_columns),
            predicate: None,
        };
        let plan = Arc::new(WritePartitionCommand::new(
            self.table.table_uri.clone(),
            Arc::new(MemoryExec::try_new(
                &partitions.into_values().collect::<Vec<_>>(),
                schema,
                None,
            )?),
        ));

        self.execute(operation, plan).await
    }
}

fn get_table_from_uri_without_update(table_uri: String) -> DeltaCommandResult<DeltaTable> {
    let backend = get_backend_for_uri_with_options(&table_uri, HashMap::new())?;
    let table = DeltaTable::new(&table_uri, backend, DeltaTableConfig::default())?;

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test_utils::{create_initialized_table, get_record_batch};

    #[tokio::test]
    async fn test_write_command() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let mut table = create_initialized_table(&partition_cols).await;
        assert_eq!(table.version, 0);

        let mut commands = DeltaCommands::try_from_uri(table.table_uri.to_string())
            .await
            .unwrap();

        commands.write(vec![batch]).await.unwrap();

        table.update().await.unwrap();
        assert_eq!(table.version, 1);

        let files = table.get_file_uris();
        assert_eq!(files.len(), 2)
    }
}
