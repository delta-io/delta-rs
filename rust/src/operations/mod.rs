//! High level delta commands that can be executed against a delta table
// TODO
// - rename to delta operations
use crate::{
    open_table,
    action::{DeltaOperation, Protocol, SaveMode},
    get_backend_for_uri_with_options,
    operations::{create::CreateCommand, transaction::DeltaTransactionPlan, write::WriteCommand},
    storage::StorageError,
    writer::{
        record_batch::divide_by_partition_values, utils::get_partition_key, DeltaWriterError,
    },
    DeltaTable, DeltaTableConfig, DeltaTableError, DeltaTableMetaData,
};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use datafusion::{
    error::DataFusionError,
    physical_plan::{collect, memory::MemoryExec, ExecutionPlan},
    prelude::SessionContext,
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
    #[error("Error in underlying DeltaTable")]
    DeltaTableError {
        /// Raw internal DeltaTableError
        #[from]
        source: DeltaTableError,
    },

    /// Errors occurring inside the DeltaWriter modules
    #[error("Error in underlying DeltaWriter")]
    DeltaWriter {
        /// Raw internal DeltaWriterError
        #[from]
        source: DeltaWriterError,
    },

    /// Error returned when errors occur in underlying storage instance
    #[error("Error in underlying storage backend")]
    Storage {
        /// Raw internal StorageError
        #[from]
        source: StorageError,
    },

    /// Error returned when errors occur in Arrow
    #[error("Error handling arrow data")]
    Arrow {
        /// Raw internal ArrowError
        #[from]
        source: ArrowError,
    },

    /// Error returned for errors internal to Datafusion
    #[error("Error in Datafusion execution engine")]
    DataFusion {
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
    pub async fn try_from_uri<T>(uri: T) -> DeltaCommandResult<Self>
    where
        T: Into<String>,
    {
        let table_uri: String = uri.into();
        let table = if let Ok(tbl) = open_table(&table_uri).await {
            Ok(tbl)
         } else {
            get_table_from_uri_without_update(table_uri)
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
            self.table.table_uri.clone(),
            self.table.version,
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
            location: self.table.table_uri.clone(),
            // TODO get the protocol from somewhere central
            protocol: Protocol {
                min_reader_version: 1,
                min_writer_version: 1,
            },
        };
        let plan = Arc::new(CreateCommand::try_new(
            &self.table.table_uri,
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
        let metadata = self.table.get_metadata()?;
        if let Some(cols) = partition_columns.as_ref() {
            if cols != &metadata.partition_columns {
                todo!("Schema updates not yet implemented")
            }
        };
        let data = if let Some(cols) = partition_columns.as_ref() {
            // TODO partitioning should probably happen in its own plan ...
            let mut partitions: HashMap<String, Vec<RecordBatch>> = HashMap::new();
            for batch in data {
                let divided =
                    divide_by_partition_values(schema.clone(), cols.clone(), &batch).unwrap();
                for part in divided {
                    let key = get_partition_key(cols, &part.partition_values)?;
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
            &self.table.table_uri,
            operation.clone(),
            data_plan,
        )?);
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

        let _ = commands
            .create(metadata.clone(), SaveMode::Ignore)
            .await
            .unwrap();

        let table = open_table(&table_uri).await.unwrap();
        assert_eq!(table.version, 0);

        let res = commands.create(metadata, SaveMode::ErrorIfExists).await;
        assert!(res.is_err())
    }

    #[tokio::test]
    async fn test_write_command() {
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let mut table = create_initialized_table(&partition_cols).await;
        assert_eq!(table.version, 0);

        let mut commands = DeltaCommands::try_from_uri(table.table_uri.to_string())
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
        assert_eq!(table.version, 1);

        let files = table.get_file_uris();
        assert_eq!(files.collect::<Vec<_>>().len(), 2)
    }
}
