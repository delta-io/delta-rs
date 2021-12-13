//! High level delta commands that can be executed against a delta table
use crate::{
    action::Protocol,
    commands::{transaction::DeltaTransactionPlan, write::WritePartitionCommand},
    open_table,
    storage::StorageError,
    write::{divide_by_partition_values, DeltaWriter, DeltaWriterError},
    DeltaTable, DeltaTableError, DeltaTableMetaData,
};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use datafusion::{
    error::DataFusionError,
    physical_plan::{collect, memory::MemoryExec},
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

/// The save mode when writing data.
pub enum SaveMode {
    /// append data to existing table
    Append,
    /// overwrite table with new data
    Overwrite,
    /// TODO
    Ignore,
    /// Raise an error if data exists
    ErrorIfExists,
}

/// High level interface for executing commands against a DeltaTable
pub struct DeltaCommands {
    table: DeltaTable,
}

impl DeltaCommands {
    /// load table from uri
    pub async fn try_from_uri(uri: String) -> DeltaCommandResult<Self> {
        let table = open_table(&uri).await?;
        Ok(Self { table })
    }

    /// create an instance of the table
    pub async fn try_with_new_table(
        table_uri: String,
        metadata: DeltaTableMetaData,
    ) -> DeltaCommandResult<Self> {
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let command = Arc::new(create::CreateCommand::new(
            table_uri.clone(),
            metadata,
            protocol,
        ));
        let _ = collect(command).await?;
        let table = open_table(&table_uri).await?;

        Ok(Self { table })
    }

    /// Write data to Delta table
    pub async fn write(&mut self, data: Vec<RecordBatch>) -> DeltaCommandResult<()> {
        let schema = data[0].schema();
        let partition_columns = self.table.get_metadata()?.partition_columns.clone();

        let mut partitions: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        for batch in data {
            // let mut partition = Vec::new();
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

        let data_plan = Arc::new(MemoryExec::try_new(
            &partitions.into_values().collect::<Vec<_>>(),
            schema,
            None,
        )?);

        let write_plan = Arc::new(WritePartitionCommand::new(
            self.table.table_uri.clone(),
            data_plan,
        ));

        let transaction = Arc::new(DeltaTransactionPlan::new(
            self.table.table_uri.clone(),
            write_plan,
        ));

        let _ = collect(transaction).await?;
        self.table.update().await?;

        Ok(())
    }
}

async fn check_table_exists(table: &DeltaTable) -> DeltaCommandResult<bool> {
    let uri = table.commit_uri_from_version(table.version);
    match table.storage.head_obj(&uri).await {
        Ok(_) => Ok(true),
        Err(StorageError::NotFound) => Ok(false),
        Err(source) => Err(DeltaCommandError::StorageError { source }),
    }
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
