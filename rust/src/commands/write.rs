//! Used to write a [RecordBatch] into a delta table.
//!
//! New Table Semantics
//!  - The schema of the [RecordBatch] is used to initialize the table.
//!  - The partition columns will be used to partition the table.
//!
//! Existing Table Semantics
//!  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
//!  - The schema of the RecordBatch will be checked and if there are new columns present
//!    they will be added to the tables schema. Conflicting columns (i.e. a INT, and a STRING)
//!    will result in an exception
//!  - The partition columns, if present are validated against the existing metadata. If not
//!    present, then the partitioning of the table is respected.
//!
//! In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
//! replace data that matches a predicate.

// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala
use super::*;
use crate::{action::Protocol, DeltaTable, DeltaTableMetaData, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use create::CreateCommand;
use std::collections::HashMap;

/// The write mode when writing data to delta table.
pub enum WriteMode {
    /// append data to existing table
    Append,
    /// overwrite table with new data
    Overwrite,
}

/// Write command
pub struct WriteCommand {
    inputs: Vec<RecordBatch>,
    partition_columns: Option<Vec<String>>,
    // TODO maybe use SendableRecordBatchStream here?
    // inputs: SendableRecordBatchStream,
    // mode: WriteMode,
}

#[async_trait]
impl DeltaCommandExec for WriteCommand {
    async fn execute(&self, table: &mut DeltaTable) -> Result<(), DeltaCommandError> {
        let table_exists = check_table_exists(table).await?;

        if !table_exists {
            let delta_schema = Schema::try_from(self.inputs[0].schema())?;

            // TODO make meta data configurable and get partitions from somewhere
            let metadata = DeltaTableMetaData::new(
                None,
                None,
                None,
                delta_schema,
                self.partition_columns.clone().unwrap_or_else(|| vec![]),
                HashMap::new(),
            );

            let protocol = Protocol {
                min_reader_version: 1,
                min_writer_version: 2,
            };

            let command = CreateCommand::new(metadata, protocol);
            command.execute(table).await?;
        }

        let mut txn = table.create_transaction(None);
        txn.write_files(self.inputs.clone()).await.unwrap();
        let _ = txn.commit(None).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test_utils::{create_bare_table, get_record_batch};

    #[tokio::test]
    async fn write_and_create_table() {
        let batch = get_record_batch(None, false);
        let command = WriteCommand {
            inputs: vec![batch],
            partition_columns: None,
            // mode: WriteMode::Append,
        };
        let mut table = create_bare_table();
        command.execute(&mut table).await.unwrap();
    }
}
