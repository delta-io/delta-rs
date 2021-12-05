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
use crate::{DeltaTable, Schema};
use async_trait::async_trait;

// use create::CreateCommand;

/// The write mode when writing data to delta table.
pub enum WriteMode {
    /// append data to existing table
    Append,
    /// overwrite table with new data
    Overwrite,
}

/// Write command
pub struct WriteCommand {
    inputs: SendableRecordBatchStream,
    mode: WriteMode,
}

#[async_trait]
impl DeltaCommandExec for WriteCommand {
    async fn execute(&self, table: &mut DeltaTable) -> Result<(), DeltaCommandError> {
        let table_exists = check_table_exists(table).await?;

        if !table_exists {
            let delta_schema = Schema::try_from(self.inputs.schema())?;
            println!("{:?}", delta_schema);
        }

        match self.mode {
            WriteMode::Append => {
                println!("Append")
            },
            WriteMode::Overwrite => {
                println!("Overwrite")
            }
        };

        // let data = collect(self.inputs).await;

        Ok(())
    }
}
