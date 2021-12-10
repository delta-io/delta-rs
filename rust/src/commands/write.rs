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
use crate::write::streams::*;
use crate::{action::Protocol, DeltaTable, DeltaTableMetaData, Schema};
// use arrow::{
//     datatypes::Schema as ArrowSchema, error::Result as ArrowResult, record_batch::RecordBatch,
// };
use async_trait::async_trait;
use create::CreateCommand;
// use futures::StreamExt;
use std::collections::HashMap;
// use std::sync::Arc;

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
    // mode: WriteMode,
}

#[async_trait]
impl DeltaCommandExec for WriteCommand {
    async fn execute(&self, table: &mut DeltaTable) -> Result<(), DeltaCommandError> {
        let table_exists = check_table_exists(table).await?;

        if !table_exists {
            let delta_schema = Schema::try_from(self.inputs.schema())?;

            // TODO make meta data configurable and get partitions from somewhere
            let metadata =
                DeltaTableMetaData::new(None, None, None, delta_schema, vec![], HashMap::new());

            let protocol = Protocol {
                min_reader_version: 1,
                min_writer_version: 2,
            };

            let command = CreateCommand::new(metadata, protocol);
            command.execute(table).await?;
        }

        todo!()

        // let metadata = table.get_metadata()?;
        // let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema)?;
        // let arrow_schema_ref = Arc::new(arrow_schema);
        // let partition_columns = metadata.partition_columns.clone();

        // let writer = Arc::new(
        //     DataWriter::new(
        //         &table.table_uri,
        //         partition_columns,
        //         arrow_schema_ref,
        //         HashMap::new(),
        //     )
        //     .unwrap()
        // );

        // let write_batch = Box::new(
        //     move |batch: ArrowResult<RecordBatch>| {
        //         let writer_inner = writer.clone();
        //         Box::pin(async move { writer_inner.write_record_batch(&batch.unwrap()).await })
        //     },
        // );

        // match self.mode {
        //     WriteMode::Append => {
        //         self.inputs
        //             .map(|batch| write_batch(batch))
        //             .collect::<Vec<_>>()
        //             .await;

        //         let version = writer.commit(&mut table, None, None).await?;
        //         // writer.write(data).await.unwrap();
        //     }
        //     WriteMode::Overwrite => {
        //         println!("Overwrite")
        //     }
        // };

        // Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::streams::SizedRecordBatchStream;
    use crate::DeltaTableConfig;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    };
    use std::path::Path;
    use std::sync::Arc;

    #[tokio::test]
    async fn write_and_create_table() {
        let stream = get_record_batch_stream();
        let command = WriteCommand {
            inputs: Box::pin(stream),
            // mode: WriteMode::Append,
        };
        let mut table = create_bare_table();
        command.execute(&mut table).await.unwrap();
    }

    fn create_bare_table() -> DeltaTable {
        // let table_dir = tempfile::tempdir().unwrap();
        // let table_path = table_dir.path();
        let table_path = Path::new("/home/robstar/github/delta-rs/data");
        let backend = Box::new(crate::storage::file::FileStorageBackend::new(
            table_path.to_str().unwrap(),
        ));
        DeltaTable::new(
            table_path.to_str().unwrap(),
            backend,
            DeltaTableConfig::default(),
        )
        .unwrap()
    }

    fn get_record_batch_stream() -> SizedRecordBatchStream {
        let int_values = Int32Array::from(vec![42, 44, 46, 48, 50, 52, 54, 56, 148, 150, 152]);
        let id_values =
            StringArray::from(vec!["A", "B", "C", "D", "E", "F", "G", "H", "D", "E", "F"]);
        let modified_values = StringArray::from(vec![
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-02",
            "2021-02-02",
            "2021-02-02",
        ]);

        // expected results from parsing json payload
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
        ]));
        let batch = Arc::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(id_values),
                    Arc::new(int_values),
                    Arc::new(modified_values),
                ],
            )
            .unwrap(),
        );
        SizedRecordBatchStream::new(schema.clone(), vec![batch])
    }
}
