//! Command for creating a new delta table
// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/CreateDeltaTableCommand.scala
use async_trait::async_trait;

use super::{DeltaCommandError, DeltaCommandExec};
use crate::{action::Protocol, DeltaTable, DeltaTableMetaData};

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
    // mode: TableCreationMode,
    metadata: DeltaTableMetaData,
    protocol: Protocol,
}

impl CreateCommand {
    /// Create new CreateCommand
    pub fn new(metadata: DeltaTableMetaData, protocol: Protocol) -> Self {
        Self { metadata, protocol }
    }
}

#[async_trait]
impl DeltaCommandExec for CreateCommand {
    async fn execute(&self, table: &mut DeltaTable) -> Result<(), DeltaCommandError> {
        // TODO populate commit info with more meaningful data
        let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String("CREATE TABLE".to_string()),
        );
        commit_info.insert(
            "userName".to_string(),
            serde_json::Value::String("test user".to_string()),
        );

        table
            .create(
                self.metadata.clone(),
                self.protocol.clone(),
                Some(commit_info),
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        action::Protocol,
        schema::{Schema, SchemaDataType, SchemaField},
        write::test_utils::create_bare_table,
        DeltaTableConfig, DeltaTableMetaData,
    };
    use std::collections::HashMap;
    use std::path::Path;

    #[tokio::test]
    async fn create_table_without_partitions() {
        let table_schema = Schema::new(vec![
            SchemaField::new(
                "id".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "value".to_string(),
                SchemaDataType::primitive("integer".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "modified".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
        ]);

        let metadata =
            DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());

        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let command = CreateCommand { metadata, protocol };
        let mut table = create_bare_table();

        command.execute(&mut table).await.unwrap();

        let table_path = Path::new(table.table_uri.as_str());
        assert!(table_path.exists());

        let log_path = table_path.join("_delta_log/00000000000000000000.json");
        assert!(log_path.exists());
        assert!(log_path.is_file())
    }
}
