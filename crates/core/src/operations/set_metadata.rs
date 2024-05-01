//! Command for updating the metadata of a delta table
// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/CreateDeltaTableCommand.scala

use futures::future::BoxFuture;

use super::transaction::{CommitBuilder, CommitProperties};
use crate::errors::{DeltaResult};
use crate::kernel::{
    Action, Metadata, StructType,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::DeltaTable;
use crate::table::state::DeltaTableState;

/// Build an operation to change the columns of a [DeltaTable]
#[derive(Debug, Clone)]
pub struct SetMetadataBuilder {
    /// The base for metadata for the table
    metadata: Metadata,

    schema: Option<StructType>,

    /// A snapshot of the to-be-checked table's state
    snapshot: DeltaTableState,

    /// Delta object store for handling data files
    log_store: LogStoreRef,

    /// Commit properties and configuration
    commit_properties: CommitProperties,
}

impl super::Operation<()> for SetMetadataBuilder {}

impl SetMetadataBuilder {
    /// Create a new [`SetMetadataBuilder`]
    pub fn new(log_store: LogStoreRef, state: DeltaTableState) -> Self {
        Self {
            metadata: state.metadata().clone(),
            schema: None,
            log_store,
            snapshot: state,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Sets the schema for the table
    pub fn with_schema(
        mut self,
        schema: StructType,
    ) -> Self {
        self.schema = Some(schema);
        self
    }
}

impl std::future::IntoFuture for SetMetadataBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;
        Box::pin(async move {

            let mut metadata = this.metadata.clone();
            if let Some(schema) = this.schema {
                metadata.schema_string = serde_json::to_string(&schema)?
            }

            let operation = DeltaOperation::SetMetadata {
                metadata: metadata.clone(),
            };

            let actions = vec![Action::Metadata(metadata)];

            CommitBuilder::from(this.commit_properties.clone())
                .with_actions(actions)
                .build(
                    Some(&this.snapshot),
                    this.log_store.clone(),
                    operation,
                )?
                .await?;

            let mut table = DeltaTable::new_with_state(this.log_store.clone(), this.snapshot.clone());
            table.update().await?;
            Ok(table)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::get_delta_schema;
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::protocol::SaveMode;

    #[tokio::test]
    async fn test_set_metadata_with_new_schema() {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().clone())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_schema().unwrap(), &table_schema);

        let new_table_schema = StructType::new(vec![
            StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "value".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "modified".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "new_column".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
        ]);

        let table = DeltaOps(table.clone())
            .set_metadata()
            .with_schema(new_table_schema.clone())
            .await
            .unwrap();

        assert_eq!(table.get_schema().unwrap(), &new_table_schema)
    }
}
