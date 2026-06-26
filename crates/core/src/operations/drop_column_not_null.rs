//! Drop the `NOT NULL` constraint on a column, making it nullable.
//!
//! This implements the equivalent of `ALTER TABLE <table> ALTER COLUMN <name> DROP NOT NULL`.
//! Only relaxing a column from non-nullable to nullable is supported. The reverse
//! (making a nullable column non-nullable) is intentionally not allowed here because it
//! requires validating existing data and/or a default value, which is out of scope.

use std::sync::Arc;

use delta_kernel::schema::StructType;
use futures::future::BoxFuture;

use super::{CustomExecuteHandler, Operation};
use crate::DeltaTable;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, SnapshotMetadataRef,
    resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTableError};

/// Drop the `NOT NULL` constraint on a top-level column, making it nullable.
pub struct DropColumnNotNullBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// The name of the column whose `NOT NULL` constraint should be dropped
    column_name: String,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for DropColumnNotNullBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl DropColumnNotNullBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            column_name: String::new(),
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the column whose `NOT NULL` constraint should be dropped
    pub fn with_column(mut self, column_name: impl Into<String>) -> Self {
        self.column_name = column_name.into();
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

fn plan_drop_column_not_null_actions(
    snapshot: SnapshotMetadataRef<'_>,
    column_name: &str,
) -> DeltaResult<(Vec<Action>, DeltaOperation)> {
    if column_name.is_empty() {
        return Err(DeltaTableError::Generic(
            "No column name provided".to_string(),
        ));
    }

    let table_schema = snapshot.table_configuration.logical_schema();

    let Some(field) = table_schema.field(column_name) else {
        return Err(DeltaTableError::Generic(format!(
            "No column with the name '{column_name}' in the schema"
        )));
    };

    if field.nullable {
        return Err(DeltaTableError::Generic(format!(
            "Column '{column_name}' is already nullable"
        )));
    }

    let mut updated_field = field.clone();
    updated_field.nullable = true;

    let updated_table_schema =
        StructType::try_new(
            table_schema
                .fields()
                .map(|f| match f.name == updated_field.name {
                    true => updated_field.clone(),
                    false => f.clone(),
                }),
        )?;

    let mut metadata = snapshot.metadata.clone();

    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&updated_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::DropColumnNotNull {
        column: updated_field.clone(),
    };

    metadata = metadata.with_schema(&updated_table_schema)?;

    let mut actions = vec![metadata.into()];

    if current_protocol != &new_protocol {
        actions.push(new_protocol.into())
    }

    Ok((actions, operation))
}

impl std::future::IntoFuture for DropColumnNotNullBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), false, None).await?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let (actions, operation) = plan_drop_column_not_null_actions(
                snapshot.snapshot().metadata_state(),
                &this.column_name,
            )?;

            let commit = CommitBuilder::from(this.commit_properties.clone())
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(this.get_custom_execute_handler())
                .build(Some(&snapshot), this.log_store.clone(), operation)
                .await?;

            this.post_execute(operation_id).await?;

            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::writer::test_utils::TestResult;

    use super::*;

    fn non_null_id_field() -> StructField {
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), false)
    }

    fn nullable_value_field() -> StructField {
        StructField::new("value", DataType::Primitive(PrimitiveType::String), true)
    }

    #[tokio::test]
    async fn drop_not_null_makes_column_nullable() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([non_null_id_field(), nullable_value_field()])
            .await?;

        assert!(!table.snapshot()?.schema().field("id").unwrap().nullable);

        let table = table.drop_column_not_null().with_column("id").await?;

        let schema = table.snapshot()?.schema();
        assert!(
            schema.field("id").unwrap().nullable,
            "id should be nullable after dropping NOT NULL"
        );
        // Other fields are left untouched.
        assert!(schema.field("value").unwrap().nullable);
        assert_eq!(
            schema.field("id").unwrap().data_type(),
            &DataType::Primitive(PrimitiveType::Integer)
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_not_null_unknown_column_errors() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([non_null_id_field()])
            .await?;

        let result = table
            .drop_column_not_null()
            .with_column("does_not_exist")
            .await;

        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn drop_not_null_already_nullable_errors() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([non_null_id_field(), nullable_value_field()])
            .await?;

        let result = table.drop_column_not_null().with_column("value").await;

        assert!(result.is_err());
        Ok(())
    }
}
