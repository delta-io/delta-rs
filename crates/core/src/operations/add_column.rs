//! Add a new column to a table

use std::sync::Arc;

use delta_kernel::schema::StructType;
use delta_kernel::table_features::ColumnMappingMode;
use futures::future::BoxFuture;
use itertools::Itertools;

use super::{CustomExecuteHandler, Operation};
use crate::errors::ColumnMappingOperation;
use crate::kernel::schema::merge_delta_struct;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt, ProtocolExt as _, SnapshotMetadataRef, StructField,
    StructTypeExt, resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Add new columns and/or nested fields to a table
pub struct AddColumnBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Fields to add/merge into schema
    fields: Option<Vec<StructField>>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl Operation for AddColumnBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl AddColumnBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            fields: None,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the fields to be added
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = StructField> + Clone) -> Self {
        self.fields = Some(fields.into_iter().collect());
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

fn plan_add_column_actions(
    snapshot: SnapshotMetadataRef<'_>,
    fields: Vec<StructField>,
) -> DeltaResult<(Vec<Action>, DeltaOperation)> {
    let mut metadata = snapshot.metadata.clone();
    let fields_right = &StructType::try_new(fields.clone())?;

    if !fields_right
        .get_generated_columns()
        .unwrap_or_default()
        .is_empty()
    {
        return Err(DeltaTableError::Generic(
            "New columns cannot be a generated column".to_string(),
        ));
    }

    let table_schema = snapshot.table_configuration.logical_schema();
    let new_table_schema = merge_delta_struct(table_schema.as_ref(), fields_right)?;

    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&new_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::AddColumn {
        fields: fields.into_iter().collect_vec(),
    };

    metadata = metadata.with_schema(&new_table_schema)?;

    let mut actions = vec![metadata.into()];

    if current_protocol != &new_protocol {
        actions.push(new_protocol.into())
    }

    Ok((actions, operation))
}

impl std::future::IntoFuture for AddColumnBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), false, None).await?;
            if snapshot
                .snapshot()
                .metadata_state()
                .table_configuration
                .column_mapping_mode()
                != ColumnMappingMode::None
            {
                return Err(DeltaTableError::unsupported_column_mapping(
                    ColumnMappingOperation::Write,
                    "ADD COLUMN",
                ));
            }

            let fields = match this.fields.clone() {
                Some(v) => v,
                None => return Err(DeltaTableError::Generic("No fields provided".to_string())),
            };
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let (actions, operation) =
                plan_add_column_actions(snapshot.snapshot().metadata_state(), fields)?;

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
    use crate::kernel::{DataType, PrimitiveType};
    use crate::{DeltaTableConfig, writer::test_utils::TestResult};

    use super::*;

    fn id_field() -> StructField {
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true)
    }

    fn added_field() -> StructField {
        StructField::new("added", DataType::Primitive(PrimitiveType::String), true)
    }

    #[tokio::test]
    async fn add_column_with_lazy_snapshot_does_not_materialize_files() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field()])
            .await?;
        let log_store = table.log_store().clone();
        let config = DeltaTableConfig {
            require_files: false,
            ..Default::default()
        };
        let snapshot = EagerSnapshot::try_new(log_store.as_ref(), config, None).await?;

        assert!(!snapshot.snapshot().has_materialized_files_for_test());

        AddColumnBuilder::new(log_store, Some(snapshot.clone()))
            .with_fields([added_field()])
            .await?;

        assert!(!snapshot.snapshot().has_materialized_files_for_test());

        Ok(())
    }
}
