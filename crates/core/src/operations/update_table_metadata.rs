//! Update table metadata operation

use std::sync::Arc;

use futures::future::BoxFuture;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::Action;
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TableMetadataUpdate {
    /// Update the table name
    TableName(String),
    /// Update the table description
    TableDescription(String),
}

impl TableMetadataUpdate {
    /// Create a validated table name update
    pub fn table_name(name: impl Into<String>) -> DeltaResult<Self> {
        let name = name.into();
        if name.len() > 255 {
            return Err(DeltaTableError::MetadataError(format!(
                "Table name cannot exceed 255 characters. Provided name has {} characters.",
                name.len()
            )));
        }
        Ok(TableMetadataUpdate::TableName(name))
    }

    /// Create a validated table description update
    pub fn table_description(description: impl Into<String>) -> DeltaResult<Self> {
        let description = description.into();
        let max_description_length = 4000;
        if description.len() > max_description_length {
            return Err(DeltaTableError::MetadataError(format!(
                "Table description cannot exceed {} characters. Provided description has {} characters.",
                max_description_length,
                description.len()
            )));
        }
        Ok(TableMetadataUpdate::TableDescription(description))
    }
}

/// Update table metadata operation
pub struct UpdateTableMetadataBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// The metadata update to apply
    update: Option<TableMetadataUpdate>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation<()> for UpdateTableMetadataBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl UpdateTableMetadataBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            update: None,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    pub fn with_update(mut self, update: TableMetadataUpdate) -> Self {
        self.update = Some(update);
        self
    }

    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

impl std::future::IntoFuture for UpdateTableMetadataBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let update = this.update.ok_or_else(|| {
                DeltaTableError::MetadataError("No metadata update specified".to_string())
            })?;

            let mut metadata = this.snapshot.metadata().clone();

            match update.clone() {
                TableMetadataUpdate::TableName(name) => {
                    if name.len() > 255 {
                        return Err(DeltaTableError::MetadataError(format!(
                            "Table name cannot exceed 255 characters. Provided name has {} characters.",
                            name.len()
                        )));
                    }
                    metadata.name = Some(name.clone());
                }
                TableMetadataUpdate::TableDescription(description) => {
                    if description.len() > 4000 {
                        return Err(DeltaTableError::MetadataError(format!(
                            "Table description cannot exceed 4,000 characters. Provided description has {} characters.",
                            description.len()
                        )));
                    }
                    metadata.description = Some(description.clone());
                }
            }

            let operation = DeltaOperation::UpdateTableMetadata {
                metadata_update: update,
            };

            let actions = vec![Action::Metadata(metadata)];

            let commit = CommitBuilder::from(this.commit_properties.clone())
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(this.custom_execute_handler.clone())
                .build(
                    Some(&this.snapshot),
                    this.log_store.clone(),
                    operation.clone(),
                )
                .await?;

            if let Some(handler) = this.custom_execute_handler {
                handler.post_execute(&this.log_store, operation_id).await?;
            }
            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}
