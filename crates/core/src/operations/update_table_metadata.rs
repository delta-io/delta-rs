//! Update table metadata operation

use std::sync::Arc;

use futures::future::BoxFuture;
use validator::Validate;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{resolve_snapshot, Action, EagerSnapshot, MetadataExt};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Validate)]
#[validate(schema(
    function = "validate_at_least_one_field",
    message = "No metadata update specified"
))]
pub struct TableMetadataUpdate {
    #[validate(length(
        min = 1,
        max = 255,
        message = "Table name cannot be empty and cannot exceed 255 characters"
    ))]
    pub name: Option<String>,

    #[validate(length(
        max = 4000,
        message = "Table description cannot exceed 4000 characters"
    ))]
    pub description: Option<String>,
}

fn validate_at_least_one_field(
    update: &TableMetadataUpdate,
) -> Result<(), validator::ValidationError> {
    if update.name.is_none() && update.description.is_none() {
        return Err(validator::ValidationError::new("no_fields_specified"));
    }
    Ok(())
}

/// Update table metadata operation
pub struct UpdateTableMetadataBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// The metadata update to apply
    update: Option<TableMetadataUpdate>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for UpdateTableMetadataBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl UpdateTableMetadataBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            update: None,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the complete metadata update
    pub fn with_update(mut self, update: TableMetadataUpdate) -> Self {
        self.update = Some(update);
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

impl std::future::IntoFuture for UpdateTableMetadataBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot = resolve_snapshot(&this.log_store, this.snapshot.clone(), false).await?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let update = this.update.ok_or_else(|| {
                DeltaTableError::MetadataError("No metadata update specified".to_string())
            })?;
            update
                .validate()
                .map_err(|e| DeltaTableError::MetadataError(format!("{e}")))?;

            let mut metadata = snapshot.metadata().clone();

            if let Some(name) = &update.name {
                metadata = metadata.with_name(name.clone())?;
            }
            if let Some(description) = &update.description {
                metadata = metadata.with_description(description.clone())?;
            }

            let operation = DeltaOperation::UpdateTableMetadata {
                metadata_update: update,
            };

            let actions = vec![Action::Metadata(metadata)];

            let commit = CommitBuilder::from(this.commit_properties.clone())
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(this.custom_execute_handler.clone())
                .build(Some(&snapshot), this.log_store.clone(), operation.clone())
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
