//! Set table properties on a table

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{resolve_snapshot, Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::DeltaResult;
use crate::DeltaTable;

/// Remove constraints from the table
pub struct SetTablePropertiesBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Name of the property
    properties: HashMap<String, String>,
    /// Raise if property doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for SetTablePropertiesBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl SetTablePropertiesBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            properties: HashMap::new(),
            raise_if_not_exists: true,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the properties to be removed
    pub fn with_properties(mut self, table_properties: HashMap<String, String>) -> Self {
        self.properties = table_properties;
        self
    }

    /// Specify if you want to raise if the property does not exist
    pub fn with_raise_if_not_exists(mut self, raise: bool) -> Self {
        self.raise_if_not_exists = raise;
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

impl std::future::IntoFuture for SetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot = resolve_snapshot(&this.log_store, this.snapshot.clone(), false).await?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let mut metadata = snapshot.metadata().clone();

            let current_protocol = snapshot.protocol();
            let properties = this.properties;

            let new_protocol = current_protocol
                .clone()
                .apply_properties_to_protocol(&properties, this.raise_if_not_exists)?;

            for (key, value) in &properties {
                metadata = metadata.add_config_key(key.clone(), value.to_string())?;
            }

            let final_protocol =
                new_protocol.move_table_properties_into_features(metadata.configuration());

            let operation = DeltaOperation::SetTableProperties { properties };

            let mut actions = vec![Action::Metadata(metadata)];

            if current_protocol.ne(&final_protocol) {
                actions.push(Action::Protocol(final_protocol));
            }

            let commit = CommitBuilder::from(this.commit_properties.clone())
                .with_actions(actions.clone())
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

#[cfg(test)]
pub mod tests {
    use crate::writer::test_utils::create_initialized_table;
    use crate::DeltaOps;
    use std::collections::HashMap;
    use std::env::temp_dir;

    #[tokio::test]
    pub async fn test_set_tbl_properties() -> crate::DeltaResult<()> {
        let temp_loc = temp_dir().join("test_table");
        let ops = DeltaOps(create_initialized_table(temp_loc.to_str().unwrap(), &[]).await);
        let props = HashMap::from([
            ("delta.minReaderVersion".to_string(), "3".to_string()),
            ("delta.minWriterVersion".to_string(), "7".to_string()),
        ]);
        ops.set_tbl_properties().with_properties(props).await?;

        Ok(())
    }
}
