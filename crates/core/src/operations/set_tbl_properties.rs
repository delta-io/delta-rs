//! Set table properties on a table

use std::collections::HashMap;

use futures::future::BoxFuture;

use super::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::Action;
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaResult;
use crate::DeltaTable;

/// Remove constraints from the table
pub struct SetTablePropertiesBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Name of the property
    properties: HashMap<String, String>,
    /// Raise if property doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl SetTablePropertiesBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            properties: HashMap::new(),
            raise_if_not_exists: true,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
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
}

impl std::future::IntoFuture for SetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let mut metadata = this.snapshot.metadata().clone();

            let current_protocol = this.snapshot.protocol();
            let properties = this.properties;

            let new_protocol = current_protocol
                .clone()
                .apply_properties_to_protocol(&properties, this.raise_if_not_exists)?;

            metadata.configuration.extend(
                properties
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k, Some(v)))
                    .collect::<HashMap<String, Option<String>>>(),
            );

            let final_protocol =
                new_protocol.move_table_properties_into_features(&metadata.configuration);

            let operation = DeltaOperation::SetTableProperties { properties };

            let mut actions = vec![Action::Metadata(metadata)];

            if current_protocol.ne(&final_protocol) {
                actions.push(Action::Protocol(final_protocol));
            }

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions.clone())
                .build(
                    Some(&this.snapshot),
                    this.log_store.clone(),
                    operation.clone(),
                )
                .await?;
            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}
