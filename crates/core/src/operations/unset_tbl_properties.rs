//! Unset table properties of the table

use crate::kernel::Action;
use crate::logstore::LogStoreRef;
use crate::operations::transaction::{CommitBuilder, CommitProperties};
use crate::protocol::DeltaOperation;
use crate::table::config::DeltaConfigKey;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use futures::future::BoxFuture;

/// Remove constraints from the table
pub struct UnsetTablePropertiesBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Name of the property
    properties: Vec<DeltaConfigKey>,
    /// Raise if property doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl UnsetTablePropertiesBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            properties: vec![],
            raise_if_not_exists: true,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the property to be removed
    pub fn with_property(mut self, name: DeltaConfigKey) -> Self {
        let mut properties = self.properties;
        properties.push(name);
        self.properties = properties;
        self
    }

    /// Specify the properties to be removed
    pub fn with_properties(mut self, names: Vec<DeltaConfigKey>) -> Self {
        let mut properties = self.properties;
        properties.extend(names);
        self.properties = properties;
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

impl std::future::IntoFuture for UnsetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            // Disallow unsetting these table properties:
            // - delta.columnMapping.mode  for ref: https://docs.delta.io/latest/delta-column-mapping.html
            if this
                .properties
                .iter()
                .any(|f| f == &DeltaConfigKey::ColumnMappingMode)
            {
                return Err(DeltaTableError::Generic(format!(
                    "Unsetting table property delta.columnMapping.mode is not allowed."
                )));
            };

            let properties = this
                .properties
                .iter()
                .map(|v| v.as_ref())
                .collect::<Vec<&str>>();

            let mut metadata = this.snapshot.metadata().clone();

            let mut incorrect_config_names: Vec<&str> = Vec::new();
            for key in &properties {
                if !metadata.configuration.contains_key(*key) {
                    incorrect_config_names.push(key)
                }
            }

            if this.raise_if_not_exists && incorrect_config_names.len() > 0 {
                return Err(DeltaTableError::Generic(format!(
                    "table properties with names: {:?} don't exists",
                    incorrect_config_names
                )));
            }

            for key in &properties {
                metadata.configuration.remove(*key);
            }

            let operation = DeltaOperation::UnsetTableProperties {
                properties: properties.iter().map(|v| v.to_string()).collect(),
            };

            let actions = vec![Action::Metadata(metadata)];

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions)
                .build(Some(&this.snapshot), this.log_store.clone(), operation)?
                .await?;

            this.snapshot
                .merge(commit.data.actions, &commit.data.operation, commit.version)?;
            Ok(DeltaTable::new_with_state(this.log_store, this.snapshot))
        })
    }
}
