//! Unset table properties of the table

use std::collections::HashMap;

use chrono::Utc;
use futures::future::BoxFuture;
use serde_json::json;

use crate::kernel::{Action, CommitInfo, IsolationLevel};
use crate::logstore::LogStoreRef;
use crate::operations::transaction::commit;
use crate::protocol::DeltaOperation;
use crate::table::config::DeltaConfigKey;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

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
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
}

impl UnsetTablePropertiesBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            properties: vec![],
            raise_if_not_exists: true,
            snapshot,
            log_store,
            app_metadata: None,
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
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.app_metadata = Some(HashMap::from_iter(metadata));
        self
    }
}

impl std::future::IntoFuture for UnsetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
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
            } else if !this.raise_if_not_exists && incorrect_config_names.len() == properties.len()
            {
                return Ok(DeltaTable::new_with_state(this.log_store, this.snapshot));
            }

            for key in &properties {
                metadata.configuration.remove(*key);
            }

            let operational_parameters =
                HashMap::from_iter([("properties".to_string(), json!(properties))]);

            let operations = DeltaOperation::UnsetTableProperties {
                properties: properties.iter().map(|v| v.to_string()).collect(),
            };

            let app_metadata = this.app_metadata.unwrap_or_default();

            let commit_info = CommitInfo {
                timestamp: Some(Utc::now().timestamp_millis()),
                operation: Some(operations.name().to_string()),
                operation_parameters: Some(operational_parameters),
                read_version: Some(this.snapshot.version()),
                isolation_level: Some(IsolationLevel::Serializable),
                is_blind_append: Some(false),
                info: app_metadata,
                ..Default::default()
            };

            let actions = vec![Action::CommitInfo(commit_info), Action::Metadata(metadata)];

            let version = commit(
                this.log_store.as_ref(),
                &actions,
                operations.clone(),
                Some(&this.snapshot),
                None,
            )
            .await?;

            this.snapshot.merge(actions, &operations, version)?;
            Ok(DeltaTable::new_with_state(this.log_store, this.snapshot))
        })
    }
}
