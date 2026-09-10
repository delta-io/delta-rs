//! Set table properties on a table

use std::collections::HashMap;

use futures::future::BoxFuture;

use crate::DeltaResult;
use crate::DeltaTable;
use crate::errors::{ColumnMappingOperation, DeltaTableError};
use crate::kernel::transaction::CommitProperties;
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, SnapshotMetadataRef,
    resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::operations::commit_actions_in_scope;
use crate::protocol::DeltaOperation;
use crate::table::config::TableProperty;

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

fn plan_set_table_properties_actions(
    snapshot: SnapshotMetadataRef<'_>,
    properties: HashMap<String, String>,
    raise_if_not_exists: bool,
) -> DeltaResult<(Vec<Action>, DeltaOperation)> {
    if properties.contains_key(TableProperty::ColumnMappingMode.as_ref()) {
        return Err(DeltaTableError::unsupported_column_mapping(
            ColumnMappingOperation::Write,
            "SET TBLPROPERTIES delta.columnMapping.mode",
        ));
    }

    let mut metadata = snapshot.metadata.clone();
    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_properties_to_protocol(&properties, raise_if_not_exists)?;

    for (key, value) in &properties {
        metadata = metadata.add_config_key(key.clone(), value.to_string())?;
    }

    let final_protocol = new_protocol.move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::SetTableProperties { properties };

    let mut actions = vec![Action::Metadata(metadata)];

    if current_protocol.ne(&final_protocol) {
        actions.push(Action::Protocol(final_protocol));
    }

    Ok((actions, operation))
}

impl std::future::IntoFuture for SetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), false, None).await?;

            let properties = this.properties;
            let (actions, operation) = plan_set_table_properties_actions(
                snapshot.snapshot().metadata_state(),
                properties,
                this.raise_if_not_exists,
            )?;

            commit_actions_in_scope(
                &this.log_store,
                &snapshot,
                this.commit_properties,
                actions,
                operation,
            )
            .await
        })
    }
}

#[cfg(test)]
/// Tests for the set-table-properties operation.
pub mod tests {
    use crate::writer::test_utils::create_initialized_table;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    /// Verify that setting table properties is persisted to table metadata.
    pub async fn test_set_tbl_properties() -> crate::DeltaResult<()> {
        let temp_loc = tempdir()?;
        let ops = create_initialized_table(temp_loc.path().to_str().unwrap(), &[]).await;
        let props = HashMap::from([
            ("delta.minReaderVersion".to_string(), "3".to_string()),
            ("delta.minWriterVersion".to_string(), "7".to_string()),
        ]);
        ops.set_tbl_properties().with_properties(props).await?;

        Ok(())
    }
}
