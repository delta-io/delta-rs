//! Unset table properties of the table

use std::collections::HashMap;
use std::str::FromStr;

use chrono::Utc;
use futures::future::BoxFuture;
use serde_json::json;

use crate::kernel::{Action, CommitInfo, IsolationLevel, Protocol};
use crate::logstore::LogStoreRef;
use crate::operations::transaction::commit;
use crate::protocol::DeltaOperation;
use crate::table::config::DeltaConfigKey;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};

/// Remove constraints from the table
pub struct UnsetTablePropertiesBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Name of the property
    properties: Option<Vec<String>>,
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
            properties: None,
            raise_if_not_exists: true,
            snapshot,
            log_store,
            app_metadata: None,
        }
    }

    /// Specify the constraint to be removed
    pub fn with_property(mut self, name: String) -> Self {
        match self.properties {
            Some(mut vec_name) => {
                vec_name.push(name);
                self.properties = Some(vec_name);
            }
            None => self.properties = Some(vec![name]),
        }
        self
    }

    /// Specify the constraint to be removed
    pub fn with_properties(mut self, names: Vec<String>) -> Self {
        match self.properties {
            Some(mut vec_name) => {
                vec_name.extend(names);
                self.properties = Some(vec_name);
            }
            None => self.properties = Some(names),
        }
        self
    }

    /// Specify if you want to raise if the constraint does not exist
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
            let properties = match this.properties {
                Some(v) => v,
                None => {
                    return Err(DeltaTableError::Generic(
                        "No properties provided".to_string(),
                    ))
                }
            };

            let mut metadata = this.snapshot.metadata()?.clone();

            // Check if names are valid config keys
            for key in &properties {
                DeltaConfigKey::from_str(key)?;
            }

            let mut incorrect_config_names: Vec<&str> = Vec::new();
            for key in &properties {
                if !metadata.configuration.contains_key(key) {
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
                metadata.configuration.remove(key);
            }

            let operational_parameters =
                HashMap::from_iter([("properties".to_string(), json!(&properties))]);

            let operations = DeltaOperation::UnsetTblProperties {
                properties: properties.clone(),
            };

            let app_metadata = match this.app_metadata {
                Some(metadata) => metadata,
                None => HashMap::default(),
            };

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
                operations,
                &this.snapshot,
                None,
            )
            .await?;

            this.snapshot
                .merge(DeltaTableState::from_actions(actions, version)?, true, true);
            Ok(DeltaTable::new_with_state(this.log_store, this.snapshot))
        })
    }
}

// #[cfg(feature = "datafusion")]
// #[cfg(test)]
// mod tests {
//     use crate::writer::test_utils::{create_bare_table, get_record_batch};
//     use crate::{DeltaOps, DeltaResult, DeltaTable};

//     async fn get_constraint_op_params(table: &mut DeltaTable) -> String {
//         let commit_info = table.history(None).await.unwrap();
//         let last_commit = &commit_info[commit_info.len() - 1];
//         last_commit
//             .operation_parameters
//             .as_ref()
//             .unwrap()
//             .get("name")
//             .unwrap()
//             .as_str()
//             .unwrap()
//             .to_owned()
//     }

//     #[tokio::test]
//     async fn drop_valid_constraint() -> DeltaResult<()> {
//         let batch = get_record_batch(None, false);
//         let write = DeltaOps(create_bare_table())
//             .write(vec![batch.clone()])
//             .await?;
//         let table = DeltaOps(write);

//         let table = table
//             .add_constraint()
//             .with_constraint("id", "value < 1000")
//             .await?;

//         let mut table = DeltaOps(table)
//             .drop_constraints()
//             .with_constraint("id")
//             .await?;

//         let expected_name = "id";
//         assert_eq!(get_constraint_op_params(&mut table).await, expected_name);
//         assert_eq!(table.metadata().unwrap().configuration.get("id"), None);
//         Ok(())
//     }

//     #[tokio::test]
//     async fn drop_invalid_constraint() -> DeltaResult<()> {
//         let batch = get_record_batch(None, false);
//         let write = DeltaOps(create_bare_table())
//             .write(vec![batch.clone()])
//             .await?;

//         let table = DeltaOps(write)
//             .drop_constraints()
//             .with_constraint("not_existing")
//             .await;
//         assert!(table.is_err());

//         Ok(())
//     }

//     #[tokio::test]
//     async fn drop_invalid_constraint_ignore() -> DeltaResult<()> {
//         let batch = get_record_batch(None, false);
//         let write = DeltaOps(create_bare_table())
//             .write(vec![batch.clone()])
//             .await?;

//         let version = write.version();

//         let table = DeltaOps(write)
//             .drop_constraints()
//             .with_constraint("not_existing")
//             .with_raise_if_not_exists(false)
//             .await?;

//         let version_after = table.version();

//         assert_eq!(version, version_after);
//         Ok(())
//     }
// }
