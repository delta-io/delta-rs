//! Drop a constraint from a table

use std::collections::HashMap;

use chrono::Utc;
use futures::future::BoxFuture;
use serde_json::json;

use crate::kernel::{Action, CommitInfo, IsolationLevel, Protocol};
use crate::logstore::LogStoreRef;
use crate::operations::transaction::commit;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};

/// Remove constraints from the table
pub struct DropConstraintBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Name of the constraint
    name: Option<String>,
    /// Raise if constraint doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
}

impl DropConstraintBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            name: None,
            raise_if_not_exists: true,
            snapshot,
            log_store,
            app_metadata: None,
        }
    }

    /// Specify the constraint to be removed
    pub fn with_constraint<S: Into<String>>(mut self, name: S) -> Self {
        self.name = Some(name.into());
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

impl std::future::IntoFuture for DropConstraintBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            let name = match this.name {
                Some(v) => v,
                None => return Err(DeltaTableError::Generic("No name provided".to_string())),
            };

            let mut metadata = this.snapshot.metadata().clone();
            let configuration_key = format!("delta.constraints.{}", name);

            let found_constraint = metadata.configuration.contains_key(&configuration_key);
            if this.raise_if_not_exists && !found_constraint {
                return Err(DeltaTableError::Generic(format!(
                    "Constraint with name: {} doesn't exists",
                    name
                )));
            } else if !this.raise_if_not_exists && !found_constraint {
                return Ok(DeltaTable::new_with_state(this.log_store, this.snapshot));
            }

            metadata
                .configuration
                .remove(format!("delta.constraints.{}", name).as_str());

            let mut contains_constraints: bool = false;
            for key in metadata.configuration.keys() {
                if key.starts_with("delta.constraints") {
                    contains_constraints = true;
                    break;
                } else {
                    continue;
                }
            }

            let operational_parameters = HashMap::from_iter([("name".to_string(), json!(&name))]);

            let operations = DeltaOperation::DropConstraint { name: name.clone() };

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

            let protocol = if !contains_constraints {
                // downgrade the protocol if possible since there are no more constraints
                let old_protocol = this.snapshot.protocol();
                Some(Protocol {
                    min_reader_version: if old_protocol.min_reader_version > 1 {
                        old_protocol.min_reader_version
                    } else {
                        1
                    },
                    min_writer_version: if old_protocol.min_writer_version > 3 {
                        old_protocol.min_writer_version
                    } else {
                        2
                    },
                    reader_features: old_protocol.reader_features.clone(),
                    writer_features: old_protocol.writer_features.clone(),
                })
            } else {
                None
            };

            let mut actions = vec![Action::CommitInfo(commit_info), Action::Metadata(metadata)];

            protocol.map(|protocol| actions.push(Action::Protocol(protocol)));

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

#[cfg(feature = "datafusion")]
#[cfg(test)]
mod tests {
    use crate::writer::test_utils::{create_bare_table, get_record_batch};
    use crate::{DeltaOps, DeltaResult, DeltaTable};

    async fn get_constraint_op_params(table: &mut DeltaTable) -> String {
        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[commit_info.len() - 1];
        last_commit
            .operation_parameters
            .as_ref()
            .unwrap()
            .get("name")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    }

    #[tokio::test]
    async fn drop_valid_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;
        let table = DeltaOps(write);

        let table = table
            .add_constraint()
            .with_constraint("id", "value < 1000")
            .await?;

        let mut table = DeltaOps(table)
            .drop_constraints()
            .with_constraint("id")
            .await?;

        let expected_name = "id";
        assert_eq!(get_constraint_op_params(&mut table).await, expected_name);
        assert_eq!(table.metadata().unwrap().configuration.get("id"), None);
        Ok(())
    }

    #[tokio::test]
    async fn drop_invalid_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;

        let table = DeltaOps(write)
            .drop_constraints()
            .with_constraint("not_existing")
            .await;
        assert!(table.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn drop_invalid_constraint_ignore() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;

        let version = write.version();

        let table = DeltaOps(write)
            .drop_constraints()
            .with_constraint("not_existing")
            .with_raise_if_not_exists(false)
            .await?;

        let version_after = table.version();

        assert_eq!(version, version_after);
        Ok(())
    }
}
