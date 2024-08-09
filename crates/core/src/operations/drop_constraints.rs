//! Drop a constraint from a table

use futures::future::BoxFuture;

use super::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::Action;
use crate::logstore::LogStoreRef;
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
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl super::Operation<()> for DropConstraintBuilder {}

impl DropConstraintBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            name: None,
            raise_if_not_exists: true,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
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
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

impl std::future::IntoFuture for DropConstraintBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let name = this
                .name
                .ok_or(DeltaTableError::Generic("No name provided".to_string()))?;

            let mut metadata = this.snapshot.metadata().clone();
            let configuration_key = format!("delta.constraints.{}", name);

            if metadata.configuration.remove(&configuration_key).is_none() {
                if this.raise_if_not_exists {
                    return Err(DeltaTableError::Generic(format!(
                        "Constraint with name: {} doesn't exists",
                        name
                    )));
                }
                return Ok(DeltaTable::new_with_state(this.log_store, this.snapshot));
            }
            let operation = DeltaOperation::DropConstraint { name: name.clone() };

            let actions = vec![Action::Metadata(metadata)];

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions)
                .build(Some(&this.snapshot), this.log_store.clone(), operation)
                .await?;

            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
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
        let last_commit = &commit_info[0];

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
    async fn drop_invalid_constraint_not_existing() -> DeltaResult<()> {
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
