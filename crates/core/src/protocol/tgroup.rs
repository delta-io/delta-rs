use super::checkpoints::create_checkpoint_for_tgroup_add;
use super::{DeltaOperation, ProtocolError};

use crate::kernel::{Action, RedirectState, TGroup};
use crate::operations::transaction::{CommitBuilder, FinalizedCommit};
use crate::table::CheckPoint;
use crate::{DeltaTable, DeltaTableError};

/// Create initial log action to add a table to a T-group
pub async fn initiate_add_to_tgroup(
    table: &DeltaTable,
    tgroup_uri: &str,
) -> Result<FinalizedCommit, DeltaTableError> {
    // Create a log with a T-group action with read-only access
    let tgroup = TGroup::new(
        &table.metadata().unwrap().id,
        tgroup_uri,
        RedirectState::ReadOnly,
    );
    let actions = vec![Action::TGroup(tgroup)];
    let operation = DeltaOperation::TGroupStart {
        tgroup_uri: String::from(tgroup_uri),
    };
    return CommitBuilder::default()
        .with_actions(actions)
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store(),
            operation,
        )
        .await;
}

/// Checkpoint a table for addition to a T-group
pub async fn create_initial_tgroup_checkpoint(table: &DeltaTable) -> Result<(), DeltaTableError> {
    create_checkpoint_for_tgroup_add(
        table
            .tgroup_log_segment
            .as_ref()
            .expect("Expected tgroup log segment")
            .version,
        table.snapshot().map_err(|_| ProtocolError::NoMetaData)?,
        table.log_store.as_ref(),
        table
            .tgroup_log_store
            .as_ref()
            .expect("Tgroup details have not been initialized for the table instance")
            .as_ref(),
        None,
    )
    .await?;
    Ok(())
}

/// Create the final log action to add a table to a T-group
pub async fn finalize_add_to_tgroup(
    table: &DeltaTable,
    tgroup_uri: &str,
    version: i64,
) -> Result<FinalizedCommit, DeltaTableError> {
    // Create a log with a T-group action for redirection
    let tgroup = TGroup::new(
        &table.metadata().unwrap().id,
        tgroup_uri,
        RedirectState::Redirect,
    );
    let actions = vec![Action::TGroup(tgroup)];
    let operation = DeltaOperation::TGroupEnd {
        tgroup_uri: String::from(tgroup_uri),
    };
    return CommitBuilder::default()
        .with_actions(actions)
        .with_version(version)
        .with_max_retries(1)
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store(),
            operation,
        )
        .await;
}

/// Checkpoint a table for addition to a T-group
pub async fn update_last_checkpoint_for_table(
    table: &DeltaTable,
    checkpoint: CheckPoint,
    tgroup_uri: String,
) -> Result<(), DeltaTableError> {
    let mut cp = checkpoint.clone();
    // cp.tgroup_uri = Some(tgroup_uri);
    // create_checkpoint_for_tgroup_add(
    //     table
    //         .tgroup_log_segment
    //         .as_ref()
    //         .expect("Expected tgroup log segment")
    //         .version,
    //     table.snapshot().map_err(|_| ProtocolError::NoMetaData)?,
    //     table.log_store.as_ref(),
    //     table
    //         .tgroup_log_store
    //         .as_ref()
    //         .expect("Tgroup details have not been initialized for the table instance")
    //         .as_ref(),
    //     None,
    // )
    // .await?;
    Ok(())
}
