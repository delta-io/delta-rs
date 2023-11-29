//! Actions are the fundamental unit of work in Delta Lake. Each action performs a single atomic
//! operation on the state of a Delta table. Actions are stored in the `_delta_log` directory of a
//! Delta table in JSON format. The log is a time series of actions that represent all the changes
//! made to a table.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub(crate) mod schemas;
mod serde_path;
pub(crate) mod types;

pub use types::*;

#[derive(Debug)]
/// The type of action that was performed on the table
pub enum ActionType {
    /// modify the data in a table by adding individual logical files
    Add,
    /// add a file containing only the data that was changed as part of the transaction
    Cdc,
    /// additional provenance information about what higher-level operation was being performed
    CommitInfo,
    /// contains a configuration (string-string map) for a named metadata domain
    DomainMetadata,
    /// changes the current metadata of the table
    Metadata,
    /// increase the version of the Delta protocol that is required to read or write a given table
    Protocol,
    /// modify the data in a table by removing individual logical files
    Remove,
    /// Transactional information
    Txn,
    /// Checkpoint metadata
    CheckpointMetadata,
    /// Sidecar
    Sidecar,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub enum Action {
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    Protocol(Protocol),
    Add(Add),
    Remove(Remove),
    Cdc(AddCDCFile),
    Txn(Txn),
    CommitInfo(CommitInfo),
    DomainMetadata(DomainMetadata),
}

impl Action {
    /// Create a commit info from a map
    pub fn commit_info(info: HashMap<String, serde_json::Value>) -> Self {
        Self::CommitInfo(CommitInfo {
            info,
            ..Default::default()
        })
    }
}
