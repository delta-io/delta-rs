//! Actions are the fundamental unit of work in Delta Lake. Each action performs a single atomic
//! operation on the state of a Delta table. Actions are stored in the `_delta_log` directory of a
//! Delta table in JSON format. The log is a time series of actions that represent all the changes
//! made to a table.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub(crate) mod actions;
pub(crate) mod fields;

pub use actions::*;

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
    Txn(Transaction),
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

impl From<Add> for Action {
    fn from(a: Add) -> Self {
        Self::Add(a)
    }
}

impl From<Remove> for Action {
    fn from(a: Remove) -> Self {
        Self::Remove(a)
    }
}

impl From<AddCDCFile> for Action {
    fn from(a: AddCDCFile) -> Self {
        Self::Cdc(a)
    }
}

impl From<Metadata> for Action {
    fn from(a: Metadata) -> Self {
        Self::Metadata(a)
    }
}

impl From<Protocol> for Action {
    fn from(a: Protocol) -> Self {
        Self::Protocol(a)
    }
}

impl From<Transaction> for Action {
    fn from(a: Transaction) -> Self {
        Self::Txn(a)
    }
}

impl From<CommitInfo> for Action {
    fn from(a: CommitInfo) -> Self {
        Self::CommitInfo(a)
    }
}

impl From<DomainMetadata> for Action {
    fn from(a: DomainMetadata) -> Self {
        Self::DomainMetadata(a)
    }
}
