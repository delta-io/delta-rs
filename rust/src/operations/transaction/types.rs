//! Types and structs used when commitind operations to a delta table
use crate::{DeltaDataTypeTimestamp, DeltaDataTypeVersion, DeltaTableError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

/// The commitInfo is a fairly flexible action within the delta specification, where arbitrary data can be stored.
/// However the reference implementation as well as delta-rs store useful information that may for instance
/// allow us to be more permissive in commit conflict resolution.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommitInfo {
    pub version: Option<DeltaDataTypeVersion>,
    pub timestamp: DeltaDataTypeTimestamp,
    pub user_id: Option<String>,
    pub user_name: Option<String>,
    pub operation: String,
    pub operation_parameters: HashMap<String, String>,
    pub read_version: Option<i64>,
    pub isolation_level: Option<IsolationLevel>,
    pub is_blind_append: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table’s history.
    Serializable,

    /// A weaker isolation level than Serializable. It ensures only that the write
    /// operations (that is, not reads) are serializable. However, this is still stronger
    /// than Snapshot isolation. WriteSerializable is the default isolation level because
    /// it provides great balance of data consistency and availability for most common operations.
    WriteSerializable,

    SnapshotIsolation,
}

impl IsolationLevel {
    /// The default isolation level to use, analogous to reference implementation
    pub fn default_level() -> Self {
        // https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1023
        IsolationLevel::Serializable
    }
}

impl AsRef<str> for IsolationLevel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Serializable => "Serializable",
            Self::WriteSerializable => "WriteSerializable",
            Self::SnapshotIsolation => "SnapshotIsolation",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => todo!(),
        }
    }
}
