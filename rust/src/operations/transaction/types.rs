//! Types and structs used when commitind operations to a delta table
use crate::DeltaTableError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
/// The isolation level applied during transaction
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

    /// SnapshotIsolation
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
            _ => Err(DeltaTableError::Generic(
                "Invalid string for IsolationLevel".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_isolation_level() {
        assert!(matches!(
            "Serializable".parse().unwrap(),
            IsolationLevel::Serializable
        ));
        assert!(matches!(
            "WriteSerializable".parse().unwrap(),
            IsolationLevel::WriteSerializable
        ));
        assert!(matches!(
            "SnapshotIsolation".parse().unwrap(),
            IsolationLevel::SnapshotIsolation
        ));
        assert!(matches!(
            IsolationLevel::Serializable.as_ref().parse().unwrap(),
            IsolationLevel::Serializable
        ));
        assert!(matches!(
            IsolationLevel::WriteSerializable.as_ref().parse().unwrap(),
            IsolationLevel::WriteSerializable
        ));
        assert!(matches!(
            IsolationLevel::SnapshotIsolation.as_ref().parse().unwrap(),
            IsolationLevel::SnapshotIsolation
        ))
    }
}
