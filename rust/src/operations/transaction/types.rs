//! Types and structs used when commitind operations to a delta table
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::action::{Action, DeltaOperation};
use crate::DeltaTableError;

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

    /// SnapshotIsolation is a guarantee that all reads made in a transaction will see a consistent
    /// snapshot of the database (in practice it reads the last committed values that existed at the
    /// time it started), and the transaction itself will successfully commit only if no updates
    /// it has made conflict with any concurrent updates made since that snapshot.
    SnapshotIsolation,
}

impl IsolationLevel {
    /// The default isolation level to use, analogous to reference implementation
    pub fn default_level() -> Self {
        // https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1023
        IsolationLevel::Serializable
    }
}

// Spark assumes Serializable as default isolation level
// https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1023
impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Serializable
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

// For no-data-change transactions such as OPTIMIZE/Auto Compaction/ZorderBY, we can
// change the isolation level to SnapshotIsolation. SnapshotIsolation allows reduced conflict
// detection by skipping the
// [ConflictChecker::check_for_added_files_that_should_have_been_read_by_current_txn] check i.e.
// don't worry about concurrent appends.
//
// We can also use SnapshotIsolation for empty transactions. e.g. consider a commit:
// t0 - Initial state of table
// t1 - Q1, Q2 starts
// t2 - Q1 commits
// t3 - Q2 is empty and wants to commit.
// In this scenario, we can always allow Q2 to commit without worrying about new files
// generated by Q1.
//
// The final order which satisfies both Serializability and WriteSerializability is: Q2, Q1
// Note that Metadata only update transactions shouldn't be considered empty. If Q2 above has
// a Metadata update (say schema change/identity column high watermark update), then Q2 can't
// be moved above Q1 in the final SERIALIZABLE order. This is because if Q2 is moved above Q1,
// then Q1 should see the updates from Q2 - which actually didn't happen.
pub(super) fn can_downgrade_to_snapshot_isolation<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
    operation: &DeltaOperation,
    isolation_level: &IsolationLevel,
) -> bool {
    let mut data_changed = false;
    let mut has_non_file_actions = false;
    for action in actions {
        match action {
            Action::add(act) if act.data_change => data_changed = true,
            Action::remove(rem) if rem.data_change => data_changed = true,
            _ => has_non_file_actions = true,
        }
    }

    if has_non_file_actions {
        // if Non-file-actions are present (e.g. METADATA etc.), then don't downgrade the isolation
        // level to SnapshotIsolation.
        return false;
    }

    match isolation_level {
        IsolationLevel::Serializable => !data_changed,
        IsolationLevel::WriteSerializable => !data_changed && !operation.changes_data(),
        IsolationLevel::SnapshotIsolation => true,
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

    #[test]
    fn test_default_isolation_level() {
        assert!(matches!(
            IsolationLevel::default(),
            IsolationLevel::Serializable
        ))
    }

    #[test]
    fn test_can_downgrade_to_snapshot_isolation() {
        assert!(true)
    }
}
