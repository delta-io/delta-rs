use std::collections::HashSet;

use once_cell::sync::Lazy;

use super::TransactionError;
use crate::kernel::{Action, ReaderFeatures, WriterFeatures};
use crate::table::state::DeltaTableState;

static READER_V2: Lazy<HashSet<ReaderFeatures>> = Lazy::new(|| {
    let mut set = HashSet::new();
    set.insert(ReaderFeatures::ColumnMapping);
    set
});

static WRITER_V2: Lazy<HashSet<WriterFeatures>> = Lazy::new(|| {
    let mut set = HashSet::new();
    set.insert(WriterFeatures::AppendOnly);
    set.insert(WriterFeatures::Invariants);
    set
});
static WRITER_V3: Lazy<HashSet<WriterFeatures>> = Lazy::new(|| {
    let mut set = WRITER_V2.clone();
    set.insert(WriterFeatures::CheckConstraints);
    set
});
static WRITER_V4: Lazy<HashSet<WriterFeatures>> = Lazy::new(|| {
    let mut set = WRITER_V3.clone();
    set.insert(WriterFeatures::ChangeDataFeed);
    set.insert(WriterFeatures::GeneratedColumns);
    set
});
static WRITER_V5: Lazy<HashSet<WriterFeatures>> = Lazy::new(|| {
    let mut set = WRITER_V4.clone();
    set.insert(WriterFeatures::ColumnMapping);
    set
});
static WRITER_V6: Lazy<HashSet<WriterFeatures>> = Lazy::new(|| {
    let mut set = WRITER_V5.clone();
    set.insert(WriterFeatures::IdentityColumns);
    set
});

pub struct ProtocolChecker {
    reader_features: HashSet<ReaderFeatures>,
    writer_features: HashSet<WriterFeatures>,
}

impl ProtocolChecker {
    /// Create a new protocol checker.
    pub fn new(
        reader_features: HashSet<ReaderFeatures>,
        writer_features: HashSet<WriterFeatures>,
    ) -> Self {
        Self {
            reader_features,
            writer_features,
        }
    }

    pub fn default_reader_version(&self) -> i32 {
        1
    }

    pub fn default_writer_version(&self) -> i32 {
        2
    }

    /// Check if delta-rs can read form the given delta table.
    pub fn can_read_from(&self, snapshot: &DeltaTableState) -> Result<(), TransactionError> {
        let required_features = match snapshot.min_reader_version() {
            0 | 1 => None,
            2 => Some(READER_V2.clone()),
            _ => snapshot.reader_features().cloned(),
        };

        if let Some(features) = required_features {
            let diff: Vec<_> = features
                .difference(&self.reader_features)
                .cloned()
                .collect();
            if !diff.is_empty() {
                return Err(TransactionError::UnsupportedReaderFeatures(diff));
            }
        };
        Ok(())
    }

    /// Check if delta-rs can write to the given delta table.
    pub fn can_write_to(&self, snapshot: &DeltaTableState) -> Result<(), TransactionError> {
        // NOTE: writers must always support all required reader features
        self.can_read_from(snapshot)?;

        let required_features = match snapshot.min_writer_version() {
            0 | 1 => None,
            2 => Some(WRITER_V2.clone()),
            3 => Some(WRITER_V3.clone()),
            4 => Some(WRITER_V4.clone()),
            5 => Some(WRITER_V5.clone()),
            6 => Some(WRITER_V6.clone()),
            _ => snapshot.writer_features().cloned(),
        };

        if let Some(features) = required_features {
            let diff: Vec<_> = features
                .difference(&self.writer_features)
                .cloned()
                .collect();
            if !diff.is_empty() {
                return Err(TransactionError::UnsupportedWriterFeatures(diff));
            }
        };
        Ok(())
    }

    pub fn can_commit(
        &self,
        snapshot: &DeltaTableState,
        actions: &[Action],
    ) -> Result<(), TransactionError> {
        self.can_write_to(snapshot)?;

        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
        let append_only_enabled = if snapshot.min_writer_version() < 2 {
            false
        } else if snapshot.min_writer_version() < 7 {
            snapshot.table_config().append_only()
        } else {
            snapshot
                .writer_features()
                .ok_or(TransactionError::WriterFeaturesRequired)?
                .contains(&WriterFeatures::AppendOnly)
                && snapshot.table_config().append_only()
        };
        if append_only_enabled {
            actions.iter().try_for_each(|action| match action {
                Action::Remove(remove) if remove.data_change => {
                    Err(TransactionError::DeltaTableAppendOnly)
                }
                _ => Ok(()),
            })?;
        }

        Ok(())
    }
}

/// The global protocol checker instance to validate table versions and features.
pub static INSTANCE: Lazy<ProtocolChecker> = Lazy::new(|| {
    let reader_features = HashSet::new();
    // reader_features.insert(ReaderFeatures::ColumnMapping);

    let mut writer_features = HashSet::new();
    writer_features.insert(WriterFeatures::AppendOnly);
    writer_features.insert(WriterFeatures::Invariants);
    // writer_features.insert(WriterFeatures::CheckConstraints);
    // writer_features.insert(WriterFeatures::ChangeDataFeed);
    // writer_features.insert(WriterFeatures::GeneratedColumns);
    // writer_features.insert(WriterFeatures::ColumnMapping);
    // writer_features.insert(WriterFeatures::IdentityColumns);

    ProtocolChecker::new(reader_features, writer_features)
});

#[cfg(test)]
mod tests {
    use super::super::test_utils::create_metadata_action;
    use super::*;
    use crate::kernel::{Action, Add, Protocol, Remove};
    use crate::DeltaConfigKey;
    use std::collections::HashMap;

    #[test]
    fn test_can_commit_append_only() {
        let append_actions = vec![Action::Add(Add {
            path: "test".to_string(),
            data_change: true,
            ..Default::default()
        })];
        let change_actions = vec![
            Action::Add(Add {
                path: "test".to_string(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "test".to_string(),
                data_change: true,
                ..Default::default()
            }),
        ];
        let neutral_actions = vec![
            Action::Add(Add {
                path: "test".to_string(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "test".to_string(),
                data_change: false,
                ..Default::default()
            }),
        ];

        let create_actions = |writer: i32, append: &str, feat: Vec<WriterFeatures>| {
            vec![
                Action::Protocol(Protocol {
                    min_reader_version: 1,
                    min_writer_version: writer,
                    writer_features: Some(feat.into_iter().collect()),
                    ..Default::default()
                }),
                create_metadata_action(
                    None,
                    Some(HashMap::from([(
                        DeltaConfigKey::AppendOnly.as_ref().to_string(),
                        Some(append.to_string()),
                    )])),
                ),
            ]
        };

        let checker = ProtocolChecker::new(HashSet::new(), WRITER_V2.clone());

        let actions = create_actions(1, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker.can_commit(&snapshot, &append_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &change_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &neutral_actions).is_ok());

        let actions = create_actions(2, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker.can_commit(&snapshot, &append_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &change_actions).is_err());
        assert!(checker.can_commit(&snapshot, &neutral_actions).is_ok());

        let actions = create_actions(2, "false", vec![]);
        let snapshot = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker.can_commit(&snapshot, &append_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &change_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &neutral_actions).is_ok());

        let actions = create_actions(7, "true", vec![WriterFeatures::AppendOnly]);
        let snapshot = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker.can_commit(&snapshot, &append_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &change_actions).is_err());
        assert!(checker.can_commit(&snapshot, &neutral_actions).is_ok());

        let actions = create_actions(7, "false", vec![WriterFeatures::AppendOnly]);
        let snapshot = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker.can_commit(&snapshot, &append_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &change_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &neutral_actions).is_ok());

        let actions = create_actions(7, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker.can_commit(&snapshot, &append_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &change_actions).is_ok());
        assert!(checker.can_commit(&snapshot, &neutral_actions).is_ok());
    }

    #[test]
    fn test_versions() {
        let checker_1 = ProtocolChecker::new(HashSet::new(), HashSet::new());
        let actions = vec![Action::Protocol(Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
            ..Default::default()
        })];
        let snapshot_1 = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker_1.can_read_from(&snapshot_1).is_ok());
        assert!(checker_1.can_write_to(&snapshot_1).is_ok());

        let checker_2 = ProtocolChecker::new(READER_V2.clone(), HashSet::new());
        let actions = vec![Action::Protocol(Protocol {
            min_reader_version: 2,
            min_writer_version: 1,
            ..Default::default()
        })];
        let snapshot_2 = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker_1.can_read_from(&snapshot_2).is_err());
        assert!(checker_1.can_write_to(&snapshot_2).is_err());
        assert!(checker_2.can_read_from(&snapshot_1).is_ok());
        assert!(checker_2.can_read_from(&snapshot_2).is_ok());
        assert!(checker_2.can_write_to(&snapshot_2).is_ok());

        let checker_3 = ProtocolChecker::new(READER_V2.clone(), WRITER_V2.clone());
        let actions = vec![Action::Protocol(Protocol {
            min_reader_version: 2,
            min_writer_version: 2,
            ..Default::default()
        })];
        let snapshot_3 = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker_1.can_read_from(&snapshot_3).is_err());
        assert!(checker_1.can_write_to(&snapshot_3).is_err());
        assert!(checker_2.can_read_from(&snapshot_3).is_ok());
        assert!(checker_2.can_write_to(&snapshot_3).is_err());
        assert!(checker_3.can_read_from(&snapshot_1).is_ok());
        assert!(checker_3.can_read_from(&snapshot_2).is_ok());
        assert!(checker_3.can_read_from(&snapshot_3).is_ok());
        assert!(checker_3.can_write_to(&snapshot_3).is_ok());

        let checker_4 = ProtocolChecker::new(READER_V2.clone(), WRITER_V3.clone());
        let actions = vec![Action::Protocol(Protocol {
            min_reader_version: 2,
            min_writer_version: 3,
            ..Default::default()
        })];
        let snapshot_4 = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker_1.can_read_from(&snapshot_4).is_err());
        assert!(checker_1.can_write_to(&snapshot_4).is_err());
        assert!(checker_2.can_read_from(&snapshot_4).is_ok());
        assert!(checker_2.can_write_to(&snapshot_4).is_err());
        assert!(checker_3.can_read_from(&snapshot_4).is_ok());
        assert!(checker_3.can_write_to(&snapshot_4).is_err());
        assert!(checker_4.can_read_from(&snapshot_1).is_ok());
        assert!(checker_4.can_read_from(&snapshot_2).is_ok());
        assert!(checker_4.can_read_from(&snapshot_3).is_ok());
        assert!(checker_4.can_read_from(&snapshot_4).is_ok());
        assert!(checker_4.can_write_to(&snapshot_4).is_ok());

        let checker_5 = ProtocolChecker::new(READER_V2.clone(), WRITER_V4.clone());
        let actions = vec![Action::Protocol(Protocol {
            min_reader_version: 2,
            min_writer_version: 4,
            ..Default::default()
        })];
        let snapshot_5 = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker_1.can_read_from(&snapshot_5).is_err());
        assert!(checker_1.can_write_to(&snapshot_5).is_err());
        assert!(checker_2.can_read_from(&snapshot_5).is_ok());
        assert!(checker_2.can_write_to(&snapshot_5).is_err());
        assert!(checker_3.can_read_from(&snapshot_5).is_ok());
        assert!(checker_3.can_write_to(&snapshot_5).is_err());
        assert!(checker_4.can_read_from(&snapshot_5).is_ok());
        assert!(checker_4.can_write_to(&snapshot_5).is_err());
        assert!(checker_5.can_read_from(&snapshot_1).is_ok());
        assert!(checker_5.can_read_from(&snapshot_2).is_ok());
        assert!(checker_5.can_read_from(&snapshot_3).is_ok());
        assert!(checker_5.can_read_from(&snapshot_4).is_ok());
        assert!(checker_5.can_read_from(&snapshot_5).is_ok());
        assert!(checker_5.can_write_to(&snapshot_5).is_ok());

        let checker_6 = ProtocolChecker::new(READER_V2.clone(), WRITER_V5.clone());
        let actions = vec![Action::Protocol(Protocol {
            min_reader_version: 2,
            min_writer_version: 5,
            ..Default::default()
        })];
        let snapshot_6 = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker_1.can_read_from(&snapshot_6).is_err());
        assert!(checker_1.can_write_to(&snapshot_6).is_err());
        assert!(checker_2.can_read_from(&snapshot_6).is_ok());
        assert!(checker_2.can_write_to(&snapshot_6).is_err());
        assert!(checker_3.can_read_from(&snapshot_6).is_ok());
        assert!(checker_3.can_write_to(&snapshot_6).is_err());
        assert!(checker_4.can_read_from(&snapshot_6).is_ok());
        assert!(checker_4.can_write_to(&snapshot_6).is_err());
        assert!(checker_5.can_read_from(&snapshot_6).is_ok());
        assert!(checker_5.can_write_to(&snapshot_6).is_err());
        assert!(checker_6.can_read_from(&snapshot_1).is_ok());
        assert!(checker_6.can_read_from(&snapshot_2).is_ok());
        assert!(checker_6.can_read_from(&snapshot_3).is_ok());
        assert!(checker_6.can_read_from(&snapshot_4).is_ok());
        assert!(checker_6.can_read_from(&snapshot_5).is_ok());
        assert!(checker_6.can_read_from(&snapshot_6).is_ok());
        assert!(checker_6.can_write_to(&snapshot_6).is_ok());

        let checker_7 = ProtocolChecker::new(READER_V2.clone(), WRITER_V6.clone());
        let actions = vec![Action::Protocol(Protocol {
            min_reader_version: 2,
            min_writer_version: 6,
            ..Default::default()
        })];
        let snapshot_7 = DeltaTableState::from_actions(actions, 1).unwrap();
        assert!(checker_1.can_read_from(&snapshot_7).is_err());
        assert!(checker_1.can_write_to(&snapshot_7).is_err());
        assert!(checker_2.can_read_from(&snapshot_7).is_ok());
        assert!(checker_2.can_write_to(&snapshot_7).is_err());
        assert!(checker_3.can_read_from(&snapshot_7).is_ok());
        assert!(checker_3.can_write_to(&snapshot_7).is_err());
        assert!(checker_4.can_read_from(&snapshot_7).is_ok());
        assert!(checker_4.can_write_to(&snapshot_7).is_err());
        assert!(checker_5.can_read_from(&snapshot_7).is_ok());
        assert!(checker_5.can_write_to(&snapshot_7).is_err());
        assert!(checker_6.can_read_from(&snapshot_7).is_ok());
        assert!(checker_6.can_write_to(&snapshot_7).is_err());
        assert!(checker_7.can_read_from(&snapshot_1).is_ok());
        assert!(checker_7.can_read_from(&snapshot_2).is_ok());
        assert!(checker_7.can_read_from(&snapshot_3).is_ok());
        assert!(checker_7.can_read_from(&snapshot_4).is_ok());
        assert!(checker_7.can_read_from(&snapshot_5).is_ok());
        assert!(checker_7.can_read_from(&snapshot_6).is_ok());
        assert!(checker_7.can_read_from(&snapshot_7).is_ok());
        assert!(checker_7.can_write_to(&snapshot_7).is_ok());
    }
}
