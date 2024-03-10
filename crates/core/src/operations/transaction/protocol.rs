use std::collections::HashSet;

use lazy_static::lazy_static;
use once_cell::sync::Lazy;

use super::{TableReference, TransactionError};
use crate::kernel::{Action, DataType, EagerSnapshot, ReaderFeatures, Schema, WriterFeatures};
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;

lazy_static! {
    static ref READER_V2: HashSet<ReaderFeatures> =
        HashSet::from_iter([ReaderFeatures::ColumnMapping]);
    static ref WRITER_V2: HashSet<WriterFeatures> =
        HashSet::from_iter([WriterFeatures::AppendOnly, WriterFeatures::Invariants]);
    static ref WRITER_V3: HashSet<WriterFeatures> = HashSet::from_iter([
        WriterFeatures::AppendOnly,
        WriterFeatures::Invariants,
        WriterFeatures::CheckConstraints
    ]);
    static ref WRITER_V4: HashSet<WriterFeatures> = HashSet::from_iter([
        WriterFeatures::AppendOnly,
        WriterFeatures::Invariants,
        WriterFeatures::CheckConstraints,
        WriterFeatures::ChangeDataFeed,
        WriterFeatures::GeneratedColumns
    ]);
    static ref WRITER_V5: HashSet<WriterFeatures> = HashSet::from_iter([
        WriterFeatures::AppendOnly,
        WriterFeatures::Invariants,
        WriterFeatures::CheckConstraints,
        WriterFeatures::ChangeDataFeed,
        WriterFeatures::GeneratedColumns,
        WriterFeatures::ColumnMapping,
    ]);
    static ref WRITER_V6: HashSet<WriterFeatures> = HashSet::from_iter([
        WriterFeatures::AppendOnly,
        WriterFeatures::Invariants,
        WriterFeatures::CheckConstraints,
        WriterFeatures::ChangeDataFeed,
        WriterFeatures::GeneratedColumns,
        WriterFeatures::ColumnMapping,
        WriterFeatures::IdentityColumns,
    ]);
}

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

    /// Check append-only at the high level (operation level)
    pub fn check_append_only(&self, snapshot: &EagerSnapshot) -> Result<(), TransactionError> {
        if snapshot.table_config().append_only() {
            return Err(TransactionError::DeltaTableAppendOnly);
        }
        Ok(())
    }

    /// Check can write_timestamp_ntz
    pub fn check_can_write_timestamp_ntz(
        &self,
        snapshot: &DeltaTableState,
        schema: &Schema,
    ) -> Result<(), TransactionError> {
        let contains_timestampntz = schema
            .fields()
            .iter()
            .any(|f| f.data_type() == &DataType::TIMESTAMPNTZ);

        let required_features: Option<&HashSet<WriterFeatures>> =
            match snapshot.protocol().min_writer_version {
                0..=6 => None,
                _ => snapshot.protocol().writer_features.as_ref(),
            };

        if let Some(table_features) = required_features {
            if !table_features.contains(&WriterFeatures::TimestampWithoutTimezone)
                && contains_timestampntz
            {
                return Err(TransactionError::WriterFeaturesRequired(
                    WriterFeatures::TimestampWithoutTimezone,
                ));
            }
        } else if contains_timestampntz {
            return Err(TransactionError::WriterFeaturesRequired(
                WriterFeatures::TimestampWithoutTimezone,
            ));
        }
        Ok(())
    }

    /// Check if delta-rs can read form the given delta table.
    pub fn can_read_from(&self, snapshot: &dyn TableReference) -> Result<(), TransactionError> {
        let required_features: Option<&HashSet<ReaderFeatures>> =
            match snapshot.protocol().min_reader_version {
                0 | 1 => None,
                2 => Some(&READER_V2),
                _ => snapshot.protocol().reader_features.as_ref(),
            };
        if let Some(features) = required_features {
            let mut diff = features.difference(&self.reader_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedReaderFeatures(
                    diff.cloned().collect(),
                ));
            }
        };
        Ok(())
    }

    /// Check if delta-rs can write to the given delta table.
    pub fn can_write_to(&self, snapshot: &dyn TableReference) -> Result<(), TransactionError> {
        // NOTE: writers must always support all required reader features
        self.can_read_from(snapshot)?;

        let required_features: Option<&HashSet<WriterFeatures>> =
            match snapshot.protocol().min_writer_version {
                0 | 1 => None,
                2 => Some(&WRITER_V2),
                3 => Some(&WRITER_V3),
                4 => Some(&WRITER_V4),
                5 => Some(&WRITER_V5),
                6 => Some(&WRITER_V6),
                _ => snapshot.protocol().writer_features.as_ref(),
            };

        if let Some(features) = required_features {
            let mut diff = features.difference(&self.writer_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedWriterFeatures(
                    diff.cloned().collect(),
                ));
            }
        };
        Ok(())
    }

    pub fn can_commit(
        &self,
        snapshot: &dyn TableReference,
        actions: &[Action],
        operation: &DeltaOperation,
    ) -> Result<(), TransactionError> {
        self.can_write_to(snapshot)?;

        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
        let append_only_enabled = if snapshot.protocol().min_writer_version < 2 {
            false
        } else if snapshot.protocol().min_writer_version < 7 {
            snapshot.config().append_only()
        } else {
            snapshot
                .protocol()
                .writer_features
                .as_ref()
                .ok_or(TransactionError::WriterFeaturesRequired(
                    WriterFeatures::AppendOnly,
                ))?
                .contains(&WriterFeatures::AppendOnly)
                && snapshot.config().append_only()
        };
        if append_only_enabled {
            match operation {
                DeltaOperation::Restore { .. } | DeltaOperation::FileSystemCheck { .. } => {}
                _ => {
                    actions.iter().try_for_each(|action| match action {
                        Action::Remove(remove) if remove.data_change => {
                            Err(TransactionError::DeltaTableAppendOnly)
                        }
                        _ => Ok(()),
                    })?;
                }
            }
        }

        Ok(())
    }
}

/// The global protocol checker instance to validate table versions and features.
///
/// This instance is used by default in all transaction operations, since feature
/// support is not configurable but rather decided at compile time.
///
/// As we implement new features, we need to update this instance accordingly.
/// resulting version support is determined by the supported table feature set.
pub static INSTANCE: Lazy<ProtocolChecker> = Lazy::new(|| {
    let mut reader_features = HashSet::new();
    reader_features.insert(ReaderFeatures::TimestampWithoutTimezone);
    // reader_features.insert(ReaderFeatures::ColumnMapping);

    let mut writer_features = HashSet::new();
    writer_features.insert(WriterFeatures::AppendOnly);
    writer_features.insert(WriterFeatures::TimestampWithoutTimezone);
    #[cfg(feature = "datafusion")]
    {
        writer_features.insert(WriterFeatures::Invariants);
        writer_features.insert(WriterFeatures::CheckConstraints);
    }
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
    use crate::protocol::SaveMode;
    use crate::table::state::DeltaTableState;
    use crate::DeltaConfigKey;
    use std::collections::HashMap;

    #[test]
    fn test_can_commit_append_only() {
        let append_actions = vec![Action::Add(Add {
            path: "test".to_string(),
            data_change: true,
            ..Default::default()
        })];
        let append_op = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };

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
        let change_op = DeltaOperation::Update { predicate: None };

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
        let neutral_op = DeltaOperation::Update { predicate: None };

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
        let snapshot = DeltaTableState::from_actions(actions).unwrap();
        let eager = snapshot.snapshot();
        assert!(checker
            .can_commit(eager, &append_actions, &append_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &change_actions, &change_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &neutral_actions, &neutral_op)
            .is_ok());

        let actions = create_actions(2, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions).unwrap();
        let eager = snapshot.snapshot();
        assert!(checker
            .can_commit(eager, &append_actions, &append_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &change_actions, &change_op)
            .is_err());
        assert!(checker
            .can_commit(eager, &neutral_actions, &neutral_op)
            .is_ok());

        let actions = create_actions(2, "false", vec![]);
        let snapshot = DeltaTableState::from_actions(actions).unwrap();
        let eager = snapshot.snapshot();
        assert!(checker
            .can_commit(eager, &append_actions, &append_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &change_actions, &change_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &neutral_actions, &neutral_op)
            .is_ok());

        let actions = create_actions(7, "true", vec![WriterFeatures::AppendOnly]);
        let snapshot = DeltaTableState::from_actions(actions).unwrap();
        let eager = snapshot.snapshot();
        assert!(checker
            .can_commit(eager, &append_actions, &append_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &change_actions, &change_op)
            .is_err());
        assert!(checker
            .can_commit(eager, &neutral_actions, &neutral_op)
            .is_ok());

        let actions = create_actions(7, "false", vec![WriterFeatures::AppendOnly]);
        let snapshot = DeltaTableState::from_actions(actions).unwrap();
        let eager = snapshot.snapshot();
        assert!(checker
            .can_commit(eager, &append_actions, &append_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &change_actions, &change_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &neutral_actions, &neutral_op)
            .is_ok());

        let actions = create_actions(7, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions).unwrap();
        let eager = snapshot.snapshot();
        assert!(checker
            .can_commit(eager, &append_actions, &append_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &change_actions, &change_op)
            .is_ok());
        assert!(checker
            .can_commit(eager, &neutral_actions, &neutral_op)
            .is_ok());
    }

    #[test]
    fn test_versions() {
        let checker_1 = ProtocolChecker::new(HashSet::new(), HashSet::new());
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: 1,
                min_writer_version: 1,
                ..Default::default()
            }),
            create_metadata_action(None, Some(HashMap::new())),
        ];
        let snapshot_1 = DeltaTableState::from_actions(actions).unwrap();
        let eager_1 = snapshot_1.snapshot();
        assert!(checker_1.can_read_from(eager_1).is_ok());
        assert!(checker_1.can_write_to(eager_1).is_ok());

        let checker_2 = ProtocolChecker::new(READER_V2.clone(), HashSet::new());
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: 2,
                min_writer_version: 1,
                ..Default::default()
            }),
            create_metadata_action(None, Some(HashMap::new())),
        ];
        let snapshot_2 = DeltaTableState::from_actions(actions).unwrap();
        let eager_2 = snapshot_2.snapshot();
        assert!(checker_1.can_read_from(eager_2).is_err());
        assert!(checker_1.can_write_to(eager_2).is_err());
        assert!(checker_2.can_read_from(eager_1).is_ok());
        assert!(checker_2.can_read_from(eager_2).is_ok());
        assert!(checker_2.can_write_to(eager_2).is_ok());

        let checker_3 = ProtocolChecker::new(READER_V2.clone(), WRITER_V2.clone());
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: 2,
                min_writer_version: 2,
                ..Default::default()
            }),
            create_metadata_action(None, Some(HashMap::new())),
        ];
        let snapshot_3 = DeltaTableState::from_actions(actions).unwrap();
        let eager_3 = snapshot_3.snapshot();
        assert!(checker_1.can_read_from(eager_3).is_err());
        assert!(checker_1.can_write_to(eager_3).is_err());
        assert!(checker_2.can_read_from(eager_3).is_ok());
        assert!(checker_2.can_write_to(eager_3).is_err());
        assert!(checker_3.can_read_from(eager_1).is_ok());
        assert!(checker_3.can_read_from(eager_2).is_ok());
        assert!(checker_3.can_read_from(eager_3).is_ok());
        assert!(checker_3.can_write_to(eager_3).is_ok());

        let checker_4 = ProtocolChecker::new(READER_V2.clone(), WRITER_V3.clone());
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: 2,
                min_writer_version: 3,
                ..Default::default()
            }),
            create_metadata_action(None, Some(HashMap::new())),
        ];
        let snapshot_4 = DeltaTableState::from_actions(actions).unwrap();
        let eager_4 = snapshot_4.snapshot();
        assert!(checker_1.can_read_from(eager_4).is_err());
        assert!(checker_1.can_write_to(eager_4).is_err());
        assert!(checker_2.can_read_from(eager_4).is_ok());
        assert!(checker_2.can_write_to(eager_4).is_err());
        assert!(checker_3.can_read_from(eager_4).is_ok());
        assert!(checker_3.can_write_to(eager_4).is_err());
        assert!(checker_4.can_read_from(eager_1).is_ok());
        assert!(checker_4.can_read_from(eager_2).is_ok());
        assert!(checker_4.can_read_from(eager_3).is_ok());
        assert!(checker_4.can_read_from(eager_4).is_ok());
        assert!(checker_4.can_write_to(eager_4).is_ok());

        let checker_5 = ProtocolChecker::new(READER_V2.clone(), WRITER_V4.clone());
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: 2,
                min_writer_version: 4,
                ..Default::default()
            }),
            create_metadata_action(None, Some(HashMap::new())),
        ];
        let snapshot_5 = DeltaTableState::from_actions(actions).unwrap();
        let eager_5 = snapshot_5.snapshot();
        assert!(checker_1.can_read_from(eager_5).is_err());
        assert!(checker_1.can_write_to(eager_5).is_err());
        assert!(checker_2.can_read_from(eager_5).is_ok());
        assert!(checker_2.can_write_to(eager_5).is_err());
        assert!(checker_3.can_read_from(eager_5).is_ok());
        assert!(checker_3.can_write_to(eager_5).is_err());
        assert!(checker_4.can_read_from(eager_5).is_ok());
        assert!(checker_4.can_write_to(eager_5).is_err());
        assert!(checker_5.can_read_from(eager_1).is_ok());
        assert!(checker_5.can_read_from(eager_2).is_ok());
        assert!(checker_5.can_read_from(eager_3).is_ok());
        assert!(checker_5.can_read_from(eager_4).is_ok());
        assert!(checker_5.can_read_from(eager_5).is_ok());
        assert!(checker_5.can_write_to(eager_5).is_ok());

        let checker_6 = ProtocolChecker::new(READER_V2.clone(), WRITER_V5.clone());
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: 2,
                min_writer_version: 5,
                ..Default::default()
            }),
            create_metadata_action(None, Some(HashMap::new())),
        ];
        let snapshot_6 = DeltaTableState::from_actions(actions).unwrap();
        let eager_6 = snapshot_6.snapshot();
        assert!(checker_1.can_read_from(eager_6).is_err());
        assert!(checker_1.can_write_to(eager_6).is_err());
        assert!(checker_2.can_read_from(eager_6).is_ok());
        assert!(checker_2.can_write_to(eager_6).is_err());
        assert!(checker_3.can_read_from(eager_6).is_ok());
        assert!(checker_3.can_write_to(eager_6).is_err());
        assert!(checker_4.can_read_from(eager_6).is_ok());
        assert!(checker_4.can_write_to(eager_6).is_err());
        assert!(checker_5.can_read_from(eager_6).is_ok());
        assert!(checker_5.can_write_to(eager_6).is_err());
        assert!(checker_6.can_read_from(eager_1).is_ok());
        assert!(checker_6.can_read_from(eager_2).is_ok());
        assert!(checker_6.can_read_from(eager_3).is_ok());
        assert!(checker_6.can_read_from(eager_4).is_ok());
        assert!(checker_6.can_read_from(eager_5).is_ok());
        assert!(checker_6.can_read_from(eager_6).is_ok());
        assert!(checker_6.can_write_to(eager_6).is_ok());

        let checker_7 = ProtocolChecker::new(READER_V2.clone(), WRITER_V6.clone());
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: 2,
                min_writer_version: 6,
                ..Default::default()
            }),
            create_metadata_action(None, Some(HashMap::new())),
        ];
        let snapshot_7 = DeltaTableState::from_actions(actions).unwrap();
        let eager_7 = snapshot_7.snapshot();
        assert!(checker_1.can_read_from(eager_7).is_err());
        assert!(checker_1.can_write_to(eager_7).is_err());
        assert!(checker_2.can_read_from(eager_7).is_ok());
        assert!(checker_2.can_write_to(eager_7).is_err());
        assert!(checker_3.can_read_from(eager_7).is_ok());
        assert!(checker_3.can_write_to(eager_7).is_err());
        assert!(checker_4.can_read_from(eager_7).is_ok());
        assert!(checker_4.can_write_to(eager_7).is_err());
        assert!(checker_5.can_read_from(eager_7).is_ok());
        assert!(checker_5.can_write_to(eager_7).is_err());
        assert!(checker_6.can_read_from(eager_7).is_ok());
        assert!(checker_6.can_write_to(eager_7).is_err());
        assert!(checker_7.can_read_from(eager_1).is_ok());
        assert!(checker_7.can_read_from(eager_2).is_ok());
        assert!(checker_7.can_read_from(eager_3).is_ok());
        assert!(checker_7.can_read_from(eager_4).is_ok());
        assert!(checker_7.can_read_from(eager_5).is_ok());
        assert!(checker_7.can_read_from(eager_6).is_ok());
        assert!(checker_7.can_read_from(eager_7).is_ok());
        assert!(checker_7.can_write_to(eager_7).is_ok());
    }
}
