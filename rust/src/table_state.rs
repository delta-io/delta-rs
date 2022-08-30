//! The module for delta table state.

use super::{
    ApplyLogError, CheckPoint, DeltaDataTypeLong, DeltaDataTypeVersion, DeltaTable,
    DeltaTableConfig, DeltaTableError, DeltaTableMetaData,
};
use crate::action::{self, Action};
use crate::delta_config;
use chrono::Utc;
use object_store::ObjectStore;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::io::{BufRead, BufReader, Cursor};

/// State snapshot currently held by the Delta Table instance.
#[derive(Default, Debug, Clone)]
pub struct DeltaTableState {
    version: DeltaDataTypeVersion,
    // A remove action should remain in the state of the table as a tombstone until it has expired.
    // A tombstone expires when the creation timestamp of the delta file exceeds the expiration
    tombstones: HashSet<action::Remove>,
    files: Vec<action::Add>,
    commit_infos: Vec<Map<String, Value>>,
    app_transaction_version: HashMap<String, DeltaDataTypeVersion>,
    min_reader_version: i32,
    min_writer_version: i32,
    current_metadata: Option<DeltaTableMetaData>,
    tombstone_retention_millis: DeltaDataTypeLong,
    log_retention_millis: DeltaDataTypeLong,
    enable_expired_log_cleanup: bool,
}

impl DeltaTableState {
    /// Create Table state with specified version
    pub fn with_version(version: DeltaDataTypeVersion) -> Self {
        Self {
            version,
            ..Self::default()
        }
    }

    /// Return table version
    pub fn version(&self) -> DeltaDataTypeVersion {
        self.version
    }

    /// Construct a delta table state object from commit version.
    pub async fn from_commit(
        table: &DeltaTable,
        version: DeltaDataTypeVersion,
    ) -> Result<Self, ApplyLogError> {
        let commit_uri = table.commit_uri_from_version(version);
        let commit_log_bytes = table.storage.get(&commit_uri).await?.bytes().await?;
        let reader = BufReader::new(Cursor::new(commit_log_bytes));

        let mut new_state = DeltaTableState::with_version(version);
        for line in reader.lines() {
            let action: action::Action = serde_json::from_str(line?.as_str())?;
            new_state.process_action(
                action,
                table.config.require_tombstones,
                table.config.require_files,
            )?;
        }

        Ok(new_state)
    }

    /// Construct a delta table state object from a list of actions
    pub fn from_actions(
        actions: Vec<Action>,
        version: DeltaDataTypeVersion,
    ) -> Result<Self, ApplyLogError> {
        let mut new_state = DeltaTableState::with_version(version);
        for action in actions {
            new_state.process_action(action, true, true)?;
        }
        Ok(new_state)
    }

    /// Update DeltaTableState with checkpoint data.
    pub fn process_checkpoint_bytes(
        &mut self,
        data: bytes::Bytes,
        table_config: &DeltaTableConfig,
    ) -> Result<(), DeltaTableError> {
        #[cfg(feature = "parquet")]
        {
            use parquet::file::reader::{FileReader, SerializedFileReader};

            let preader = SerializedFileReader::new(data)?;
            let schema = preader.metadata().file_metadata().schema();
            if !schema.is_group() {
                return Err(DeltaTableError::from(action::ActionError::Generic(
                    "Action record in checkpoint should be a struct".to_string(),
                )));
            }
            for record in preader.get_row_iter(None)? {
                self.process_action(
                    action::Action::from_parquet_record(schema, &record)?,
                    table_config.require_tombstones,
                    table_config.require_files,
                )?;
            }
        }

        #[cfg(feature = "parquet2")]
        {
            use crate::action::parquet2_read::actions_from_row_group;
            use parquet2::read::read_metadata;

            let mut reader = std::io::Cursor::new(data);
            let metadata = read_metadata(&mut reader)?;

            for row_group in metadata.row_groups {
                for action in actions_from_row_group(row_group, &mut reader)
                    .map_err(action::ActionError::from)?
                {
                    self.process_action(
                        action,
                        table_config.require_tombstones,
                        table_config.require_files,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Construct a delta table state object from checkpoint.
    pub async fn from_checkpoint(
        table: &DeltaTable,
        check_point: &CheckPoint,
    ) -> Result<Self, DeltaTableError> {
        let checkpoint_data_paths = table.get_checkpoint_data_paths(check_point);
        let mut new_state = Self::with_version(check_point.version);

        for f in &checkpoint_data_paths {
            let obj = table.storage.get(f).await?.bytes().await?;
            new_state.process_checkpoint_bytes(obj, &table.config)?;
        }

        Ok(new_state)
    }

    /// List of commit info maps.
    pub fn commit_infos(&self) -> &Vec<Map<String, Value>> {
        &self.commit_infos
    }

    /// Retention of tombstone in milliseconds.
    pub fn tombstone_retention_millis(&self) -> DeltaDataTypeLong {
        self.tombstone_retention_millis
    }

    /// Retention of logs in milliseconds.
    pub fn log_retention_millis(&self) -> DeltaDataTypeLong {
        self.log_retention_millis
    }

    /// Whether to clean up expired checkpoints and delta logs.
    pub fn enable_expired_log_cleanup(&self) -> bool {
        self.enable_expired_log_cleanup
    }

    /// Full list of tombstones (remove actions) representing files removed from table state).
    pub fn all_tombstones(&self) -> &HashSet<action::Remove> {
        &self.tombstones
    }

    /// List of unexpired tombstones (remove actions) representing files removed from table state.
    /// The retention period is set by `deletedFileRetentionDuration` with default value of 1 week.
    pub fn unexpired_tombstones(&self) -> impl Iterator<Item = &action::Remove> {
        let retention_timestamp = Utc::now().timestamp_millis() - self.tombstone_retention_millis;
        self.tombstones
            .iter()
            .filter(move |t| t.deletion_timestamp.unwrap_or(0) > retention_timestamp)
    }

    /// Full list of add actions representing all parquet files that are part of the current
    /// delta table state.
    pub fn files(&self) -> &Vec<action::Add> {
        self.files.as_ref()
    }

    /// HashMap containing the last txn version stored for every app id writing txn
    /// actions.
    pub fn app_transaction_version(&self) -> &HashMap<String, DeltaDataTypeVersion> {
        &self.app_transaction_version
    }

    /// The min reader version required by the protocol.
    pub fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    /// The min writer version required by the protocol.
    pub fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    /// The most recent metadata of the table.
    pub fn current_metadata(&self) -> Option<&DeltaTableMetaData> {
        self.current_metadata.as_ref()
    }

    /// Merges new state information into our state
    ///
    /// The DeltaTableState also carries the version information for the given state,
    /// as there is a one-to-one match between a table state and a version. In merge/update
    /// scenarios we cannot infer the intended / correct version number. By default this
    /// function will update the tracked version if the version on `new_state` is larger then the
    /// currently set version however it is up to the caller to update the `version` field according
    /// to the version the merged state represents.
    pub fn merge(
        &mut self,
        mut new_state: DeltaTableState,
        require_tombstones: bool,
        require_files: bool,
    ) {
        if !new_state.tombstones.is_empty() {
            self.files
                .retain(|a| !new_state.tombstones.contains(a.path.as_str()));
        }

        if require_tombstones && require_files {
            new_state.tombstones.into_iter().for_each(|r| {
                self.tombstones.insert(r);
            });

            if !new_state.files.is_empty() {
                new_state.files.iter().for_each(|s| {
                    self.tombstones.remove(s.path.as_str());
                });
            }
        }

        if require_files {
            self.files.append(&mut new_state.files);
        }

        if new_state.min_reader_version > 0 {
            self.min_reader_version = new_state.min_reader_version;
            self.min_writer_version = new_state.min_writer_version;
        }

        if new_state.current_metadata.is_some() {
            self.tombstone_retention_millis = new_state.tombstone_retention_millis;
            self.log_retention_millis = new_state.log_retention_millis;
            self.enable_expired_log_cleanup = new_state.enable_expired_log_cleanup;
            self.current_metadata = new_state.current_metadata.take();
        }

        new_state
            .app_transaction_version
            .drain()
            .for_each(|(app_id, version)| {
                *self
                    .app_transaction_version
                    .entry(app_id)
                    .or_insert(version) = version
            });

        if !new_state.commit_infos.is_empty() {
            self.commit_infos.append(&mut new_state.commit_infos);
        }

        if self.version < new_state.version {
            self.version = new_state.version
        }
    }

    /// Process given action by updating current state.
    fn process_action(
        &mut self,
        action: action::Action,
        require_tombstones: bool,
        require_files: bool,
    ) -> Result<(), ApplyLogError> {
        match action {
            action::Action::add(v) => {
                if require_files {
                    self.files.push(v.path_decoded()?);
                }
            }
            action::Action::remove(v) => {
                if require_tombstones && require_files {
                    let v = v.path_decoded()?;
                    self.tombstones.insert(v);
                }
            }
            action::Action::protocol(v) => {
                self.min_reader_version = v.min_reader_version;
                self.min_writer_version = v.min_writer_version;
            }
            action::Action::metaData(v) => {
                let md = DeltaTableMetaData::try_from(v)
                    .map_err(|e| ApplyLogError::InvalidJson { source: e })?;
                self.tombstone_retention_millis = delta_config::TOMBSTONE_RETENTION
                    .get_interval_from_metadata(&md)?
                    .as_millis() as i64;
                self.log_retention_millis = delta_config::LOG_RETENTION
                    .get_interval_from_metadata(&md)?
                    .as_millis() as i64;
                self.enable_expired_log_cleanup =
                    delta_config::ENABLE_EXPIRED_LOG_CLEANUP.get_boolean_from_metadata(&md)?;
                self.current_metadata = Some(md);
            }
            action::Action::txn(v) => {
                *self
                    .app_transaction_version
                    .entry(v.app_id)
                    .or_insert(v.version) = v.version;
            }
            action::Action::commitInfo(v) => {
                self.commit_infos.push(v);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn state_records_new_txn_version() {
        let mut app_transaction_version = HashMap::new();
        app_transaction_version.insert("abc".to_string(), 1);
        app_transaction_version.insert("xyz".to_string(), 1);

        let mut state = DeltaTableState {
            version: -1,
            files: vec![],
            commit_infos: vec![],
            tombstones: HashSet::new(),
            current_metadata: None,
            min_reader_version: 1,
            min_writer_version: 1,
            app_transaction_version,
            tombstone_retention_millis: 0,
            log_retention_millis: 0,
            enable_expired_log_cleanup: true,
        };

        let txn_action = action::Action::txn(action::Txn {
            app_id: "abc".to_string(),
            version: 2,
            last_updated: Some(0),
        });

        let _ = state.process_action(txn_action, false, true).unwrap();

        assert_eq!(2, *state.app_transaction_version().get("abc").unwrap());
        assert_eq!(1, *state.app_transaction_version().get("xyz").unwrap());
    }
}
