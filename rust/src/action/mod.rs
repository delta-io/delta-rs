//! Actions included in Delta table transaction logs

#![allow(non_camel_case_types)]

#[cfg(feature = "parquet")]
mod parquet_read;

#[cfg(feature = "parquet2")]
pub mod parquet2_read;

use crate::{schema::*, DeltaTableMetaData};
use percent_encoding::percent_decode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Error returned when an invalid Delta log action is encountered.
#[derive(thiserror::Error, Debug)]
pub enum ActionError {
    /// The action contains an invalid field.
    #[error("Invalid action field: {0}")]
    InvalidField(String),
    /// A parquet log checkpoint file contains an invalid action.
    #[error("Invalid action in parquet row: {0}")]
    InvalidRow(String),
    /// A generic action error. The wrapped error string describes the details.
    #[error("Generic action error: {0}")]
    Generic(String),
    #[cfg(feature = "parquet2")]
    #[error("Failed to parse parquet checkpoint: {}", .source)]
    /// Error returned when parsing checkpoint parquet using the parquet2 crate.
    ParquetParseError {
        /// Parquet error details returned when parsing the checkpoint parquet
        #[from]
        source: parquet2_read::ParseError,
    },
    #[cfg(feature = "parquet")]
    #[error("Failed to parse parquet checkpoint: {}", .source)]
    /// Error returned when parsing checkpoint parquet using the parquet crate.
    ParquetParseError {
        /// Parquet error details returned when parsing the checkpoint parquet
        #[from]
        source: parquet::errors::ParquetError,
    },
}

fn decode_path(raw_path: &str) -> Result<String, ActionError> {
    percent_decode(raw_path.as_bytes())
        .decode_utf8()
        .map(|c| c.to_string())
        .map_err(|e| ActionError::InvalidField(format!("Decode path failed for action: {}", e)))
}

/// Struct used to represent minValues and maxValues in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnValueStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnValueStat>),
    /// Json representation of statistics.
    Value(Value),
}

impl ColumnValueStat {
    /// Returns the HashMap representation of the ColumnValueStat.
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnValueStat>> {
        match self {
            ColumnValueStat::Column(m) => Some(m),
            _ => None,
        }
    }

    /// Returns the serde_json representation of the ColumnValueStat.
    pub fn as_value(&self) -> Option<&Value> {
        match self {
            ColumnValueStat::Value(v) => Some(v),
            _ => None,
        }
    }
}

/// Struct used to represent nullCount in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnCountStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnCountStat>),
    /// Json representation of statistics.
    Value(DeltaDataTypeLong),
}

impl ColumnCountStat {
    /// Returns the HashMap representation of the ColumnCountStat.
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnCountStat>> {
        match self {
            ColumnCountStat::Column(m) => Some(m),
            _ => None,
        }
    }

    /// Returns the serde_json representation of the ColumnCountStat.
    pub fn as_value(&self) -> Option<DeltaDataTypeLong> {
        match self {
            ColumnCountStat::Value(v) => Some(*v),
            _ => None,
        }
    }
}

/// Statistics associated with Add actions contained in the Delta log.
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// Number of records in the file associated with the log action.
    pub num_records: DeltaDataTypeLong,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: HashMap<String, ColumnValueStat>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, ColumnValueStat>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, ColumnCountStat>,
}

/// File stats parsed from raw parquet format.
#[derive(Debug, Default)]
pub struct StatsParsed {
    /// Number of records in the file associated with the log action.
    pub num_records: DeltaDataTypeLong,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    #[cfg(feature = "parquet")]
    pub min_values: HashMap<String, parquet::record::Field>,
    /// Contains a value smaller than all values present in the file for all columns.
    #[cfg(feature = "parquet2")]
    pub min_values: HashMap<String, String>,
    /// Contains a value larger than all values present in the file for all columns.
    #[cfg(feature = "parquet")]
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, parquet::record::Field>,
    #[cfg(feature = "parquet2")]
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, String>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, DeltaDataTypeLong>,
}

/// Delta log action that describes a parquet data file that is part of the table.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    /// A relative path, from the root of the table, to a file that should be added to the table
    pub path: String,
    /// The size of this file in bytes
    pub size: DeltaDataTypeLong,
    /// A map from partition column to value for this file
    pub partition_values: HashMap<String, Option<String>>,
    /// Partition values stored in raw parquet struct format. In this struct, the column names
    /// correspond to the partition columns and the values are stored in their corresponding data
    /// type. This is a required field when the table is partitioned and the table property
    /// delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
    /// column can be omitted.
    ///
    /// This field is only available in add action records read from checkpoints
    #[cfg(feature = "parquet")]
    #[serde(skip_serializing, skip_deserializing)]
    pub partition_values_parsed: Option<parquet::record::Row>,
    /// Partition values stored in raw parquet struct format. In this struct, the column names
    /// correspond to the partition columns and the values are stored in their corresponding data
    /// type. This is a required field when the table is partitioned and the table property
    /// delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
    /// column can be omitted.
    ///
    /// This field is only available in add action records read from checkpoints
    #[cfg(feature = "parquet2")]
    #[serde(skip_serializing, skip_deserializing)]
    pub partition_values_parsed: Option<String>,
    /// The time this file was created, as milliseconds since the epoch
    pub modification_time: DeltaDataTypeTimestamp,
    /// When false the file must already be present in the table or the records in the added file
    /// must be contained in one or more remove actions in the same version
    ///
    /// streaming queries that are tailing the transaction log can use this flag to skip actions
    /// that would not affect the final results.
    pub data_change: bool,
    /// Contains statistics (e.g., count, min/max values for columns) about the data in this file
    pub stats: Option<String>,
    /// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
    /// raw parquet format. This field needs to be written when statistics are available and the
    /// table property: delta.checkpoint.writeStatsAsStruct is set to true.
    ///
    /// This field is only available in add action records read from checkpoints
    #[cfg(feature = "parquet")]
    #[serde(skip_serializing, skip_deserializing)]
    pub stats_parsed: Option<parquet::record::Row>,
    /// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
    /// raw parquet format. This field needs to be written when statistics are available and the
    /// table property: delta.checkpoint.writeStatsAsStruct is set to true.
    ///
    /// This field is only available in add action records read from checkpoints
    #[cfg(feature = "parquet2")]
    #[serde(skip_serializing, skip_deserializing)]
    pub stats_parsed: Option<String>,
    /// Map containing metadata about this file
    pub tags: Option<HashMap<String, Option<String>>>,
}

impl Add {
    /// Returns the Add action with path decoded.
    pub fn path_decoded(self) -> Result<Self, ActionError> {
        decode_path(&self.path).map(|path| Self { path, ..self })
    }

    /// Get whatever stats are available. Uses (parquet struct) parsed_stats if present falling back to json stats.
    pub fn get_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        match self.get_stats_parsed() {
            Ok(Some(stats)) => Ok(Some(stats)),
            Ok(None) => self.get_json_stats(),
            Err(e) => {
                log::error!(
                    "Error when reading parquet stats {:?} {e}. Attempting to read json stats",
                    self.stats_parsed
                );
                self.get_json_stats()
            }
        }
    }

    /// Returns the serde_json representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    pub fn get_json_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        self.stats
            .as_ref()
            .map_or(Ok(None), |s| serde_json::from_str(s))
    }
}

/// Describes the data format of files in the table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Format {
    /// Name of the encoding for files in this table.
    provider: String,
    /// A map containing configuration options for the format.
    options: HashMap<String, Option<String>>,
}

impl Format {
    /// Allows creation of a new action::Format
    pub fn new(provider: String, options: Option<HashMap<String, Option<String>>>) -> Self {
        let options = options.unwrap_or_default();
        Self { provider, options }
    }

    /// Return the Format provider
    pub fn get_provider(self) -> String {
        self.provider
    }
}

// Assuming this is a more appropriate default than derived Default
impl Default for Format {
    fn default() -> Self {
        Self {
            provider: "parquet".to_string(),
            options: Default::default(),
        }
    }
}

/// Action that describes the metadata of the table.
/// This is a top-level action in Delta log entries.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MetaData {
    /// Unique identifier for this table
    pub id: Guid,
    /// User-provided identifier for this table
    pub name: Option<String>,
    /// User-provided description for this table
    pub description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub format: Format,
    /// Schema of the table
    pub schema_string: String,
    /// An array containing the names of columns by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: Option<DeltaDataTypeTimestamp>,
    /// A map containing configuration options for the table
    pub configuration: HashMap<String, Option<String>>,
}

impl MetaData {
    /// Returns the table schema from the embedded schema string contained within the metadata
    /// action.
    pub fn get_schema(&self) -> Result<Schema, serde_json::error::Error> {
        serde_json::from_str(&self.schema_string)
    }
}

/// Represents a tombstone (deleted file) in the Delta log.
/// This is a top-level action in Delta log entries.
#[derive(Serialize, Deserialize, Clone, Eq, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// The path of the file that is removed from the table.
    pub path: String,
    /// The timestamp when the remove was added to table state.
    pub deletion_timestamp: Option<DeltaDataTypeTimestamp>,
    /// Whether data is changed by the remove. A table optimize will report this as false for
    /// example, since it adds and removes files by combining many files into one.
    pub data_change: bool,
    /// When true the fields partitionValues, size, and tags are present
    ///
    /// NOTE: Although it's defined as required in scala delta implementation, but some writes
    /// it's still nullable so we keep it as Option<> for compatibly.
    pub extended_file_metadata: Option<bool>,
    /// A map from partition column to value for this file.
    pub partition_values: Option<HashMap<String, Option<String>>>,
    /// Size of this file in bytes
    pub size: Option<DeltaDataTypeLong>,
    /// Map containing metadata about this file
    pub tags: Option<HashMap<String, Option<String>>>,
}

impl Hash for Remove {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

/// Borrow `Remove` as str so we can look at tombstones hashset in `DeltaTableState` by using
/// a path from action `Add`.
impl Borrow<str> for Remove {
    fn borrow(&self) -> &str {
        self.path.as_ref()
    }
}

impl PartialEq for Remove {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.deletion_timestamp == other.deletion_timestamp
            && self.data_change == other.data_change
            && self.extended_file_metadata == other.extended_file_metadata
            && self.partition_values == other.partition_values
            && self.size == other.size
            && self.tags == other.tags
    }
}

impl Remove {
    /// Returns the Remove action with path decoded.
    pub fn path_decoded(self) -> Result<Self, ActionError> {
        decode_path(&self.path).map(|path| Self { path, ..self })
    }
}

/// Action used by streaming systems to track progress using application-specific versions to
/// enable idempotency.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Txn {
    /// A unique identifier for the application performing the transaction.
    pub app_id: String,
    /// An application-specific numeric identifier for this transaction.
    pub version: DeltaDataTypeVersion,
    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    pub last_updated: Option<DeltaDataTypeTimestamp>,
}

/// Action used to increase the version of the Delta protocol required to read or write to the
/// table.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// Minimum version of the Delta read protocol a client must implement to correctly read the
    /// table.
    pub min_reader_version: DeltaDataTypeInt,
    /// Minimum version of the Delta write protocol a client must implement to correctly read the
    /// table.
    pub min_writer_version: DeltaDataTypeInt,
}

type CommitInfo = Map<String, Value>;

/// Represents an action in the Delta log. The Delta log is an aggregate of all actions performed
/// on the table, so the full list of actions is required to properly read a table.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    /// Changes the current metadata of the table. Must be present in the first version of a table.
    /// Subsequent `metaData` actions completely overwrite previous metadata.
    metaData(MetaData),
    /// Adds a file to the table state.
    add(Add),
    /// Removes a file from the table state.
    remove(Remove),
    /// Used by streaming systems to track progress externally with application specific version
    /// identifiers.
    txn(Txn),
    /// Describes the minimum reader and writer versions required to read or write to the table.
    protocol(Protocol),
    /// Describes commit provenance information for the table.
    commitInfo(CommitInfo),
}

/// Operation performed when creating a new log entry with one or more actions.
/// This is a key element of the `CommitInfo` action.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DeltaOperation {
    /// Represents a Delta `Create` operation.
    /// Would usually only create the table, if also data is written,
    /// a `Write` operations is more appropriate
    Create {
        /// The save mode used during the create.
        mode: SaveMode,
        /// The storage location of the new table
        location: String,
        /// The min reader and writer protocol versions of the table
        protocol: Protocol,
        /// Metadata associated with the new table
        metadata: DeltaTableMetaData,
    },

    /// Represents a Delta `Write` operation.
    /// Write operations will typically only include `Add` actions.
    #[serde(rename_all = "camelCase")]
    Write {
        /// The save mode used during the write.
        mode: SaveMode,
        /// The columns the write is partitioned by.
        partition_by: Option<Vec<String>>,
        /// The predicate used during the write.
        predicate: Option<String>,
    },
    /// Represents a Delta `StreamingUpdate` operation.
    #[serde(rename_all = "camelCase")]
    StreamingUpdate {
        /// The output mode the streaming writer is using.
        output_mode: OutputMode,
        /// The query id of the streaming writer.
        query_id: String,
        /// The epoch id of the written micro-batch.
        epoch_id: i64,
    },

    #[serde(rename_all = "camelCase")]
    /// Represents a `Optimize` operation
    Optimize {
        // TODO: Create a string representation of the filter passed to optimize
        /// The filter used to determine which partitions to filter
        predicate: Option<String>,
        /// Target optimize size
        target_size: DeltaDataTypeLong,
    }, // TODO: Add more operations
}

impl DeltaOperation {
    /// Retrieve basic commit information to be added to Delta commits
    pub fn get_commit_info(&self) -> Map<String, Value> {
        let mut commit_info = Map::<String, Value>::new();
        let operation = match &self {
            DeltaOperation::Create { .. } => "delta-rs.Create",
            DeltaOperation::Write { .. } => "delta-rs.Write",
            DeltaOperation::StreamingUpdate { .. } => "delta-rs.StreamingUpdate",
            DeltaOperation::Optimize { .. } => "delta-rs.Optimize",
        };
        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String(operation.into()),
        );

        if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(self) {
            commit_info.insert(
                "operationParameters".to_string(),
                map.values().next().unwrap().clone(),
            );
        };

        commit_info
    }
}

/// The SaveMode used when performing a DeltaOperation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SaveMode {
    /// Files will be appended to the target location.
    Append,
    /// The target location will be overwritten.
    Overwrite,
    /// If files exist for the target, the operation must fail.
    ErrorIfExists,
    /// If files exist for the target, the operation must not proceed or change any data.
    Ignore,
}

/// The OutputMode used in streaming operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum OutputMode {
    /// Only new rows will be written when new data is available.
    Append,
    /// The full output (all rows) will be written whenever new data is available.
    Complete,
    /// Only rows with updates will be written when new or changed data is available.
    Update,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_table_stats() {
        let action = Add {
            stats: Some(
                serde_json::json!({
                    "numRecords": 22,
                    "minValues": {"a": 1, "nested": {"b": 2, "c": "a"}},
                    "maxValues": {"a": 10, "nested": {"b": 20, "c": "z"}},
                    "nullCount": {"a": 1, "nested": {"b": 0, "c": 1}},
                })
                .to_string(),
            ),
            ..Default::default()
        };

        let stats = action.get_stats().unwrap().unwrap();

        assert_eq!(stats.num_records, 22);

        assert_eq!(
            stats.min_values["a"].as_value().unwrap(),
            &serde_json::json!(1)
        );
        assert_eq!(
            stats.min_values["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(2)
        );
        assert_eq!(
            stats.min_values["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("a")
        );

        assert_eq!(
            stats.max_values["a"].as_value().unwrap(),
            &serde_json::json!(10)
        );
        assert_eq!(
            stats.max_values["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(20)
        );
        assert_eq!(
            stats.max_values["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("z")
        );

        assert_eq!(stats.null_count["a"].as_value().unwrap(), 1);
        assert_eq!(
            stats.null_count["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            0
        );
        assert_eq!(
            stats.null_count["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            1
        );
    }
}
