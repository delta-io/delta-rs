//! Actions included in Delta table transaction logs

#![allow(non_camel_case_types)]

#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod checkpoints;
#[cfg(feature = "parquet2")]
pub mod parquet2_read;
#[cfg(feature = "parquet")]
mod parquet_read;
mod serde_path;
mod time_utils;

#[cfg(feature = "arrow")]
use arrow_schema::ArrowError;
use futures::StreamExt;
use lazy_static::lazy_static;
use log::*;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::take;
use std::str::FromStr;

use crate::errors::DeltaResult;
use crate::storage::ObjectStoreRef;
use crate::table::config::IsolationLevel;
use crate::table::DeltaTableMetaData;
use crate::{schema::*, table::CheckPoint};

/// Error returned when an invalid Delta log action is encountered.
#[allow(missing_docs)]
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    #[error("Table state does not contain metadata")]
    NoMetaData,

    #[error("Checkpoint file not found")]
    CheckpointNotFound,

    #[error("End of transaction log")]
    EndOfLog,

    /// The action contains an invalid field.
    #[error("Invalid action field: {0}")]
    InvalidField(String),

    /// A parquet log checkpoint file contains an invalid action.
    #[error("Invalid action in parquet row: {0}")]
    InvalidRow(String),

    /// A transaction log contains invalid deletion vector storage type
    #[error("Invalid deletion vector storage type: {0}")]
    InvalidDeletionVectorStorageType(String),

    /// A generic action error. The wrapped error string describes the details.
    #[error("Generic action error: {0}")]
    Generic(String),

    #[cfg(any(feature = "parquet", feature = "parquet2"))]
    /// Error returned when parsing checkpoint parquet using the parquet crate.
    #[error("Failed to parse parquet checkpoint: {source}")]
    ParquetParseError {
        /// Parquet error details returned when parsing the checkpoint parquet
        #[cfg(feature = "parquet2")]
        #[from]
        source: parquet2::error::Error,
        /// Parquet error details returned when parsing the checkpoint parquet
        #[cfg(feature = "parquet")]
        #[from]
        source: parquet::errors::ParquetError,
    },

    /// Faild to serialize operation
    #[error("Failed to serialize operation: {source}")]
    SerializeOperation {
        #[from]
        /// The source error
        source: serde_json::Error,
    },

    /// Error returned when converting the schema to Arrow format failed.
    #[cfg(feature = "arrow")]
    #[error("Failed to convert into Arrow schema: {}", .source)]
    Arrow {
        /// Arrow error details returned when converting the schema in Arrow format failed
        #[from]
        source: ArrowError,
    },

    /// Passthrough error returned when calling ObjectStore.
    #[error("ObjectStoreError: {source}")]
    ObjectStore {
        /// The source ObjectStoreError.
        #[from]
        source: ObjectStoreError,
    },

    #[error("Io: {source}")]
    IO {
        #[from]
        source: std::io::Error,
    },
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
    Value(i64),
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
    pub fn as_value(&self) -> Option<i64> {
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
    pub num_records: i64,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: HashMap<String, ColumnValueStat>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, ColumnValueStat>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, ColumnCountStat>,
}

/// Statistics associated with Add actions contained in the Delta log.
/// min_values, max_values and null_count are optional to allow them to be missing
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct PartialStats {
    /// Number of records in the file associated with the log action.
    pub num_records: i64,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: Option<HashMap<String, ColumnValueStat>>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: Option<HashMap<String, ColumnValueStat>>,
    /// The number of null values for all columns.
    pub null_count: Option<HashMap<String, ColumnCountStat>>,
}

impl PartialStats {
    /// Fills in missing HashMaps
    pub fn as_stats(&mut self) -> Stats {
        let min_values = take(&mut self.min_values);
        let max_values = take(&mut self.max_values);
        let null_count = take(&mut self.null_count);
        Stats {
            num_records: self.num_records,
            min_values: match min_values {
                Some(minv) => minv,
                None => HashMap::default(),
            },
            max_values: match max_values {
                Some(maxv) => maxv,
                None => HashMap::default(),
            },
            null_count: match null_count {
                Some(nc) => nc,
                None => HashMap::default(),
            },
        }
    }
}

/// File stats parsed from raw parquet format.
#[derive(Debug, Default)]
pub struct StatsParsed {
    /// Number of records in the file associated with the log action.
    pub num_records: i64,

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
    pub null_count: HashMap<String, i64>,
}

/// Delta AddCDCFile action that describes a parquet CDC data file.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct AddCDCFile {
    /// A relative path, from the root of the table, or an
    /// absolute path to a CDC file
    #[serde(with = "serde_path")]
    pub path: String,
    /// The size of this file in bytes
    pub size: i64,
    /// A map from partition column to value for this file
    pub partition_values: HashMap<String, Option<String>>,
    /// Should always be set to false because they do not change the underlying data of the table
    pub data_change: bool,
    /// Map containing metadata about this file
    pub tags: Option<HashMap<String, Option<String>>>,
}

///Storage type of deletion vector
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde()]
pub enum StorageType {
    /// Stored at relative path derived from a UUID.
    #[serde(rename = "u")]
    UuidRelativePath,
    /// Stored as inline string.
    #[serde(rename = "i")]
    Inline,
    /// Stored at an absolute path.
    #[serde(rename = "p")]
    AbsolutePath,
}

impl Default for StorageType {
    fn default() -> Self {
        Self::UuidRelativePath // seems to be used by Databricks and therefore most common
    }
}

impl FromStr for StorageType {
    type Err = ProtocolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "u" => Ok(Self::UuidRelativePath),
            "i" => Ok(Self::Inline),
            "p" => Ok(Self::AbsolutePath),
            _ => Err(ProtocolError::InvalidDeletionVectorStorageType(
                s.to_string(),
            )),
        }
    }
}

impl ToString for StorageType {
    fn to_string(&self) -> String {
        match self {
            Self::UuidRelativePath => "u".to_string(),
            Self::Inline => "i".to_string(),
            Self::AbsolutePath => "p".to_string(),
        }
    }
}

/// Describes deleted rows of a parquet file as part of an add or remove action
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeletionVector {
    ///storageType of the deletion vector. p = Absolute Path, i = Inline, u = UUid Relative Path
    pub storage_type: StorageType,

    ///If storageType = 'u' then <random prefix - optional><base85 encoded uuid>
    ///If storageType = 'i' then <base85 encoded bytes> of the deletion vector data
    ///If storageType = 'p' then <absolute path>
    pub path_or_inline_dv: String,

    ///Start of the data for this DV in number of bytes from the beginning of the file it is stored in. Always None (absent in JSON) when storageType = 'i'.
    pub offset: Option<i32>,

    ///Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    ///Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

impl PartialEq for DeletionVector {
    fn eq(&self, other: &Self) -> bool {
        self.storage_type == other.storage_type
            && self.path_or_inline_dv == other.path_or_inline_dv
            && self.offset == other.offset
            && self.size_in_bytes == other.size_in_bytes
            && self.cardinality == other.cardinality
    }
}

impl Eq for DeletionVector {}

/// Delta log action that describes a parquet data file that is part of the table.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    /// A relative path, from the root of the table, to a file that should be added to the table
    #[serde(with = "serde_path")]
    pub path: String,
    /// The size of this file in bytes
    pub size: i64,
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
    pub modification_time: i64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,

    /// Metadata about deletion vector
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVector>,
}

impl Hash for Add {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl PartialEq for Add {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.size == other.size
            && self.partition_values == other.partition_values
            && self.modification_time == other.modification_time
            && self.data_change == other.data_change
            && self.stats == other.stats
            && self.tags == other.tags
            && self.deletion_vector == other.deletion_vector
    }
}

impl Eq for Add {}

impl Add {
    /// Get whatever stats are available. Uses (parquet struct) parsed_stats if present falling back to json stats.
    #[cfg(any(feature = "parquet", feature = "parquet2"))]
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

    /// Get whatever stats are available.
    #[cfg(not(any(feature = "parquet", feature = "parquet2")))]
    pub fn get_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        self.get_json_stats()
    }

    /// Returns the serde_json representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    pub fn get_json_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        let ps: Result<Option<PartialStats>, serde_json::error::Error> = self
            .stats
            .as_ref()
            .map_or(Ok(None), |s| serde_json::from_str(s));

        match ps {
            Ok(Some(mut partial)) => Ok(Some(partial.as_stats())),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
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

/// Return a default empty schema to be used for edge-cases when a schema is missing
fn default_schema() -> String {
    warn!("A `metaData` action was missing a `schemaString` and has been given an empty schema");
    r#"{"type":"struct",  "fields": []}"#.into()
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
    #[serde(default = "default_schema")]
    pub schema_string: String,
    /// An array containing the names of columns by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: Option<i64>,
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

impl TryFrom<DeltaTableMetaData> for MetaData {
    type Error = ProtocolError;

    fn try_from(metadata: DeltaTableMetaData) -> Result<Self, Self::Error> {
        let schema_string = serde_json::to_string(&metadata.schema)
            .map_err(|source| ProtocolError::SerializeOperation { source })?;
        Ok(Self {
            id: metadata.id,
            name: metadata.name,
            description: metadata.description,
            format: metadata.format,
            schema_string,
            partition_columns: metadata.partition_columns,
            created_time: metadata.created_time,
            configuration: metadata.configuration,
        })
    }
}

/// Represents a tombstone (deleted file) in the Delta log.
/// This is a top-level action in Delta log entries.
#[derive(Serialize, Deserialize, Clone, Eq, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// The path of the file that is removed from the table.
    #[serde(with = "serde_path")]
    pub path: String,
    /// The timestamp when the remove was added to table state.
    pub deletion_timestamp: Option<i64>,
    /// Whether data is changed by the remove. A table optimize will report this as false for
    /// example, since it adds and removes files by combining many files into one.
    pub data_change: bool,
    /// When true the fields partitionValues, size, and tags are present
    ///
    /// NOTE: Although it's defined as required in scala delta implementation, but some writes
    /// it's still nullable so we keep it as Option<> for compatibly.
    pub extended_file_metadata: Option<bool>,
    /// A map from partition column to value for this file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, Option<String>>>,
    /// Size of this file in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Map containing metadata about this file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
    /// Metadata about deletion vector
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVector>,
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
            && self.deletion_vector == other.deletion_vector
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
    pub version: i64,
    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    pub last_updated: Option<i64>,
}

/// Action used to increase the version of the Delta protocol required to read or write to the
/// table.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// Minimum version of the Delta read protocol a client must implement to correctly read the
    /// table.
    pub min_reader_version: i32,
    /// Minimum version of the Delta write protocol a client must implement to correctly read the
    /// table.
    pub min_writer_version: i32,
}

/// The commitInfo is a fairly flexible action within the delta specification, where arbitrary data can be stored.
/// However the reference implementation as well as delta-rs store useful information that may for instance
/// allow us to be more permissive in commit conflict resolution.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CommitInfo {
    /// Timestamp in millis when the commit was created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    /// Id of the user invoking the commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// Name of the user invoking the commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    /// The operation performed during the
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// Parameters used for table operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_parameters: Option<HashMap<String, serde_json::Value>>,
    /// Version of the table when the operation was started
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_version: Option<i64>,
    /// The isolation level of the commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<IsolationLevel>,
    /// TODO
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_blind_append: Option<bool>,
    /// Delta engine which created the commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine_info: Option<String>,
    /// Additional provenance information for the commit
    #[serde(flatten, default)]
    pub info: Map<String, serde_json::Value>,
}

/// The domain metadata action contains a configuration (string) for a named metadata domain
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DomainMetaData {
    /// Identifier for this domain (system or user-provided)
    pub domain: String,
    /// String containing configuration for the metadata domain
    pub configuration: String,
    /// When `true` the action serves as a tombstone
    pub removed: bool,
}

/// Represents an action in the Delta log. The Delta log is an aggregate of all actions performed
/// on the table, so the full list of actions is required to properly read a table.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    /// Changes the current metadata of the table. Must be present in the first version of a table.
    /// Subsequent `metaData` actions completely overwrite previous metadata.
    metaData(MetaData),
    /// Adds CDC a file to the table state.
    cdc(AddCDCFile),
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
    /// Describe s the configuration for a named metadata domain
    domainMetadata(DomainMetaData),
}

impl Action {
    /// Create a commit info from a map
    pub fn commit_info(info: Map<String, serde_json::Value>) -> Self {
        Self::commitInfo(CommitInfo {
            info,
            ..Default::default()
        })
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// Used to record the operations performed to the Delta Log
pub struct MergePredicate {
    /// The type of merge operation performed
    pub action_type: String,
    /// The predicate used for the merge operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
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

    /// Delete data matching predicate from delta table
    Delete {
        /// The condition the to be deleted data must match
        predicate: Option<String>,
    },

    /// Update data matching predicate from delta table
    Update {
        /// The update predicate
        predicate: Option<String>,
    },

    /// Merge data with a source data with the following predicate
    #[serde(rename_all = "camelCase")]
    Merge {
        /// The merge predicate
        predicate: Option<String>,

        /// Match operations performed
        matched_predicates: Vec<MergePredicate>,

        /// Not Match operations performed
        not_matched_predicates: Vec<MergePredicate>,

        /// Not Match by Source operations performed
        not_matched_by_source_predicates: Vec<MergePredicate>,
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
        target_size: i64,
    },
    #[serde(rename_all = "camelCase")]
    /// Represents a `FileSystemCheck` operation
    FileSystemCheck {},

    /// Represents a `Restore` operation
    Restore {
        /// Version to restore
        version: Option<i64>,
        ///Datetime to restore
        datetime: Option<i64>,
    }, // TODO: Add more operations
}

impl DeltaOperation {
    /// A human readable name for the operation
    pub fn name(&self) -> &str {
        // operation names taken from https://learn.microsoft.com/en-us/azure/databricks/delta/history#--operation-metrics-keys
        match &self {
            DeltaOperation::Create {
                mode: SaveMode::Overwrite,
                ..
            } => "CREATE OR REPLACE TABLE",
            DeltaOperation::Create { .. } => "CREATE TABLE",
            DeltaOperation::Write { .. } => "WRITE",
            DeltaOperation::Delete { .. } => "DELETE",
            DeltaOperation::Update { .. } => "UPDATE",
            DeltaOperation::Merge { .. } => "MERGE",
            DeltaOperation::StreamingUpdate { .. } => "STREAMING UPDATE",
            DeltaOperation::Optimize { .. } => "OPTIMIZE",
            DeltaOperation::FileSystemCheck { .. } => "FSCK",
            DeltaOperation::Restore { .. } => "RESTORE",
        }
    }

    /// Parameters configured for operation.
    pub fn operation_parameters(&self) -> DeltaResult<HashMap<String, Value>> {
        if let Some(Some(Some(map))) = serde_json::to_value(self)
            .map_err(|err| ProtocolError::SerializeOperation { source: err })?
            .as_object()
            .map(|p| p.values().next().map(|q| q.as_object()))
        {
            Ok(map
                .iter()
                .filter(|item| !item.1.is_null())
                .map(|(k, v)| {
                    (
                        k.to_owned(),
                        serde_json::Value::String(if v.is_string() {
                            String::from(v.as_str().unwrap())
                        } else {
                            v.to_string()
                        }),
                    )
                })
                .collect())
        } else {
            Err(ProtocolError::Generic(
                "Operation parameters serialized into unexpected shape".into(),
            )
            .into())
        }
    }

    /// Denotes if the operation changes the data contained in the table
    pub fn changes_data(&self) -> bool {
        match self {
            Self::Optimize { .. } => false,
            Self::Create { .. }
            | Self::FileSystemCheck {}
            | Self::StreamingUpdate { .. }
            | Self::Write { .. }
            | Self::Delete { .. }
            | Self::Merge { .. }
            | Self::Update { .. }
            | Self::Restore { .. } => true,
        }
    }

    /// Retrieve basic commit information to be added to Delta commits
    pub fn get_commit_info(&self) -> CommitInfo {
        // TODO infer additional info from operation parameters ...
        CommitInfo {
            operation: Some(self.name().into()),
            operation_parameters: self.operation_parameters().ok(),
            ..Default::default()
        }
    }

    /// Get predicate expression applied when the operation reads data from the table.
    pub fn read_predicate(&self) -> Option<String> {
        match self {
            // TODO add more operations
            Self::Write { predicate, .. } => predicate.clone(),
            Self::Delete { predicate, .. } => predicate.clone(),
            Self::Update { predicate, .. } => predicate.clone(),
            Self::Merge { predicate, .. } => predicate.clone(),
            _ => None,
        }
    }

    /// Denotes if the operation reads the entire table
    pub fn read_whole_table(&self) -> bool {
        match self {
            // TODO just adding one operation example, as currently none of the
            // implemented operations scan the entire table.
            Self::Write { predicate, .. } if predicate.is_none() => false,
            _ => false,
        }
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

pub(crate) async fn get_last_checkpoint(
    object_store: &ObjectStoreRef,
) -> Result<CheckPoint, ProtocolError> {
    let last_checkpoint_path = Path::from_iter(["_delta_log", "_last_checkpoint"]);
    debug!("loading checkpoint from {last_checkpoint_path}");
    match object_store.get(&last_checkpoint_path).await {
        Ok(data) => Ok(serde_json::from_slice(&data.bytes().await?)?),
        Err(ObjectStoreError::NotFound { .. }) => {
            match find_latest_check_point_for_version(object_store, i64::MAX).await {
                Ok(Some(cp)) => Ok(cp),
                _ => Err(ProtocolError::CheckpointNotFound),
            }
        }
        Err(err) => Err(ProtocolError::ObjectStore { source: err }),
    }
}

pub(crate) async fn find_latest_check_point_for_version(
    object_store: &ObjectStoreRef,
    version: i64,
) -> Result<Option<CheckPoint>, ProtocolError> {
    lazy_static! {
        static ref CHECKPOINT_REGEX: Regex =
            Regex::new(r"^_delta_log/(\d{20})\.checkpoint\.parquet$").unwrap();
        static ref CHECKPOINT_PARTS_REGEX: Regex =
            Regex::new(r"^_delta_log/(\d{20})\.checkpoint\.\d{10}\.(\d{10})\.parquet$").unwrap();
    }

    let mut cp: Option<CheckPoint> = None;
    let mut stream = object_store.list(Some(object_store.log_path())).await?;

    while let Some(obj_meta) = stream.next().await {
        // Exit early if any objects can't be listed.
        // We exclude the special case of a not found error on some of the list entities.
        // This error mainly occurs for local stores when a temporary file has been deleted by
        // concurrent writers or if the table is vacuumed by another client.
        let obj_meta = match obj_meta {
            Ok(meta) => Ok(meta),
            Err(ObjectStoreError::NotFound { .. }) => continue,
            Err(err) => Err(err),
        }?;
        if let Some(captures) = CHECKPOINT_REGEX.captures(obj_meta.location.as_ref()) {
            let curr_ver_str = captures.get(1).unwrap().as_str();
            let curr_ver: i64 = curr_ver_str.parse().unwrap();
            if curr_ver > version {
                // skip checkpoints newer than max version
                continue;
            }
            if cp.is_none() || curr_ver > cp.unwrap().version {
                cp = Some(CheckPoint::new(curr_ver, 0, None));
            }
            continue;
        }

        if let Some(captures) = CHECKPOINT_PARTS_REGEX.captures(obj_meta.location.as_ref()) {
            let curr_ver_str = captures.get(1).unwrap().as_str();
            let curr_ver: i64 = curr_ver_str.parse().unwrap();
            if curr_ver > version {
                // skip checkpoints newer than max version
                continue;
            }
            if cp.is_none() || curr_ver > cp.unwrap().version {
                let parts_str = captures.get(2).unwrap().as_str();
                let parts = parts_str.parse().unwrap();
                cp = Some(CheckPoint::new(curr_ver, 0, Some(parts)));
            }
            continue;
        }
    }

    Ok(cp)
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

    #[test]
    fn test_load_table_partial_stats() {
        let action = Add {
            stats: Some(
                serde_json::json!({
                    "numRecords": 22
                })
                .to_string(),
            ),
            ..Default::default()
        };

        let stats = action.get_stats().unwrap().unwrap();

        assert_eq!(stats.num_records, 22);
        assert_eq!(stats.min_values.len(), 0);
        assert_eq!(stats.max_values.len(), 0);
        assert_eq!(stats.null_count.len(), 0);
    }

    #[test]
    fn test_read_commit_info() {
        let raw = r#"
        {
            "timestamp": 1670892998177,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "partitionBy": "[\"c1\",\"c2\"]"
            },
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "operationMetrics": {
                "numFiles": "3",
                "numOutputRows": "3",
                "numOutputBytes": "1356"
            },
            "engineInfo": "Apache-Spark/3.3.1 Delta-Lake/2.2.0",
            "txnId": "046a258f-45e3-4657-b0bf-abfb0f76681c"
        }"#;

        let info = serde_json::from_str::<CommitInfo>(raw);
        assert!(info.is_ok());

        // assert that commit info has no required filelds
        let raw = "{}";
        let info = serde_json::from_str::<CommitInfo>(raw);
        assert!(info.is_ok());

        // arbitrary field data may be added to commit
        let raw = r#"
        {
            "timestamp": 1670892998177,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "partitionBy": "[\"c1\",\"c2\"]"
            },
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "operationMetrics": {
                "numFiles": "3",
                "numOutputRows": "3",
                "numOutputBytes": "1356"
            },
            "engineInfo": "Apache-Spark/3.3.1 Delta-Lake/2.2.0",
            "txnId": "046a258f-45e3-4657-b0bf-abfb0f76681c",
            "additionalField": "more data",
            "additionalStruct": {
                "key": "value",
                "otherKey": 123
            }
        }"#;

        let info = serde_json::from_str::<CommitInfo>(raw).expect("should parse");
        assert!(info.info.contains_key("additionalField"));
        assert!(info.info.contains_key("additionalStruct"));
    }

    #[test]
    fn test_read_domain_metadata() {
        let buf = r#"{"domainMetadata":{"domain":"delta.liquid","configuration":"{\"clusteringColumns\":[{\"physicalName\":[\"id\"]}],\"domainName\":\"delta.liquid\"}","removed":false}}"#;
        let _action: Action =
            serde_json::from_str(buf).expect("Expected to be able to deserialize");
    }

    #[cfg(feature = "arrow")]
    mod arrow_tests {
        use arrow::array::{self, ArrayRef, StructArray};
        use arrow::compute::kernels::cast_utils::Parser;
        use arrow::compute::sort_to_indices;
        use arrow::datatypes::{DataType, Date32Type, Field, Fields, TimestampMicrosecondType};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        fn sort_batch_by(batch: &RecordBatch, column: &str) -> arrow::error::Result<RecordBatch> {
            let sort_column = batch.column(batch.schema().column_with_name(column).unwrap().0);
            let sort_indices = sort_to_indices(sort_column, None, None)?;
            let schema = batch.schema();
            let sorted_columns: Vec<(&String, ArrayRef)> = schema
                .fields()
                .iter()
                .zip(batch.columns().iter())
                .map(|(field, column)| {
                    Ok((
                        field.name(),
                        arrow::compute::take(column, &sort_indices, None)?,
                    ))
                })
                .collect::<arrow::error::Result<_>>()?;
            RecordBatch::try_from_iter(sorted_columns)
        }
        #[tokio::test]
        async fn test_with_partitions() {
            // test table with partitions
            let path = "./tests/data/delta-0.8.0-null-partition";
            let table = crate::open_table(path).await.unwrap();
            let actions = table.get_state().add_actions_table(true).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();

            let mut expected_columns: Vec<(&str, ArrayRef)> = vec![
        ("path", Arc::new(array::StringArray::from(vec![
            "k=A/part-00000-b1f1dbbb-70bc-4970-893f-9bb772bf246e.c000.snappy.parquet",
            "k=__HIVE_DEFAULT_PARTITION__/part-00001-8474ac85-360b-4f58-b3ea-23990c71b932.c000.snappy.parquet"
        ]))),
        ("size_bytes", Arc::new(array::Int64Array::from(vec![460, 460]))),
        ("modification_time", Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
            1627990384000, 1627990384000
        ]))),
        ("data_change", Arc::new(array::BooleanArray::from(vec![true, true]))),
        ("partition.k", Arc::new(array::StringArray::from(vec![Some("A"), None]))),
    ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(expected, actions);

            let actions = table.get_state().add_actions_table(false).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();

            expected_columns[4] = (
                "partition_values",
                Arc::new(array::StructArray::new(
                    Fields::from(vec![Field::new("k", DataType::Utf8, true)]),
                    vec![Arc::new(array::StringArray::from(vec![Some("A"), None])) as ArrayRef],
                    None,
                )),
            );
            let expected = RecordBatch::try_from_iter(expected_columns).unwrap();

            assert_eq!(expected, actions);
        }
        #[tokio::test]
        async fn test_with_deletion_vector() {
            // test table with partitions
            let path = "./tests/data/table_with_deletion_logs";
            let table = crate::open_table(path).await.unwrap();
            let actions = table.get_state().add_actions_table(true).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();
            let actions = actions
                .project(&[
                    actions.schema().index_of("path").unwrap(),
                    actions.schema().index_of("size_bytes").unwrap(),
                    actions
                        .schema()
                        .index_of("deletionVector.storageType")
                        .unwrap(),
                    actions
                        .schema()
                        .index_of("deletionVector.pathOrInlineDiv")
                        .unwrap(),
                    actions.schema().index_of("deletionVector.offset").unwrap(),
                    actions
                        .schema()
                        .index_of("deletionVector.sizeInBytes")
                        .unwrap(),
                    actions
                        .schema()
                        .index_of("deletionVector.cardinality")
                        .unwrap(),
                ])
                .unwrap();
            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-cb251d5e-b665-437a-a9a7-fbfc5137c77d.c000.snappy.parquet",
                    ])),
                ),
                ("size_bytes", Arc::new(array::Int64Array::from(vec![10499]))),
                (
                    "deletionVector.storageType",
                    Arc::new(array::StringArray::from(vec!["u"])),
                ),
                (
                    "deletionVector.pathOrInlineDiv",
                    Arc::new(array::StringArray::from(vec!["Q6Kt3y1b)0MgZSWwPunr"])),
                ),
                (
                    "deletionVector.offset",
                    Arc::new(array::Int32Array::from(vec![1])),
                ),
                (
                    "deletionVector.sizeInBytes",
                    Arc::new(array::Int32Array::from(vec![36])),
                ),
                (
                    "deletionVector.cardinality",
                    Arc::new(array::Int64Array::from(vec![2])),
                ),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(expected, actions);

            let actions = table.get_state().add_actions_table(false).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();
            let actions = actions
                .project(&[
                    actions.schema().index_of("path").unwrap(),
                    actions.schema().index_of("size_bytes").unwrap(),
                    actions.schema().index_of("deletionVector").unwrap(),
                ])
                .unwrap();
            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-cb251d5e-b665-437a-a9a7-fbfc5137c77d.c000.snappy.parquet",
                    ])),
                ),
                ("size_bytes", Arc::new(array::Int64Array::from(vec![10499]))),
                (
                    "deletionVector",
                    Arc::new(array::StructArray::new(
                        Fields::from(vec![
                            Field::new("storageType", DataType::Utf8, false),
                            Field::new("pathOrInlineDiv", DataType::Utf8, false),
                            Field::new("offset", DataType::Int32, true),
                            Field::new("sizeInBytes", DataType::Int32, false),
                            Field::new("cardinality", DataType::Int64, false),
                        ]),
                        vec![
                            Arc::new(array::StringArray::from(vec!["u"])) as ArrayRef,
                            Arc::new(array::StringArray::from(vec!["Q6Kt3y1b)0MgZSWwPunr"]))
                                as ArrayRef,
                            Arc::new(array::Int32Array::from(vec![1])) as ArrayRef,
                            Arc::new(array::Int32Array::from(vec![36])) as ArrayRef,
                            Arc::new(array::Int64Array::from(vec![2])) as ArrayRef,
                        ],
                        None,
                    )),
                ),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns).unwrap();

            assert_eq!(expected, actions);
        }
        #[tokio::test]
        async fn test_without_partitions() {
            // test table without partitions
            let path = "./tests/data/simple_table";
            let table = crate::open_table(path).await.unwrap();

            let actions = table.get_state().add_actions_table(true).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();

            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
                        "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
                        "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
                        "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
                        "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
                    ])),
                ),
                (
                    "size_bytes",
                    Arc::new(array::Int64Array::from(vec![262, 262, 429, 429, 429])),
                ),
                (
                    "modification_time",
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                        1587968626000,
                        1587968602000,
                        1587968602000,
                        1587968602000,
                        1587968602000,
                    ])),
                ),
                (
                    "data_change",
                    Arc::new(array::BooleanArray::from(vec![
                        true, true, true, true, true,
                    ])),
                ),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(expected, actions);

            let actions = table.get_state().add_actions_table(false).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();

            // For now, this column is ignored.
            // expected_columns.push((
            //     "partition_values",
            //     new_null_array(&DataType::Struct(vec![]), 5),
            // ));
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(expected, actions);
        }

        #[tokio::test]
        async fn test_with_stats() {
            // test table with stats
            let path = "./tests/data/delta-0.8.0";
            let table = crate::open_table(path).await.unwrap();
            let actions = table.get_state().add_actions_table(true).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();

            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet",
                        "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
                    ])),
                ),
                (
                    "size_bytes",
                    Arc::new(array::Int64Array::from(vec![440, 440])),
                ),
                (
                    "modification_time",
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                        1615043776000,
                        1615043767000,
                    ])),
                ),
                (
                    "data_change",
                    Arc::new(array::BooleanArray::from(vec![true, true])),
                ),
                ("num_records", Arc::new(array::Int64Array::from(vec![2, 2]))),
                (
                    "null_count.value",
                    Arc::new(array::Int64Array::from(vec![0, 0])),
                ),
                ("min.value", Arc::new(array::Int32Array::from(vec![2, 0]))),
                ("max.value", Arc::new(array::Int32Array::from(vec![4, 2]))),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(expected, actions);
        }

        #[tokio::test]
        async fn test_only_struct_stats() {
            // test table with no json stats
            let path = "./tests/data/delta-1.2.1-only-struct-stats";
            let mut table = crate::open_table(path).await.unwrap();
            table.load_version(1).await.unwrap();

            let actions = table.get_state().add_actions_table(true).unwrap();

            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
                    ])),
                ),
                ("size_bytes", Arc::new(array::Int64Array::from(vec![5489]))),
                (
                    "modification_time",
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                        1666652373000,
                    ])),
                ),
                (
                    "data_change",
                    Arc::new(array::BooleanArray::from(vec![true])),
                ),
                ("num_records", Arc::new(array::Int64Array::from(vec![1]))),
                (
                    "null_count.integer",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                ("min.integer", Arc::new(array::Int32Array::from(vec![0]))),
                ("max.integer", Arc::new(array::Int32Array::from(vec![0]))),
                (
                    "null_count.null",
                    Arc::new(array::Int64Array::from(vec![1])),
                ),
                ("min.null", Arc::new(array::NullArray::new(1))),
                ("max.null", Arc::new(array::NullArray::new(1))),
                (
                    "null_count.boolean",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                ("min.boolean", Arc::new(array::NullArray::new(1))),
                ("max.boolean", Arc::new(array::NullArray::new(1))),
                (
                    "null_count.double",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.double",
                    Arc::new(array::Float64Array::from(vec![1.234])),
                ),
                (
                    "max.double",
                    Arc::new(array::Float64Array::from(vec![1.234])),
                ),
                (
                    "null_count.decimal",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.decimal",
                    Arc::new(
                        array::Decimal128Array::from_iter_values([-567800])
                            .with_precision_and_scale(8, 5)
                            .unwrap(),
                    ),
                ),
                (
                    "max.decimal",
                    Arc::new(
                        array::Decimal128Array::from_iter_values([-567800])
                            .with_precision_and_scale(8, 5)
                            .unwrap(),
                    ),
                ),
                (
                    "null_count.string",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.string",
                    Arc::new(array::StringArray::from(vec!["string"])),
                ),
                (
                    "max.string",
                    Arc::new(array::StringArray::from(vec!["string"])),
                ),
                (
                    "null_count.binary",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                ("min.binary", Arc::new(array::NullArray::new(1))),
                ("max.binary", Arc::new(array::NullArray::new(1))),
                (
                    "null_count.date",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.date",
                    Arc::new(array::Date32Array::from(vec![Date32Type::parse(
                        "2022-10-24",
                    )])),
                ),
                (
                    "max.date",
                    Arc::new(array::Date32Array::from(vec![Date32Type::parse(
                        "2022-10-24",
                    )])),
                ),
                (
                    "null_count.timestamp",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.timestamp",
                    Arc::new(array::TimestampMicrosecondArray::from(vec![
                        TimestampMicrosecondType::parse("2022-10-24T22:59:32.846Z"),
                    ])),
                ),
                (
                    "max.timestamp",
                    Arc::new(array::TimestampMicrosecondArray::from(vec![
                        TimestampMicrosecondType::parse("2022-10-24T22:59:32.846Z"),
                    ])),
                ),
                (
                    "null_count.struct.struct_element",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.struct.struct_element",
                    Arc::new(array::StringArray::from(vec!["struct_value"])),
                ),
                (
                    "max.struct.struct_element",
                    Arc::new(array::StringArray::from(vec!["struct_value"])),
                ),
                ("null_count.map", Arc::new(array::Int64Array::from(vec![0]))),
                (
                    "null_count.array",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.nested_struct.struct_element.nested_struct_element",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.nested_struct.struct_element.nested_struct_element",
                    Arc::new(array::StringArray::from(vec!["nested_struct_value"])),
                ),
                (
                    "max.nested_struct.struct_element.nested_struct_element",
                    Arc::new(array::StringArray::from(vec!["nested_struct_value"])),
                ),
                (
                    "null_count.struct_of_array_of_map.struct_element",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "tags.INSERTION_TIME",
                    Arc::new(array::StringArray::from(vec!["1666652373000000"])),
                ),
                (
                    "tags.OPTIMIZE_TARGET_SIZE",
                    Arc::new(array::StringArray::from(vec!["268435456"])),
                ),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(
                expected
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<&str>>(),
                actions
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<&str>>()
            );
            assert_eq!(expected, actions);

            let actions = table.get_state().add_actions_table(false).unwrap();
            // For brevity, just checking a few nested columns in stats

            assert_eq!(
                actions
                    .get_field_at_path(&[
                        "null_count",
                        "nested_struct",
                        "struct_element",
                        "nested_struct_element"
                    ])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::Int64Array>()
                    .unwrap(),
                &array::Int64Array::from(vec![0]),
            );

            assert_eq!(
                actions
                    .get_field_at_path(&[
                        "min",
                        "nested_struct",
                        "struct_element",
                        "nested_struct_element"
                    ])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap(),
                &array::StringArray::from(vec!["nested_struct_value"]),
            );

            assert_eq!(
                actions
                    .get_field_at_path(&[
                        "max",
                        "nested_struct",
                        "struct_element",
                        "nested_struct_element"
                    ])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap(),
                &array::StringArray::from(vec!["nested_struct_value"]),
            );

            assert_eq!(
                actions
                    .get_field_at_path(&["null_count", "struct_of_array_of_map", "struct_element"])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::Int64Array>()
                    .unwrap(),
                &array::Int64Array::from(vec![0])
            );

            assert_eq!(
                actions
                    .get_field_at_path(&["tags", "OPTIMIZE_TARGET_SIZE"])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap(),
                &array::StringArray::from(vec!["268435456"])
            );
        }

        /// Trait to make it easier to access nested fields
        trait NestedTabular {
            fn get_field_at_path(&self, path: &[&str]) -> Option<ArrayRef>;
        }

        impl NestedTabular for RecordBatch {
            fn get_field_at_path(&self, path: &[&str]) -> Option<ArrayRef> {
                // First, get array in the batch
                let (first_key, remainder) = path.split_at(1);
                let mut col = self.column(self.schema().column_with_name(first_key[0])?.0);

                if remainder.is_empty() {
                    return Some(Arc::clone(col));
                }

                for segment in remainder {
                    col = col
                        .as_any()
                        .downcast_ref::<StructArray>()?
                        .column_by_name(segment)?;
                }

                Some(Arc::clone(col))
            }
        }
    }
}
