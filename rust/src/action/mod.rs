//! Actions included in Delta table transaction logs

#![allow(non_camel_case_types)]

#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod checkpoints;
#[cfg(feature = "parquet2")]
pub mod parquet2_read;
#[cfg(feature = "parquet")]
mod parquet_read;

#[cfg(all(feature = "arrow"))]
use arrow_schema::ArrowError;
use futures::StreamExt;
use lazy_static::lazy_static;
use log::*;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use percent_encoding::percent_decode;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::delta_config::IsolationLevel;
use crate::errors::DeltaResult;
use crate::storage::ObjectStoreRef;
use crate::{delta::CheckPoint, schema::*, DeltaTableMetaData};

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

    /// A generic action error. The wrapped error string describes the details.
    #[error("Generic action error: {0}")]
    Generic(String),

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
    #[cfg(all(feature = "arrow"))]
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

fn decode_path(raw_path: &str) -> Result<String, ProtocolError> {
    percent_decode(raw_path.as_bytes())
        .decode_utf8()
        .map(|c| c.to_string())
        .map_err(|e| ProtocolError::InvalidField(format!("Decode path failed for action: {e}")))
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

/// Delta log action that describes a parquet data file that is part of the table.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    /// A relative path, from the root of the table, to a file that should be added to the table
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
    pub tags: Option<HashMap<String, Option<String>>>,
}

impl Hash for Add {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl Add {
    /// Returns the Add action with path decoded.
    pub fn path_decoded(self) -> Result<Self, ProtocolError> {
        decode_path(&self.path).map(|path| Self { path, ..self })
    }

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
    pub partition_values: Option<HashMap<String, Option<String>>>,
    /// Size of this file in bytes
    pub size: Option<i64>,
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
    pub fn path_decoded(self) -> Result<Self, ProtocolError> {
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
    // TODO: Add more operations
}

impl DeltaOperation {
    /// A human readable name for the operation
    pub fn name(&self) -> &str {
        // operation names taken from https://learn.microsoft.com/en-us/azure/databricks/delta/history#--operation-metrics-keys
        match &self {
            DeltaOperation::Create { mode, .. } if matches!(mode, SaveMode::Overwrite) => {
                "CREATE OR REPLACE TABLE"
            }
            DeltaOperation::Create { .. } => "CREATE TABLE",
            DeltaOperation::Write { .. } => "WRITE",
            DeltaOperation::Delete { .. } => "DELETE",
            DeltaOperation::StreamingUpdate { .. } => "STREAMING UPDATE",
            DeltaOperation::Optimize { .. } => "OPTIMIZE",
            DeltaOperation::FileSystemCheck { .. } => "FSCK",
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
            | Self::Delete { .. } => true,
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
            Regex::new(r#"^_delta_log/(\d{20})\.checkpoint\.parquet$"#).unwrap();
        static ref CHECKPOINT_PARTS_REGEX: Regex =
            Regex::new(r#"^_delta_log/(\d{20})\.checkpoint\.\d{10}\.(\d{10})\.parquet$"#).unwrap();
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
                cp = Some(CheckPoint {
                    version: curr_ver,
                    size: 0,
                    parts: None,
                });
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
                cp = Some(CheckPoint {
                    version: curr_ver,
                    size: 0,
                    parts: Some(parts),
                });
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
}
