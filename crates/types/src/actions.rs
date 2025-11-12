use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use delta_kernel::table_features::{ReaderFeature, WriterFeature};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub enum Action {
    #[serde(rename = "metaData")]
    Metadata(delta_kernel::actions::Metadata),
    Protocol(delta_kernel::actions::Protocol),
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

impl From<delta_kernel::actions::Metadata> for Action {
    fn from(a: delta_kernel::actions::Metadata) -> Self {
        Self::Metadata(a)
    }
}

impl From<delta_kernel::actions::Protocol> for Action {
    fn from(a: delta_kernel::actions::Protocol) -> Self {
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

/// High level table features
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum TableFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Append Only Tables
    AppendOnly,
    /// Table invariants
    Invariants,
    /// Check constraints on columns
    CheckConstraints,
    /// CDF on a table
    ChangeDataFeed,
    /// Columns with generated values
    GeneratedColumns,
    /// ID Columns
    IdentityColumns,
    /// Row tracking on tables
    RowTracking,
    /// domain specific metadata
    DomainMetadata,
    /// Iceberg compatibility support
    IcebergCompatV1,
}

impl FromStr for TableFeatures {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "columnMapping" => Ok(TableFeatures::ColumnMapping),
            "deletionVectors" => Ok(TableFeatures::DeletionVectors),
            "timestampNtz" => Ok(TableFeatures::TimestampWithoutTimezone),
            "v2Checkpoint" => Ok(TableFeatures::V2Checkpoint),
            "appendOnly" => Ok(TableFeatures::AppendOnly),
            "invariants" => Ok(TableFeatures::Invariants),
            "checkConstraints" => Ok(TableFeatures::CheckConstraints),
            "changeDataFeed" => Ok(TableFeatures::ChangeDataFeed),
            "generatedColumns" => Ok(TableFeatures::GeneratedColumns),
            "identityColumns" => Ok(TableFeatures::IdentityColumns),
            "rowTracking" => Ok(TableFeatures::RowTracking),
            "domainMetadata" => Ok(TableFeatures::DomainMetadata),
            "icebergCompatV1" => Ok(TableFeatures::IcebergCompatV1),
            _ => Err(()),
        }
    }
}

impl AsRef<str> for TableFeatures {
    fn as_ref(&self) -> &str {
        match self {
            TableFeatures::ColumnMapping => "columnMapping",
            TableFeatures::DeletionVectors => "deletionVectors",
            TableFeatures::TimestampWithoutTimezone => "timestampNtz",
            TableFeatures::V2Checkpoint => "v2Checkpoint",
            TableFeatures::AppendOnly => "appendOnly",
            TableFeatures::Invariants => "invariants",
            TableFeatures::CheckConstraints => "checkConstraints",
            TableFeatures::ChangeDataFeed => "changeDataFeed",
            TableFeatures::GeneratedColumns => "generatedColumns",
            TableFeatures::IdentityColumns => "identityColumns",
            TableFeatures::RowTracking => "rowTracking",
            TableFeatures::DomainMetadata => "domainMetadata",
            TableFeatures::IcebergCompatV1 => "icebergCompatV1",
        }
    }
}

impl fmt::Display for TableFeatures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl TryFrom<&TableFeatures> for ReaderFeature {
    type Error = strum::ParseError;

    fn try_from(value: &TableFeatures) -> Result<Self, Self::Error> {
        ReaderFeature::try_from(value.as_ref())
    }
}

impl TryFrom<&TableFeatures> for WriterFeature {
    type Error = strum::ParseError;

    fn try_from(value: &TableFeatures) -> Result<Self, Self::Error> {
        WriterFeature::try_from(value.as_ref())
    }
}

impl TableFeatures {
    /// Convert table feature to respective reader or/and write feature
    pub fn to_reader_writer_features(&self) -> (Option<ReaderFeature>, Option<WriterFeature>) {
        let reader_feature = ReaderFeature::try_from(self)
            .ok()
            .and_then(|feature| match feature {
                ReaderFeature::Unknown(_) => None,
                _ => Some(feature),
            });
        let writer_feature = WriterFeature::try_from(self)
            .ok()
            .and_then(|feature| match feature {
                WriterFeature::Unknown(_) => None,
                _ => Some(feature),
            });
        (reader_feature, writer_feature)
    }
}

///Storage type of deletion vector
#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq)]
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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "u" => Ok(Self::UuidRelativePath),
            "i" => Ok(Self::Inline),
            "p" => Ok(Self::AbsolutePath),
            _ => Err(format!("Unknown storage format: '{s}'.")),
        }
    }
}

impl AsRef<str> for StorageType {
    fn as_ref(&self) -> &str {
        match self {
            Self::UuidRelativePath => "u",
            Self::Inline => "i",
            Self::AbsolutePath => "p",
        }
    }
}

impl Display for StorageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
/// Defines a deletion vector
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: StorageType,

    /// Three format options are currently proposed:
    /// - If `storageType = 'u'` then `<random prefix - optional><base85 encoded uuid>`:
    ///   The deletion vector is stored in a file with a path relative to the data
    ///   directory of this Delta table, and the file name can be reconstructed from
    ///   the UUID. See Derived Fields for how to reconstruct the file name. The random
    ///   prefix is recovered as the extra characters before the (20 characters fixed length) uuid.
    /// - If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector
    ///   is stored inline in the log. The format used is the `RoaringBitmapArray`
    ///   format also used when the DV is stored on disk and described in [Deletion Vector Format].
    /// - If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an
    ///   absolute path given by this path, which has the same format as the `path` field
    ///   in the `add`/`remove` actions.
    ///
    /// [Deletion Vector Format]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
    pub path_or_inline_dv: String,

    /// Start of the data for this DV in number of bytes from the beginning of the file it is stored in.
    /// Always None (absent in JSON) when `storageType = 'i'`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
/// Defines an add action
pub struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    #[serde(with = "crate::serde_path")]
    pub path: String,

    /// A map from partition column to value for this logical file.
    pub partition_values: HashMap<String, Option<String>>,

    /// The size of this data file in bytes
    pub size: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// Contains [statistics] (e.g., count, min/max values for columns) about the data in this logical file.
    ///
    /// [statistics]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
    pub stats: Option<String>,

    /// Map containing metadata about this logical file.
    pub tags: Option<HashMap<String, Option<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// Information about deletion vector (DV) associated with this add action
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub default_row_commit_version: Option<i64>,

    /// The name of the clustering implementation
    pub clustering_provider: Option<String>,
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

/// Represents a tombstone (deleted file) in the Delta log.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    #[serde(with = "crate::serde_path")]
    pub path: String,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// The time this logical file was created, as milliseconds since the epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, Option<String>>>,

    /// The size of this data file in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,

    /// Map containing metadata about this logical file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,

    /// Information about deletion vector (DV) associated with this add action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,
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

impl Eq for Remove {}
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AddCDCFile {
    /// A relative path, from the root of the table, or an
    /// absolute path to a CDC file
    #[serde(with = "crate::serde_path")]
    pub path: String,

    /// The size of this file in bytes
    pub size: i64,

    /// A map from partition column to value for this file
    pub partition_values: HashMap<String, Option<String>>,

    /// Should always be set to false because they do not change the underlying data of the table
    pub data_change: bool,

    /// Map containing metadata about this file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// Action used by streaming systems to track progress using application-specific versions to
/// enable idempotency.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    /// A unique identifier for the application performing the transaction.
    pub app_id: String,

    /// An application-specific numeric identifier for this transaction.
    pub version: i64,

    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<i64>,
}

impl Transaction {
    /// Create a new application transactions. See [`Txn`] for details.
    pub fn new(app_id: impl ToString, version: i64) -> Self {
        Self::new_with_last_update(app_id, version, None)
    }

    /// Create a new application transactions. See [`Txn`] for details.
    pub fn new_with_last_update(
        app_id: impl ToString,
        version: i64,
        last_updated: Option<i64>,
    ) -> Self {
        Transaction {
            app_id: app_id.to_string(),
            version,
            last_updated,
        }
    }
}

/// The commitInfo is a fairly flexible action within the delta specification, where arbitrary data can be stored.
/// However the reference implementation as well as delta-rs store useful information that may for instance
/// allow us to be more permissive in commit conflict resolution.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
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
    pub info: HashMap<String, serde_json::Value>,

    /// User defined metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_metadata: Option<String>,
}

/// The domain metadata action contains a configuration (string) for a named metadata domain
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DomainMetadata {
    /// Identifier for this domain (system or user-provided)
    pub domain: String,

    /// String containing configuration for the metadata domain
    pub configuration: String,

    /// When `true` the action serves as a tombstone
    pub removed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
/// This action is only allowed in checkpoints following V2 spec. It describes the details about the checkpoint.
pub struct CheckpointMetadata {
    /// The flavor of the V2 checkpoint. Allowed values: "flat".
    pub flavor: String,

    /// Map containing any additional metadata about the v2 spec checkpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// The sidecar action references a sidecar file which provides some of the checkpoint's file actions.
/// This action is only allowed in checkpoints following V2 spec.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Sidecar {
    /// The name of the sidecar file (not a path).
    /// The file must reside in the _delta_log/_sidecars directory.
    pub file_name: String,

    /// The size of the sidecar file in bytes
    pub size_in_bytes: i64,

    /// The time this sidecar file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// Type of sidecar. Valid values are: "fileaction".
    /// This could be extended in future to allow different kinds of sidecars.
    #[serde(rename = "type")]
    pub sidecar_type: String,

    /// Map containing any additional metadata about the checkpoint sidecar file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
/// The isolation level applied during transaction
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table's history.
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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => Err("Invalid string for IsolationLevel".into()),
        }
    }
}
