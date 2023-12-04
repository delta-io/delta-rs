use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
// use std::io::{Cursor, Read};
// use std::sync::Arc;

// use roaring::RoaringTreemap;
use log::warn;
use serde::{Deserialize, Serialize};
use url::Url;

use super::super::schema::StructType;
use super::super::{error::Error, DeltaResult};
use super::serde_path;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
/// Defines a file format used in table
pub struct Format {
    /// Name of the encoding for files in this table
    pub provider: String,
    /// A map containing configuration options for the format
    pub options: HashMap<String, Option<String>>,
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

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
            options: HashMap::new(),
        }
    }
}

/// Return a default empty schema to be used for edge-cases when a schema is missing
fn default_schema() -> String {
    warn!("A `metaData` action was missing a `schemaString` and has been given an empty schema");
    r#"{"type":"struct",  "fields": []}"#.into()
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
/// Defines a metadata action
pub struct Metadata {
    /// Unique identifier for this table
    pub id: String,
    /// User-provided identifier for this table
    pub name: Option<String>,
    /// User-provided description for this table
    pub description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub format: Format,
    /// Schema of the table
    #[serde(default = "default_schema")]
    pub schema_string: String,
    /// Column names by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: Option<i64>,
    /// Configuration options for the metadata action
    pub configuration: HashMap<String, Option<String>>,
}

impl Metadata {
    /// Create a new metadata action
    pub fn new(
        id: impl Into<String>,
        format: Format,
        schema_string: impl Into<String>,
        partition_columns: impl IntoIterator<Item = impl Into<String>>,
        configuration: Option<HashMap<String, Option<String>>>,
    ) -> Self {
        Self {
            id: id.into(),
            format,
            schema_string: schema_string.into(),
            partition_columns: partition_columns.into_iter().map(|c| c.into()).collect(),
            configuration: configuration.unwrap_or_default(),
            name: None,
            description: None,
            created_time: None,
        }
    }

    /// set the table name in the metadata action
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// set the table description in the metadata action
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// set the table creation time in the metadata action
    pub fn with_created_time(mut self, created_time: i64) -> Self {
        self.created_time = Some(created_time);
        self
    }

    /// get the table schema
    pub fn schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
/// Defines a protocol action
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    pub reader_features: Option<HashSet<ReaderFeatures>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    pub writer_features: Option<HashSet<WriterFeatures>>,
}

impl Protocol {
    /// Create a new protocol action
    pub fn new(min_reader_version: i32, min_wrriter_version: i32) -> Self {
        Self {
            min_reader_version,
            min_writer_version: min_wrriter_version,
            reader_features: None,
            writer_features: None,
        }
    }

    /// set the reader features in the protocol action
    pub fn with_reader_features(
        mut self,
        reader_features: impl IntoIterator<Item = impl Into<ReaderFeatures>>,
    ) -> Self {
        self.reader_features = Some(reader_features.into_iter().map(|c| c.into()).collect());
        self
    }

    /// set the writer features in the protocol action
    pub fn with_writer_features(
        mut self,
        writer_features: impl IntoIterator<Item = impl Into<WriterFeatures>>,
    ) -> Self {
        self.writer_features = Some(writer_features.into_iter().map(|c| c.into()).collect());
        self
    }
}

/// Features table readers can support as well as let users know
/// what is supported
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ReaderFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    #[serde(alias = "timestampNtz")]
    TimestampWithoutTimezone,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// If we do not match any other reader features
    #[serde(untagged)]
    Other(String),
}

#[cfg(all(not(feature = "parquet2"), feature = "parquet"))]
impl From<&parquet::record::Field> for ReaderFeatures {
    fn from(value: &parquet::record::Field) -> Self {
        match value {
            parquet::record::Field::Str(feature) => match feature.as_str() {
                "columnMapping" => ReaderFeatures::ColumnMapping,
                "deletionVectors" => ReaderFeatures::DeletionVectors,
                "timestampNtz" => ReaderFeatures::TimestampWithoutTimezone,
                "v2Checkpoint" => ReaderFeatures::V2Checkpoint,
                f => ReaderFeatures::Other(f.to_string()),
            },
            f => ReaderFeatures::Other(f.to_string()),
        }
    }
}

impl From<String> for ReaderFeatures {
    fn from(value: String) -> Self {
        value.as_str().into()
    }
}

impl From<&str> for ReaderFeatures {
    fn from(value: &str) -> Self {
        match value {
            "columnMapping" => ReaderFeatures::ColumnMapping,
            "deletionVectors" => ReaderFeatures::DeletionVectors,
            "timestampNtz" => ReaderFeatures::TimestampWithoutTimezone,
            "v2Checkpoint" => ReaderFeatures::V2Checkpoint,
            f => ReaderFeatures::Other(f.to_string()),
        }
    }
}

impl AsRef<str> for ReaderFeatures {
    fn as_ref(&self) -> &str {
        match self {
            ReaderFeatures::ColumnMapping => "columnMapping",
            ReaderFeatures::DeletionVectors => "deletionVectors",
            ReaderFeatures::TimestampWithoutTimezone => "timestampNtz",
            ReaderFeatures::V2Checkpoint => "v2Checkpoint",
            ReaderFeatures::Other(f) => f,
        }
    }
}

impl fmt::Display for ReaderFeatures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// Features table writers can support as well as let users know
/// what is supported
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum WriterFeatures {
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
    /// Mapping of one column to another
    ColumnMapping,
    /// ID Columns
    IdentityColumns,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// Row tracking on tables
    RowTracking,
    /// timestamps without timezone support
    #[serde(alias = "timestampNtz")]
    TimestampWithoutTimezone,
    /// domain specific metadata
    DomainMetadata,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Iceberg compatibility support
    IcebergCompatV1,
    /// If we do not match any other reader features
    #[serde(untagged)]
    Other(String),
}

impl From<String> for WriterFeatures {
    fn from(value: String) -> Self {
        value.as_str().into()
    }
}

impl From<&str> for WriterFeatures {
    fn from(value: &str) -> Self {
        match value {
            "appendOnly" => WriterFeatures::AppendOnly,
            "invariants" => WriterFeatures::Invariants,
            "checkConstraints" => WriterFeatures::CheckConstraints,
            "changeDataFeed" => WriterFeatures::ChangeDataFeed,
            "generatedColumns" => WriterFeatures::GeneratedColumns,
            "columnMapping" => WriterFeatures::ColumnMapping,
            "identityColumns" => WriterFeatures::IdentityColumns,
            "deletionVectors" => WriterFeatures::DeletionVectors,
            "rowTracking" => WriterFeatures::RowTracking,
            "timestampNtz" => WriterFeatures::TimestampWithoutTimezone,
            "domainMetadata" => WriterFeatures::DomainMetadata,
            "v2Checkpoint" => WriterFeatures::V2Checkpoint,
            "icebergCompatV1" => WriterFeatures::IcebergCompatV1,
            f => WriterFeatures::Other(f.to_string()),
        }
    }
}

impl AsRef<str> for WriterFeatures {
    fn as_ref(&self) -> &str {
        match self {
            WriterFeatures::AppendOnly => "appendOnly",
            WriterFeatures::Invariants => "invariants",
            WriterFeatures::CheckConstraints => "checkConstraints",
            WriterFeatures::ChangeDataFeed => "changeDataFeed",
            WriterFeatures::GeneratedColumns => "generatedColumns",
            WriterFeatures::ColumnMapping => "columnMapping",
            WriterFeatures::IdentityColumns => "identityColumns",
            WriterFeatures::DeletionVectors => "deletionVectors",
            WriterFeatures::RowTracking => "rowTracking",
            WriterFeatures::TimestampWithoutTimezone => "timestampNtz",
            WriterFeatures::DomainMetadata => "domainMetadata",
            WriterFeatures::V2Checkpoint => "v2Checkpoint",
            WriterFeatures::IcebergCompatV1 => "icebergCompatV1",
            WriterFeatures::Other(f) => f,
        }
    }
}

impl fmt::Display for WriterFeatures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[cfg(all(not(feature = "parquet2"), feature = "parquet"))]
impl From<&parquet::record::Field> for WriterFeatures {
    fn from(value: &parquet::record::Field) -> Self {
        match value {
            parquet::record::Field::Str(feature) => match feature.as_str() {
                "appendOnly" => WriterFeatures::AppendOnly,
                "invariants" => WriterFeatures::Invariants,
                "checkConstraints" => WriterFeatures::CheckConstraints,
                "changeDataFeed" => WriterFeatures::ChangeDataFeed,
                "generatedColumns" => WriterFeatures::GeneratedColumns,
                "columnMapping" => WriterFeatures::ColumnMapping,
                "identityColumns" => WriterFeatures::IdentityColumns,
                "deletionVectors" => WriterFeatures::DeletionVectors,
                "rowTracking" => WriterFeatures::RowTracking,
                "timestampNtz" => WriterFeatures::TimestampWithoutTimezone,
                "domainMetadata" => WriterFeatures::DomainMetadata,
                "v2Checkpoint" => WriterFeatures::V2Checkpoint,
                "icebergCompatV1" => WriterFeatures::IcebergCompatV1,
                f => WriterFeatures::Other(f.to_string()),
            },
            f => WriterFeatures::Other(f.to_string()),
        }
    }
}

///Storage type of deletion vector
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "u" => Ok(Self::UuidRelativePath),
            "i" => Ok(Self::Inline),
            "p" => Ok(Self::AbsolutePath),
            _ => Err(Error::DeletionVector(format!(
                "Unknown storage format: '{s}'."
            ))),
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

impl ToString for StorageType {
    fn to_string(&self) -> String {
        self.as_ref().into()
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

impl DeletionVectorDescriptor {
    /// get a unique idenitfier for the deletion vector
    pub fn unique_id(&self) -> String {
        if let Some(offset) = self.offset {
            format!(
                "{}{}@{offset}",
                self.storage_type.as_ref(),
                self.path_or_inline_dv
            )
        } else {
            format!("{}{}", self.storage_type.as_ref(), self.path_or_inline_dv)
        }
    }

    /// get the absolute path of the deletion vector
    pub fn absolute_path(&self, parent: &Url) -> DeltaResult<Option<Url>> {
        match &self.storage_type {
            StorageType::UuidRelativePath => {
                let prefix_len = self.path_or_inline_dv.len() as i32 - 20;
                if prefix_len < 0 {
                    return Err(Error::DeletionVector("Invalid length".to_string()));
                }
                let decoded = z85::decode(&self.path_or_inline_dv[(prefix_len as usize)..])
                    .map_err(|_| Error::DeletionVector("Failed to decode DV uuid".to_string()))?;
                let uuid = uuid::Uuid::from_slice(&decoded)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let mut dv_suffix = format!("deletion_vector_{uuid}.bin");
                if prefix_len > 0 {
                    dv_suffix = format!(
                        "{}/{}",
                        &self.path_or_inline_dv[..(prefix_len as usize)],
                        dv_suffix
                    );
                }
                let dv_path = parent
                    .join(&dv_suffix)
                    .map_err(|_| Error::DeletionVector(format!("invalid path: {}", dv_suffix)))?;
                Ok(Some(dv_path))
            }
            StorageType::AbsolutePath => {
                Ok(Some(Url::parse(&self.path_or_inline_dv).map_err(|_| {
                    Error::DeletionVector(format!("invalid path: {}", self.path_or_inline_dv))
                })?))
            }
            StorageType::Inline => Ok(None),
        }
    }

    // TODO read only required byte ranges
    // pub fn read(
    //     &self,
    //     fs_client: Arc<dyn FileSystemClient>,
    //     parent: Url,
    // ) -> DeltaResult<RoaringTreemap> {
    //     match self.absolute_path(&parent)? {
    //         None => {
    //             let bytes = z85::decode(&self.path_or_inline_dv)
    //                 .map_err(|_| Error::DeletionVector("Failed to decode DV".to_string()))?;
    //             RoaringTreemap::deserialize_from(&bytes[12..])
    //                 .map_err(|err| Error::DeletionVector(err.to_string()))
    //         }
    //         Some(path) => {
    //             let offset = self.offset;
    //             let size_in_bytes = self.size_in_bytes;
    //
    //             let dv_data = fs_client
    //                 .read_files(vec![(path, None)])?
    //                 .next()
    //                 .ok_or(Error::MissingData("No deletion Vector data".to_string()))??;
    //
    //             let mut cursor = Cursor::new(dv_data);
    //             if let Some(offset) = offset {
    //                 // TODO should we read the datasize from the DV file?
    //                 // offset plus datasize bytes
    //                 cursor.set_position((offset + 4) as u64);
    //             }
    //
    //             let mut buf = vec![0; 4];
    //             cursor
    //                 .read(&mut buf)
    //                 .map_err(|err| Error::DeletionVector(err.to_string()))?;
    //             let magic =
    //                 i32::from_le_bytes(buf.try_into().map_err(|_| {
    //                     Error::DeletionVector("filed to read magic bytes".to_string())
    //                 })?);
    //             println!("magic  --> : {}", magic);
    //             // assert!(magic == 1681511377);
    //
    //             let mut buf = vec![0; size_in_bytes as usize];
    //             cursor
    //                 .read(&mut buf)
    //                 .map_err(|err| Error::DeletionVector(err.to_string()))?;
    //
    //             RoaringTreemap::deserialize_from(Cursor::new(buf))
    //                 .map_err(|err| Error::DeletionVector(err.to_string()))
    //         }
    //     }
    // }
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
    #[serde(with = "serde_path")]
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

    // TODO remove migration filds added to not do too many business logic changes in one PR
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
    /// Partition values parsed for parquet2
    #[cfg(feature = "parquet2")]
    #[serde(skip_serializing, skip_deserializing)]
    pub partition_values_parsed: Option<String>,

    /// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
    /// raw parquet format. This field needs to be written when statistics are available and the
    /// table property: delta.checkpoint.writeStatsAsStruct is set to true.
    ///
    /// This field is only available in add action records read from checkpoints
    #[cfg(feature = "parquet")]
    #[serde(skip_serializing, skip_deserializing)]
    pub stats_parsed: Option<parquet::record::Row>,
    /// Stats parsed for parquet2
    #[cfg(feature = "parquet2")]
    #[serde(skip_serializing, skip_deserializing)]
    pub stats_parsed: Option<String>,
}

impl Add {
    /// get the unique id of the deletion vector, if any
    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.clone().map(|dv| dv.unique_id())
    }

    /// set the base row id of the add action
    pub fn with_base_row_id(mut self, base_row_id: i64) -> Self {
        self.base_row_id = Some(base_row_id);
        self
    }
}

/// Represents a tombstone (deleted file) in the Delta log.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
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

impl Remove {
    /// get the unique id of the deletion vector, if any
    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.clone().map(|dv| dv.unique_id())
    }
}

/// Delta AddCDCFile action that describes a parquet CDC data file.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// Action used by streaming systems to track progress using application-specific versions to
/// enable idempotency.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Txn {
    /// A unique identifier for the application performing the transaction.
    pub app_id: String,

    /// An application-specific numeric identifier for this transaction.
    pub version: i64,

    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<i64>,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => Err(Error::Generic("Invalid string for IsolationLevel".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    // use std::sync::Arc;

    // use object_store::local::LocalFileSystem;

    use crate::kernel::PrimitiveType;

    use super::*;
    // use crate::client::filesystem::ObjectStoreFileSystemClient;
    // use crate::executor::tokio::TokioBackgroundExecutor;

    fn dv_relateive() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".parse().unwrap(),
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_absolute() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "p".parse().unwrap(),
            path_or_inline_dv:
                "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_inline() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "i".parse().unwrap(),
            path_or_inline_dv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".to_string(),
            offset: None,
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_example() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".parse().unwrap(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }
    }

    #[test]
    fn test_deletion_vector_absolute_path() {
        let parent = Url::parse("s3://mytable/").unwrap();

        let relative = dv_relateive();
        let expected =
            Url::parse("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, relative.absolute_path(&parent).unwrap().unwrap());

        let absolute = dv_absolute();
        let expected =
            Url::parse("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, absolute.absolute_path(&parent).unwrap().unwrap());

        let inline = dv_inline();
        assert_eq!(None, inline.absolute_path(&parent).unwrap());

        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let dv_url = parent
            .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin")
            .unwrap();
        let example = dv_example();
        assert_eq!(dv_url, example.absolute_path(&parent).unwrap().unwrap());
    }

    #[test]
    fn test_primitive() {
        let types: PrimitiveType = serde_json::from_str("\"string\"").unwrap();
        println!("{:?}", types);
    }

    // #[test]
    // fn test_deletion_vector_read() {
    //     let store = Arc::new(LocalFileSystem::new());
    //     let path =
    //         std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
    //     let parent = url::Url::from_directory_path(path).unwrap();
    //     let root = object_store::path::Path::from(parent.path());
    //     let fs_client = Arc::new(ObjectStoreFileSystemClient::new(
    //         store,
    //         root,
    //         Arc::new(TokioBackgroundExecutor::new()),
    //     ));
    //
    //     let example = dv_example();
    //     let tree_map = example.read(fs_client, parent).unwrap();
    //
    //     let expected: Vec<u64> = vec![0, 9];
    //     let found = tree_map.iter().collect::<Vec<_>>();
    //     assert_eq!(found, expected)
    // }
}
