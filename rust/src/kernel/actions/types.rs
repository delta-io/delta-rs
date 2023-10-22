use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::Arc;

use roaring::RoaringTreemap;
use url::Url;

use super::super::schema::StructType;
use crate::{DeltaResult, Error, FileSystemClient};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Format {
    /// Name of the encoding for files in this table
    pub provider: String,
    /// A map containing configuration options for the format
    pub options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub schema_string: String,
    /// Column names by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: Option<i64>,
    /// Configuration options for the metadata action
    pub configuration: HashMap<String, Option<String>>,
}

impl Metadata {
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

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_created_time(mut self, created_time: i64) -> Self {
        self.created_time = Some(created_time);
        self
    }

    pub fn schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    pub reader_features: Option<Vec<String>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    pub writer_features: Option<Vec<String>>,
}

impl Protocol {
    pub fn new(min_reader_version: i32, min_wrriter_version: i32) -> Self {
        Self {
            min_reader_version,
            min_writer_version: min_wrriter_version,
            reader_features: None,
            writer_features: None,
        }
    }

    pub fn with_reader_features(
        mut self,
        reader_features: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.reader_features = Some(reader_features.into_iter().map(|c| c.into()).collect());
        self
    }

    pub fn with_writer_features(
        mut self,
        writer_features: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.writer_features = Some(writer_features.into_iter().map(|c| c.into()).collect());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: String,

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
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

impl DeletionVectorDescriptor {
    pub fn unique_id(&self) -> String {
        if let Some(offset) = self.offset {
            format!("{}{}@{offset}", self.storage_type, self.path_or_inline_dv)
        } else {
            format!("{}{}", self.storage_type, self.path_or_inline_dv)
        }
    }

    pub fn absolute_path(&self, parent: &Url) -> DeltaResult<Option<Url>> {
        match self.storage_type.as_str() {
            "u" => {
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
            "p" => Ok(Some(Url::parse(&self.path_or_inline_dv).map_err(|_| {
                Error::DeletionVector(format!("invalid path: {}", self.path_or_inline_dv))
            })?)),
            "i" => Ok(None),
            other => Err(Error::DeletionVector(format!(
                "Unknown storage format: '{other}'."
            ))),
        }
    }

    // TODO read only required byte ranges
    pub fn read(
        &self,
        fs_client: Arc<dyn FileSystemClient>,
        parent: Url,
    ) -> DeltaResult<RoaringTreemap> {
        match self.absolute_path(&parent)? {
            None => {
                let bytes = z85::decode(&self.path_or_inline_dv)
                    .map_err(|_| Error::DeletionVector("Failed to decode DV".to_string()))?;
                RoaringTreemap::deserialize_from(&bytes[12..])
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
            Some(path) => {
                let offset = self.offset;
                let size_in_bytes = self.size_in_bytes;

                println!("path  --> : {}", path);
                println!("offset  --> : {:?}", offset);
                println!("size_in_bytes  --> : {}", size_in_bytes);

                let dv_data = fs_client
                    .read_files(vec![(path, None)])?
                    .next()
                    .ok_or(Error::MissingData("No deletion Vector data".to_string()))??;

                let mut cursor = Cursor::new(dv_data);
                if let Some(offset) = offset {
                    // TODO should we read the datasize from the DV file?
                    // offset plus datasize bytes
                    cursor.set_position((offset + 4) as u64);
                }

                let mut buf = vec![0; 4];
                cursor
                    .read(&mut buf)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let magic =
                    i32::from_le_bytes(buf.try_into().map_err(|_| {
                        Error::DeletionVector("filed to read magic bytes".to_string())
                    })?);
                println!("magic  --> : {}", magic);
                // assert!(magic == 1681511377);

                let mut buf = vec![0; size_in_bytes as usize];
                cursor
                    .read(&mut buf)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;

                RoaringTreemap::deserialize_from(Cursor::new(buf))
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
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
    pub tags: HashMap<String, Option<String>>,

    /// Information about deletion vector (DV) associated with this add action
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub default_row_commit_version: Option<i64>,
}

impl Add {
    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.clone().map(|dv| dv.unique_id())
    }

    pub fn with_base_row_id(mut self, base_row_id: i64) -> Self {
        self.base_row_id = Some(base_row_id);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub deletion_timestamp: Option<i64>,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    pub extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    pub partition_values: Option<HashMap<String, Option<String>>>,

    /// The size of this data file in bytes
    pub size: Option<i64>,

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
}

impl Remove {
    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.clone().map(|dv| dv.unique_id())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::client::filesystem::ObjectStoreFileSystemClient;
    use crate::executor::tokio::TokioBackgroundExecutor;

    fn dv_relateive() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_absolute() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "p".to_string(),
            path_or_inline_dv:
                "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_inline() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "i".to_string(),
            path_or_inline_dv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".to_string(),
            offset: None,
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_example() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
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
    fn test_deletion_vector_read() {
        let store = Arc::new(LocalFileSystem::new());
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let root = object_store::path::Path::from(parent.path());
        let fs_client = Arc::new(ObjectStoreFileSystemClient::new(
            store,
            root,
            Arc::new(TokioBackgroundExecutor::new()),
        ));

        let example = dv_example();
        let tree_map = example.read(fs_client, parent).unwrap();

        let expected: Vec<u64> = vec![0, 9];
        let found = tree_map.iter().collect::<Vec<_>>();
        assert_eq!(found, expected)
    }
}
