use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::str::FromStr;

use delta_kernel::schema::{DataType, StructField};
use delta_kernel::table_features::{ReaderFeature, WriterFeature};
use serde::{Deserialize, Serialize};

use crate::kernel::{error::Error, DeltaResult};
use crate::kernel::{StructType, StructTypeExt};
use crate::TableProperty;

pub use delta_kernel::actions::{Metadata, Protocol};

/// Please don't use, this API will be leaving shortly!
#[deprecated(since = "0.27.0", note = "stop-gap for adopting kernel actions")]
pub fn new_metadata(
    schema: &StructType,
    partition_columns: impl IntoIterator<Item = impl ToString>,
    configuration: impl IntoIterator<Item = (impl ToString, impl ToString)>,
) -> DeltaResult<Metadata> {
    // this ugliness is a stop-gap until we resolve: https://github.com/delta-io/delta-kernel-rs/issues/1055
    let value = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "name": None::<String>,
        "description": None::<String>,
        "format": { "provider": "parquet", "options": {} },
        "schemaString": serde_json::to_string(schema)?,
        "partitionColumns": partition_columns.into_iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "configuration": configuration.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<HashMap<_, _>>(),
        "createdTime": chrono::Utc::now().timestamp_millis(),
    });
    Ok(serde_json::from_value(value)?)
}

/// Extension trait for Metadata action
///
/// This trait is a stop-gap to adopt the Metadata action from delta-kernel-rs
/// while the update / mutation APIs are being implemented. It allows us to implement
/// additional APIs on the Metadata action and hide specifics of how we do the updates.
#[deprecated(since = "0.27.0", note = "stop-gap for adopting kernel actions")]
pub trait MetadataExt {
    fn with_table_id(self, table_id: String) -> DeltaResult<Metadata>;

    fn with_name(self, name: String) -> DeltaResult<Metadata>;

    fn with_description(self, description: String) -> DeltaResult<Metadata>;

    fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata>;

    fn add_config_key(self, key: String, value: String) -> DeltaResult<Metadata>;

    fn remove_config_key(self, key: &str) -> DeltaResult<Metadata>;
}

impl MetadataExt for Metadata {
    fn with_table_id(self, table_id: String) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": table_id,
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_name(self, name: String) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": self.id(),
            "name": name,
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_description(self, description: String) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": description,
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(schema)?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn add_config_key(self, key: String, value: String) -> DeltaResult<Metadata> {
        let mut config = self.configuration().clone();
        config.insert(key, value);
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": config,
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn remove_config_key(self, key: &str) -> DeltaResult<Metadata> {
        let mut config = self.configuration().clone();
        config.remove(key);
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": config,
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }
}

/// checks if table contains timestamp_ntz in any field including nested fields.
pub fn contains_timestampntz<'a>(mut fields: impl Iterator<Item = &'a StructField>) -> bool {
    fn _check_type(dtype: &DataType) -> bool {
        match dtype {
            &DataType::TIMESTAMP_NTZ => true,
            DataType::Array(inner) => _check_type(inner.element_type()),
            DataType::Struct(inner) => inner.fields().any(|f| _check_type(f.data_type())),
            _ => false,
        }
    }
    fields.any(|f| _check_type(f.data_type()))
}

/// Extension trait for delta-kernel Protocol action.
///
/// Allows us to extend the Protocol struct with additional methods
/// to update the protocol actions.
#[deprecated(since = "0.27.0", note = "stop-gap for adopting kernel actions")]
pub(crate) trait ProtocolExt {
    fn reader_features_set(&self) -> Option<HashSet<ReaderFeature>>;
    fn writer_features_set(&self) -> Option<HashSet<WriterFeature>>;
    fn append_reader_features(self, reader_features: &[ReaderFeature]) -> Protocol;
    fn append_writer_features(self, writer_features: &[WriterFeature]) -> Protocol;
    fn move_table_properties_into_features(
        self,
        configuration: &HashMap<String, String>,
    ) -> Protocol;
    fn apply_column_metadata_to_protocol(self, schema: &StructType) -> DeltaResult<Protocol>;
    fn apply_properties_to_protocol(
        self,
        new_properties: &HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> DeltaResult<Protocol>;
}

impl ProtocolExt for Protocol {
    fn reader_features_set(&self) -> Option<HashSet<ReaderFeature>> {
        self.reader_features()
            .map(|features| features.iter().cloned().collect())
    }

    fn writer_features_set(&self) -> Option<HashSet<WriterFeature>> {
        self.writer_features()
            .map(|features| features.iter().cloned().collect())
    }

    fn append_reader_features(self, reader_features: &[ReaderFeature]) -> Protocol {
        let mut inner = ProtocolInner::from_kernel(&self);
        inner = inner.append_reader_features(reader_features.iter().cloned());
        inner.as_kernel()
    }

    fn append_writer_features(self, writer_features: &[WriterFeature]) -> Protocol {
        let mut inner = ProtocolInner::from_kernel(&self);
        inner = inner.append_writer_features(writer_features.iter().cloned());
        inner.as_kernel()
    }

    fn move_table_properties_into_features(
        self,
        configuration: &HashMap<String, String>,
    ) -> Protocol {
        let mut inner = ProtocolInner::from_kernel(&self);
        inner = inner.move_table_properties_into_features(configuration);
        inner.as_kernel()
    }

    fn apply_column_metadata_to_protocol(self, schema: &StructType) -> DeltaResult<Protocol> {
        let mut inner = ProtocolInner::from_kernel(&self);
        inner = inner.apply_column_metadata_to_protocol(schema)?;
        Ok(inner.as_kernel())
    }

    fn apply_properties_to_protocol(
        self,
        new_properties: &HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> DeltaResult<Protocol> {
        let mut inner = ProtocolInner::from_kernel(&self);
        inner = inner.apply_properties_to_protocol(new_properties, raise_if_not_exists)?;
        Ok(inner.as_kernel())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
/// Temporary Shim to facilitate adoption of kernel protocol.
///
/// This is more or less our old local implementation of protocol. We keep it around
/// to use the various update and translation methods defined in this struct and
/// use it to proxy updates to the kernel protocol action.
///
// TODO: Remove once we can use kernel protocol update APIs.
#[deprecated(
    since = "0.27.0",
    note = "Just an internal shim for adopting kernel actions"
)]
pub(crate) struct ProtocolInner {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<HashSet<ReaderFeature>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<HashSet<WriterFeature>>,
}

impl Default for ProtocolInner {
    fn default() -> Self {
        Self {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        }
    }
}

impl ProtocolInner {
    /// Create a new protocol action
    pub fn new(min_reader_version: i32, min_writer_version: i32) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
            reader_features: None,
            writer_features: None,
        }
    }

    pub(crate) fn from_kernel(value: &Protocol) -> ProtocolInner {
        // this ugliness is a stop-gap until we resolve: https://github.com/delta-io/delta-kernel-rs/issues/1055
        serde_json::from_value(serde_json::to_value(value).unwrap()).unwrap()
    }

    pub(crate) fn as_kernel(&self) -> Protocol {
        // this ugliness is a stop-gap until we resolve: https://github.com/delta-io/delta-kernel-rs/issues/1055
        serde_json::from_value(serde_json::to_value(self).unwrap()).unwrap()
    }

    /// Append the reader features in the protocol action, automatically bumps min_reader_version
    pub fn append_reader_features(
        mut self,
        reader_features: impl IntoIterator<Item = impl Into<ReaderFeature>>,
    ) -> Self {
        let all_reader_features = reader_features
            .into_iter()
            .map(Into::into)
            .collect::<HashSet<_>>();
        if !all_reader_features.is_empty() {
            self.min_reader_version = 3;
            match self.reader_features {
                Some(mut features) => {
                    features.extend(all_reader_features);
                    self.reader_features = Some(features);
                }
                None => self.reader_features = Some(all_reader_features),
            };
        }
        self
    }

    /// Append the writer features in the protocol action, automatically bumps min_writer_version
    pub fn append_writer_features(
        mut self,
        writer_features: impl IntoIterator<Item = impl Into<WriterFeature>>,
    ) -> Self {
        let all_writer_features = writer_features
            .into_iter()
            .map(|c| c.into())
            .collect::<HashSet<_>>();
        if !all_writer_features.is_empty() {
            self.min_writer_version = 7;

            match self.writer_features {
                Some(mut features) => {
                    features.extend(all_writer_features);
                    self.writer_features = Some(features);
                }
                None => self.writer_features = Some(all_writer_features),
            };
        }
        self
    }

    /// Converts existing properties into features if the reader_version is >=3 or writer_version >=7
    /// only converts features that are "true"
    pub fn move_table_properties_into_features(
        mut self,
        configuration: &HashMap<String, String>,
    ) -> Self {
        fn parse_bool(value: &str) -> bool {
            value.to_ascii_lowercase().parse::<bool>().is_ok_and(|v| v)
        }

        if self.min_writer_version >= 7 {
            // TODO: move this is in future to use delta_kernel::table_properties
            let mut converted_writer_features = configuration
                .iter()
                .filter(|(_, value)| value.to_ascii_lowercase().parse::<bool>().is_ok_and(|v| v))
                .filter_map(|(key, value)| match key.as_str() {
                    "delta.enableChangeDataFeed" if parse_bool(value) => {
                        Some(WriterFeature::ChangeDataFeed)
                    }
                    "delta.appendOnly" if parse_bool(value) => Some(WriterFeature::AppendOnly),
                    "delta.enableDeletionVectors" if parse_bool(value) => {
                        Some(WriterFeature::DeletionVectors)
                    }
                    "delta.enableRowTracking" if parse_bool(value) => {
                        Some(WriterFeature::RowTracking)
                    }
                    "delta.checkpointPolicy" if value == "v2" => Some(WriterFeature::V2Checkpoint),
                    _ => None,
                })
                .collect::<HashSet<WriterFeature>>();

            if configuration
                .keys()
                .any(|v| v.starts_with("delta.constraints."))
            {
                converted_writer_features.insert(WriterFeature::CheckConstraints);
            }

            match self.writer_features {
                Some(mut features) => {
                    features.extend(converted_writer_features);
                    self.writer_features = Some(features);
                }
                None => self.writer_features = Some(converted_writer_features),
            }
        }
        if self.min_reader_version >= 3 {
            let converted_reader_features = configuration
                .iter()
                .filter_map(|(key, value)| match key.as_str() {
                    "delta.enableDeletionVectors" if parse_bool(value) => {
                        Some(ReaderFeature::DeletionVectors)
                    }
                    "delta.checkpointPolicy" if value == "v2" => Some(ReaderFeature::V2Checkpoint),
                    _ => None,
                })
                .collect::<HashSet<ReaderFeature>>();
            match self.reader_features {
                Some(mut features) => {
                    features.extend(converted_reader_features);
                    self.reader_features = Some(features);
                }
                None => self.reader_features = Some(converted_reader_features),
            }
        }
        self
    }

    /// Will apply the column metadata to the protocol by either bumping the version or setting
    /// features
    pub fn apply_column_metadata_to_protocol(mut self, schema: &StructType) -> DeltaResult<Self> {
        let generated_cols = schema.get_generated_columns()?;
        let invariants = schema.get_invariants()?;
        let contains_timestamp_ntz = self.contains_timestampntz(schema.fields());

        if contains_timestamp_ntz {
            self = self.enable_timestamp_ntz()
        }

        if !generated_cols.is_empty() {
            self = self.enable_generated_columns()
        }

        if !invariants.is_empty() {
            self = self.enable_invariants()
        }

        Ok(self)
    }

    /// Will apply the properties to the protocol by either bumping the version or setting
    /// features
    pub fn apply_properties_to_protocol(
        mut self,
        new_properties: &HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> DeltaResult<Self> {
        let mut parsed_properties: HashMap<TableProperty, String> = HashMap::new();

        for (key, value) in new_properties {
            if let Ok(parsed_key) = key.parse::<TableProperty>() {
                parsed_properties.insert(parsed_key, value.to_string());
            } else if raise_if_not_exists {
                return Err(Error::Generic(format!(
                    "Error parsing property '{key}':'{value}'",
                )));
            }
        }

        // Check and update delta.minReaderVersion
        if let Some(min_reader_version) = parsed_properties.get(&TableProperty::MinReaderVersion) {
            let new_min_reader_version = min_reader_version.parse::<i32>();
            match new_min_reader_version {
                Ok(version) => match version {
                    1..=3 => {
                        if version > self.min_reader_version {
                            self.min_reader_version = version
                        }
                    }
                    _ => {
                        return Err(Error::Generic(format!("delta.minReaderVersion = '{min_reader_version}' is invalid, valid values are ['1','2','3']")))
                    }
                },
                Err(_) => {
                    return Err(Error::Generic(format!("delta.minReaderVersion = '{min_reader_version}' is invalid, valid values are ['1','2','3']")))
                }
            }
        }

        // Check and update delta.minWriterVersion
        if let Some(min_writer_version) = parsed_properties.get(&TableProperty::MinWriterVersion) {
            let new_min_writer_version = min_writer_version.parse::<i32>();
            match new_min_writer_version {
                Ok(version) => match version {
                    2..=7 => {
                        if version > self.min_writer_version {
                            self.min_writer_version = version
                        }
                    }
                    _ => {
                        return Err(Error::Generic(format!("delta.minWriterVersion = '{min_writer_version}' is invalid, valid values are ['2','3','4','5','6','7']")))
                    }
                },
                Err(_) => {
                    return Err(Error::Generic(format!("delta.minWriterVersion = '{min_writer_version}' is invalid, valid values are ['2','3','4','5','6','7']")))
                }
            }
        }

        // Check enableChangeDataFeed and bump protocol or add writerFeature if writer versions is >=7
        if let Some(enable_cdf) = parsed_properties.get(&TableProperty::EnableChangeDataFeed) {
            let if_enable_cdf = enable_cdf.to_ascii_lowercase().parse::<bool>();
            match if_enable_cdf {
                Ok(true) => {
                    if self.min_writer_version >= 7 {
                        match self.writer_features {
                            Some(mut features) => {
                                features.insert(WriterFeature::ChangeDataFeed);
                                self.writer_features = Some(features);
                            }
                            None => {
                                self.writer_features =
                                    Some(HashSet::from([WriterFeature::ChangeDataFeed]))
                            }
                        }
                    } else if self.min_writer_version <= 3 {
                        self.min_writer_version = 4
                    }
                }
                Ok(false) => {}
                _ => {
                    return Err(Error::Generic(format!("delta.enableChangeDataFeed = '{enable_cdf}' is invalid, valid values are ['true']")))
                }
            }
        }

        if let Some(enable_dv) = parsed_properties.get(&TableProperty::EnableDeletionVectors) {
            let if_enable_dv = enable_dv.to_ascii_lowercase().parse::<bool>();
            match if_enable_dv {
                Ok(true) => {
                    let writer_features = match self.writer_features {
                        Some(mut features) => {
                            features.insert(WriterFeature::DeletionVectors);
                            features
                        }
                        None => HashSet::from([WriterFeature::DeletionVectors]),
                    };
                    let reader_features = match self.reader_features {
                        Some(mut features) => {
                            features.insert(ReaderFeature::DeletionVectors);
                            features
                        }
                        None => HashSet::from([ReaderFeature::DeletionVectors]),
                    };
                    self.min_reader_version = 3;
                    self.min_writer_version = 7;
                    self.writer_features = Some(writer_features);
                    self.reader_features = Some(reader_features);
                }
                Ok(false) => {}
                _ => {
                    return Err(Error::Generic(format!("delta.enableDeletionVectors = '{enable_dv}' is invalid, valid values are ['true']")))
                }
            }
        }
        Ok(self)
    }

    /// checks if table contains timestamp_ntz in any field including nested fields.
    fn contains_timestampntz<'a>(&self, fields: impl Iterator<Item = &'a StructField>) -> bool {
        contains_timestampntz(fields)
    }

    /// Enable timestamp_ntz in the protocol
    fn enable_timestamp_ntz(mut self) -> Self {
        self = self.append_reader_features([ReaderFeature::TimestampWithoutTimezone]);
        self = self.append_writer_features([WriterFeature::TimestampWithoutTimezone]);
        self
    }

    /// Enabled generated columns
    fn enable_generated_columns(mut self) -> Self {
        if self.min_writer_version < 4 {
            self.min_writer_version = 4;
        }
        if self.min_writer_version >= 7 {
            self = self.append_writer_features([WriterFeature::GeneratedColumns]);
        }
        self
    }

    /// Enabled generated columns
    fn enable_invariants(mut self) -> Self {
        if self.min_writer_version >= 7 {
            self = self.append_writer_features([WriterFeature::Invariants]);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_protocol() {
        // protocol json data
        let raw = serde_json::json!(
            {
              "minReaderVersion": 3,
              "minWriterVersion": 7,
              "readerFeatures": ["catalogOwned"],
              "writerFeatures": ["catalogOwned", "invariants", "appendOnly"]
            }
        );
        let protocol: Protocol = serde_json::from_value(raw).unwrap();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(
            protocol.reader_features(),
            Some(vec![ReaderFeature::Unknown("catalogOwned".to_owned())].as_slice())
        );
        assert_eq!(
            protocol.writer_features(),
            Some(
                vec![
                    WriterFeature::Unknown("catalogOwned".to_owned()),
                    WriterFeature::Invariants,
                    WriterFeature::AppendOnly
                ]
                .as_slice()
            )
        );
    }

    // #[test]
    // fn test_deletion_vector_read() {
    //     let store = Arc::new(LocalFileSystem::new());
    //     let path =
    //         std::fs::canonicalize(PathBuf::from("../deltalake-test/tests/data/table-with-dv-small/")).unwrap();
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
