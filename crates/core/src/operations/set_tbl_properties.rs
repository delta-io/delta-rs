//! Set table properties on a table

use std::collections::{HashMap, HashSet};

use futures::future::BoxFuture;
use maplit::hashset;

use super::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{Action, Protocol, ReaderFeatures, WriterFeatures};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaConfigKey;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};

/// Remove constraints from the table
pub struct SetTablePropertiesBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Name of the property
    properties: HashMap<String, String>,
    /// Raise if property doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl SetTablePropertiesBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            properties: HashMap::new(),
            raise_if_not_exists: true,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the properties to be removed
    pub fn with_properties(mut self, table_properties: HashMap<String, String>) -> Self {
        self.properties = table_properties;
        self
    }

    /// Specify if you want to raise if the property does not exist
    pub fn with_raise_if_not_exists(mut self, raise: bool) -> Self {
        self.raise_if_not_exists = raise;
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

/// Will apply the properties to the protocol by either bumping the version or setting
/// features
pub fn apply_properties_to_protocol(
    current_protocol: &Protocol,
    new_properties: &HashMap<String, String>,
    raise_if_not_exists: bool,
) -> DeltaResult<Protocol> {
    let mut parsed_properties: HashMap<DeltaConfigKey, String> = HashMap::new();

    for (key, value) in new_properties {
        if let Ok(parsed_key) = key.parse::<DeltaConfigKey>() {
            parsed_properties.insert(parsed_key, value.to_string());
        } else if raise_if_not_exists {
            return Err(DeltaTableError::Generic(format!(
                "Error parsing property '{}':'{}'",
                key, value
            )));
        }
    }

    let mut new_protocol = current_protocol.clone();

    // Check and update delta.minReaderVersion
    if let Some(min_reader_version) = parsed_properties.get(&DeltaConfigKey::MinReaderVersion) {
        let new_min_reader_version = min_reader_version.parse::<i32>();
        match new_min_reader_version {
            Ok(version) => match version {
                1..=3 => {
                    if version > new_protocol.min_reader_version {
                        new_protocol.min_reader_version = version
                    }
                }
                _ => {
                    return Err(DeltaTableError::Generic(format!(
                        "delta.minReaderVersion = '{}' is invalid, valid values are ['1','2','3']",
                        min_reader_version
                    )))
                }
            },
            Err(_) => {
                return Err(DeltaTableError::Generic(format!(
                    "delta.minReaderVersion = '{}' is invalid, valid values are ['1','2','3']",
                    min_reader_version
                )))
            }
        }
    }

    // Check and update delta.minWriterVersion
    if let Some(min_writer_version) = parsed_properties.get(&DeltaConfigKey::MinWriterVersion) {
        let new_min_writer_version = min_writer_version.parse::<i32>();
        match new_min_writer_version {
            Ok(version) => match version {
                2..=7 => {
                    if version > new_protocol.min_writer_version {
                        new_protocol.min_writer_version = version
                    }
                }
                _ => {
                    return Err(DeltaTableError::Generic(format!(
                        "delta.minWriterVersion = '{}' is invalid, valid values are ['2','3','4','5','6','7']",
                        min_writer_version
                    )))
                }
            },
            Err(_) => {
                return Err(DeltaTableError::Generic(format!(
                    "delta.minWriterVersion = '{}' is invalid, valid values are ['2','3','4','5','6','7']",
                    min_writer_version
                )))
            }
        }
    }

    // Check enableChangeDataFeed and bump protocol or add writerFeature if writer versions is >=7
    if let Some(enable_cdf) = parsed_properties.get(&DeltaConfigKey::EnableChangeDataFeed) {
        let if_enable_cdf = enable_cdf.to_ascii_lowercase().parse::<bool>();
        match if_enable_cdf {
            Ok(true) => {
                if new_protocol.min_writer_version >= 7 {
                    match new_protocol.writer_features {
                        Some(mut features) => {
                            features.insert(WriterFeatures::ChangeDataFeed);
                            new_protocol.writer_features = Some(features);
                        }
                        None => {
                            new_protocol.writer_features =
                                Some(hashset! {WriterFeatures::ChangeDataFeed})
                        }
                    }
                } else if new_protocol.min_writer_version <= 3 {
                    new_protocol.min_writer_version = 4
                }
            }
            Ok(false) => {}
            _ => {
                return Err(DeltaTableError::Generic(format!(
                    "delta.enableChangeDataFeed = '{}' is invalid, valid values are ['true']",
                    enable_cdf
                )))
            }
        }
    }

    if let Some(enable_dv) = parsed_properties.get(&DeltaConfigKey::EnableDeletionVectors) {
        let if_enable_dv = enable_dv.to_ascii_lowercase().parse::<bool>();
        match if_enable_dv {
            Ok(true) => {
                let writer_features = match new_protocol.writer_features {
                    Some(mut features) => {
                        features.insert(WriterFeatures::DeletionVectors);
                        features
                    }
                    None => hashset! {WriterFeatures::DeletionVectors},
                };
                let reader_features = match new_protocol.reader_features {
                    Some(mut features) => {
                        features.insert(ReaderFeatures::DeletionVectors);
                        features
                    }
                    None => hashset! {ReaderFeatures::DeletionVectors},
                };
                new_protocol.min_reader_version = 3;
                new_protocol.min_writer_version = 7;
                new_protocol.writer_features = Some(writer_features);
                new_protocol.reader_features = Some(reader_features);
            }
            Ok(false) => {}
            _ => {
                return Err(DeltaTableError::Generic(format!(
                    "delta.enableDeletionVectors = '{}' is invalid, valid values are ['true']",
                    enable_dv
                )))
            }
        }
    }

    Ok(new_protocol)
}

/// Converts existing properties into features if the reader_version is >=3 or writer_version >=3
/// only converts features that are "true"
pub fn convert_properties_to_features(
    mut new_protocol: Protocol,
    configuration: &HashMap<String, Option<String>>,
) -> Protocol {
    if new_protocol.min_writer_version >= 7 {
        let mut converted_writer_features = configuration
            .iter()
            .filter(|(_, value)| {
                value.as_ref().map_or(false, |v| {
                    v.to_ascii_lowercase().parse::<bool>().is_ok_and(|v| v)
                })
            })
            .collect::<HashMap<&String, &Option<String>>>()
            .keys()
            .map(|key| (*key).clone().into())
            .filter(|v| !matches!(v, WriterFeatures::Other(_)))
            .collect::<HashSet<WriterFeatures>>();

        if configuration
            .keys()
            .any(|v| v.contains("delta.constraints."))
        {
            converted_writer_features.insert(WriterFeatures::CheckConstraints);
        }

        match new_protocol.writer_features {
            Some(mut features) => {
                features.extend(converted_writer_features);
                new_protocol.writer_features = Some(features);
            }
            None => new_protocol.writer_features = Some(converted_writer_features),
        }
    }
    if new_protocol.min_reader_version >= 3 {
        let converted_reader_features = configuration
            .iter()
            .filter(|(_, value)| {
                value.as_ref().map_or(false, |v| {
                    v.to_ascii_lowercase().parse::<bool>().is_ok_and(|v| v)
                })
            })
            .map(|(key, _)| (*key).clone().into())
            .filter(|v| !matches!(v, ReaderFeatures::Other(_)))
            .collect::<HashSet<ReaderFeatures>>();
        match new_protocol.reader_features {
            Some(mut features) => {
                features.extend(converted_reader_features);
                new_protocol.reader_features = Some(features);
            }
            None => new_protocol.reader_features = Some(converted_reader_features),
        }
    }
    new_protocol
}

impl std::future::IntoFuture for SetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let mut metadata = this.snapshot.metadata().clone();

            let current_protocol = this.snapshot.protocol();
            let properties = this.properties;

            let new_protocol = apply_properties_to_protocol(
                current_protocol,
                &properties,
                this.raise_if_not_exists,
            )?;

            metadata.configuration.extend(
                properties
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k, Some(v)))
                    .collect::<HashMap<String, Option<String>>>(),
            );

            let final_protocol =
                convert_properties_to_features(new_protocol, &metadata.configuration);

            let operation = DeltaOperation::SetTableProperties { properties };

            let mut actions = vec![Action::Metadata(metadata)];

            if current_protocol.ne(&final_protocol) {
                actions.push(Action::Protocol(final_protocol));
            }

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions.clone())
                .build(
                    Some(&this.snapshot),
                    this.log_store.clone(),
                    operation.clone(),
                )
                .await?;
            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}
