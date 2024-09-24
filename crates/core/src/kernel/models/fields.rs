//! Schema definitions for action types
use std::sync::Arc;

use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
use lazy_static::lazy_static;

use super::ActionType;

impl ActionType {
    /// Returns the type of the corresponding field in the delta log schema
    pub(crate) fn schema_field(&self) -> &StructField {
        match self {
            Self::Metadata => &METADATA_FIELD,
            Self::Protocol => &PROTOCOL_FIELD,
            Self::CommitInfo => &COMMIT_INFO_FIELD,
            Self::Add => &ADD_FIELD,
            Self::Remove => &REMOVE_FIELD,
            Self::Cdc => &CDC_FIELD,
            Self::Txn => &TXN_FIELD,
            Self::DomainMetadata => &DOMAIN_METADATA_FIELD,
            Self::CheckpointMetadata => &CHECKPOINT_METADATA_FIELD,
            Self::Sidecar => &SIDECAR_FIELD,
        }
    }
}

lazy_static! {
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
    static ref METADATA_FIELD: StructField = StructField::new(
        "metaData",
        StructType::new(vec![
            StructField::new("id", DataType::STRING, true),
            StructField::new("name", DataType::STRING, true),
            StructField::new("description", DataType::STRING, true),
            StructField::new(
                "format",
                StructType::new(vec![
                    StructField::new("provider", DataType::STRING, true),
                    StructField::new(
                        "options",
                        MapType::new(
                            DataType::STRING,
                            DataType::STRING,
                            true,
                        ),
                        false,
                    ),
                ]),
                false,
            ),
            StructField::new("schemaString", DataType::STRING, true),
            StructField::new(
                "partitionColumns",
                ArrayType::new(DataType::STRING, false),
                true,
            ),
            StructField::new("createdTime", DataType::LONG, true),
            StructField::new(
                "configuration",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                false,
            ),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#protocol-evolution
    static ref PROTOCOL_FIELD: StructField = StructField::new(
        "protocol",
        StructType::new(vec![
            StructField::new("minReaderVersion", DataType::INTEGER, true),
            StructField::new("minWriterVersion", DataType::INTEGER, true),
            StructField::new(
                "readerFeatures",
                ArrayType::new(DataType::STRING, true),
                true,
            ),
            StructField::new(
                "writerFeatures",
                ArrayType::new(DataType::STRING, true),
                true,
            ),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information
    static ref COMMIT_INFO_FIELD: StructField = StructField::new(
        "commitInfo",
        StructType::new(vec![
            StructField::new("timestamp", DataType::LONG, false),
            StructField::new("operation", DataType::STRING, false),
            StructField::new("isolationLevel", DataType::STRING, true),
            StructField::new("isBlindAppend", DataType::BOOLEAN, true),
            StructField::new("txnId", DataType::STRING, true),
            StructField::new("readVersion", DataType::LONG, true),
            StructField::new(
                "operationParameters",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                true,
            ),
            StructField::new(
                "operationMetrics",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                true,
            ),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
    static ref ADD_FIELD: StructField = StructField::new(
        "add",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, true),
            partition_values_field(),
            StructField::new("size", DataType::LONG, true),
            StructField::new("modificationTime", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, true),
            StructField::new("stats", DataType::STRING, true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::LONG, true),
            StructField::new("defaultRowCommitVersion", DataType::LONG, true),
            StructField::new("clusteringProvider", DataType::STRING, true),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
    static ref REMOVE_FIELD: StructField = StructField::new(
        "remove",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, true),
            StructField::new("deletionTimestamp", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, true),
            StructField::new("extendedFileMetadata", DataType::BOOLEAN, true),
            partition_values_field(),
            StructField::new("size", DataType::LONG, true),
            StructField::new("stats", DataType::STRING, true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::LONG, true),
            StructField::new("defaultRowCommitVersion", DataType::LONG, true),
        ]),
        true,
    );
    static ref REMOVE_FIELD_CHECKPOINT: StructField = StructField::new(
        "remove",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            StructField::new("deletionTimestamp", DataType::LONG, true),
            StructField::new("dataChange", DataType::BOOLEAN, false),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file
    static ref CDC_FIELD: StructField = StructField::new(
        "cdc",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            partition_values_field(),
            StructField::new("size", DataType::LONG, false),
            StructField::new("dataChange", DataType::BOOLEAN, false),
            tags_field(),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers
    static ref TXN_FIELD: StructField = StructField::new(
        "txn",
        StructType::new(vec![
            StructField::new("appId", DataType::STRING, false),
            StructField::new("version", DataType::LONG, false),
            StructField::new("lastUpdated", DataType::LONG, true),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata
    static ref DOMAIN_METADATA_FIELD: StructField = StructField::new(
        "domainMetadata",
        StructType::new(vec![
            StructField::new("domain", DataType::STRING, false),
            StructField::new(
                "configuration",
                MapType::new(
                    DataType::STRING,
                    DataType::STRING,
                    true,
                ),
                true,
            ),
            StructField::new("removed", DataType::BOOLEAN, false),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-metadata
    static ref CHECKPOINT_METADATA_FIELD: StructField = StructField::new(
        "checkpointMetadata",
        StructType::new(vec![
            StructField::new("flavor", DataType::STRING, false),
            tags_field(),
        ]),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
    static ref SIDECAR_FIELD: StructField = StructField::new(
        "sidecar",
        StructType::new(vec![
            StructField::new("path", DataType::STRING, false),
            StructField::new("sizeInBytes", DataType::LONG, true),
            StructField::new("modificationTime", DataType::LONG, false),
            StructField::new("type", DataType::STRING, false),
            tags_field(),
        ]),
        true,
    );

    static ref LOG_SCHEMA: StructType = StructType::new(
        vec![
            ADD_FIELD.clone(),
            CDC_FIELD.clone(),
            COMMIT_INFO_FIELD.clone(),
            DOMAIN_METADATA_FIELD.clone(),
            METADATA_FIELD.clone(),
            PROTOCOL_FIELD.clone(),
            REMOVE_FIELD.clone(),
            TXN_FIELD.clone(),
        ]
    );
}

fn tags_field() -> StructField {
    StructField::new(
        "tags",
        MapType::new(DataType::STRING, DataType::STRING, true),
        true,
    )
}

fn partition_values_field() -> StructField {
    StructField::new(
        "partitionValues",
        MapType::new(DataType::STRING, DataType::STRING, true),
        true,
    )
}

fn deletion_vector_field() -> StructField {
    StructField::new(
        "deletionVector",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("storageType", DataType::STRING, true),
            StructField::new("pathOrInlineDv", DataType::STRING, true),
            StructField::new("offset", DataType::INTEGER, true),
            StructField::new("sizeInBytes", DataType::INTEGER, true),
            StructField::new("cardinality", DataType::LONG, true),
        ]))),
        true,
    )
}

#[cfg(test)]
pub(crate) fn log_schema() -> &'static StructType {
    &LOG_SCHEMA
}

pub(crate) fn log_schema_ref() -> &'static Arc<StructType> {
    lazy_static! {
        static ref LOG_SCHEMA_REF: Arc<StructType> = Arc::new(LOG_SCHEMA.clone());
    }
    &LOG_SCHEMA_REF
}
