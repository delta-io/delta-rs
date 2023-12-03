//! Schema definitions for action types

use lazy_static::lazy_static;

use super::ActionType;
use crate::kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

lazy_static! {
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
    static ref METADATA_FIELD: StructField = StructField::new(
        "metaData",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("id", DataType::string(), false),
            StructField::new("name", DataType::string(), true),
            StructField::new("description", DataType::string(), true),
            StructField::new(
                "format",
                DataType::Struct(Box::new(StructType::new(vec![
                    StructField::new("provider", DataType::string(), false),
                    StructField::new(
                        "configuration",
                        DataType::Map(Box::new(MapType::new(
                            DataType::string(),
                            DataType::string(),
                            true,
                        ))),
                        true,
                    ),
                ]))),
                false,
            ),
            StructField::new("schemaString", DataType::string(), false),
            StructField::new(
                "partitionColumns",
                DataType::Array(Box::new(ArrayType::new(DataType::string(), false))),
                false,
            ),
            StructField::new("createdTime", DataType::long(), true),
            StructField::new(
                "configuration",
                DataType::Map(Box::new(MapType::new(
                    DataType::string(),
                    DataType::string(),
                    true,
                ))),
                false,
            ),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#protocol-evolution
    static ref PROTOCOL_FIELD: StructField = StructField::new(
        "protocol",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("minReaderVersion", DataType::integer(), false),
            StructField::new("minWriterVersion", DataType::integer(), false),
            StructField::new(
                "readerFeatures",
                DataType::Array(Box::new(ArrayType::new(DataType::string(), false))),
                true,
            ),
            StructField::new(
                "writerFeatures",
                DataType::Array(Box::new(ArrayType::new(DataType::string(), false))),
                true,
            ),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information
    static ref COMMIT_INFO_FIELD: StructField = StructField::new(
        "commitInfo",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("timestamp", DataType::timestamp(), false),
            StructField::new("operation", DataType::string(), false),
            StructField::new("isolationLevel", DataType::string(), true),
            StructField::new("isBlindAppend", DataType::boolean(), true),
            StructField::new("txnId", DataType::string(), true),
            StructField::new("readVersion", DataType::long(), true),
            StructField::new(
                "operationParameters",
                DataType::Map(Box::new(MapType::new(
                    DataType::string(),
                    DataType::string(),
                    true,
                ))),
                true,
            ),
            StructField::new(
                "operationMetrics",
                DataType::Map(Box::new(MapType::new(
                    DataType::string(),
                    DataType::string(),
                    true,
                ))),
                true,
            ),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
    static ref ADD_FIELD: StructField = StructField::new(
        "add",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("path", DataType::string(), false),
            partition_values_field(),
            StructField::new("size", DataType::long(), false),
            StructField::new("modificationTime", DataType::timestamp(), false),
            StructField::new("dataChange", DataType::boolean(), false),
            StructField::new("stats", DataType::string(), true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::long(), true),
            StructField::new("defaultRowCommitVersion", DataType::long(), true),
            StructField::new("clusteringProvider", DataType::string(), true),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
    static ref REMOVE_FIELD: StructField = StructField::new(
        "remove",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("path", DataType::string(), false),
            StructField::new("deletionTimestamp", DataType::timestamp(), true),
            StructField::new("dataChange", DataType::boolean(), false),
            StructField::new("extendedFileMetadata", DataType::boolean(), true),
            partition_values_field(),
            StructField::new("size", DataType::long(), true),
            StructField::new("stats", DataType::string(), true),
            tags_field(),
            deletion_vector_field(),
            StructField::new("baseRowId", DataType::long(), true),
            StructField::new("defaultRowCommitVersion", DataType::long(), true),
        ]))),
        true,
    );
    static ref REMOVE_FIELD_CHECKPOINT: StructField = StructField::new(
        "remove",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("path", DataType::string(), false),
            StructField::new("deletionTimestamp", DataType::timestamp(), true),
            StructField::new("dataChange", DataType::boolean(), false),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file
    static ref CDC_FIELD: StructField = StructField::new(
        "cdc",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("path", DataType::string(), false),
            partition_values_field(),
            StructField::new("size", DataType::long(), false),
            StructField::new("dataChange", DataType::boolean(), false),
            tags_field(),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers
    static ref TXN_FIELD: StructField = StructField::new(
        "txn",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("appId", DataType::string(), false),
            StructField::new("version", DataType::long(), false),
            StructField::new("lastUpdated", DataType::timestamp(), true),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata
    static ref DOMAIN_METADATA_FIELD: StructField = StructField::new(
        "domainMetadata",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("domain", DataType::string(), false),
            StructField::new(
                "configuration",
                DataType::Map(Box::new(MapType::new(
                    DataType::string(),
                    DataType::string(),
                    true,
                ))),
                false,
            ),
            StructField::new("removed", DataType::boolean(), false),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-metadata
    static ref CHECKPOINT_METADATA_FIELD: StructField = StructField::new(
        "checkpointMetadata",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("flavor", DataType::string(), false),
            tags_field(),
        ]))),
        true,
    );
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
    static ref SIDECAR_FIELD: StructField = StructField::new(
        "sidecar",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("path", DataType::string(), false),
            StructField::new("sizeInBytes", DataType::long(), false),
            StructField::new("modificationTime", DataType::timestamp(), false),
            StructField::new("type", DataType::string(), false),
            tags_field(),
        ]))),
        true,
    );
}

fn tags_field() -> StructField {
    StructField::new(
        "tags",
        DataType::Map(Box::new(MapType::new(
            DataType::string(),
            DataType::string(),
            true,
        ))),
        true,
    )
}

fn partition_values_field() -> StructField {
    StructField::new(
        "partitionValues",
        DataType::Map(Box::new(MapType::new(
            DataType::string(),
            DataType::string(),
            true,
        ))),
        false,
    )
}

fn deletion_vector_field() -> StructField {
    StructField::new(
        "deletionVector",
        DataType::Struct(Box::new(StructType::new(vec![
            StructField::new("storageType", DataType::string(), false),
            StructField::new("pathOrInlineDv", DataType::string(), false),
            StructField::new("offset", DataType::integer(), true),
            StructField::new("sizeInBytes", DataType::integer(), false),
            StructField::new("cardinality", DataType::long(), false),
        ]))),
        true,
    )
}

impl ActionType {
    /// Returns the type of the corresponding field in the delta log schema
    pub fn schema_field(&self) -> &StructField {
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
