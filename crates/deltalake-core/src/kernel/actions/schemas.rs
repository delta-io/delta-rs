//! Schema definitions for action types

use lazy_static::lazy_static;

use crate::kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

lazy_static! {
    static ref METADATA_FIELD: StructType = StructType::new(vec![
        StructField::new("id", DataType::string(), false),
        StructField::new("name", DataType::string(), true),
        StructField::new("description", DataType::string(), true),
        StructField::new("format", DataType::string(), true),
        StructField::new("schemaString", DataType::string(), false),
        StructField::new("createdTime", DataType::long(), true),
        StructField::new(
            "partitionColumns",
            DataType::Array(Box::new(ArrayType::new(DataType::string(), true))),
            false
        ),
        StructField::new(
            "configuration",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
    ]);
    static ref PROTOCOL_FIELD: StructType = StructType::new(vec![
        StructField::new("minReaderVersion", DataType::integer(), false),
        StructField::new("minWriterVersion", DataType::integer(), false),
        StructField::new(
            "readerFeatures",
            DataType::Array(Box::new(ArrayType::new(DataType::string(), false))),
            true
        ),
        StructField::new(
            "writerFeatures",
            DataType::Array(Box::new(ArrayType::new(DataType::string(), false))),
            true
        ),
    ]);
    static ref COMMIT_INFO_FIELD: StructType = StructType::new(vec![
        StructField::new("timestamp", DataType::long(), false),
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
                true
            ))),
            true
        ),
        StructField::new(
            "operationMetrics",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
    ]);
    static ref ADD_FIELD: StructType = StructType::new(vec![
        StructField::new("path", DataType::string(), false),
        StructField::new("size", DataType::long(), false),
        StructField::new("modificationTime", DataType::long(), false),
        StructField::new("dataChange", DataType::boolean(), false),
        StructField::new("stats", DataType::string(), true),
        StructField::new(
            "partitionValues",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
        StructField::new(
            "tags",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
        StructField::new(
            "deletionVector",
            DataType::Struct(Box::new(StructType::new(vec![
                StructField::new("storageType", DataType::string(), false),
                StructField::new("pathOrInlineDv", DataType::string(), false),
                StructField::new("offset", DataType::integer(), true),
                StructField::new("sizeInBytes", DataType::integer(), false),
                StructField::new("cardinality", DataType::long(), false),
            ]))),
            true
        ),
        StructField::new("baseRowId", DataType::long(), true),
        StructField::new("defaultRowCommitVersion", DataType::long(), true),
    ]);
    static ref REMOVE_FIELD: StructType = StructType::new(vec![
        StructField::new("path", DataType::string(), true),
        StructField::new("deletionTimestamp", DataType::long(), true),
        StructField::new("dataChange", DataType::boolean(), true),
        StructField::new("extendedFileMetadata", DataType::boolean(), true),
        StructField::new("size", DataType::long(), true),
        StructField::new(
            "partitionValues",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
        StructField::new(
            "tags",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
    ]);
    static ref CDC_FIELD: StructType = StructType::new(vec![
        StructField::new("path", DataType::string(), true),
        StructField::new("size", DataType::long(), true),
        StructField::new("dataChange", DataType::boolean(), true),
        StructField::new(
            "partitionValues",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
        StructField::new(
            "tags",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
    ]);
    static ref TXN_FIELD: StructType = StructType::new(vec![
        StructField::new("appId", DataType::string(), true),
        StructField::new("version", DataType::long(), true),
        StructField::new("lastUpdated", DataType::long(), true),
    ]);
    static ref DOMAIN_METADATA_FIELD: StructType = StructType::new(vec![
        StructField::new("domain", DataType::string(), false),
        StructField::new(
            "configuration",
            DataType::Map(Box::new(MapType::new(
                DataType::string(),
                DataType::string(),
                true
            ))),
            true
        ),
        StructField::new("removedFiles", DataType::boolean(), true),
    ]);
    static ref HIGH_WATERMARK_FIELD: StructType = StructType::new(vec![StructField::new(
        "highWatermark",
        DataType::long(),
        true
    ),]);
}
