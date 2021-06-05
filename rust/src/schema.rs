#![allow(non_snake_case, non_camel_case_types)]

use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

/// Type alias for a string expected to match a GUID/UUID format
pub type Guid = String;
/// Type alias for i64/Delta long
pub type DeltaDataTypeLong = i64;
/// Type alias representing the expected type (i64) of a Delta table version.
pub type DeltaDataTypeVersion = DeltaDataTypeLong;
/// Type alias representing the expected type (i64/ms since Unix epoch) of a Delta timestamp.
pub type DeltaDataTypeTimestamp = DeltaDataTypeLong;
/// Type alias for i32/Delta int
pub type DeltaDataTypeInt = i32;

/// Represents a struct field defined in the Delta table schema.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SchemaTypeStruct {
    // type field is always the string "struct", so we are ignoring it here
    r#type: String,
    fields: Vec<SchemaField>,
}

impl SchemaTypeStruct {
    /// Returns the list of fields contained within the column struct.
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }
}

/// Describes a specific field of the Delta table schema.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaField {
    // Name of this (possibly nested) column
    name: String,
    r#type: SchemaDataType,
    // Boolean denoting whether this field can be null
    nullable: bool,
    // A JSON map containing information about this column. Keys prefixed with Delta are reserved
    // for the implementation.
    metadata: HashMap<String, String>,
}

impl SchemaField {
    /// The column name of the schema field.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// The data type of the schema field. SchemaDataType defines the possible values.
    pub fn get_type(&self) -> &SchemaDataType {
        &self.r#type
    }

    /// Whether the column/field is nullable.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Additional metadata about the column/field.
    pub fn get_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

/// Schema definition for array type fields.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaTypeArray {
    // type field is always the string "array", so we are ignoring it here
    r#type: String,
    // The type of element stored in this array represented as a string containing the name of a
    // primitive type, a struct definition, an array definition or a map definition
    elementType: Box<SchemaDataType>,
    // Boolean denoting whether this array can contain one or more null values
    containsNull: bool,
}

impl SchemaTypeArray {
    /// The data type of each element contained in the array.
    pub fn get_element_type(&self) -> &SchemaDataType {
        &self.elementType
    }

    /// Whether the column/field is allowed to contain null elements.
    pub fn contains_null(&self) -> bool {
        self.containsNull
    }
}

/// Schema definition for map type fields.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaTypeMap {
    r#type: String,
    keyType: Box<SchemaDataType>,
    valueType: Box<SchemaDataType>,
    valueContainsNull: bool,
}

impl SchemaTypeMap {
    /// The type of element used for the key of this map, represented as a string containing the
    /// name of a primitive type, a struct definition, an array definition or a map definition
    pub fn get_key_type(&self) -> &SchemaDataType {
        &self.keyType
    }

    /// The type of element contained in the value of this map, represented as a string containing the
    /// name of a primitive type, a struct definition, an array definition or a map definition
    pub fn get_value_type(&self) -> &SchemaDataType {
        &self.valueType
    }

    /// Whether the value field is allowed to contain null elements.
    pub fn get_value_contains_null(&self) -> bool {
        self.valueContainsNull
    }
}

/*
 * List of primitive types:
 *   string: utf8
 *   long  // undocumented, i64?
 *   integer: i32
 *   short: i16
 *   byte: i8
 *   float: f32
 *   double: f64
 *   boolean: bool
 *   binary: a sequence of binary data
 *   date: A calendar date, represented as a year-month-day triple without a timezone
 *   timestamp: Microsecond precision timestamp without a timezone
 */
/// Enum with variants for each top level schema data type.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum SchemaDataType {
    /// Variant representing non-array, non-map, non-struct fields. Wrapped value will contain the
    /// the string name of the primitive type.
    primitive(String),
    /// Variant representing a struct.
    r#struct(SchemaTypeStruct),
    /// Variant representing an array.
    array(SchemaTypeArray),
    /// Variant representing a map.
    map(SchemaTypeMap),
}

/// Represents the schema of the delta table.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    r#type: String,
    fields: Vec<SchemaField>,
}

impl Schema {
    /// Returns the list of fields that make up the schema definition of the table.
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }
}

/// Error representing a failure while creating the delta log schema.
#[derive(thiserror::Error, Debug)]
pub enum DeltaLogSchemaError {
    /// Error returned when reading the checkpoint failed.
    #[error("Failed to read checkpoint: {}", .source)]
    ParquetError {
        /// Parquet error details returned when reading the checkpoint failed.
        #[from]
        source: ParquetError,
    },
    /// Error returned when converting the schema in Arrow format failed.
    #[error("Failed to convert into Arrow schema: {}", .source)]
    ArrowError {
        /// Arrow error details returned when converting the schema in Arrow format failed
        #[from]
        source: ArrowError,
    },
    /// Error returned when JSON de-serialization of schema components fails.
    #[error("serde_json::Error: {source}")]
    JSONSerialization {
        /// The source serde_json::Error.
        #[from]
        source: serde_json::Error,
    },
}

/// Factory for creating a Delta log schema for a specific table schema.
/// REF: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-schema
pub struct DeltaLogSchemaFactory {
    common_fields: HashMap<String, Vec<SchemaField>>,
}

impl DeltaLogSchemaFactory {
    /// Creates a new DeltaLogSchemaFactory which can be used to create Schema's representing the
    /// Delta log for specific tables.
    pub fn new() -> Self {
        // TODO: map<string, string> is not supported by arrow currently.
        // See:
        // * https://github.com/apache/arrow-rs/issues/395
        // * https://github.com/apache/arrow-rs/issues/396

        let meta_data_fields = json!([
            { "name": "id", "type": "string", "nullable": true, "metadata": {} },
            { "name": "name", "type": "string", "nullable": true, "metadata": {} },
            { "name": "description", "type": "string", "nullable": true, "metadata": {} },
            { "name": "schemaString", "type": "string", "nullable": true, "metadata": {} },
            { "name": "createdTime", "type": "long", "nullable": true, "metadata": {} },
            { 
                "name": "partitionColumns",
                "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true,
                },
                "nullable": true,
                "metadata": {} },
            { 
                "name": "format",
                "type": {
                    "type": "struct",
                    "fields": [{
                        "name": "provider",
                        "type": "string",
                        "nullable": true,
                        "metadata": {},
                    },/*{
                        "name": "options",
                        "type": {
                            "type": "map",
                            "keyType": "string",
                            "valueType": "string",
                            "valueContainsNull": true,
                        },
                        "nullable": true,
                        "metadata": {}
                    }*/]
                },
                "nullable": true,
                "metadata": {}
            },
            /*{
                "name": "configuration",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": "string",
                    "valueContainsNull": true,
                },
                "nullable": true,
                "metadata": {}
            }*/]);

        let protocol_fields = json!([
            { "name": "minReaderVersion", "type": "integer", "nullable": true, "metadata": {} },
            { "name": "minWriterVersion", "type": "integer", "nullable": true, "metadata": {} }
        ]);

        let txn_fields = json!([
            { "name": "appId", "type": "string", "nullable": true, "metadata": {} },
            { "name": "version", "type": "long", "nullable": true, "metadata": {} }
        ]);

        let add_fields = json!([
            { "name": "path", "type": "string", "nullable": true, "metadata": {} },
            { "name": "size", "type": "long", "nullable": true, "metadata": {} },
            { "name": "modificationTime", "type": "long", "nullable": true, "metadata": {} },
            { "name": "dataChange", "type": "boolean", "nullable": true, "metadata": {} },
            { "name": "stats", "type": "string", "nullable": true, "metadata": {} },
            /*{
                "name": "partitionValues",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": "string",
                    "valueContainsNull": true,
                },
                "nullable": true,
                "metadata": {},
            }*/
        ]);

        let remove_fields = json!([
            { "name": "path", "type": "string", "nullable": true, "metadata": {} },
            { "name": "size", "type": "long", "nullable": true, "metadata": {} },
            { "name": "modificationTime", "type": "long", "nullable": true, "metadata": {} },
            { "name": "dataChange", "type": "boolean", "nullable": true, "metadata": {}, },
            { "name": "stats", "type": "string", "nullable": true, "metadata": {},
            },/*{
                "name": "partitionValues",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": "string",
                    "valueContainsNull": true,
                },
                "nullable": true,
                "metadata": {},

            }*/]);

        let mut map = HashMap::new();

        map.insert(
            "metaData".to_string(),
            serde_json::from_value(meta_data_fields).unwrap(),
        );
        map.insert(
            "protocol".to_string(),
            serde_json::from_value(protocol_fields).unwrap(),
        );
        map.insert(
            "txn".to_string(),
            serde_json::from_value(txn_fields).unwrap(),
        );
        map.insert(
            "add".to_string(),
            serde_json::from_value(add_fields).unwrap(),
        );
        map.insert(
            "remove".to_string(),
            serde_json::from_value(remove_fields).unwrap(),
        );

        Self { common_fields: map }
    }

    /// Creates a Schema representing the delta log for a specific delta table.
    /// Merges fields from the table schema into the delta log schema.
    pub fn delta_log_schema_for_table(
        &self,
        table_schema: &Schema,
        partition_columns: &[String],
    ) -> Result<Schema, DeltaLogSchemaError> {
        let (partition_fields, non_partition_fields): (Vec<SchemaField>, Vec<SchemaField>) =
            table_schema
                .fields
                .iter()
                .map(|f| f.to_owned())
                .partition(|field| partition_columns.contains(&field.name));

        let fields: Vec<SchemaField> = self
            .common_fields
            .iter()
            .map(|(name, fields)| match name.as_str() {
                "add" => {
                    let mut fields = fields.clone();

                    if partition_fields.len() > 0 {
                        let partition_values_parsed = SchemaField {
                            name: "partitionValues_parsed".to_string(),
                            nullable: true,
                            metadata: HashMap::new(),
                            r#type: SchemaDataType::r#struct(SchemaTypeStruct {
                                r#type: "struct".to_string(),
                                fields: partition_fields.clone(),
                            }),
                        };
                        fields.push(partition_values_parsed);
                    }

                    if non_partition_fields.len() > 0 {
                        let stats_parsed = SchemaField {
                            name: "stats_parsed".to_string(),
                            nullable: true,
                            metadata: HashMap::new(),
                            r#type: SchemaDataType::r#struct(SchemaTypeStruct {
                                r#type: "struct".to_string(),
                                fields: non_partition_fields.clone(),
                            }),
                        };

                        fields.push(stats_parsed);
                    }


                    SchemaField {
                        name: name.clone(),
                        nullable: true,
                        metadata: HashMap::new(),
                        r#type: SchemaDataType::r#struct(SchemaTypeStruct {
                            r#type: "struct".to_string(),
                            fields,
                        }),
                    }
                }
                _ => SchemaField {
                    name: name.clone(),
                    nullable: true,
                    metadata: HashMap::new(),
                    r#type: SchemaDataType::r#struct(SchemaTypeStruct {
                        r#type: "struct".to_string(),
                        fields: fields.clone(),
                    }),
                },
            })
            .collect();

        Ok(Schema {
            r#type: "struct".to_string(),
            fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_log_schema_factory_creates_schema() {
        let _factory = DeltaLogSchemaFactory::new();

        // TODO:
    }
}

