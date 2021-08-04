#![allow(non_snake_case, non_camel_case_types)]

use serde::{Deserialize, Serialize};
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
#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
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
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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
    /// Create a new SchemaField from scratch
    pub fn new(
        name: String,
        r#type: SchemaDataType,
        nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            r#type,
            nullable,
            metadata,
        }
    }

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
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Schema {
    r#type: String,
    fields: Vec<SchemaField>,
}

impl Schema {
    /// Returns the list of fields that make up the schema definition of the table.
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }

    /// Create a new Schema using a vector of SchemaFields
    pub fn new(r#type: String, fields: Vec<SchemaField>) -> Self {
        Self { r#type, fields }
    }
}
