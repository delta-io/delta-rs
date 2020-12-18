#![allow(non_snake_case, non_camel_case_types)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub type GUID = String;
pub type DeltaDataTypeLong = i64;
pub type DeltaDataTypeVersion = DeltaDataTypeLong;
pub type DeltaDataTypeTimestamp = DeltaDataTypeLong;
pub type DeltaDataTypeInt = i32;

// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SchemaTypeStruct {
    // type field is alwsy the string "struct", so we are ignoring it here
    r#type: String,
    fields: Vec<SchemaField>,
}

impl SchemaTypeStruct {
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaField {
    // Name of this (possibly nested) column
    name: String,
    // String containing the name of a primitive type, a struct definition, an array definition or
    // a map definition
    r#type: SchemaDataType,
    // Boolean denoting whether this field can be null
    nullable: bool,
    // A JSON map containing information about this column. Keys prefixed with Delta are reserved
    // for the implementation.
    metadata: HashMap<String, String>,
}

impl SchemaField {
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type(&self) -> &SchemaDataType {
        &self.r#type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn get_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaTypeArray {
    // type field is alwsy the string "array", so we are ignoring it here
    r#type: String,
    // The type of element stored in this array represented as a string containing the name of a
    // primitive type, a struct definition, an array definition or a map definition
    elementType: Box<SchemaDataType>,
    // Boolean denoting whether this array can contain one or more null values
    containsNull: bool,
}

impl SchemaTypeArray {
    pub fn get_element_type(&self) -> &SchemaDataType {
        &self.elementType
    }

    pub fn contains_null(&self) -> bool {
        self.containsNull
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaTypeMap {
    r#type: String,
    // The type of element used for the key of this map, represented as a string containing the
    // name of a primitive type, a struct definition, an array definition or a map definition
    keyType: Box<SchemaDataType>,
    // The type of element used for the key of this map, represented as a string containing the
    // name of a primitive type, a struct definition, an array definition or a map definition
    valueType: Box<SchemaDataType>,
}

impl SchemaTypeMap {
    pub fn get_key_type(&self) -> &SchemaDataType {
        &self.keyType
    }

    pub fn get_value_type(&self) -> &SchemaDataType {
        &self.valueType
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
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SchemaDataType {
    primitive(String),
    r#struct(SchemaTypeStruct),
    array(SchemaTypeArray),
    map(SchemaTypeMap),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    r#type: String,
    fields: Vec<SchemaField>,
}

impl Schema {
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }
}
