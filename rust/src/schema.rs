//! Delta Table schema implementation.
#![allow(non_snake_case, non_camel_case_types)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

use crate::errors::DeltaTableError;

/// Type alias for a string expected to match a GUID/UUID format
pub type Guid = String;

static STRUCT_TAG: &str = "struct";
static ARRAY_TAG: &str = "array";
static MAP_TAG: &str = "map";

/// An invariant for a column that is enforced on all writes to a Delta table.
#[derive(Eq, PartialEq, Debug, Default, Clone)]
pub struct Invariant {
    /// The full path to the field.
    pub field_name: String,
    /// The SQL string that must always evaluate to true.
    pub invariant_sql: String,
}

impl Invariant {
    /// Create a new invariant
    pub fn new(field_name: &str, invariant_sql: &str) -> Self {
        Self {
            field_name: field_name.to_string(),
            invariant_sql: invariant_sql.to_string(),
        }
    }
}

/// Represents a struct field defined in the Delta table schema.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format
#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct SchemaTypeStruct {
    r#type: Cow<'static, str>,
    fields: Vec<SchemaField>,
}

impl SchemaTypeStruct {
    /// Create a new Schema using a vector of SchemaFields
    pub fn new(fields: Vec<SchemaField>) -> Self {
        let tag = Cow::Borrowed(STRUCT_TAG);
        Self {
            r#type: tag,
            fields,
        }
    }

    /// Returns the list of fields contained within the column struct.
    pub fn get_fields(&self) -> &Vec<SchemaField> {
        &self.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn get_field_with_name(&self, name: &str) -> Result<&SchemaField, DeltaTableError> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize, DeltaTableError> {
        for i in 0..self.fields.len() {
            if self.fields[i].get_name() == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> = self.fields.iter().map(|f| f.name.clone()).collect();
        Err(DeltaTableError::Generic(format!(
            "Unable to get field named \"{name}\". Valid fields: {valid_fields:?}"
        )))
    }

    /// Get all invariants in the schemas
    pub fn get_invariants(&self) -> Result<Vec<Invariant>, DeltaTableError> {
        let mut remaining_fields: Vec<(String, SchemaField)> = self
            .get_fields()
            .iter()
            .map(|field| (field.name.clone(), field.clone()))
            .collect();
        let mut invariants: Vec<Invariant> = Vec::new();

        let add_segment = |prefix: &str, segment: &str| -> String {
            if prefix.is_empty() {
                segment.to_owned()
            } else {
                format!("{prefix}.{segment}")
            }
        };

        while let Some((field_path, field)) = remaining_fields.pop() {
            match field.r#type {
                SchemaDataType::r#struct(inner) => {
                    remaining_fields.extend(
                        inner
                            .get_fields()
                            .iter()
                            .map(|field| {
                                let new_prefix = add_segment(&field_path, &field.name);
                                (new_prefix, field.clone())
                            })
                            .collect::<Vec<(String, SchemaField)>>(),
                    );
                }
                SchemaDataType::array(inner) => {
                    let element_field_name = add_segment(&field_path, "element");
                    remaining_fields.push((
                        element_field_name,
                        SchemaField::new("".to_string(), *inner.elementType, false, HashMap::new()),
                    ));
                }
                SchemaDataType::map(inner) => {
                    let key_field_name = add_segment(&field_path, "key");
                    remaining_fields.push((
                        key_field_name,
                        SchemaField::new("".to_string(), *inner.keyType, false, HashMap::new()),
                    ));
                    let value_field_name = add_segment(&field_path, "value");
                    remaining_fields.push((
                        value_field_name,
                        SchemaField::new("".to_string(), *inner.valueType, false, HashMap::new()),
                    ));
                }
                _ => {}
            }
            // JSON format: {"expression": {"expression": "<SQL STRING>"} }
            if let Some(Value::String(invariant_json)) = field.metadata.get("delta.invariants") {
                let json: Value = serde_json::from_str(invariant_json).map_err(|e| {
                    DeltaTableError::InvalidInvariantJson {
                        json_err: e,
                        line: invariant_json.to_string(),
                    }
                })?;
                if let Value::Object(json) = json {
                    if let Some(Value::Object(expr1)) = json.get("expression") {
                        if let Some(Value::String(sql)) = expr1.get("expression") {
                            invariants.push(Invariant::new(&field_path, sql));
                        }
                    }
                }
            }
        }
        Ok(invariants)
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
    metadata: HashMap<String, Value>,
}

impl SchemaField {
    /// Create a new SchemaField from scratch
    pub fn new(
        name: String,
        r#type: SchemaDataType,
        nullable: bool,
        metadata: HashMap<String, Value>,
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
    pub fn get_metadata(&self) -> &HashMap<String, Value> {
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
    /// Create a new SchemaTypeArray
    pub fn new(elementType: Box<SchemaDataType>, containsNull: bool) -> Self {
        Self {
            r#type: String::from(ARRAY_TAG),
            elementType,
            containsNull,
        }
    }

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
    /// Create a new SchemaTypeMap
    pub fn new(
        keyType: Box<SchemaDataType>,
        valueType: Box<SchemaDataType>,
        valueContainsNull: bool,
    ) -> Self {
        Self {
            r#type: String::from(MAP_TAG),
            keyType,
            valueType,
            valueContainsNull,
        }
    }

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

/// Enum with variants for each top level schema data type.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(untagged)]
pub enum SchemaDataType {
    /// Variant representing non-array, non-map, non-struct fields. Wrapped value will contain the
    /// the string name of the primitive type.
    ///
    /// Valid values are:
    ///  * string: utf8
    ///  * long  // undocumented, i64?
    ///  * integer: i32
    ///  * short: i16
    ///  * byte: i8
    ///  * float: f32
    ///  * double: f64
    ///  * boolean: bool
    ///  * binary: a sequence of binary data
    ///  * date: A calendar date, represented as a year-month-day triple without a timezone
    ///  * timestamp: Microsecond precision timestamp without a timezone
    ///  * decimal: Signed decimal number with fixed precision (maximum number of digits) and scale (number of digits on right side of dot), where the precision and scale can be up to 38
    primitive(String),
    /// Variant representing a struct.
    r#struct(SchemaTypeStruct),
    /// Variant representing an array.
    array(SchemaTypeArray),
    /// Variant representing a map.
    map(SchemaTypeMap),
}

/// Represents the schema of the delta table.
pub type Schema = SchemaTypeStruct;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_invariants() {
        let schema: Schema = serde_json::from_value(json!({
            "type": "struct",
            "fields": [{"name": "x", "type": "string", "nullable": true, "metadata": {}}]
        }))
        .unwrap();
        let invariants = schema.get_invariants().unwrap();
        assert_eq!(invariants.len(), 0);

        let schema: Schema = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "x", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"x > 2\"} }"
                }},
                {"name": "y", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"y < 4\"} }"
                }}
            ]
        }))
        .unwrap();
        let invariants = schema.get_invariants().unwrap();
        assert_eq!(invariants.len(), 2);
        assert!(invariants.contains(&Invariant::new("x", "x > 2")));
        assert!(invariants.contains(&Invariant::new("y", "y < 4")));

        let schema: Schema = serde_json::from_value(json!({
            "type": "struct",
            "fields": [{
                "name": "a_map",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [{
                                "name": "d",
                                "type": "integer",
                                "metadata": {
                                    "delta.invariants": "{\"expression\": { \"expression\": \"a_map.value.element.d < 4\"} }"
                                },
                                "nullable": false
                            }]
                        },
                        "containsNull": false
                    },
                    "valueContainsNull": false
                },
                "nullable": false,
                "metadata": {}
            }]
        })).unwrap();
        let invariants = schema.get_invariants().unwrap();
        assert_eq!(invariants.len(), 1);
        assert_eq!(
            invariants[0],
            Invariant::new("a_map.value.element.d", "a_map.value.element.d < 4")
        );
    }
}
