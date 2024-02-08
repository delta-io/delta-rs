//! Delta table schema

use std::borrow::Borrow;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::error::Error;
use crate::kernel::DataCheck;

/// Type alias for a top level schema
pub type Schema = StructType;
/// Schema reference type
pub type SchemaRef = Arc<StructType>;

/// A value that can be stored in the metadata of a Delta table schema entity.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum MetadataValue {
    /// A number value
    Number(i32),
    /// A string value
    String(String),
    /// A Boolean value
    Boolean(bool),
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&String> for MetadataValue {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<i32> for MetadataValue {
    fn from(value: i32) -> Self {
        Self::Number(value)
    }
}

impl From<bool> for MetadataValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<Value> for MetadataValue {
    fn from(value: Value) -> Self {
        Self::String(value.to_string())
    }
}

#[derive(Debug)]
#[allow(missing_docs)]
pub enum ColumnMetadataKey {
    ColumnMappingId,
    ColumnMappingPhysicalName,
    GenerationExpression,
    IdentityStart,
    IdentityStep,
    IdentityHighWaterMark,
    IdentityAllowExplicitInsert,
    Invariants,
}

impl AsRef<str> for ColumnMetadataKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::ColumnMappingId => "delta.columnMapping.id",
            Self::ColumnMappingPhysicalName => "delta.columnMapping.physicalName",
            Self::GenerationExpression => "delta.generationExpression",
            Self::IdentityAllowExplicitInsert => "delta.identity.allowExplicitInsert",
            Self::IdentityHighWaterMark => "delta.identity.highWaterMark",
            Self::IdentityStart => "delta.identity.start",
            Self::IdentityStep => "delta.identity.step",
            Self::Invariants => "delta.invariants",
        }
    }
}

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

impl DataCheck for Invariant {
    fn get_name(&self) -> &str {
        &self.field_name
    }

    fn get_expression(&self) -> &str {
        &self.invariant_sql
    }
}

/// Represents a struct field defined in the Delta table schema.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct StructField {
    /// Name of this (possibly nested) column
    pub name: String,
    /// The data type of this field
    #[serde(rename = "type")]
    pub data_type: DataType,
    /// Denotes whether this Field can be null
    pub nullable: bool,
    /// A JSON map containing information about this column
    pub metadata: HashMap<String, MetadataValue>,
}

impl Hash for StructField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Borrow<str> for StructField {
    fn borrow(&self) -> &str {
        self.name.as_ref()
    }
}

impl Eq for StructField {}

impl StructField {
    /// Creates a new field
    pub fn new(name: impl Into<String>, data_type: impl Into<DataType>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            metadata: HashMap::default(),
        }
    }

    /// Creates a new field with metadata
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata = metadata
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    /// Get the value of a specific metadata key
    pub fn get_config_value(&self, key: &ColumnMetadataKey) -> Option<&MetadataValue> {
        self.metadata.get(key.as_ref())
    }

    #[inline]
    /// Returns the name of the column
    pub fn name(&self) -> &String {
        &self.name
    }

    #[inline]
    /// Returns whether the column is nullable
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the physical name of the column
    /// Equals the name if column mapping is not enabled on table
    pub fn physical_name(&self) -> Result<&str, Error> {
        // Even on mapping type id the physical name should be there for partitions
        let phys_name = self.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName);
        match phys_name {
            None => Ok(&self.name),
            Some(MetadataValue::Boolean(_)) => Ok(&self.name),
            Some(MetadataValue::String(s)) => Ok(s),
            Some(MetadataValue::Number(_)) => Err(Error::MetadataError(
                "Unexpected type for physical name".to_string(),
            )),
        }
    }

    #[inline]
    /// Returns the data type of the column
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    /// Returns the metadata of the column
    pub const fn metadata(&self) -> &HashMap<String, MetadataValue> {
        &self.metadata
    }
}

/// A struct is used to represent both the top-level schema of the table
/// as well as struct columns that contain nested columns.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct StructType {
    #[serde(rename = "type")]
    /// The type of this struct
    pub type_name: String,
    /// The type of element stored in this array
    pub fields: Vec<StructField>,
}

impl StructType {
    /// Creates a new struct type
    pub fn new(fields: Vec<StructField>) -> Self {
        Self {
            type_name: "struct".into(),
            fields,
        }
    }

    /// Returns an immutable reference of the fields in the struct
    pub fn fields(&self) -> &Vec<StructField> {
        &self.fields
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize, Error> {
        let (idx, _) = self
            .fields()
            .iter()
            .enumerate()
            .find(|(_, b)| b.name() == name)
            .ok_or_else(|| {
                let valid_fields: Vec<_> = self.fields.iter().map(|f| f.name()).collect();
                Error::Schema(format!(
                    "Unable to get field named \"{name}\". Valid fields: {valid_fields:?}"
                ))
            })?;
        Ok(idx)
    }

    /// Returns a reference of a specific [`StructField`] instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&StructField, Error> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Get all invariants in the schemas
    pub fn get_invariants(&self) -> Result<Vec<Invariant>, Error> {
        let mut remaining_fields: Vec<(String, StructField)> = self
            .fields()
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
            match field.data_type() {
                DataType::Struct(inner) => {
                    remaining_fields.extend(
                        inner
                            .fields()
                            .iter()
                            .map(|field| {
                                let new_prefix = add_segment(&field_path, &field.name);
                                (new_prefix, field.clone())
                            })
                            .collect::<Vec<(String, StructField)>>(),
                    );
                }
                DataType::Array(inner) => {
                    let element_field_name = add_segment(&field_path, "element");
                    remaining_fields.push((
                        element_field_name,
                        StructField::new("".to_string(), inner.element_type.clone(), false),
                    ));
                }
                DataType::Map(inner) => {
                    let key_field_name = add_segment(&field_path, "key");
                    remaining_fields.push((
                        key_field_name,
                        StructField::new("".to_string(), inner.key_type.clone(), false),
                    ));
                    let value_field_name = add_segment(&field_path, "value");
                    remaining_fields.push((
                        value_field_name,
                        StructField::new("".to_string(), inner.value_type.clone(), false),
                    ));
                }
                _ => {}
            }
            // JSON format: {"expression": {"expression": "<SQL STRING>"} }
            if let Some(MetadataValue::String(invariant_json)) =
                field.metadata.get(ColumnMetadataKey::Invariants.as_ref())
            {
                let json: Value = serde_json::from_str(invariant_json).map_err(|e| {
                    Error::InvalidInvariantJson {
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

impl FromIterator<StructField> for StructType {
    fn from_iter<T: IntoIterator<Item = StructField>>(iter: T) -> Self {
        Self {
            type_name: "struct".into(),
            fields: iter.into_iter().collect(),
        }
    }
}

impl<'a> FromIterator<&'a StructField> for StructType {
    fn from_iter<T: IntoIterator<Item = &'a StructField>>(iter: T) -> Self {
        Self {
            type_name: "struct".into(),
            fields: iter.into_iter().cloned().collect(),
        }
    }
}

impl<const N: usize> From<[StructField; N]> for StructType {
    fn from(value: [StructField; N]) -> Self {
        Self {
            type_name: "struct".into(),
            fields: value.to_vec(),
        }
    }
}

impl<'a, const N: usize> From<[&'a StructField; N]> for StructType {
    fn from(value: [&'a StructField; N]) -> Self {
        Self {
            type_name: "struct".into(),
            fields: value.into_iter().cloned().collect(),
        }
    }
}

impl<'a> IntoIterator for &'a StructType {
    type Item = &'a StructField;
    type IntoIter = std::slice::Iter<'a, StructField>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.iter()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
/// An array stores a variable length collection of items of some type.
pub struct ArrayType {
    #[serde(rename = "type")]
    /// The type of this struct
    pub type_name: String,
    /// The type of element stored in this array
    pub element_type: DataType,
    /// Denoting whether this array can contain one or more null values
    pub contains_null: bool,
}

impl ArrayType {
    /// Creates a new array type
    pub fn new(element_type: DataType, contains_null: bool) -> Self {
        Self {
            type_name: "array".into(),
            element_type,
            contains_null,
        }
    }

    #[inline]
    /// Returns the element type of the array
    pub const fn element_type(&self) -> &DataType {
        &self.element_type
    }

    #[inline]
    /// Returns whether the array can contain null values
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
/// A map stores an arbitrary length collection of key-value pairs
pub struct MapType {
    #[serde(rename = "type")]
    /// The type of this struct
    pub type_name: String,
    /// The type of element used for the key of this map
    pub key_type: DataType,
    /// The type of element used for the value of this map
    pub value_type: DataType,
    /// Denoting whether this array can contain one or more null values
    #[serde(default = "default_true")]
    pub value_contains_null: bool,
}

impl MapType {
    /// Creates a new map type
    pub fn new(key_type: DataType, value_type: DataType, value_contains_null: bool) -> Self {
        Self {
            type_name: "map".into(),
            key_type,
            value_type,
            value_contains_null,
        }
    }

    #[inline]
    /// Returns the key type of the map
    pub const fn key_type(&self) -> &DataType {
        &self.key_type
    }

    #[inline]
    /// Returns the value type of the map
    pub const fn value_type(&self) -> &DataType {
        &self.value_type
    }

    #[inline]
    /// Returns whether the map can contain null values
    pub const fn value_contains_null(&self) -> bool {
        self.value_contains_null
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
/// Primitive types supported by Delta
pub enum PrimitiveType {
    /// UTF-8 encoded string of characters
    String,
    /// i64: 8-byte signed integer. Range: -9223372036854775808 to 9223372036854775807
    Long,
    /// i32: 4-byte signed integer. Range: -2147483648 to 2147483647
    Integer,
    /// i16: 2-byte signed integer numbers. Range: -32768 to 32767
    Short,
    /// i8: 1-byte signed integer number. Range: -128 to 127
    Byte,
    /// f32: 4-byte single-precision floating-point numbers
    Float,
    /// f64: 8-byte double-precision floating-point numbers
    Double,
    /// bool: boolean values
    Boolean,
    /// Binary: uninterpreted binary data
    Binary,
    /// Date: Calendar date (year, month, day)
    Date,
    /// Microsecond precision timestamp, adjusted to UTC.
    Timestamp,
    // TODO: timestamp without timezone
    #[serde(
        serialize_with = "serialize_decimal",
        deserialize_with = "deserialize_decimal",
        untagged
    )]
    /// Decimal: arbitrary precision decimal numbers
    Decimal(u8, i8),
}

fn serialize_decimal<S: serde::Serializer>(
    precision: &u8,
    scale: &i8,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("decimal({},{})", precision, scale))
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<(u8, i8), D::Error>
where
    D: serde::Deserializer<'de>,
{
    let str_value = String::deserialize(deserializer)?;
    if !str_value.starts_with("decimal(") || !str_value.ends_with(')') {
        return Err(serde::de::Error::custom(format!(
            "Invalid decimal: {}",
            str_value
        )));
    }

    let mut parts = str_value[8..str_value.len() - 1].split(',');
    let precision = parts
        .next()
        .and_then(|part| part.trim().parse::<u8>().ok())
        .ok_or_else(|| {
            serde::de::Error::custom(format!("Invalid precision in decimal: {}", str_value))
        })?;
    let scale = parts
        .next()
        .and_then(|part| part.trim().parse::<i8>().ok())
        .ok_or_else(|| {
            serde::de::Error::custom(format!("Invalid scale in decimal: {}", str_value))
        })?;

    Ok((precision, scale))
}

impl Display for PrimitiveType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Integer => write!(f, "integer"),
            PrimitiveType::Short => write!(f, "short"),
            PrimitiveType::Byte => write!(f, "byte"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Binary => write!(f, "binary"),
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::Decimal(precision, scale) => {
                write!(f, "decimal({},{})", precision, scale)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged, rename_all = "camelCase")]
/// Top level delta tdatatypes
pub enum DataType {
    /// UTF-8 encoded string of characters
    Primitive(PrimitiveType),
    /// An array stores a variable length collection of items of some type.
    Array(Box<ArrayType>),
    /// A struct is used to represent both the top-level schema of the table as well
    /// as struct columns that contain nested columns.
    Struct(Box<StructType>),
    /// A map stores an arbitrary length collection of key-value pairs
    /// with a single keyType and a single valueType
    Map(Box<MapType>),
}

impl From<MapType> for DataType {
    fn from(map_type: MapType) -> Self {
        DataType::Map(Box::new(map_type))
    }
}

impl From<StructType> for DataType {
    fn from(struct_type: StructType) -> Self {
        DataType::Struct(Box::new(struct_type))
    }
}

impl From<ArrayType> for DataType {
    fn from(array_type: ArrayType) -> Self {
        DataType::Array(Box::new(array_type))
    }
}

#[allow(missing_docs)]
impl DataType {
    pub const STRING: Self = DataType::Primitive(PrimitiveType::String);
    pub const LONG: Self = DataType::Primitive(PrimitiveType::Long);
    pub const INTEGER: Self = DataType::Primitive(PrimitiveType::Integer);
    pub const SHORT: Self = DataType::Primitive(PrimitiveType::Short);
    pub const BYTE: Self = DataType::Primitive(PrimitiveType::Byte);
    pub const FLOAT: Self = DataType::Primitive(PrimitiveType::Float);
    pub const DOUBLE: Self = DataType::Primitive(PrimitiveType::Double);
    pub const BOOLEAN: Self = DataType::Primitive(PrimitiveType::Boolean);
    pub const BINARY: Self = DataType::Primitive(PrimitiveType::Binary);
    pub const DATE: Self = DataType::Primitive(PrimitiveType::Date);
    pub const TIMESTAMP: Self = DataType::Primitive(PrimitiveType::Timestamp);

    pub fn decimal(precision: u8, scale: i8) -> Self {
        DataType::Primitive(PrimitiveType::Decimal(precision, scale))
    }

    pub fn struct_type(fields: Vec<StructField>) -> Self {
        DataType::Struct(Box::new(StructType::new(fields)))
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Primitive(p) => write!(f, "{}", p),
            DataType::Array(a) => write!(f, "array<{}>", a.element_type),
            DataType::Struct(s) => {
                write!(f, "struct<")?;
                for (i, field) in s.fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Map(m) => write!(f, "map<{}, {}>", m.key_type, m.value_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_serde_data_types() {
        let data = r#"
        {
            "name": "a",
            "type": "integer",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(
            field.data_type,
            DataType::Primitive(PrimitiveType::Integer)
        ));

        let data = r#"
        {
            "name": "c",
            "type": {
                "type": "array",
                "elementType": "integer",
                "containsNull": false
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Array(_)));

        let data = r#"
        {
            "name": "e",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "d",
                            "type": "integer",
                            "nullable": false,
                            "metadata": {}
                        }
                    ]
                },
                "containsNull": true
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Array(_)));
        match field.data_type {
            DataType::Array(array) => assert!(matches!(array.element_type, DataType::Struct(_))),
            _ => unreachable!(),
        }

        let data = r#"
        {
            "name": "f",
            "type": {
                "type": "map",
                "keyType": "string",
                "valueType": "string",
                "valueContainsNull": true
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Map(_)));
    }

    #[test]
    fn test_roundtrip_decimal() {
        let data = r#"
        {
            "name": "a",
            "type": "decimal(10, 2)",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(
            field.data_type,
            DataType::Primitive(PrimitiveType::Decimal(10, 2))
        ));

        let json_str = serde_json::to_string(&field).unwrap();
        assert_eq!(
            json_str,
            r#"{"name":"a","type":"decimal(10,2)","nullable":false,"metadata":{}}"#
        );
    }

    #[test]
    fn test_field_metadata() {
        let data = r#"
        {
            "name": "e",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "d",
                            "type": "integer",
                            "nullable": false,
                            "metadata": {
                                "delta.columnMapping.id": 5,
                                "delta.columnMapping.physicalName": "col-a7f4159c-53be-4cb0-b81a-f7e5240cfc49"
                            }
                        }
                    ]
                },
                "containsNull": true
            },
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 4,
                "delta.columnMapping.physicalName": "col-5f422f40-de70-45b2-88ab-1d5c90e94db1"
            }
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();

        let col_id = field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .unwrap();
        assert!(matches!(col_id, MetadataValue::Number(num) if *num == 4));
        let physical_name = field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .unwrap();
        assert!(
            matches!(physical_name, MetadataValue::String(name) if *name == "col-5f422f40-de70-45b2-88ab-1d5c90e94db1")
        );
    }

    #[test]
    fn test_read_schemas() {
        let file = std::fs::File::open("./tests/serde/schema.json").unwrap();
        let schema: Result<StructType, _> = serde_json::from_reader(file);
        assert!(schema.is_ok());

        let file = std::fs::File::open("./tests/serde/checkpoint_schema.json").unwrap();
        let schema: Result<StructType, _> = serde_json::from_reader(file);
        assert!(schema.is_ok())
    }

    #[test]
    fn test_get_invariants() {
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [{"name": "x", "type": "string", "nullable": true, "metadata": {}}]
        }))
        .unwrap();
        let invariants = schema.get_invariants().unwrap();
        assert_eq!(invariants.len(), 0);

        let schema: StructType = serde_json::from_value(json!({
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

        let schema: StructType = serde_json::from_value(json!({
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

    /// <https://github.com/delta-io/delta-rs/issues/2152>
    #[test]
    fn test_identity_columns() {
        let buf = r#"{"type":"struct","fields":[{"name":"ID_D_DATE","type":"long","nullable":true,"metadata":{"delta.identity.start":1,"delta.identity.step":1,"delta.identity.allowExplicitInsert":false}},{"name":"TXT_DateKey","type":"string","nullable":true,"metadata":{}}]}"#;
        let _schema: StructType = serde_json::from_str(buf).expect("Failed to load");
    }
}
