//! Delta table schema

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, DecimalType, MapType, MetadataValue, PrimitiveType,
    StructField, StructType,
};
use serde_json::Value;
use uuid::Uuid;

use crate::kernel::error::Error;
use crate::schema::DataCheck;
use crate::table::GeneratedColumn;

/// Metadata key for column mapping physical name
pub const COLUMN_MAPPING_PHYSICAL_NAME_KEY: &str = "delta.columnMapping.physicalName";
/// Metadata key for column mapping column ID
pub const COLUMN_MAPPING_ID_KEY: &str = "delta.columnMapping.id";

/// Type alias for a top level schema
pub type Schema = StructType;
/// Schema reference type
pub type SchemaRef = Arc<StructType>;

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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Trait to add convenience functions to struct type
pub trait StructTypeExt {
    /// Get all invariants in the schemas
    fn get_invariants(&self) -> Result<Vec<Invariant>, Error>;

    /// Get all generated column expressions
    fn get_generated_columns(&self) -> Result<Vec<GeneratedColumn>, Error>;

    /// Add column mapping metadata to all fields in the schema.
    /// This generates unique physical names (UUIDs) and assigns sequential column IDs.
    /// Returns a new schema with column mapping metadata added to all fields.
    fn with_column_mapping_metadata(&self) -> Result<StructType, Error>;

    /// Add column mapping metadata to all fields in the schema, starting from the given column ID.
    /// This generates unique physical names (UUIDs) and assigns sequential column IDs.
    /// Returns a tuple of (new schema, max column ID used).
    fn with_column_mapping_metadata_from(
        &self,
        starting_column_id: i64,
    ) -> Result<(StructType, i64), Error>;

    /// Get the maximum column ID present in the schema's column mapping metadata.
    /// Returns None if no fields have column mapping IDs.
    fn get_max_column_id(&self) -> Option<i64>;

    /// Build a mapping from logical column names to physical column names
    /// based on the column mapping metadata in this schema.
    fn get_logical_to_physical_mapping(&self) -> HashMap<String, String>;

    /// Build a mapping from logical column names to column IDs
    /// based on the column mapping metadata in this schema.
    fn get_logical_to_id_mapping(&self) -> HashMap<String, i32>;

    /// Build both mappings (logical-to-physical and logical-to-id) in a single traversal.
    /// This is more efficient when both mappings are needed.
    fn get_column_mappings(&self) -> (HashMap<String, String>, HashMap<String, i32>);

    /// Add column mapping metadata only to fields that don't already have it.
    /// This is useful during schema evolution when new columns are added to a table
    /// that already has column mapping enabled.
    /// Returns a tuple of (new schema, max column ID used).
    fn with_column_mapping_metadata_for_new_fields(
        &self,
        starting_column_id: i64,
    ) -> Result<(StructType, i64), Error>;

    /// Copy column mapping metadata from another schema to this schema.
    /// For fields that exist in both schemas, copies the column mapping metadata
    /// (physicalName and id) from the source schema. Fields not found in the source
    /// schema are left unchanged.
    fn with_column_mapping_metadata_from_schema(
        &self,
        source_schema: &StructType,
    ) -> Result<StructType, Error>;
}

impl StructTypeExt for StructType {
    /// Get all get_generated_columns in the schemas
    fn get_generated_columns(&self) -> Result<Vec<GeneratedColumn>, Error> {
        let mut remaining_fields: Vec<(String, StructField)> = self
            .fields()
            .map(|field| (field.name.clone(), field.clone()))
            .collect();
        let mut generated_cols: Vec<GeneratedColumn> = Vec::new();

        while let Some((field_path, field)) = remaining_fields.pop() {
            if let Some(MetadataValue::String(generated_col_string)) = field
                .metadata
                .get(ColumnMetadataKey::GenerationExpression.as_ref())
            {
                generated_cols.push(GeneratedColumn::new(
                    &field_path,
                    generated_col_string,
                    field.data_type(),
                ));
            }
        }
        Ok(generated_cols)
    }

    /// Get all invariants in the schemas
    fn get_invariants(&self) -> Result<Vec<Invariant>, Error> {
        let mut remaining_fields: Vec<(String, StructField)> = self
            .fields()
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
                if let Value::Object(json) = json
                    && let Some(Value::Object(expr1)) = json.get("expression")
                    && let Some(Value::String(sql)) = expr1.get("expression")
                {
                    invariants.push(Invariant::new(&field_path, sql));
                }
            }
        }
        Ok(invariants)
    }

    /// Add column mapping metadata to all fields in the schema.
    /// This generates unique physical names (UUIDs) and assigns sequential column IDs starting from 1.
    fn with_column_mapping_metadata(&self) -> Result<StructType, Error> {
        self.with_column_mapping_metadata_from(1).map(|(schema, _)| schema)
    }

    /// Add column mapping metadata to all fields in the schema, starting from the given column ID.
    /// This generates unique physical names (UUIDs) and assigns sequential column IDs.
    /// Returns a tuple of (new schema, max column ID used).
    fn with_column_mapping_metadata_from(
        &self,
        starting_column_id: i64,
    ) -> Result<(StructType, i64), Error> {
        let mut column_id_counter = starting_column_id;

        fn add_metadata_to_field(
            field: &StructField,
            counter: &mut i64,
        ) -> Result<StructField, Error> {
            // Generate physical name and column ID
            let physical_name = format!("col-{}", Uuid::new_v4());
            let column_id = *counter;
            *counter += 1;

            // Build new metadata with column mapping info
            let mut metadata: Vec<(String, MetadataValue)> = field
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            metadata.push((
                COLUMN_MAPPING_ID_KEY.to_string(),
                MetadataValue::Number(column_id),
            ));
            metadata.push((
                COLUMN_MAPPING_PHYSICAL_NAME_KEY.to_string(),
                MetadataValue::String(physical_name),
            ));

            // Handle nested types recursively
            let new_data_type = match field.data_type() {
                DataType::Struct(inner) => {
                    let new_fields: Result<Vec<StructField>, Error> = inner
                        .fields()
                        .map(|f| add_metadata_to_field(f, counter))
                        .collect();
                    DataType::Struct(Box::new(
                        StructType::try_new(new_fields?).map_err(|e| Error::Generic(e.to_string()))?,
                    ))
                }
                DataType::Array(inner) => {
                    // For arrays, the element type might need metadata if it's a struct
                    if let DataType::Struct(elem_struct) = inner.element_type() {
                        let new_fields: Result<Vec<StructField>, Error> = elem_struct
                            .fields()
                            .map(|f| add_metadata_to_field(f, counter))
                            .collect();
                        DataType::Array(Box::new(ArrayType::new(
                            DataType::Struct(Box::new(
                                StructType::try_new(new_fields?)
                                    .map_err(|e| Error::Generic(e.to_string()))?,
                            )),
                            inner.contains_null(),
                        )))
                    } else {
                        field.data_type().clone()
                    }
                }
                DataType::Map(inner) => {
                    // For maps, value type might need metadata if it's a struct
                    let new_key_type = inner.key_type().clone();
                    let new_value_type = if let DataType::Struct(val_struct) = inner.value_type() {
                        let new_fields: Result<Vec<StructField>, Error> = val_struct
                            .fields()
                            .map(|f| add_metadata_to_field(f, counter))
                            .collect();
                        DataType::Struct(Box::new(
                            StructType::try_new(new_fields?)
                                .map_err(|e| Error::Generic(e.to_string()))?,
                        ))
                    } else {
                        inner.value_type().clone()
                    };
                    DataType::Map(Box::new(MapType::new(
                        new_key_type,
                        new_value_type,
                        inner.value_contains_null(),
                    )))
                }
                _ => field.data_type().clone(),
            };

            // Create new field with updated data type and metadata
            let new_field = if field.is_nullable() {
                StructField::nullable(field.name(), new_data_type)
            } else {
                StructField::not_null(field.name(), new_data_type)
            };

            Ok(new_field.with_metadata(metadata))
        }

        let new_fields: Result<Vec<StructField>, Error> = self
            .fields()
            .map(|f| add_metadata_to_field(f, &mut column_id_counter))
            .collect();

        let schema = StructType::try_new(new_fields?).map_err(|e| {
            Error::Generic(format!(
                "Failed to create schema with column mapping metadata: {}",
                e
            ))
        })?;

        // The max column ID used is counter - 1 (since counter is incremented after each assignment)
        let max_column_id = column_id_counter - 1;
        Ok((schema, max_column_id))
    }

    /// Get the maximum column ID present in the schema's column mapping metadata.
    /// Returns None if no fields have column mapping IDs.
    fn get_max_column_id(&self) -> Option<i64> {
        fn find_max_in_field(field: &StructField) -> Option<i64> {
            let mut max_id = None;

            // Check this field's column ID
            if let Some(MetadataValue::Number(id)) = field.metadata().get(COLUMN_MAPPING_ID_KEY) {
                max_id = Some(*id);
            }

            // Recursively check nested types
            match field.data_type() {
                DataType::Struct(inner) => {
                    for nested_field in inner.fields() {
                        if let Some(nested_max) = find_max_in_field(nested_field) {
                            max_id = Some(max_id.map_or(nested_max, |m: i64| m.max(nested_max)));
                        }
                    }
                }
                DataType::Array(inner) => {
                    if let DataType::Struct(elem_struct) = inner.element_type() {
                        for nested_field in elem_struct.fields() {
                            if let Some(nested_max) = find_max_in_field(nested_field) {
                                max_id = Some(max_id.map_or(nested_max, |m: i64| m.max(nested_max)));
                            }
                        }
                    }
                }
                DataType::Map(inner) => {
                    if let DataType::Struct(val_struct) = inner.value_type() {
                        for nested_field in val_struct.fields() {
                            if let Some(nested_max) = find_max_in_field(nested_field) {
                                max_id = Some(max_id.map_or(nested_max, |m: i64| m.max(nested_max)));
                            }
                        }
                    }
                }
                _ => {}
            }

            max_id
        }

        let mut max_id: Option<i64> = None;
        for field in self.fields() {
            if let Some(field_max) = find_max_in_field(field) {
                max_id = Some(max_id.map_or(field_max, |m: i64| m.max(field_max)));
            }
        }
        max_id
    }

    /// Build a mapping from logical column names to physical column names
    fn get_logical_to_physical_mapping(&self) -> HashMap<String, String> {
        self.get_column_mappings().0
    }

    /// Build a mapping from logical column names to column IDs
    fn get_logical_to_id_mapping(&self) -> HashMap<String, i32> {
        self.get_column_mappings().1
    }

    /// Build both mappings (logical-to-physical and logical-to-id) in a single traversal.
    /// This is more efficient when both mappings are needed.
    fn get_column_mappings(&self) -> (HashMap<String, String>, HashMap<String, i32>) {
        fn collect_mappings(
            schema: &StructType,
            physical_map: &mut HashMap<String, String>,
            id_map: &mut HashMap<String, i32>,
        ) {
            for field in schema.fields() {
                let logical_name = field.name().to_string();

                // Get physical name from metadata if present
                if let Some(MetadataValue::String(physical_name)) =
                    field.metadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
                    && &logical_name != physical_name
                {
                    physical_map.insert(logical_name.clone(), physical_name.clone());
                }

                // Get column ID from metadata if present
                if let Some(MetadataValue::Number(id)) =
                    field.metadata().get(COLUMN_MAPPING_ID_KEY)
                {
                    id_map.insert(logical_name.clone(), *id as i32);
                }

                // Recursively handle nested types that can contain structs
                match field.data_type() {
                    DataType::Struct(nested) => {
                        collect_mappings(nested.as_ref(), physical_map, id_map);
                    }
                    DataType::Array(inner) => {
                        if let DataType::Struct(elem_struct) = inner.element_type() {
                            collect_mappings(elem_struct.as_ref(), physical_map, id_map);
                        }
                    }
                    DataType::Map(inner) => {
                        if let DataType::Struct(val_struct) = inner.value_type() {
                            collect_mappings(val_struct.as_ref(), physical_map, id_map);
                        }
                    }
                    _ => {}
                }
            }
        }

        let mut physical_mappings = HashMap::new();
        let mut id_mappings = HashMap::new();
        collect_mappings(self, &mut physical_mappings, &mut id_mappings);
        (physical_mappings, id_mappings)
    }

    /// Add column mapping metadata only to fields that don't already have it.
    /// This is useful during schema evolution when new columns are added to a table
    /// that already has column mapping enabled.
    fn with_column_mapping_metadata_for_new_fields(
        &self,
        starting_column_id: i64,
    ) -> Result<(StructType, i64), Error> {
        let mut column_id_counter = starting_column_id;

        fn add_metadata_to_field_if_needed(
            field: &StructField,
            counter: &mut i64,
        ) -> Result<StructField, Error> {
            // Check if field already has column mapping metadata
            let has_column_id = field.metadata().contains_key(COLUMN_MAPPING_ID_KEY);
            let has_physical_name = field.metadata().contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY);

            // Build new metadata
            let mut metadata: Vec<(String, MetadataValue)> = field
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            // Only add column mapping metadata if it doesn't exist
            if !has_column_id {
                let column_id = *counter;
                *counter += 1;
                metadata.push((
                    COLUMN_MAPPING_ID_KEY.to_string(),
                    MetadataValue::Number(column_id),
                ));
            }

            if !has_physical_name {
                let physical_name = format!("col-{}", Uuid::new_v4());
                metadata.push((
                    COLUMN_MAPPING_PHYSICAL_NAME_KEY.to_string(),
                    MetadataValue::String(physical_name),
                ));
            }

            // Handle nested types recursively
            let new_data_type = match field.data_type() {
                DataType::Struct(inner) => {
                    let new_fields: Result<Vec<StructField>, Error> = inner
                        .fields()
                        .map(|f| add_metadata_to_field_if_needed(f, counter))
                        .collect();
                    DataType::Struct(Box::new(
                        StructType::try_new(new_fields?).map_err(|e| Error::Generic(e.to_string()))?,
                    ))
                }
                DataType::Array(inner) => {
                    if let DataType::Struct(elem_struct) = inner.element_type() {
                        let new_fields: Result<Vec<StructField>, Error> = elem_struct
                            .fields()
                            .map(|f| add_metadata_to_field_if_needed(f, counter))
                            .collect();
                        DataType::Array(Box::new(ArrayType::new(
                            DataType::Struct(Box::new(
                                StructType::try_new(new_fields?)
                                    .map_err(|e| Error::Generic(e.to_string()))?,
                            )),
                            inner.contains_null(),
                        )))
                    } else {
                        field.data_type().clone()
                    }
                }
                DataType::Map(inner) => {
                    let new_key_type = inner.key_type().clone();
                    let new_value_type = if let DataType::Struct(val_struct) = inner.value_type() {
                        let new_fields: Result<Vec<StructField>, Error> = val_struct
                            .fields()
                            .map(|f| add_metadata_to_field_if_needed(f, counter))
                            .collect();
                        DataType::Struct(Box::new(
                            StructType::try_new(new_fields?)
                                .map_err(|e| Error::Generic(e.to_string()))?,
                        ))
                    } else {
                        inner.value_type().clone()
                    };
                    DataType::Map(Box::new(MapType::new(
                        new_key_type,
                        new_value_type,
                        inner.value_contains_null(),
                    )))
                }
                _ => field.data_type().clone(),
            };

            // Create new field with updated data type and metadata
            let new_field = if field.is_nullable() {
                StructField::nullable(field.name(), new_data_type)
            } else {
                StructField::not_null(field.name(), new_data_type)
            };

            Ok(new_field.with_metadata(metadata))
        }

        let new_fields: Result<Vec<StructField>, Error> = self
            .fields()
            .map(|f| add_metadata_to_field_if_needed(f, &mut column_id_counter))
            .collect();

        let schema = StructType::try_new(new_fields?).map_err(|e| {
            Error::Generic(format!(
                "Failed to create schema with column mapping metadata: {}",
                e
            ))
        })?;

        // The max column ID used is counter - 1 (since counter is incremented after each assignment)
        // But only if any new IDs were assigned
        let max_column_id = if column_id_counter > starting_column_id {
            column_id_counter - 1
        } else {
            starting_column_id - 1 // No new IDs assigned, return starting - 1
        };
        Ok((schema, max_column_id))
    }

    /// Copy column mapping metadata from another schema to this schema.
    fn with_column_mapping_metadata_from_schema(
        &self,
        source_schema: &StructType,
    ) -> Result<StructType, Error> {
        fn copy_metadata_from_source(
            field: &StructField,
            source_schema: &StructType,
        ) -> Result<StructField, Error> {
            // Try to find the corresponding field in source schema
            let source_field = source_schema.fields().find(|f| f.name() == field.name());

            // Build metadata, copying column mapping info from source if found
            let mut metadata: Vec<(String, MetadataValue)> = field
                .metadata()
                .iter()
                .filter(|(k, _)| {
                    k.as_str() != COLUMN_MAPPING_ID_KEY
                        && k.as_str() != COLUMN_MAPPING_PHYSICAL_NAME_KEY
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            // Copy column mapping metadata from source if available
            if let Some(src) = source_field {
                if let Some(id) = src.metadata().get(COLUMN_MAPPING_ID_KEY) {
                    metadata.push((COLUMN_MAPPING_ID_KEY.to_string(), id.clone()));
                }
                if let Some(physical_name) = src.metadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
                    metadata.push((COLUMN_MAPPING_PHYSICAL_NAME_KEY.to_string(), physical_name.clone()));
                }
            }

            // Handle nested types recursively
            let new_data_type = match field.data_type() {
                DataType::Struct(inner) => {
                    // Find the corresponding source struct
                    let source_inner = source_field.and_then(|sf| {
                        if let DataType::Struct(s) = sf.data_type() {
                            Some(s.as_ref())
                        } else {
                            None
                        }
                    });
                    let new_fields: Result<Vec<StructField>, Error> = inner
                        .fields()
                        .map(|f| {
                            if let Some(src_inner) = source_inner {
                                copy_metadata_from_source(f, src_inner)
                            } else {
                                Ok(f.clone())
                            }
                        })
                        .collect();
                    DataType::Struct(Box::new(
                        StructType::try_new(new_fields?).map_err(|e| Error::Generic(e.to_string()))?,
                    ))
                }
                DataType::Array(inner) => {
                    if let DataType::Struct(elem_struct) = inner.element_type() {
                        let source_elem = source_field.and_then(|sf| {
                            if let DataType::Array(arr) = sf.data_type() {
                                if let DataType::Struct(s) = arr.element_type() {
                                    return Some(s.as_ref());
                                }
                            }
                            None
                        });
                        let new_fields: Result<Vec<StructField>, Error> = elem_struct
                            .fields()
                            .map(|f| {
                                if let Some(src_elem) = source_elem {
                                    copy_metadata_from_source(f, src_elem)
                                } else {
                                    Ok(f.clone())
                                }
                            })
                            .collect();
                        DataType::Array(Box::new(ArrayType::new(
                            DataType::Struct(Box::new(
                                StructType::try_new(new_fields?)
                                    .map_err(|e| Error::Generic(e.to_string()))?,
                            )),
                            inner.contains_null(),
                        )))
                    } else {
                        field.data_type().clone()
                    }
                }
                DataType::Map(inner) => {
                    let new_key_type = inner.key_type().clone();
                    let new_value_type = if let DataType::Struct(val_struct) = inner.value_type() {
                        let source_val = source_field.and_then(|sf| {
                            if let DataType::Map(map) = sf.data_type() {
                                if let DataType::Struct(s) = map.value_type() {
                                    return Some(s.as_ref());
                                }
                            }
                            None
                        });
                        let new_fields: Result<Vec<StructField>, Error> = val_struct
                            .fields()
                            .map(|f| {
                                if let Some(src_val) = source_val {
                                    copy_metadata_from_source(f, src_val)
                                } else {
                                    Ok(f.clone())
                                }
                            })
                            .collect();
                        DataType::Struct(Box::new(
                            StructType::try_new(new_fields?)
                                .map_err(|e| Error::Generic(e.to_string()))?,
                        ))
                    } else {
                        inner.value_type().clone()
                    };
                    DataType::Map(Box::new(MapType::new(
                        new_key_type,
                        new_value_type,
                        inner.value_contains_null(),
                    )))
                }
                _ => field.data_type().clone(),
            };

            // Create new field with updated data type and metadata
            let new_field = if field.is_nullable() {
                StructField::nullable(field.name(), new_data_type)
            } else {
                StructField::not_null(field.name(), new_data_type)
            };

            Ok(new_field.with_metadata(metadata))
        }

        let new_fields: Result<Vec<StructField>, Error> = self
            .fields()
            .map(|f| copy_metadata_from_source(f, source_schema))
            .collect();

        StructType::try_new(new_fields?).map_err(|e| {
            Error::Generic(format!(
                "Failed to create schema with copied column mapping metadata: {}",
                e
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_get_generated_columns() {
        let schema: StructType = serde_json::from_value(json!(
            {
                "type":"struct",
                "fields":[
                    {"name":"id","type":"integer","nullable":true,"metadata":{}},
                    {"name":"gc","type":"integer","nullable":true,"metadata":{}}]
            }
        ))
        .unwrap();
        let cols = schema.get_generated_columns().unwrap();
        assert_eq!(cols.len(), 0);

        let schema: StructType = serde_json::from_value(json!(
            {
                "type":"struct",
                "fields":[
                    {"name":"id","type":"integer","nullable":true,"metadata":{}},
                    {"name":"gc","type":"integer","nullable":true,"metadata":{"delta.generationExpression":"5"}}]
            }
        )).unwrap();
        let cols = schema.get_generated_columns().unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].data_type, DataType::INTEGER);
        assert_eq!(cols[0].validation_expr, "gc <=> 5");

        let schema: StructType = serde_json::from_value(json!(
            {
                "type":"struct",
                "fields":[
                    {"name":"id","type":"integer","nullable":true,"metadata":{}},
                    {"name":"gc","type":"integer","nullable":true,"metadata":{"delta.generationExpression":"5"}},
                    {"name":"id2","type":"integer","nullable":true,"metadata":{"delta.generationExpression":"id * 10"}},]
            }
        )).unwrap();
        let cols = schema.get_generated_columns().unwrap();
        assert_eq!(cols.len(), 2);
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

    #[test]
    fn test_column_mapping_metadata_flat_schema() {
        // Test adding column mapping metadata to a flat schema
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        }))
        .unwrap();

        let mapped_schema = schema.with_column_mapping_metadata().unwrap();

        // Verify each field has column mapping metadata
        for (i, field) in mapped_schema.fields().enumerate() {
            let metadata = field.metadata();
            assert!(
                metadata.contains_key(COLUMN_MAPPING_ID_KEY),
                "Field {} missing column ID",
                field.name()
            );
            assert!(
                metadata.contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY),
                "Field {} missing physical name",
                field.name()
            );

            // Column IDs should start at 1 and increment
            if let Some(MetadataValue::Number(id)) = metadata.get(COLUMN_MAPPING_ID_KEY) {
                assert_eq!(*id, (i + 1) as i64);
            }

            // Physical names should start with "col-"
            if let Some(MetadataValue::String(physical)) =
                metadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
            {
                assert!(physical.starts_with("col-"), "Physical name should start with 'col-'");
            }
        }
    }

    #[test]
    fn test_column_mapping_metadata_nested_struct() {
        // Test adding column mapping metadata to a schema with nested struct
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {
                    "name": "address",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "street", "type": "string", "nullable": true, "metadata": {}},
                            {"name": "city", "type": "string", "nullable": true, "metadata": {}}
                        ]
                    },
                    "nullable": true,
                    "metadata": {}
                }
            ]
        }))
        .unwrap();

        let mapped_schema = schema.with_column_mapping_metadata().unwrap();

        // Verify top-level fields have metadata
        assert_eq!(mapped_schema.fields().count(), 2);

        // Find the nested struct and verify its fields have metadata too
        let address_field = mapped_schema.fields().find(|f| f.name() == "address").unwrap();
        if let DataType::Struct(nested) = address_field.data_type() {
            for field in nested.fields() {
                assert!(
                    field.metadata().contains_key(COLUMN_MAPPING_ID_KEY),
                    "Nested field {} missing column ID",
                    field.name()
                );
                assert!(
                    field.metadata().contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY),
                    "Nested field {} missing physical name",
                    field.name()
                );
            }
        } else {
            panic!("Expected address to be a struct type");
        }
    }

    #[test]
    fn test_column_mapping_get_mappings() {
        // Test get_column_mappings() returns correct mappings
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": false,
                    "metadata": {
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "col-abc-123"
                    }
                },
                {
                    "name": "user name",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "delta.columnMapping.id": 2,
                        "delta.columnMapping.physicalName": "col-def-456"
                    }
                }
            ]
        }))
        .unwrap();

        let (physical_map, id_map) = schema.get_column_mappings();

        // Verify physical name mappings
        assert_eq!(physical_map.len(), 2);
        assert_eq!(physical_map.get("id"), Some(&"col-abc-123".to_string()));
        assert_eq!(physical_map.get("user name"), Some(&"col-def-456".to_string()));

        // Verify ID mappings
        assert_eq!(id_map.len(), 2);
        assert_eq!(id_map.get("id"), Some(&1));
        assert_eq!(id_map.get("user name"), Some(&2));
    }

    #[test]
    fn test_column_mapping_metadata_array_of_structs() {
        // Test adding column mapping metadata to array of structs
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {
                    "name": "items",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [
                                {"name": "name", "type": "string", "nullable": true, "metadata": {}},
                                {"name": "price", "type": "double", "nullable": true, "metadata": {}}
                            ]
                        },
                        "containsNull": true
                    },
                    "nullable": true,
                    "metadata": {}
                }
            ]
        }))
        .unwrap();

        let mapped_schema = schema.with_column_mapping_metadata().unwrap();

        // Find the array field and verify its element struct fields have metadata
        let items_field = mapped_schema.fields().find(|f| f.name() == "items").unwrap();
        if let DataType::Array(array_type) = items_field.data_type() {
            if let DataType::Struct(elem_struct) = array_type.element_type() {
                for field in elem_struct.fields() {
                    assert!(
                        field.metadata().contains_key(COLUMN_MAPPING_ID_KEY),
                        "Array element field {} missing column ID",
                        field.name()
                    );
                    assert!(
                        field.metadata().contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY),
                        "Array element field {} missing physical name",
                        field.name()
                    );
                }
            } else {
                panic!("Expected array element to be a struct type");
            }
        } else {
            panic!("Expected items to be an array type");
        }
    }

    #[test]
    fn test_with_column_mapping_metadata_from() {
        // Test starting column IDs from a specific value
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        }))
        .unwrap();

        let (mapped_schema, max_id) = schema.with_column_mapping_metadata_from(10).unwrap();

        // Verify max column ID
        assert_eq!(max_id, 11, "Max column ID should be 11 (10 + 2 fields - 1)");

        // Verify column IDs start from 10
        for (i, field) in mapped_schema.fields().enumerate() {
            if let Some(MetadataValue::Number(id)) = field.metadata().get(COLUMN_MAPPING_ID_KEY) {
                assert_eq!(*id, (10 + i) as i64, "Column ID should be {} for field {}", 10 + i, field.name());
            }
        }
    }

    #[test]
    fn test_get_max_column_id() {
        // Test getting max column ID from schema
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": false,
                    "metadata": {
                        "delta.columnMapping.id": 5,
                        "delta.columnMapping.physicalName": "col-abc"
                    }
                },
                {
                    "name": "name",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "delta.columnMapping.id": 10,
                        "delta.columnMapping.physicalName": "col-def"
                    }
                }
            ]
        }))
        .unwrap();

        let max_id = schema.get_max_column_id();
        assert_eq!(max_id, Some(10), "Max column ID should be 10");
    }

    #[test]
    fn test_get_max_column_id_no_metadata() {
        // Test getting max column ID when no column mapping metadata exists
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        }))
        .unwrap();

        let max_id = schema.get_max_column_id();
        assert_eq!(max_id, None, "Max column ID should be None when no metadata exists");
    }

    #[test]
    fn test_with_column_mapping_metadata_for_new_fields() {
        // Create a schema with some fields already having column mapping metadata
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {
                    "name": "existing",
                    "type": "integer",
                    "nullable": false,
                    "metadata": {
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "col-existing"
                    }
                },
                {
                    "name": "new_field",
                    "type": "string",
                    "nullable": true,
                    "metadata": {}
                }
            ]
        }))
        .unwrap();

        let (mapped_schema, max_id) = schema.with_column_mapping_metadata_for_new_fields(5).unwrap();

        // Verify max column ID
        assert_eq!(max_id, 5, "Max column ID should be 5");

        // Verify existing field keeps its original ID
        let existing_field = mapped_schema.fields().find(|f| f.name() == "existing").unwrap();
        if let Some(MetadataValue::Number(id)) = existing_field.metadata().get(COLUMN_MAPPING_ID_KEY) {
            assert_eq!(*id, 1, "Existing field should keep ID 1");
        }

        // Verify new field gets the new ID
        let new_field = mapped_schema.fields().find(|f| f.name() == "new_field").unwrap();
        if let Some(MetadataValue::Number(id)) = new_field.metadata().get(COLUMN_MAPPING_ID_KEY) {
            assert_eq!(*id, 5, "New field should get ID 5");
        }
        assert!(
            new_field.metadata().contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY),
            "New field should have physical name"
        );
    }

    #[test]
    fn test_with_column_mapping_metadata_from_schema() {
        // Source schema with column mapping metadata
        let source_schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": false,
                    "metadata": {
                        "delta.columnMapping.id": 1,
                        "delta.columnMapping.physicalName": "col-id-123"
                    }
                },
                {
                    "name": "name",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "delta.columnMapping.id": 2,
                        "delta.columnMapping.physicalName": "col-name-456"
                    }
                }
            ]
        }))
        .unwrap();

        // Target schema without column mapping metadata (simulating Arrow schema conversion)
        let target_schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}},
                {"name": "new_field", "type": "double", "nullable": true, "metadata": {}}
            ]
        }))
        .unwrap();

        let result = target_schema.with_column_mapping_metadata_from_schema(&source_schema).unwrap();

        // Verify id field got metadata from source
        let id_field = result.fields().find(|f| f.name() == "id").unwrap();
        if let Some(MetadataValue::Number(id)) = id_field.metadata().get(COLUMN_MAPPING_ID_KEY) {
            assert_eq!(*id, 1, "id field should have ID 1 from source");
        } else {
            panic!("id field should have column mapping ID");
        }
        if let Some(MetadataValue::String(pn)) = id_field.metadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
            assert_eq!(pn, "col-id-123", "id field should have physical name from source");
        }

        // Verify name field got metadata from source
        let name_field = result.fields().find(|f| f.name() == "name").unwrap();
        if let Some(MetadataValue::Number(id)) = name_field.metadata().get(COLUMN_MAPPING_ID_KEY) {
            assert_eq!(*id, 2, "name field should have ID 2 from source");
        }

        // Verify new_field has no column mapping metadata (it wasn't in source)
        let new_field = result.fields().find(|f| f.name() == "new_field").unwrap();
        assert!(
            !new_field.metadata().contains_key(COLUMN_MAPPING_ID_KEY),
            "new_field should not have column mapping ID"
        );
        assert!(
            !new_field.metadata().contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY),
            "new_field should not have column mapping physical name"
        );
    }
}
