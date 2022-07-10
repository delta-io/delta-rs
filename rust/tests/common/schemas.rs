use deltalake::{Schema, SchemaDataType, SchemaField};
use std::collections::HashMap;

/// Basic schema
pub fn get_xy_date_schema() -> Schema {
    return Schema::new(vec![
        SchemaField::new(
            "x".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "y".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "date".to_owned(),
            SchemaDataType::primitive("string".to_owned()),
            false,
            HashMap::new(),
        ),
    ]);
}

/// Schema that contains a column prefiexed with _
pub fn get_vacuum_underscore_schema() -> Schema {
    return Schema::new(vec![
        SchemaField::new(
            "x".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "y".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "_date".to_owned(),
            SchemaDataType::primitive("string".to_owned()),
            false,
            HashMap::new(),
        ),
    ]);
}
