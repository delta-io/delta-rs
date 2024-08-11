use std::collections::HashMap;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::{DataType, PrimitiveType, StructField, StructType};

mod actions;
mod data;

pub use actions::*;
pub use data::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStats {
    pub num_records: i64,
    pub null_count: HashMap<String, Value>,
    pub min_values: HashMap<String, Value>,
    pub max_values: HashMap<String, Value>,
}

impl FileStats {
    pub fn new(num_records: i64) -> Self {
        Self {
            num_records,
            null_count: HashMap::new(),
            min_values: HashMap::new(),
            max_values: HashMap::new(),
        }
    }
}

pub struct TestSchemas;

impl TestSchemas {
    /// A simple flat schema with  string and integer columns.
    ///
    /// ### Columns
    /// - id: string
    /// - value: integer
    /// - modified: string
    pub fn simple() -> &'static StructType {
        lazy_static! {
            static ref _simple: StructType = StructType::new(vec![
                StructField::new(
                    "id".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true
                ),
                StructField::new(
                    "value".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    true
                ),
                StructField::new(
                    "modified".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true
                ),
            ]);
        }
        &_simple
    }
}
