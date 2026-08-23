use std::collections::HashMap;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::{DataType, PrimitiveType, StructField, StructType};

mod actions;
mod data;

pub use actions::*;
pub use data::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Per-file statistics fixture mirroring the `stats` JSON written into `Add` actions.
#[serde(rename_all = "camelCase")]
pub struct FileStats {
    /// Number of records in the file.
    pub num_records: i64,
    /// Per-column null counts.
    pub null_count: HashMap<String, Value>,
    /// Per-column minimum values.
    pub min_values: HashMap<String, Value>,
    /// Per-column maximum values.
    pub max_values: HashMap<String, Value>,
}

impl FileStats {
    /// Create empty statistics for a file with `num_records` rows.
    pub fn new(num_records: i64) -> Self {
        Self {
            num_records,
            null_count: HashMap::new(),
            min_values: HashMap::new(),
            max_values: HashMap::new(),
        }
    }
}

/// Collection of reusable test schemas.
pub struct TestSchemas;

impl TestSchemas {
    /// A simple flat schema with  string and integer columns.
    ///
    /// ### Columns
    /// - id: string
    /// - value: integer
    /// - modified: string
    pub fn simple() -> &'static StructType {
        static SIMPLE: LazyLock<StructType> = LazyLock::new(|| {
            StructType::try_new(vec![
                StructField::new(
                    "id".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
                StructField::new(
                    "value".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    true,
                ),
                StructField::new(
                    "modified".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .expect("Failed to construct StructType")
        });

        &SIMPLE
    }
}
