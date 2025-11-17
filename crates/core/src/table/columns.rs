//! Constraints and generated column mappings
use serde::{Deserialize, Serialize};

use crate::kernel::DataType;
use crate::table::DataCheck;
use std::any::Any;

/// A constraint in a check constraint
#[derive(Eq, PartialEq, Debug, Default, Clone, Serialize, Deserialize)]
pub struct Constraint {
    /// The full path to the field.
    pub name: String,
    /// The SQL string that must always evaluate to true.
    pub expr: String,
}

impl Constraint {
    /// Create a new invariant
    pub fn new(field_name: &str, invariant_sql: &str) -> Self {
        Self {
            name: field_name.to_string(),
            expr: invariant_sql.to_string(),
        }
    }
}

impl DataCheck for Constraint {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_expression(&self) -> &str {
        &self.expr
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A generated column
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct GeneratedColumn {
    /// The full path to the field.
    pub name: String,
    /// The SQL string that generate the column value.
    pub generation_expr: String,
    /// The SQL string that must always evaluate to true.
    pub validation_expr: String,
    /// Data Type
    pub data_type: DataType,
}

impl GeneratedColumn {
    /// Create a new invariant
    pub fn new(field_name: &str, sql_generation: &str, data_type: &DataType) -> Self {
        Self {
            name: field_name.to_string(),
            generation_expr: sql_generation.to_string(),
            validation_expr: format!("{field_name} <=> {sql_generation}"), // update to
            data_type: data_type.clone(),
        }
    }

    pub fn get_generation_expression(&self) -> &str {
        &self.generation_expr
    }
}

impl DataCheck for GeneratedColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_expression(&self) -> &str {
        &self.validation_expr
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
