use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::kernel::schema::{COLUMN_MAPPING_PHYSICAL_NAME_KEY, cast_record_batch};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{ColumnStatistics, Result, not_impl_err};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};

/// A Schema Adapter Factory which provides casting record batches from parquet to meet
/// delta lake conventions.
#[derive(Debug)]
pub(crate) struct DeltaSchemaAdapterFactory {}

impl SchemaAdapterFactory for DeltaSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DeltaSchemaAdapter {
            projected_table_schema,
            table_schema,
        })
    }
}

pub(crate) struct DeltaSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by
    /// the mapping.
    projected_table_schema: SchemaRef,
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter for DeltaSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        // First try to find by physical name (for column mapping tables)
        if let Some(physical_name) = field.metadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
            if let Some((idx, _)) = file_schema.fields.find(physical_name) {
                return Some(idx);
            }
        }
        // Fall back to logical name
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut physical_to_logical: HashMap<String, String> = HashMap::new();

        // Build lookup maps for O(1) access instead of O(m) linear search per field
        // Maps: physical_name -> (logical_name, field) and logical_name -> field
        let mut physical_name_lookup: HashMap<&str, (&str, &Arc<Field>)> = HashMap::new();
        let mut logical_name_lookup: HashMap<&str, &Arc<Field>> = HashMap::new();

        for field in self.projected_table_schema.fields().iter() {
            if let Some(physical_name) = field.metadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
                physical_name_lookup.insert(physical_name.as_str(), (field.name(), field));
            }
            logical_name_lookup.insert(field.name(), field);
        }

        // Now iterate through file fields with O(1) lookups
        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            let file_field_name = file_field.name();

            // Check physical name first (for column mapping tables), then logical name
            let matched_field = physical_name_lookup
                .get(file_field_name.as_str())
                .map(|(logical, field)| (*logical, *field))
                .or_else(|| {
                    logical_name_lookup
                        .get(file_field_name.as_str())
                        .map(|field| (field.name().as_str(), *field))
                });

            if let Some((logical_name, _field)) = matched_field {
                projection.push(file_idx);
                // Record mapping if physical differs from logical
                if file_field_name != logical_name {
                    physical_to_logical
                        .insert(file_field_name.to_string(), logical_name.to_string());
                }
            }
        }

        Ok((
            Arc::new(SchemaMapping {
                projected_schema: self.projected_table_schema.clone(),
                physical_to_logical,
            }),
            projection,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct SchemaMapping {
    projected_schema: SchemaRef,
    /// Mapping from physical column names (in parquet files) to logical column names (in table schema).
    /// Only contains entries for columns where the physical name differs from the logical name.
    physical_to_logical: HashMap<String, String>,
}

impl SchemaMapper for SchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // If there are column mappings, rename physical columns to logical names before casting
        let batch = if !self.physical_to_logical.is_empty() {
            let schema = batch.schema();
            let new_fields: Vec<_> = schema
                .fields()
                .iter()
                .map(|field| {
                    if let Some(logical_name) = self.physical_to_logical.get(field.name()) {
                        Arc::new(Field::new(
                            logical_name,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ))
                    } else {
                        field.clone()
                    }
                })
                .collect();
            let new_schema = Arc::new(Schema::new(new_fields));
            RecordBatch::try_new(new_schema, batch.columns().to_vec())?
        } else {
            batch
        };
        let record_batch = cast_record_batch(&batch, self.projected_schema.clone(), false, true)?;
        Ok(record_batch)
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        not_impl_err!("Mapping column statistics is not implemented for DeltaSchemaAdapter")
    }
}
