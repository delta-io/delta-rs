use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::kernel::schema::cast_record_batch;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{ColumnStatistics, Result, not_impl_err};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use delta_kernel::table_features::ColumnMappingMode;

/// A Schema Adapter Factory which provides casting record batches from parquet to meet
/// delta lake conventions.
#[derive(Debug)]
pub(crate) struct DeltaSchemaAdapterFactory {
    pub column_mapping_mode: ColumnMappingMode,
}

impl SchemaAdapterFactory for DeltaSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DeltaSchemaAdapter {
            projected_table_schema,
            table_schema,
            column_mapping_mode: self.column_mapping_mode,
        })
    }
}

pub(crate) struct DeltaSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by
    /// the mapping.
    projected_table_schema: SchemaRef,
    /// Schema for the table
    table_schema: SchemaRef,
    column_mapping_mode: ColumnMappingMode,
}

impl SchemaAdapter for DeltaSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);

        let physical_key_name = match self.column_mapping_mode {
            ColumnMappingMode::None => return Some(file_schema.fields.find(field.name())?.0),
            ColumnMappingMode::Name => "delta.columnMapping.physicalName",
            ColumnMappingMode::Id => "delta.columnMapping.id",
        };

        field
            .metadata()
            .get(physical_key_name)
            .and_then(|physical_name| file_schema.fields.find(physical_name.as_str()))
            .map(|(field_id, _)| field_id)
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(self.projected_table_schema.fields().len());
        let mut logical_fields = Vec::new();

        if self.column_mapping_mode == ColumnMappingMode::None {
            for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
                if self
                    .projected_table_schema
                    .fields
                    .find(file_field.name())
                    .is_some()
                {
                    projection.push(file_idx)
                }
            }
        } else {
            for table_field in self.projected_table_schema.fields().iter() {
                let file_idx = self
                    .resolve_column(table_field, file_schema)
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "Column '{}' not found in file schema",
                            table_field.name(),
                        ))
                    })?;

                projection.push(file_idx);
                logical_fields.push(self.strip_metadata(table_field));
            }
        }

        let logical_schema = if self.column_mapping_mode == ColumnMappingMode::None {
            self.projected_table_schema.clone()
        } else {
            let fields: Vec<_> = logical_fields.iter().map(|f| (**f).clone()).collect();
            Arc::new(Schema::new(fields))
        };

        Ok((Arc::new(SchemaMapping { logical_schema }), projection))
    }
}

impl DeltaSchemaAdapter {
    fn resolve_column(&self, table_field: &Field, file_schema: &Schema) -> Option<usize> {
        if let Some((file_idx, _)) = file_schema.fields.find(table_field.name()) {
            return Some(file_idx);
        }

        let physical_name = self.get_physical_name(table_field)?;
        file_schema
            .fields
            .find(physical_name.as_str())
            .map(|(idx, _)| idx)
    }

    fn get_physical_name(&self, table_field: &Field) -> Option<String> {
        let key = match self.column_mapping_mode {
            ColumnMappingMode::None => return None,
            ColumnMappingMode::Name => "delta.columnMapping.physicalName",
            ColumnMappingMode::Id => "delta.columnMapping.id",
        };

        if let Some(physical_name) = table_field.metadata().get(key) {
            return Some(physical_name.clone());
        }

        None
    }

    fn strip_metadata(&self, field: &Field) -> Arc<Field> {
        let stripped_metadata: HashMap<String, String> = field
            .metadata()
            .iter()
            .filter(|(k, _)| !k.starts_with("delta.columnMapping"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Arc::new(field.clone().with_metadata(stripped_metadata))
    }
}

#[derive(Debug)]
pub(crate) struct SchemaMapping {
    logical_schema: SchemaRef,
}

impl SchemaMapper for SchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // Cast individual columns to match the target schema types
        // This handles type differences like timestamp precision mismatches
        let batch_columns = batch.columns().to_vec();
        let mut casted_columns = Vec::with_capacity(batch_columns.len());

        for (col, target_field) in batch_columns
            .iter()
            .zip(self.logical_schema.fields().iter())
        {
            if col.data_type() == target_field.data_type() {
                // Types match, use column as-is
                casted_columns.push(col.clone());
            } else {
                // Types differ, cast the column
                match cast_with_options(col, target_field.data_type(), &CastOptions::default()) {
                    Ok(casted) => casted_columns.push(casted),
                    Err(e) => {
                        return Err(datafusion::error::DataFusionError::Internal(format!(
                            "Failed to cast column '{}' from {} to {}: {}",
                            target_field.name(),
                            col.data_type(),
                            target_field.data_type(),
                            e
                        )))
                    }
                }
            }
        }

        RecordBatch::try_new_with_options(
            self.logical_schema.clone(),
            casted_columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        not_impl_err!("Mapping column statistics is not implemented for DeltaSchemaAdapter")
    }
}
