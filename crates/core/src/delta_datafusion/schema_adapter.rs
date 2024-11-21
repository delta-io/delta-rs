use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};

use crate::operations::cast::cast_record_batch;

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
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if self.table_schema.fields().find(file_field.name()).is_some() {
                projection.push(file_idx);
            }
        }

        Ok((
            Arc::new(SchemaMapping {
                projected_schema: self.projected_table_schema.clone(),
                table_schema: self.table_schema.clone(),
            }),
            projection,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct SchemaMapping {
    projected_schema: SchemaRef,
    table_schema: SchemaRef,
}

impl SchemaMapper for SchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let record_batch = cast_record_batch(&batch, self.projected_schema.clone(), false, true)?;
        Ok(record_batch)
    }

    fn map_partial_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let partial_table_schema = Arc::new(Schema::new(
            batch
                .schema()
                .fields()
                .iter()
                .filter_map(|batch_field| {
                    self.table_schema.field_with_name(batch_field.name()).ok()
                })
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let record_batch = cast_record_batch(&batch, partial_table_schema, false, true)?;
        Ok(record_batch)
    }
}
