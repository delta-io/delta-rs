//! Provide common cast functionality for callers
//!
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{DataType, Fields, SchemaRef as ArrowSchemaRef};

use std::sync::Arc;

use crate::DeltaResult;

fn cast_record_batch_columns(
    batch: &RecordBatch,
    fields: &Fields,
    cast_options: &CastOptions,
) -> Result<Vec<Arc<(dyn Array)>>, arrow_schema::ArrowError> {
    fields
        .iter()
        .map(|f| {
            let col = batch.column_by_name(f.name()).unwrap();
            if let (DataType::Struct(_), DataType::Struct(child_fields)) =
                (col.data_type(), f.data_type())
            {
                let child_batch = RecordBatch::from(StructArray::from(col.into_data()));
                let child_columns =
                    cast_record_batch_columns(&child_batch, child_fields, cast_options)?;
                Ok(Arc::new(StructArray::new(
                    child_fields.clone(),
                    child_columns.clone(),
                    None,
                )) as ArrayRef)
            } else if !col.data_type().equals_datatype(f.data_type()) {
                cast_with_options(col, f.data_type(), cast_options)
            } else {
                Ok(col.clone())
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Cast recordbatch to a new target_schema, by casting each column array
pub fn cast_record_batch(
    batch: &RecordBatch,
    target_schema: ArrowSchemaRef,
    safe: bool,
) -> DeltaResult<RecordBatch> {
    let cast_options = CastOptions {
        safe,
        ..Default::default()
    };

    let columns = cast_record_batch_columns(batch, target_schema.fields(), &cast_options)?;
    Ok(RecordBatch::try_new(target_schema, columns)?)
}
