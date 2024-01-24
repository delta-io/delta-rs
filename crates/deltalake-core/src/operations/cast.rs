//! Provide common cast functionality for callers
//!
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{DataType, Fields, SchemaRef as ArrowSchemaRef};

use std::sync::Arc;

use crate::DeltaResult;

fn cast_struct(
    struct_array: &StructArray,
    fields: &Fields,
    cast_options: &CastOptions,
) -> Result<Vec<Arc<(dyn Array)>>, arrow_schema::ArrowError> {
    fields
        .iter()
        .map(|field| {
            let col_opt = struct_array.column_by_name(field.name());
            if col_opt.is_none() {
                return Err(arrow_schema::ArrowError::SchemaError(format!("Missing column {}", field.name())));
            }

            let col = col_opt.unwrap();
            if let (DataType::Struct(_), DataType::Struct(child_fields)) =
                (col.data_type(), field.data_type())
            {
                let child_struct = StructArray::from(col.into_data());
                let s = cast_struct(&child_struct, child_fields, cast_options)?;
                Ok(Arc::new(StructArray::new(
                    child_fields.clone(),
                    s,
                    child_struct.nulls().map(ToOwned::to_owned),
                )) as ArrayRef)
            } else if is_cast_required(col.data_type(), field.data_type()) {
                cast_with_options(col, field.data_type(), cast_options)
            } else {
                Ok(col.clone())
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

fn is_cast_required(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::List(a_item), DataType::List(b_item)) => {
            // If list item name is not the default('item') the list must be casted
            !a.equals_datatype(b) || a_item.name() != b_item.name()
        }
        (_, _) => !a.equals_datatype(b),
    }
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

    let s = StructArray::new(
        batch.schema().as_ref().to_owned().fields,
        batch.columns().to_owned(),
        None,
    );

    let columns = cast_struct(&s, target_schema.fields(), &cast_options)?;
    Ok(RecordBatch::try_new(target_schema, columns)?)
}

#[cfg(test)]
mod tests {
    use crate::operations::cast::{cast_record_batch, is_cast_required};
    use arrow::array::ArrayData;
    use arrow_array::{Array, ArrayRef, ListArray, RecordBatch};
    use arrow_buffer::Buffer;
    use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
    use std::sync::Arc;

    #[test]
    fn test_cast_record_batch_with_list_non_default_item() {
        let array = Arc::new(make_list_array()) as ArrayRef;
        let source_schema = Schema::new(vec![Field::new(
            "list_column",
            array.data_type().clone(),
            false,
        )]);
        let record_batch = RecordBatch::try_new(Arc::new(source_schema), vec![array]).unwrap();

        let fields = Fields::from(vec![Field::new_list(
            "list_column",
            Field::new("item", DataType::Int8, false),
            false,
        )]);
        let target_schema = Arc::new(Schema::new(fields)) as SchemaRef;

        let result = cast_record_batch(&record_batch, target_schema, false);

        let schema = result.unwrap().schema();
        let field = schema.column_with_name("list_column").unwrap().1;
        if let DataType::List(list_item) = field.data_type() {
            assert_eq!(list_item.name(), "item");
        } else {
            panic!("Not a list");
        }
    }

    fn make_list_array() -> ListArray {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);

        let list_data_type = DataType::List(Arc::new(Field::new("element", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        ListArray::from(list_data)
    }

    #[test]
    fn test_is_cast_required_with_list() {
        let field1 = DataType::List(FieldRef::from(Field::new("item", DataType::Int32, false)));
        let field2 = DataType::List(FieldRef::from(Field::new("item", DataType::Int32, false)));

        assert!(!is_cast_required(&field1, &field2));
    }

    #[test]
    fn test_is_cast_required_with_list_non_default_item() {
        let field1 = DataType::List(FieldRef::from(Field::new("item", DataType::Int32, false)));
        let field2 = DataType::List(FieldRef::from(Field::new(
            "element",
            DataType::Int32,
            false,
        )));

        assert!(is_cast_required(&field1, &field2));
    }
}
