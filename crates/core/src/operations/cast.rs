//! Provide common cast functionality for callers
//!
use crate::kernel::{
    ArrayType, DataType as DeltaDataType, MapType, MetadataValue, StructField, StructType,
};
use arrow_array::{new_null_array, Array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{ArrowError, DataType, Fields, SchemaRef as ArrowSchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

use crate::DeltaResult;

fn try_merge_metadata(
    left: &mut HashMap<String, MetadataValue>,
    right: &HashMap<String, MetadataValue>,
) -> Result<(), ArrowError> {
    for (k, v) in right {
        if let Some(vl) = left.get(k) {
            if vl != v {
                return Err(ArrowError::SchemaError(format!(
                    "Cannot merge metadata with different values for key {}",
                    k
                )));
            }
        } else {
            left.insert(k.clone(), v.clone());
        }
    }
    Ok(())
}

pub(crate) fn merge_struct(
    left: &StructType,
    right: &StructType,
) -> Result<StructType, ArrowError> {
    let mut errors = Vec::new();
    let merged_fields: Result<Vec<StructField>, ArrowError> = left
        .fields()
        .map(|field| {
            let right_field = right.field(field.name());
            if let Some(right_field) = right_field {
                let type_or_not = merge_type(field.data_type(), right_field.data_type());
                match type_or_not {
                    Err(e) => {
                        errors.push(e.to_string());
                        Err(e)
                    }
                    Ok(f) => {
                        let mut new_field = StructField::new(
                            field.name(),
                            f,
                            field.is_nullable() || right_field.is_nullable(),
                        );

                        new_field.metadata = field.metadata.clone();
                        try_merge_metadata(&mut new_field.metadata, &right_field.metadata)?;
                        Ok(new_field)
                    }
                }
            } else {
                Ok(field.clone())
            }
        })
        .collect();
    match merged_fields {
        Ok(mut fields) => {
            for field in right.fields() {
                if !left.field(field.name()).is_some() {
                    fields.push(field.clone());
                }
            }

            Ok(StructType::new(fields))
        }
        Err(e) => {
            errors.push(e.to_string());
            Err(ArrowError::SchemaError(errors.join("\n")))
        }
    }
}

pub(crate) fn merge_type(
    left: &DeltaDataType,
    right: &DeltaDataType,
) -> Result<DeltaDataType, ArrowError> {
    if left == right {
        return Ok(left.clone());
    }
    match (left, right) {
        (DeltaDataType::Array(a), DeltaDataType::Array(b)) => {
            let merged = merge_type(&a.element_type, &b.element_type)?;
            Ok(DeltaDataType::Array(Box::new(ArrayType::new(
                merged,
                a.contains_null() || b.contains_null(),
            ))))
        }
        (DeltaDataType::Map(a), DeltaDataType::Map(b)) => {
            let merged_key = merge_type(&a.key_type, &b.key_type)?;
            let merged_value = merge_type(&a.value_type, &b.value_type)?;
            Ok(DeltaDataType::Map(Box::new(MapType::new(
                merged_key,
                merged_value,
                a.value_contains_null() || b.value_contains_null(),
            ))))
        }
        (DeltaDataType::Struct(a), DeltaDataType::Struct(b)) => {
            let merged = merge_struct(a, b)?;
            Ok(DeltaDataType::Struct(Box::new(merged)))
        }
        (a, b) => Err(ArrowError::SchemaError(format!(
            "Cannot merge types {} and {}",
            a, b
        ))),
    }
}

pub(crate) fn merge_schema(
    left: ArrowSchemaRef,
    right: ArrowSchemaRef,
) -> Result<ArrowSchemaRef, ArrowError> {
    let left_delta: StructType = left.try_into()?;
    let right_delta: StructType = right.try_into()?;
    let merged: StructType = merge_struct(&left_delta, &right_delta)?;
    Ok(Arc::new((&merged).try_into()?))
}

fn cast_struct(
    struct_array: &StructArray,
    fields: &Fields,
    cast_options: &CastOptions,
    add_missing: bool,
) -> Result<Vec<Arc<(dyn Array)>>, arrow_schema::ArrowError> {
    fields
        .iter()
        .map(|field| {
            let col_or_not = struct_array.column_by_name(field.name());
            match col_or_not {
                None => match add_missing {
                    true => Ok(new_null_array(field.data_type(), struct_array.len())),
                    false => Err(arrow_schema::ArrowError::SchemaError(format!(
                        "Could not find column {0}",
                        field.name()
                    ))),
                },
                Some(col) => {
                    if let (DataType::Struct(_), DataType::Struct(child_fields)) =
                        (col.data_type(), field.data_type())
                    {
                        let child_struct = StructArray::from(col.into_data());
                        let s =
                            cast_struct(&child_struct, child_fields, cast_options, add_missing)?;
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
                }
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
    add_missing: bool,
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
    let columns = cast_struct(&s, target_schema.fields(), &cast_options, add_missing)?;
    Ok(RecordBatch::try_new(target_schema, columns)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::ArrayData;
    use arrow_array::{Array, ArrayRef, ListArray, RecordBatch};
    use arrow_buffer::Buffer;
    use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
    use itertools::Itertools;

    use crate::kernel::{
        ArrayType as DeltaArrayType, DataType as DeltaDataType, StructField as DeltaStructField,
        StructType as DeltaStructType,
    };
    use crate::operations::cast::MetadataValue;
    use crate::operations::cast::{cast_record_batch, is_cast_required};

    #[test]
    fn test_merge_schema_with_dict() {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            DataType::LargeUtf8,
            true,
        )]));

        let result = super::merge_schema(left_schema, right_schema).unwrap();
        assert_eq!(result.fields().len(), 1);
        let delta_type: DeltaDataType = result.fields()[0].data_type().try_into().unwrap();
        assert_eq!(delta_type, DeltaDataType::STRING);
        assert!(result.fields()[0].is_nullable());
    }

    #[test]
    fn test_merge_schema_with_meta() {
        let mut left_meta = HashMap::new();
        left_meta.insert("a".to_string(), "a1".to_string());
        let left_schema = DeltaStructType::new(vec![DeltaStructField::new(
            "f",
            DeltaDataType::STRING,
            false,
        )
        .with_metadata(left_meta)]);
        let mut right_meta = HashMap::new();
        right_meta.insert("b".to_string(), "b2".to_string());
        let right_schema = DeltaStructType::new(vec![DeltaStructField::new(
            "f",
            DeltaDataType::STRING,
            true,
        )
        .with_metadata(right_meta)]);

        let result = super::merge_struct(&left_schema, &right_schema).unwrap();
        let fields = result.fields().collect_vec();
        assert_eq!(fields.len(), 1);
        let delta_type = fields[0].data_type();
        assert_eq!(delta_type, &DeltaDataType::STRING);
        let mut expected_meta = HashMap::new();
        expected_meta.insert("a".to_string(), MetadataValue::String("a1".to_string()));
        expected_meta.insert("b".to_string(), MetadataValue::String("b2".to_string()));
        assert_eq!(fields[0].metadata(), &expected_meta);
    }

    #[test]
    fn test_merge_schema_with_nested() {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        )]));
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, false))),
            true,
        )]));

        let result = super::merge_schema(left_schema, right_schema).unwrap();
        assert_eq!(result.fields().len(), 1);
        let delta_type: DeltaDataType = result.fields()[0].data_type().try_into().unwrap();
        assert_eq!(
            delta_type,
            DeltaDataType::Array(Box::new(DeltaArrayType::new(DeltaDataType::STRING, false)))
        );
        assert!(result.fields()[0].is_nullable());
    }

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

        let result = cast_record_batch(&record_batch, target_schema, false, false);

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
