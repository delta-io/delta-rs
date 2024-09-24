//! Provide common cast functionality for callers
//!
use arrow_array::cast::AsArray;
use arrow_array::{
    new_null_array, Array, ArrayRef, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch,
    RecordBatchOptions, StructArray,
};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, SchemaRef as ArrowSchemaRef};
use std::sync::Arc;

pub(crate) mod merge_schema;

use crate::DeltaResult;

fn cast_struct(
    struct_array: &StructArray,
    fields: &Fields,
    cast_options: &CastOptions,
    add_missing: bool,
) -> Result<StructArray, ArrowError> {
    StructArray::try_new(
        fields.to_owned(),
        fields
            .iter()
            .map(|field| {
                let col_or_not = struct_array.column_by_name(field.name());
                match col_or_not {
                    None => {
                        if add_missing && field.is_nullable() {
                            Ok(new_null_array(field.data_type(), struct_array.len()))
                        } else {
                            Err(ArrowError::SchemaError(format!(
                                "Could not find column {}",
                                field.name()
                            )))
                        }
                    }
                    Some(col) => cast_field(col, field, cast_options, add_missing),
                }
            })
            .collect::<Result<Vec<_>, _>>()?,
        struct_array.nulls().map(ToOwned::to_owned),
    )
}

fn cast_list<T: OffsetSizeTrait>(
    array: &GenericListArray<T>,
    field: &FieldRef,
    cast_options: &CastOptions,
    add_missing: bool,
) -> Result<GenericListArray<T>, ArrowError> {
    let values = cast_field(array.values(), field, cast_options, add_missing)?;
    GenericListArray::<T>::try_new(
        field.clone(),
        array.offsets().clone(),
        values,
        array.nulls().cloned(),
    )
}

fn cast_map(
    array: &MapArray,
    entries_field: &FieldRef,
    sorted: bool,
    cast_options: &CastOptions,
    add_missing: bool,
) -> Result<MapArray, ArrowError> {
    match entries_field.data_type() {
        DataType::Struct(entry_fields) => {
            let entries = cast_struct(array.entries(), entry_fields, cast_options, add_missing)?;
            MapArray::try_new(
                entries_field.clone(),
                array.offsets().to_owned(),
                entries,
                array.nulls().cloned(),
                sorted,
            )
        }
        _ => Err(ArrowError::CastError(
            "Map entries must be a struct".to_string(),
        )),
    }
}

fn cast_field(
    col: &ArrayRef,
    field: &FieldRef,
    cast_options: &CastOptions,
    add_missing: bool,
) -> Result<ArrayRef, ArrowError> {
    let (col_type, field_type) = (col.data_type(), field.data_type());

    match (col_type, field_type) {
        (DataType::Struct(_), DataType::Struct(child_fields)) => {
            let child_struct = StructArray::from(col.into_data());
            Ok(Arc::new(cast_struct(
                &child_struct,
                child_fields,
                cast_options,
                add_missing,
            )?) as ArrayRef)
        }
        (DataType::List(_), DataType::List(child_fields)) => Ok(Arc::new(cast_list(
            col.as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "Expected a list for {} but got {}",
                        field.name(),
                        col_type
                    ))
                })?,
            child_fields,
            cast_options,
            add_missing,
        )?) as ArrayRef),
        (DataType::LargeList(_), DataType::LargeList(child_fields)) => Ok(Arc::new(cast_list(
            col.as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "Expected a list for {} but got {}",
                        field.name(),
                        col_type
                    ))
                })?,
            child_fields,
            cast_options,
            add_missing,
        )?) as ArrayRef),
        // TODO: add list view cast
        (DataType::Map(_, _), DataType::Map(child_fields, sorted)) => Ok(Arc::new(cast_map(
            col.as_map_opt().ok_or_else(|| {
                ArrowError::CastError(format!(
                    "Expected a map for {} but got {}",
                    field.name(),
                    col_type
                ))
            })?,
            child_fields,
            *sorted,
            cast_options,
            add_missing,
        )?) as ArrayRef),
        _ if is_cast_required(col_type, field_type) => {
            cast_with_options(col, field_type, cast_options)
        }
        _ => Ok(col.clone()),
    }
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
    let struct_array = cast_struct(&s, target_schema.fields(), &cast_options, add_missing)?;

    Ok(RecordBatch::try_new_with_options(
        target_schema,
        struct_array.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;

    use super::merge_schema::{merge_arrow_schema, merge_delta_struct};
    use arrow::array::types::Int32Type;
    use arrow::array::{
        new_empty_array, new_null_array, Array, ArrayData, ArrayRef, AsArray, Int32Array,
        ListArray, PrimitiveArray, RecordBatch, StringArray, StructArray,
    };
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
    use delta_kernel::schema::MetadataValue;
    use itertools::Itertools;

    use crate::kernel::{
        ArrayType as DeltaArrayType, DataType as DeltaDataType, StructField as DeltaStructField,
        StructType as DeltaStructType,
    };
    use crate::operations::cast::{cast_record_batch, is_cast_required};

    #[test]
    fn test_merge_arrow_schema_with_dict() {
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

        let result = merge_arrow_schema(left_schema, right_schema, true).unwrap();
        assert_eq!(result.fields().len(), 1);
        let delta_type: DeltaDataType = result.fields()[0].data_type().try_into().unwrap();
        assert_eq!(delta_type, DeltaDataType::STRING);
        assert!(result.fields()[0].is_nullable());
    }

    #[test]
    fn test_merge_delta_schema_with_meta() {
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

        let result = merge_delta_struct(&left_schema, &right_schema).unwrap();
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
    fn test_merge_arrow_schema_with_nested() {
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

        let result = merge_arrow_schema(left_schema, right_schema, true).unwrap();
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

    #[test]
    fn test_add_missing_null_fields_with_no_missing_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let result = cast_record_batch(&batch, schema.clone(), false, true).unwrap();
        assert_eq!(result.schema(), schema);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.column(0).deref().as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter([1, 2, 3])
        );
        assert_eq!(
            result.column(1).deref().as_string(),
            &StringArray::from(vec![Some("a"), None, Some("c")])
        );
    }

    #[test]
    fn test_add_missing_null_fields_with_missing_beginning() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field2",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("c"),
            ]))],
        )
        .unwrap();

        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, true),
            Field::new("field2", DataType::Utf8, true),
        ]));
        let result = cast_record_batch(&batch, new_schema.clone(), false, true).unwrap();
        assert_eq!(result.schema(), new_schema);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.column(0).deref().as_primitive::<Int32Type>(),
            new_null_array(&DataType::Int32, 3)
                .deref()
                .as_primitive::<Int32Type>()
        );
        assert_eq!(
            result.column(1).deref().as_string(),
            &StringArray::from(vec![Some("a"), None, Some("c")])
        );
    }

    #[test]
    fn test_add_missing_null_fields_with_missing_end() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field1",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
        ]));
        let result = cast_record_batch(&batch, new_schema.clone(), false, true).unwrap();
        assert_eq!(result.schema(), new_schema);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.column(0).deref().as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from(vec![Some(1), Some(2), Some(3)])
        );
        assert_eq!(
            result.column(1).deref().as_string::<i32>(),
            new_null_array(&DataType::Utf8, 3).deref().as_string()
        );
    }

    #[test]
    fn test_add_missing_null_fields_error_on_missing_non_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field1",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, false),
        ]));
        let result = cast_record_batch(&batch, new_schema.clone(), false, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_add_missing_null_fields_nested_struct_missing() {
        let nested_fields = Fields::from(vec![Field::new("nested1", DataType::Utf8, true)]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Struct(nested_fields.clone()), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    nested_fields,
                    vec![Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef],
                    None,
                )),
            ],
        )
        .unwrap();
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(vec![
                    Field::new("nested1", DataType::Utf8, true),
                    Field::new("nested2", DataType::Utf8, true),
                ])),
                true,
            ),
        ]));
        let result = cast_record_batch(&batch, new_schema.clone(), false, true).unwrap();
        assert_eq!(result.schema(), new_schema);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.column(0).deref().as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter([1, 2, 3])
        );
        let struct_column = result.column(1).deref().as_struct();
        assert_eq!(struct_column.num_columns(), 2);
        assert_eq!(
            struct_column.column(0).deref().as_string(),
            &StringArray::from(vec![Some("a"), None, Some("c")])
        );
        assert_eq!(
            struct_column.column(1).deref().as_string::<i32>(),
            new_null_array(&DataType::Utf8, 3).deref().as_string()
        );
    }

    #[test]
    fn test_add_missing_null_fields_nested_struct_missing_non_nullable() {
        let nested_fields = Fields::from(vec![Field::new("nested1", DataType::Utf8, false)]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Struct(nested_fields.clone()), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    nested_fields,
                    vec![new_null_array(&DataType::Utf8, 3)],
                    Some(NullBuffer::new_null(3)),
                )),
            ],
        )
        .unwrap();
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Struct(Fields::from(vec![
                    Field::new("nested1", DataType::Utf8, false),
                    Field::new("nested2", DataType::Utf8, true),
                ])),
                true,
            ),
        ]));
        let result = cast_record_batch(&batch, new_schema.clone(), false, true).unwrap();
        assert_eq!(result.schema(), new_schema);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.column(0).deref().as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter([1, 2, 3])
        );
        let struct_column = result.column(1).deref().as_struct();
        assert_eq!(struct_column.num_columns(), 2);
        let expected: [Option<&str>; 3] = Default::default();
        assert_eq!(
            struct_column.column(0).deref().as_string(),
            &StringArray::from(Vec::from(expected))
        );
        assert_eq!(
            struct_column.column(1).deref().as_string::<i32>(),
            new_null_array(&DataType::Utf8, 3).deref().as_string(),
        );
    }

    #[test]
    fn test_add_missing_null_fields_list_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field1",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::List(Arc::new(Field::new("nested1", DataType::Utf8, true))),
                true,
            ),
        ]));
        let result = cast_record_batch(&batch, new_schema.clone(), false, true).unwrap();
        assert_eq!(result.schema(), new_schema);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.column(0).deref().as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter([1, 2, 3])
        );
        let list_column = result.column(1).deref().as_list::<i32>();
        assert_eq!(list_column.len(), 3);
        assert_eq!(list_column.value_offsets(), &[0, 0, 0, 0]);
        assert_eq!(
            list_column.values().deref().as_string::<i32>(),
            new_empty_array(&DataType::Utf8).deref().as_string()
        )
    }

    #[test]
    fn test_add_missing_null_fields_map_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field1",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let new_schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new(
                "field2",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        true,
                    )),
                    false,
                ),
                true,
            ),
        ]));
        let result = cast_record_batch(&batch, new_schema.clone(), false, true).unwrap();
        assert_eq!(result.schema(), new_schema);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.column(0).deref().as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter([1, 2, 3])
        );
        let map_column = result.column(1).deref().as_map();
        assert_eq!(map_column.len(), 3);
        assert_eq!(map_column.offsets().as_ref(), &[0; 4]);
        assert_eq!(
            map_column.keys().deref().as_string::<i32>(),
            new_empty_array(&DataType::Utf8).deref().as_string()
        );
        assert_eq!(
            map_column.values().deref().as_string::<i32>(),
            new_empty_array(&DataType::Utf8).deref().as_string()
        );
    }
}
