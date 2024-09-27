//! Provide schema merging for delta schemas
//!
use std::collections::HashMap;

use arrow::datatypes::DataType::Dictionary;
use arrow_schema::{
    ArrowError, DataType, Field as ArrowField, Fields, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};

use crate::kernel::{ArrayType, DataType as DeltaDataType, MapType, StructField, StructType};

fn try_merge_metadata<T: std::cmp::PartialEq + Clone>(
    left: &mut HashMap<String, T>,
    right: &HashMap<String, T>,
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

pub(crate) fn merge_delta_type(
    left: &DeltaDataType,
    right: &DeltaDataType,
) -> Result<DeltaDataType, ArrowError> {
    if left == right {
        return Ok(left.clone());
    }
    match (left, right) {
        (DeltaDataType::Array(a), DeltaDataType::Array(b)) => {
            let merged = merge_delta_type(&a.element_type, &b.element_type)?;
            Ok(DeltaDataType::Array(Box::new(ArrayType::new(
                merged,
                a.contains_null() || b.contains_null(),
            ))))
        }
        (DeltaDataType::Map(a), DeltaDataType::Map(b)) => {
            let merged_key = merge_delta_type(&a.key_type, &b.key_type)?;
            let merged_value = merge_delta_type(&a.value_type, &b.value_type)?;
            Ok(DeltaDataType::Map(Box::new(MapType::new(
                merged_key,
                merged_value,
                a.value_contains_null() || b.value_contains_null(),
            ))))
        }
        (DeltaDataType::Struct(a), DeltaDataType::Struct(b)) => {
            let merged = merge_delta_struct(a, b)?;
            Ok(DeltaDataType::Struct(Box::new(merged)))
        }
        (a, b) => Err(ArrowError::SchemaError(format!(
            "Cannot merge types {} and {}",
            a, b
        ))),
    }
}

pub(crate) fn merge_delta_struct(
    left: &StructType,
    right: &StructType,
) -> Result<StructType, ArrowError> {
    let mut errors = Vec::new();
    let merged_fields: Result<Vec<StructField>, ArrowError> = left
        .fields()
        .map(|field| {
            let right_field = right.field(field.name());
            if let Some(right_field) = right_field {
                let type_or_not = merge_delta_type(field.data_type(), right_field.data_type());
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

                        new_field.metadata.clone_from(&field.metadata);
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

pub(crate) fn merge_arrow_field(
    left: &ArrowField,
    right: &ArrowField,
    preserve_new_fields: bool,
) -> Result<ArrowField, ArrowError> {
    if left == right {
        return Ok(left.clone());
    }

    let (table_type, batch_type) = (left.data_type(), right.data_type());

    match (table_type, batch_type) {
        (Dictionary(key_type, value_type), _)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            ) && matches!(
                batch_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            Ok(ArrowField::new(
                right.name(),
                Dictionary(key_type.clone(), Box::new(batch_type.clone())),
                left.is_nullable() || right.is_nullable(),
            ))
        }
        (Dictionary(key_type, value_type), _)
            if matches!(
                value_type.as_ref(),
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary
            ) && matches!(
                batch_type,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) =>
        {
            Ok(ArrowField::new(
                right.name(),
                Dictionary(key_type.clone(), Box::new(batch_type.clone())),
                left.is_nullable() || right.is_nullable(),
            ))
        }
        (Dictionary(_, value_type), _) if value_type.equals_datatype(batch_type) => Ok(left
            .clone()
            .with_nullable(left.is_nullable() || right.is_nullable())),

        (_, Dictionary(_, value_type))
            if matches!(
                table_type,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            ) && matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            Ok(right
                .clone()
                .with_nullable(left.is_nullable() || right.is_nullable()))
        }
        (_, Dictionary(_, value_type))
            if matches!(
                table_type,
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary
            ) && matches!(
                value_type.as_ref(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) =>
        {
            Ok(right
                .clone()
                .with_nullable(left.is_nullable() || right.is_nullable()))
        }
        (_, Dictionary(_, value_type)) if value_type.equals_datatype(table_type) => Ok(right
            .clone()
            .with_nullable(left.is_nullable() || right.is_nullable())),
        // With Utf8/binary we always take  the right type since that is coming from the incoming data
        // by doing that we allow passthrough of any string flavor
        (
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        )
        | (
            DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
        ) => Ok(ArrowField::new(
            left.name(),
            batch_type.clone(),
            right.is_nullable() || left.is_nullable(),
        )),
        (
            DataType::List(left_child_fields) | DataType::LargeList(left_child_fields),
            DataType::LargeList(right_child_fields),
        ) => {
            let merged =
                merge_arrow_field(left_child_fields, right_child_fields, preserve_new_fields)?;
            Ok(ArrowField::new(
                left.name(),
                DataType::LargeList(merged.into()),
                right.is_nullable() || left.is_nullable(),
            ))
        }
        (
            DataType::List(left_child_fields) | DataType::LargeList(left_child_fields),
            DataType::List(right_child_fields),
        ) => {
            let merged =
                merge_arrow_field(left_child_fields, right_child_fields, preserve_new_fields)?;
            Ok(ArrowField::new(
                left.name(),
                DataType::List(merged.into()),
                right.is_nullable() || left.is_nullable(),
            ))
        }
        (DataType::Struct(left_child_fields), DataType::Struct(right_child_fields)) => {
            let merged =
                merge_arrow_vec_fields(left_child_fields, right_child_fields, preserve_new_fields)?;
            Ok(ArrowField::new(
                left.name(),
                DataType::Struct(merged.into()),
                right.is_nullable() || left.is_nullable(),
            ))
        }
        (DataType::Map(left_field, left_sorted), DataType::Map(right_field, right_sorted))
            if left_sorted == right_sorted =>
        {
            let merged = merge_arrow_field(left_field, right_field, preserve_new_fields)?;
            Ok(ArrowField::new(
                left.name(),
                DataType::Map(merged.into(), *right_sorted),
                right.is_nullable() || left.is_nullable(),
            ))
        }
        _ => {
            let mut new_field = left.clone();
            match new_field.try_merge(right) {
                Ok(()) => (),
                Err(_err) => {
                    // We cannot keep the table field here, there is some weird behavior where
                    // Decimal(5,1) can be safely casted into Decimal(4,1) with out loss of data
                    // Then our stats parser fails to parse this decimal(1000.1) into Decimal(4,1)
                    // even though datafusion was able to write it into parquet
                    // We manually have to check if the decimal in the recordbatch is a subset of the table decimal
                    if let (
                        DataType::Decimal128(left_precision, left_scale)
                        | DataType::Decimal256(left_precision, left_scale),
                        DataType::Decimal128(right_precision, right_scale),
                    ) = (right.data_type(), new_field.data_type())
                    {
                        if !(left_precision <= right_precision && left_scale <= right_scale) {
                            return Err(ArrowError::SchemaError(format!(
                                "Cannot merge field {} from {} to {}",
                                right.name(),
                                right.data_type(),
                                new_field.data_type()
                            )));
                        }
                    };
                    // If it's not Decimal datatype, the new_field remains the left table field.
                }
            };
            Ok(new_field)
        }
    }
}

/// Merges Arrow Table schema and Arrow Batch Schema, by allowing Large/View Types to passthrough.
// Sometimes fields can't be merged because they are not the same types. So table has int32,
// but batch int64. We want the preserve the table type. At later stage we will call cast_record_batch
// which will cast the batch int64->int32. This is desired behaviour so we can have flexibility
// in the batch data types. But preserve the correct table and parquet types.
//
// Preserve_new_fields can also be disabled if you just want to only use the passthrough functionality
pub(crate) fn merge_arrow_schema(
    table_schema: ArrowSchemaRef,
    batch_schema: ArrowSchemaRef,
    preserve_new_fields: bool,
) -> Result<ArrowSchemaRef, ArrowError> {
    let table_fields = table_schema.fields();
    let batch_fields = batch_schema.fields();

    let merged_schema = ArrowSchema::new(merge_arrow_vec_fields(
        table_fields,
        batch_fields,
        preserve_new_fields,
    )?)
    .into();
    Ok(merged_schema)
}

fn merge_arrow_vec_fields(
    table_fields: &Fields,
    batch_fields: &Fields,
    preserve_new_fields: bool,
) -> Result<Vec<ArrowField>, ArrowError> {
    let mut errors = Vec::with_capacity(table_fields.len());
    let merged_fields: Result<Vec<ArrowField>, ArrowError> = table_fields
        .iter()
        .map(|field| {
            let right_field = batch_fields.find(field.name());
            if let Some((_, right_field)) = right_field {
                let field_or_not =
                    merge_arrow_field(field.as_ref(), right_field, preserve_new_fields);
                match field_or_not {
                    Err(e) => {
                        errors.push(e.to_string());
                        Err(e)
                    }
                    Ok(mut f) => {
                        let mut field_matadata = f.metadata().clone();
                        try_merge_metadata(&mut field_matadata, right_field.metadata())?;
                        f.set_metadata(field_matadata);
                        Ok(f)
                    }
                }
            } else {
                Ok(field.as_ref().clone())
            }
        })
        .collect();
    match merged_fields {
        Ok(mut fields) => {
            if preserve_new_fields {
                for field in batch_fields.into_iter() {
                    if table_fields.find(field.name()).is_none() {
                        fields.push(field.as_ref().clone());
                    }
                }
            }
            Ok(fields)
        }
        Err(e) => {
            errors.push(e.to_string());
            Err(ArrowError::SchemaError(errors.join("\n")))
        }
    }
}
