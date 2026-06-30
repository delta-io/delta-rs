use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, DataType, Fields};

/// Returns `true` if any field in `from_fields` is nullable while the
/// correspondingly-named field in `to_fields` is non-nullable (i.e. a
/// nullability relaxation), recursing into struct fields.
///
/// Fields that are not present in `to_fields` are ignored here; those are new
/// columns handled by the regular schema-merge path. This is used to detect
/// that a `SchemaMode::Merge` write must relax a column from non-nullable to
/// nullable, which [`try_cast_schema`] alone cannot detect because it only
/// considers data-type castability.
pub(crate) fn has_nullability_relaxation(from_fields: &Fields, to_fields: &Fields) -> bool {
    from_fields.iter().any(|f| {
        let Some((_, target_field)) = to_fields.find(f.name()) else {
            return false;
        };
        if f.is_nullable() && !target_field.is_nullable() {
            return true;
        }
        if let (DataType::Struct(from_nested), DataType::Struct(to_nested)) =
            (f.data_type(), target_field.data_type())
        {
            return has_nullability_relaxation(from_nested, to_nested);
        }
        false
    })
}

pub(crate) fn try_cast_schema(from_fields: &Fields, to_fields: &Fields) -> Result<(), ArrowError> {
    if from_fields.len() != to_fields.len() {
        return Err(ArrowError::SchemaError(format!(
            "Cannot cast schema, number of fields does not match: {} vs {}",
            from_fields.len(),
            to_fields.len()
        )));
    }

    from_fields
        .iter()
        .map(|f| {
            if let Some((_, target_field)) = to_fields.find(f.name()) {
                if let (DataType::Struct(fields0), DataType::Struct(fields1)) =
                    (f.data_type(), target_field.data_type())
                {
                    try_cast_schema(fields0, fields1)
                } else {
                    match (f.data_type(), target_field.data_type()) {
                        (
                            DataType::Decimal128(left_precision, left_scale) | DataType::Decimal256(left_precision, left_scale),
                            DataType::Decimal128(right_precision, right_scale)
                        ) => {
                            if left_precision <= right_precision && left_scale <= right_scale {
                                Ok(())
                            } else {
                                Err(ArrowError::SchemaError(format!(
                                    "Cannot cast field {} from {} to {}",
                                    f.name(),
                                    f.data_type(),
                                    target_field.data_type()
                                )))
                            }
                        },
                        (
                            _,
                            DataType::Decimal256(_, _),
                        ) => {
                            unreachable!("Target field can never be Decimal 256. According to the protocol: 'The precision and scale can be up to 38.'")
                        },
                        (left, right) => {
                            if !can_cast_types(left, right) {
                                Err(ArrowError::SchemaError(format!(
                                    "Cannot cast field {} from {} to {}",
                                    f.name(),
                                    f.data_type(),
                                    target_field.data_type()
                                )))
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
            } else {
                Err(ArrowError::SchemaError(format!(
                    "Field {} not found in schema",
                    f.name()
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(())
}
