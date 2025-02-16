use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, DataType, Fields};

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
