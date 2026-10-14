use arrow_schema::{ArrowError, DataType, Fields};

use crate::kernel::schema::cast::can_cast_safely;

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
                            if !can_cast_safely(left, right) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    fn make_fields(types: &[(&str, DataType)]) -> Fields {
        types
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), true))
            .collect()
    }

    #[test]
    fn test_try_cast_schema_rejects_float_to_int() {
        let from = make_fields(&[("x", DataType::Float64)]);
        let to = make_fields(&[("x", DataType::Int64)]);

        let err = try_cast_schema(&from, &to).expect_err("float64 -> int64 must be rejected");
        assert!(
            err.to_string()
                .contains("Cannot cast field x from Float64 to Int64"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_try_cast_schema_rejects_nested_float_to_int() {
        let from = make_fields(&[(
            "nested",
            DataType::Struct(make_fields(&[("x", DataType::Float64)])),
        )]);
        let to = make_fields(&[(
            "nested",
            DataType::Struct(make_fields(&[("x", DataType::Int64)])),
        )]);

        let err =
            try_cast_schema(&from, &to).expect_err("nested float64 -> int64 must be rejected");
        assert!(
            err.to_string()
                .contains("Cannot cast field x from Float64 to Int64"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_try_cast_schema_allows_int_widening() {
        let from = make_fields(&[("x", DataType::Int32)]);
        let to = make_fields(&[("x", DataType::Int64)]);
        try_cast_schema(&from, &to).expect("int32 -> int64 should be allowed");
    }

    #[test]
    fn test_try_cast_schema_allows_float_widening() {
        let from = make_fields(&[("x", DataType::Float32)]);
        let to = make_fields(&[("x", DataType::Float64)]);
        try_cast_schema(&from, &to).expect("float32 -> float64 should be allowed");
    }

    #[test]
    fn test_try_cast_schema_allows_int_to_float() {
        let from = make_fields(&[("x", DataType::Int64)]);
        let to = make_fields(&[("x", DataType::Float64)]);
        try_cast_schema(&from, &to).expect("int64 -> float64 should be allowed");
    }

    #[test]
    fn test_try_cast_schema_allows_int_narrowing() {
        // Int narrowing is allowed at the schema level; value-range failures are caught
        // later when the cast actually runs.
        let from = make_fields(&[("x", DataType::Int64)]);
        let to = make_fields(&[("x", DataType::Int32)]);
        try_cast_schema(&from, &to).expect("int64 -> int32 should be allowed");
    }

    #[test]
    fn test_try_cast_schema_allows_float_narrowing() {
        // Float narrowing is allowed, out-of-range values will saturate to +/- inf
        let from = make_fields(&[("x", DataType::Float64)]);
        let to = make_fields(&[("x", DataType::Float32)]);
        try_cast_schema(&from, &to).expect("float64 -> float32 should be allowed");
    }

    #[test]
    fn test_try_cast_schema_allows_signed_to_unsigned() {
        let from = make_fields(&[("x", DataType::Int32)]);
        let to = make_fields(&[("x", DataType::UInt32)]);
        try_cast_schema(&from, &to).expect("int32 -> uint32 should be allowed");
    }
}
