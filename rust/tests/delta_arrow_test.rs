extern crate deltalake;
use arrow::datatypes::DataType as ArrowDataType;
use std::convert::TryFrom;

#[test]
fn test_arrow_from_delta_decimal_type() {
    let precision = 20;
    let scale = 2;
    let decimal_type = String::from(format!["decimal({p},{s})", p = precision, s = scale]);
    let decimal_field = deltalake::SchemaDataType::primitive(decimal_type);
    assert_eq!(
        <ArrowDataType as TryFrom<&deltalake::SchemaDataType>>::try_from(&decimal_field).unwrap(),
        ArrowDataType::Decimal(precision, scale)
    );
}

#[test]
fn test_arrow_from_delta_wrong_decimal_type() {
    let precision = 20;
    let scale = "wrong";
    let decimal_type = String::from(format!["decimal({p},{s})", p = precision, s = scale]);
    let _error = format!(
        "Invalid precision or scale decimal type for Arrow: {}",
        scale
    );
    let decimal_field = deltalake::SchemaDataType::primitive(decimal_type);
    assert!(matches!(
        <ArrowDataType as TryFrom<&deltalake::SchemaDataType>>::try_from(&decimal_field)
            .unwrap_err(),
        arrow::error::ArrowError::SchemaError(_error),
    ));
}
