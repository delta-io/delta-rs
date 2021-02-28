extern crate deltalake;
use arrow::datatypes::DataType as ArrowDataType;

#[test]
fn test_arrow_from_delta_decimal_type() {
    let precision = 20;
    let scale = 2;
    let decimal_type = String::from(format!["decimal({p},{s})", p=precision, s=scale]);
    let decimal_field = deltalake::SchemaDataType::primitive(decimal_type);
    assert_eq!(
        <ArrowDataType as From<&deltalake::SchemaDataType>>::from(&decimal_field),
        ArrowDataType::Decimal(precision, scale)
    );
}