use crate::schema;
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use arrow::error::ArrowError;
use lazy_static::lazy_static;
use regex::Regex;
use std::convert::TryFrom;

impl TryFrom<&schema::Schema> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(s: &schema::Schema) -> Result<Self, ArrowError> {
        let fields = s
            .get_fields()
            .iter()
            .map(|field| <ArrowField as TryFrom<&schema::SchemaField>>::try_from(field))
            .collect::<Result<Vec<ArrowField>, ArrowError>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<&schema::SchemaField> for ArrowField {
    type Error = ArrowError;

    fn try_from(f: &schema::SchemaField) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            f.get_name(),
            ArrowDataType::try_from(f.get_type())?,
            f.is_nullable(),
        ))
    }
}

impl TryFrom<&schema::SchemaTypeArray> for ArrowField {
    type Error = ArrowError;

    fn try_from(a: &schema::SchemaTypeArray) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            "",
            ArrowDataType::try_from(a.get_element_type())?,
            a.contains_null(),
        ))
    }
}

impl TryFrom<&schema::SchemaDataType> for ArrowDataType {
    type Error = ArrowError;

    fn try_from(t: &schema::SchemaDataType) -> Result<Self, ArrowError> {
        match t {
            schema::SchemaDataType::primitive(p) => {
                lazy_static! {
                    static ref DECIMAL_REGEX: Regex =
                        Regex::new(r"\((\d{1,2}),(\d{1,2})\)").unwrap();
                }
                match p.as_str() {
                    "string" => Ok(ArrowDataType::Utf8),
                    "long" => Ok(ArrowDataType::Int64), // undocumented type
                    "integer" => Ok(ArrowDataType::Int32),
                    "short" => Ok(ArrowDataType::Int16),
                    "byte" => Ok(ArrowDataType::Int8),
                    "float" => Ok(ArrowDataType::Float32),
                    "double" => Ok(ArrowDataType::Float64),
                    "boolean" => Ok(ArrowDataType::Boolean),
                    "binary" => Ok(ArrowDataType::Binary),
                    decimal if DECIMAL_REGEX.is_match(decimal) => {
                        let extract = DECIMAL_REGEX.captures(decimal).ok_or_else(|| {
                            ArrowError::SchemaError(format!(
                                "Invalid decimal type for Arrow: {}",
                                decimal.to_string()
                            ))
                        })?;
                        let precision = extract
                            .get(1)
                            .and_then(|v| v.as_str().parse::<usize>().ok());
                        let scale = extract
                            .get(2)
                            .and_then(|v| v.as_str().parse::<usize>().ok());
                        match (precision, scale) {
                            (Some(p), Some(s)) => Ok(ArrowDataType::Decimal(p, s)),
                            _ => Err(ArrowError::SchemaError(format!(
                                "Invalid precision or scale decimal type for Arrow: {}",
                                decimal.to_string()
                            ))),
                        }
                    }
                    "date" => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone.
                        Ok(ArrowDataType::Date32)
                    }
                    "timestamp" => {
                        // Microsecond precision timestamp without a timezone.
                        Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                    s => Err(ArrowError::SchemaError(format!(
                        "Invalid data type for Arrow: {}",
                        s.to_string()
                    ))),
                }
            }
            schema::SchemaDataType::r#struct(s) => Ok(ArrowDataType::Struct(
                s.get_fields()
                    .iter()
                    .map(|f| <ArrowField as TryFrom<&schema::SchemaField>>::try_from(f))
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?,
            )),
            schema::SchemaDataType::array(a) => {
                Ok(ArrowDataType::List(Box::new(<ArrowField as TryFrom<
                    &schema::SchemaTypeArray,
                >>::try_from(
                    a
                )?)))
            }
            schema::SchemaDataType::map(m) => Ok(ArrowDataType::Dictionary(
                Box::new(
                    <ArrowDataType as TryFrom<&schema::SchemaDataType>>::try_from(
                        m.get_key_type(),
                    )?,
                ),
                Box::new(
                    <ArrowDataType as TryFrom<&schema::SchemaDataType>>::try_from(
                        m.get_value_type(),
                    )?,
                ),
            )),
        }
    }
}
