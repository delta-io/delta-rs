use regex::Regex;
use lazy_static::lazy_static;
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use crate::schema;

impl From<&schema::Schema> for ArrowSchema {
    fn from(s: &schema::Schema) -> Self {
        let fields = s
            .get_fields()
            .iter()
            .map(|field| <ArrowField as From<&schema::SchemaField>>::from(field))
            .collect();

        ArrowSchema::new(fields)
    }
}

impl From<&schema::SchemaField> for ArrowField {
    fn from(f: &schema::SchemaField) -> Self {
        ArrowField::new(
            f.get_name(),
            ArrowDataType::from(f.get_type()),
            f.is_nullable(),
        )
    }
}

impl From<&schema::SchemaTypeArray> for ArrowField {
    fn from(a: &schema::SchemaTypeArray) -> Self {
        ArrowField::new(
            "",
            ArrowDataType::from(a.get_element_type()),
            a.contains_null(),
        )
    }
}

impl From<&schema::SchemaDataType> for ArrowDataType {
    fn from(t: &schema::SchemaDataType) -> Self {
        match t {
            schema::SchemaDataType::primitive(p) => {
                lazy_static! {
                    static ref DECIMAL_REGEX: Regex = Regex::new(r"\((\d{1,2}),(\d{1,2})\)").unwrap();
                }
                match p.as_str() {
                    "string" => ArrowDataType::Utf8,
                    "long" => ArrowDataType::Int64, // undocumented type
                    "integer" => ArrowDataType::Int32,
                    "short" => ArrowDataType::Int16,
                    "byte" => ArrowDataType::Int8,
                    "float" => ArrowDataType::Float32,
                    "double" => ArrowDataType::Float64,
                    "boolean" => ArrowDataType::Boolean,
                    "binary" => ArrowDataType::Binary,
                    decimal if DECIMAL_REGEX.is_match(decimal) => {
                        let extract = DECIMAL_REGEX.captures(decimal).unwrap();
                        let precision = extract.get(1).unwrap().as_str().parse::<usize>().unwrap();
                        let scale = extract.get(2).unwrap().as_str().parse::<usize>().unwrap();
                        ArrowDataType::Decimal(precision, scale)
                    },
                    "date" => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone.
                        panic!("date is not supported in arrow");
                    }
                    "timestamp" => {
                        // Microsecond precision timestamp without a timezone.
                        ArrowDataType::Time64(TimeUnit::Microsecond)
                    }
                    s => {
                        panic!("unexpected delta schema type: {}", s);
                    }
                }
            }
            schema::SchemaDataType::r#struct(s) => ArrowDataType::Struct(
                s.get_fields()
                    .iter()
                    .map(|f| <ArrowField as From<&schema::SchemaField>>::from(f))
                    .collect(),
            ),
            schema::SchemaDataType::array(a) => ArrowDataType::List(Box::new(
                <ArrowField as From<&schema::SchemaTypeArray>>::from(a),
            )),
            schema::SchemaDataType::map(m) => ArrowDataType::Dictionary(
                Box::new(<ArrowDataType as From<&schema::SchemaDataType>>::from(
                    m.get_key_type(),
                )),
                Box::new(<ArrowDataType as From<&schema::SchemaDataType>>::from(
                    m.get_value_type(),
                )),
            ),
        }
    }
}
