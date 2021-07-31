//! Conversion between Delta Table schema and Arrow schema

use crate::schema;
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef, TimeUnit,
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
            "element",
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
                        // Issue: https://github.com/delta-io/delta/issues/643
                        Ok(ArrowDataType::Timestamp(TimeUnit::Nanosecond, None))
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
            // NOTE: this doesn't currently support maps with string keys
            // See below arrow-rs issues for adding arrow::datatypes::DataType::Map to support a
            // more general map type:
            // https://github.com/apache/arrow-rs/issues/395
            // https://github.com/apache/arrow-rs/issues/396
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

pub(crate) fn delta_log_schema_for_table(
    table_schema: ArrowSchema,
    partition_columns: &[String],
) -> SchemaRef {
    lazy_static! {
        static ref SCHEMA_FIELDS: Vec<ArrowField> = vec![
            ArrowField::new(
                "metaData",
                ArrowDataType::Struct(vec![
                    ArrowField::new("id", ArrowDataType::Utf8, true),
                    ArrowField::new("name", ArrowDataType::Utf8, true),
                    ArrowField::new("description", ArrowDataType::Utf8, true),
                    ArrowField::new("schemaString", ArrowDataType::Utf8, true),
                    ArrowField::new("createdTime", ArrowDataType::Int64, true),
                    ArrowField::new("partitionColumns", ArrowDataType::List(Box::new(
                        ArrowField::new("element", ArrowDataType::Utf8, true))), true),
                    ArrowField::new("format", ArrowDataType::Struct(vec![
                        ArrowField::new("provider", ArrowDataType::Utf8, true),
                        // TODO: Add "options" after ArrowDataType::Map support
                        ]), true),
                ]),
                true
            ),
            ArrowField::new(
                "protocol",
                ArrowDataType::Struct(vec![
                    ArrowField::new("minReaderVersion", ArrowDataType::Int32, true),
                    ArrowField::new("minWriterVersion", ArrowDataType::Int32, true),
                ]),
                true
            ),
            ArrowField::new(
                "txn",
                ArrowDataType::Struct(vec![
                    ArrowField::new("appId", ArrowDataType::Utf8, true),
                    ArrowField::new("version", ArrowDataType::Int64, true),
                ]),
                true
            ),
            ArrowField::new(
                "remove",
                ArrowDataType::Struct(vec![
                    ArrowField::new("path", ArrowDataType::Utf8, true),
                    ArrowField::new("deletionTimestamp", ArrowDataType::Int64, true),
                    ArrowField::new("dataChange", ArrowDataType::Boolean, true),
                    ArrowField::new("extendedFileMetadata", ArrowDataType::Boolean, true),
                    ArrowField::new("size", ArrowDataType::Int64, true),
                    // TODO: Add "partitionValues" after ArrowDataType::Map support
                    // TODO: Add "tags" after ArrowDataType::Map support
                ]),
                true
            )
        ];
        static ref ADD_FIELDS: Vec<ArrowField> = vec![
            ArrowField::new("path", ArrowDataType::Utf8, true),
            ArrowField::new("size", ArrowDataType::Int64, true),
            ArrowField::new("modificationTime", ArrowDataType::Int64, true),
            ArrowField::new("dataChange", ArrowDataType::Boolean, true),
            ArrowField::new("stats", ArrowDataType::Utf8, true),
            // TODO: Add "partitionValues" after ArrowDataType::Map support
            // TODO: Add "tags" after ArrowDataType::Map support
        ];
    }

    let (partition_fields, non_partition_fields): (Vec<ArrowField>, Vec<ArrowField>) = table_schema
        .fields()
        .iter()
        .map(|f| f.to_owned())
        .partition(|field| partition_columns.contains(field.name()));

    let mut stats_parsed_fields: Vec<ArrowField> =
        vec![ArrowField::new("numRecords", ArrowDataType::Int64, true)];

    if !non_partition_fields.is_empty() {
        stats_parsed_fields.extend(["minValues", "maxValues", "nullCount"].iter().map(|name| {
            ArrowField::new(
                name,
                ArrowDataType::Struct(non_partition_fields.clone()),
                true,
            )
        }));
    }

    let mut add_fields = ADD_FIELDS.clone();

    add_fields.push(ArrowField::new(
        "stats_parsed",
        ArrowDataType::Struct(stats_parsed_fields),
        true,
    ));

    if !partition_fields.is_empty() {
        add_fields.push(ArrowField::new(
            "partitionValues_parsed",
            ArrowDataType::Struct(partition_fields),
            true,
        ));
    }

    let mut schema_fields = SCHEMA_FIELDS.clone();
    schema_fields.push(ArrowField::new(
        "add",
        ArrowDataType::Struct(add_fields),
        true,
    ));

    let arrow_schema = ArrowSchema::new(schema_fields);

    std::sync::Arc::new(arrow_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn delta_log_schema_for_table_test() {
        // NOTE: We should future proof the checkpoint schema in case action schema changes.
        // See https://github.com/delta-io/delta-rs/issues/287

        let table_schema = ArrowSchema::new(vec![
            ArrowField::new("pcol", ArrowDataType::Int32, true),
            ArrowField::new("col1", ArrowDataType::Int32, true),
        ]);
        let partition_columns = vec!["pcol".to_string()];
        let log_schema = delta_log_schema_for_table(table_schema, partition_columns.as_slice());

        let expected_fields = vec!["metaData", "protocol", "txn", "remove", "add"];
        for f in log_schema.fields().iter() {
            assert!(expected_fields.contains(&f.name().as_str()));
        }
        let add_fields: Vec<_> = log_schema
            .fields()
            .iter()
            .filter(|f| f.name() == "add")
            .map(|f| {
                if let ArrowDataType::Struct(fields) = f.data_type() {
                    fields.iter().map(|f| f.clone())
                } else {
                    unreachable!();
                }
            })
            .flatten()
            .collect();
        assert_eq!(7, add_fields.len());

        let add_field_map: HashMap<_, _> = add_fields
            .iter()
            .map(|f| (f.name().to_owned(), f.clone()))
            .collect();

        let partition_values_parsed = add_field_map.get("partitionValues_parsed").unwrap();
        if let ArrowDataType::Struct(fields) = partition_values_parsed.data_type() {
            assert_eq!(1, fields.len());
            let field = fields.get(0).unwrap().to_owned();
            assert_eq!(ArrowField::new("pcol", ArrowDataType::Int32, true), field);
        } else {
            unreachable!();
        }

        let stats_parsed = add_field_map.get("stats_parsed").unwrap();
        if let ArrowDataType::Struct(fields) = stats_parsed.data_type() {
            assert_eq!(4, fields.len());

            let field_map: HashMap<_, _> = fields
                .iter()
                .map(|f| (f.name().to_owned(), f.clone()))
                .collect();

            for (k, v) in field_map.iter() {
                match k.as_ref() {
                    "minValues" | "maxValues" | "nullCount" => match v.data_type() {
                        ArrowDataType::Struct(fields) => {
                            assert_eq!(1, fields.len());
                            let field = fields.get(0).unwrap().to_owned();
                            assert_eq!(ArrowField::new("col1", ArrowDataType::Int32, true), field);
                        }
                        _ => unreachable!(),
                    },
                    "numRecords" => {}
                    _ => panic!(),
                }
            }
        } else {
            unreachable!();
        }
    }
}
