//! Conversion between Delta Table schema and Arrow schema

use crate::schema;
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use arrow::error::ArrowError;
use lazy_static::lazy_static;
use regex::Regex;
use std::convert::TryFrom;
use std::sync::Arc;

impl TryFrom<&schema::Schema> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(s: &schema::Schema) -> Result<Self, ArrowError> {
        let fields = s
            .get_fields()
            .iter()
            .map(<ArrowField as TryFrom<&schema::SchemaField>>::try_from)
            .collect::<Result<Vec<ArrowField>, ArrowError>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<&schema::SchemaField> for ArrowField {
    type Error = ArrowError;

    fn try_from(f: &schema::SchemaField) -> Result<Self, ArrowError> {
        let metadata = f
            .get_metadata()
            .iter()
            .map(|(key, val)| Ok((key.clone(), serde_json::to_string(val)?)))
            .collect::<Result<_, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;

        let field = ArrowField::new(
            f.get_name(),
            ArrowDataType::try_from(f.get_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata);

        Ok(field)
    }
}

impl TryFrom<&schema::SchemaTypeArray> for ArrowField {
    type Error = ArrowError;

    fn try_from(a: &schema::SchemaTypeArray) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            "item",
            ArrowDataType::try_from(a.get_element_type())?,
            a.contains_null(),
        ))
    }
}

impl TryFrom<&schema::SchemaTypeMap> for ArrowField {
    type Error = ArrowError;

    fn try_from(a: &schema::SchemaTypeMap) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            "entries",
            ArrowDataType::Struct(
                vec![
                    ArrowField::new("key", ArrowDataType::try_from(a.get_key_type())?, false),
                    ArrowField::new(
                        "value",
                        ArrowDataType::try_from(a.get_value_type())?,
                        a.get_value_contains_null(),
                    ),
                ]
                .into(),
            ),
            false, // always non-null
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
                                "Invalid decimal type for Arrow: {decimal}"
                            ))
                        })?;
                        let precision = extract.get(1).and_then(|v| v.as_str().parse::<u8>().ok());
                        let scale = extract.get(2).and_then(|v| v.as_str().parse::<i8>().ok());
                        match (precision, scale) {
                            // TODO how do we decide which variant (128 / 256) to use?
                            (Some(p), Some(s)) => Ok(ArrowDataType::Decimal128(p, s)),
                            _ => Err(ArrowError::SchemaError(format!(
                                "Invalid precision or scale decimal type for Arrow: {decimal}"
                            ))),
                        }
                    }
                    "date" => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone. Stored as 4 bytes integer representing days sinece 1970-01-01
                        Ok(ArrowDataType::Date32)
                    }
                    "timestamp" => {
                        // Issue: https://github.com/delta-io/delta/issues/643
                        Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                    s => Err(ArrowError::SchemaError(format!(
                        "Invalid data type for Arrow: {s}"
                    ))),
                }
            }
            schema::SchemaDataType::r#struct(s) => Ok(ArrowDataType::Struct(
                s.get_fields()
                    .iter()
                    .map(<ArrowField as TryFrom<&schema::SchemaField>>::try_from)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            schema::SchemaDataType::array(a) => {
                Ok(ArrowDataType::List(Arc::new(<ArrowField as TryFrom<
                    &schema::SchemaTypeArray,
                >>::try_from(
                    a
                )?)))
            }
            schema::SchemaDataType::map(m) => Ok(ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new(
                                "keys",
                                <ArrowDataType as TryFrom<&schema::SchemaDataType>>::try_from(
                                    m.get_key_type(),
                                )?,
                                false,
                            ),
                            ArrowField::new(
                                "values",
                                <ArrowDataType as TryFrom<&schema::SchemaDataType>>::try_from(
                                    m.get_value_type(),
                                )?,
                                m.get_value_contains_null(),
                            ),
                        ]
                        .into(),
                    ),
                    true,
                )),
                false,
            )),
        }
    }
}

impl TryFrom<&ArrowSchema> for schema::Schema {
    type Error = ArrowError;
    fn try_from(arrow_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        let new_fields: Result<Vec<schema::SchemaField>, _> = arrow_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().try_into())
            .collect();
        Ok(schema::Schema::new(new_fields?))
    }
}

impl TryFrom<ArrowSchemaRef> for schema::Schema {
    type Error = ArrowError;

    fn try_from(arrow_schema: ArrowSchemaRef) -> Result<Self, ArrowError> {
        arrow_schema.as_ref().try_into()
    }
}

impl TryFrom<&ArrowField> for schema::SchemaField {
    type Error = ArrowError;
    fn try_from(arrow_field: &ArrowField) -> Result<Self, ArrowError> {
        Ok(schema::SchemaField::new(
            arrow_field.name().clone(),
            arrow_field.data_type().try_into()?,
            arrow_field.is_nullable(),
            arrow_field
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect(),
        ))
    }
}

impl TryFrom<&ArrowDataType> for schema::SchemaDataType {
    type Error = ArrowError;
    fn try_from(arrow_datatype: &ArrowDataType) -> Result<Self, ArrowError> {
        match arrow_datatype {
            ArrowDataType::Utf8 => Ok(schema::SchemaDataType::primitive("string".to_string())),
            ArrowDataType::LargeUtf8 => Ok(schema::SchemaDataType::primitive("string".to_string())),
            ArrowDataType::Int64 => Ok(schema::SchemaDataType::primitive("long".to_string())), // undocumented type
            ArrowDataType::Int32 => Ok(schema::SchemaDataType::primitive("integer".to_string())),
            ArrowDataType::Int16 => Ok(schema::SchemaDataType::primitive("short".to_string())),
            ArrowDataType::Int8 => Ok(schema::SchemaDataType::primitive("byte".to_string())),
            ArrowDataType::UInt64 => Ok(schema::SchemaDataType::primitive("long".to_string())), // undocumented type
            ArrowDataType::UInt32 => Ok(schema::SchemaDataType::primitive("integer".to_string())),
            ArrowDataType::UInt16 => Ok(schema::SchemaDataType::primitive("short".to_string())),
            ArrowDataType::UInt8 => Ok(schema::SchemaDataType::primitive("byte".to_string())),
            ArrowDataType::Float32 => Ok(schema::SchemaDataType::primitive("float".to_string())),
            ArrowDataType::Float64 => Ok(schema::SchemaDataType::primitive("double".to_string())),
            ArrowDataType::Boolean => Ok(schema::SchemaDataType::primitive("boolean".to_string())),
            ArrowDataType::Binary => Ok(schema::SchemaDataType::primitive("binary".to_string())),
            ArrowDataType::FixedSizeBinary(_) => {
                Ok(schema::SchemaDataType::primitive("binary".to_string()))
            }
            ArrowDataType::LargeBinary => {
                Ok(schema::SchemaDataType::primitive("binary".to_string()))
            }
            ArrowDataType::Decimal128(p, s) => Ok(schema::SchemaDataType::primitive(format!(
                "decimal({p},{s})"
            ))),
            ArrowDataType::Decimal256(p, s) => Ok(schema::SchemaDataType::primitive(format!(
                "decimal({p},{s})"
            ))),
            ArrowDataType::Date32 => Ok(schema::SchemaDataType::primitive("date".to_string())),
            ArrowDataType::Date64 => Ok(schema::SchemaDataType::primitive("date".to_string())),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
                Ok(schema::SchemaDataType::primitive("timestamp".to_string()))
            }
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(schema::SchemaDataType::primitive("timestamp".to_string()))
            }
            ArrowDataType::Struct(fields) => {
                let converted_fields: Result<Vec<schema::SchemaField>, _> = fields
                    .iter()
                    .map(|field| field.as_ref().try_into())
                    .collect();
                Ok(schema::SchemaDataType::r#struct(
                    schema::SchemaTypeStruct::new(converted_fields?),
                ))
            }
            ArrowDataType::List(field) => {
                Ok(schema::SchemaDataType::array(schema::SchemaTypeArray::new(
                    Box::new((*field).data_type().try_into()?),
                    (*field).is_nullable(),
                )))
            }
            ArrowDataType::LargeList(field) => {
                Ok(schema::SchemaDataType::array(schema::SchemaTypeArray::new(
                    Box::new((*field).data_type().try_into()?),
                    (*field).is_nullable(),
                )))
            }
            ArrowDataType::FixedSizeList(field, _) => {
                Ok(schema::SchemaDataType::array(schema::SchemaTypeArray::new(
                    Box::new((*field).data_type().try_into()?),
                    (*field).is_nullable(),
                )))
            }
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = struct_fields[0].data_type().try_into()?;
                    let value_type = struct_fields[1].data_type().try_into()?;
                    let value_type_nullable = struct_fields[1].is_nullable();
                    Ok(schema::SchemaDataType::map(schema::SchemaTypeMap::new(
                        Box::new(key_type),
                        Box::new(value_type),
                        value_type_nullable,
                    )))
                } else {
                    panic!("DataType::Map should contain a struct field child");
                }
            }
            s => Err(ArrowError::SchemaError(format!(
                "Invalid data type for Delta Lake: {s}"
            ))),
        }
    }
}

/// Returns an arrow schema representing the delta log for use in checkpoints
///
/// # Arguments
///
/// * `table_schema` - The arrow schema representing the table backed by the delta log
/// * `partition_columns` - The list of partition columns of the table.
/// * `use_extended_remove_schema` - Whether to include extended file metadata in remove action schema.
///    Required for compatibility with different versions of Databricks runtime.
pub(crate) fn delta_log_schema_for_table(
    table_schema: ArrowSchema,
    partition_columns: &[String],
    use_extended_remove_schema: bool,
) -> ArrowSchemaRef {
    lazy_static! {
        static ref SCHEMA_FIELDS: Vec<ArrowField> = vec![
            ArrowField::new(
                "metaData",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("id", ArrowDataType::Utf8, true),
                        ArrowField::new("name", ArrowDataType::Utf8, true),
                        ArrowField::new("description", ArrowDataType::Utf8, true),
                        ArrowField::new("schemaString", ArrowDataType::Utf8, true),
                        ArrowField::new("createdTime", ArrowDataType::Int64, true),
                        ArrowField::new(
                            "partitionColumns",
                            ArrowDataType::List(Arc::new(ArrowField::new(
                                "element",
                                ArrowDataType::Utf8,
                                true
                            ))),
                            true
                        ),
                        ArrowField::new(
                            "configuration",
                            ArrowDataType::Map(
                                Arc::new(ArrowField::new(
                                    "key_value",
                                    ArrowDataType::Struct(
                                        vec![
                                            ArrowField::new("key", ArrowDataType::Utf8, false),
                                            ArrowField::new("value", ArrowDataType::Utf8, true),
                                        ]
                                        .into()
                                    ),
                                    false
                                )),
                                false
                            ),
                            true
                        ),
                        ArrowField::new(
                            "format",
                            ArrowDataType::Struct(
                                vec![
                                    ArrowField::new("provider", ArrowDataType::Utf8, true),
                                    ArrowField::new(
                                        "options",
                                        ArrowDataType::Map(
                                            Arc::new(ArrowField::new(
                                                "key_value",
                                                ArrowDataType::Struct(
                                                    vec![
                                                        ArrowField::new(
                                                            "key",
                                                            ArrowDataType::Utf8,
                                                            false
                                                        ),
                                                        ArrowField::new(
                                                            "value",
                                                            ArrowDataType::Utf8,
                                                            true
                                                        ),
                                                    ]
                                                    .into()
                                                ),
                                                false
                                            )),
                                            false
                                        ),
                                        false
                                    )
                                ]
                                .into()
                            ),
                            true
                        ),
                    ]
                    .into()
                ),
                true
            ),
            ArrowField::new(
                "protocol",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("minReaderVersion", ArrowDataType::Int32, true),
                        ArrowField::new("minWriterVersion", ArrowDataType::Int32, true),
                    ]
                    .into()
                ),
                true
            ),
            ArrowField::new(
                "txn",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("appId", ArrowDataType::Utf8, true),
                        ArrowField::new("version", ArrowDataType::Int64, true),
                    ]
                    .into()
                ),
                true
            ),
        ];
        static ref ADD_FIELDS: Vec<ArrowField> = vec![
            ArrowField::new("path", ArrowDataType::Utf8, true),
            ArrowField::new("size", ArrowDataType::Int64, true),
            ArrowField::new("modificationTime", ArrowDataType::Int64, true),
            ArrowField::new("dataChange", ArrowDataType::Boolean, true),
            ArrowField::new("stats", ArrowDataType::Utf8, true),
            ArrowField::new(
                "partitionValues",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "key_value",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new("key", ArrowDataType::Utf8, false),
                                ArrowField::new("value", ArrowDataType::Utf8, true),
                            ]
                            .into()
                        ),
                        false
                    )),
                    false
                ),
                true
            ),
            ArrowField::new(
                "tags",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "key_value",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new("key", ArrowDataType::Utf8, false),
                                ArrowField::new("value", ArrowDataType::Utf8, true),
                            ]
                            .into()
                        ),
                        false
                    )),
                    false
                ),
                true
            )
        ];
        static ref REMOVE_FIELDS: Vec<ArrowField> = vec![
            ArrowField::new("path", ArrowDataType::Utf8, true),
            ArrowField::new("deletionTimestamp", ArrowDataType::Int64, true),
            ArrowField::new("dataChange", ArrowDataType::Boolean, true),
            ArrowField::new("extendedFileMetadata", ArrowDataType::Boolean, true),
        ];
        static ref REMOVE_EXTENDED_FILE_METADATA_FIELDS: Vec<ArrowField> = vec![
            ArrowField::new("size", ArrowDataType::Int64, true),
            ArrowField::new(
                "partitionValues",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "key_value",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new("key", ArrowDataType::Utf8, false),
                                ArrowField::new("value", ArrowDataType::Utf8, true),
                            ]
                            .into()
                        ),
                        false
                    )),
                    false
                ),
                true
            ),
            ArrowField::new(
                "tags",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "key_value",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new("key", ArrowDataType::Utf8, false),
                                ArrowField::new("value", ArrowDataType::Utf8, true),
                            ]
                            .into()
                        ),
                        false
                    )),
                    false
                ),
                true
            )
        ];
    }

    // create add fields according to the specific data table schema
    let (partition_fields, non_partition_fields): (Vec<ArrowFieldRef>, Vec<ArrowFieldRef>) =
        table_schema
            .fields()
            .iter()
            .map(|field| field.to_owned())
            .partition(|field| partition_columns.contains(field.name()));

    let mut stats_parsed_fields: Vec<ArrowField> =
        vec![ArrowField::new("numRecords", ArrowDataType::Int64, true)];
    if !non_partition_fields.is_empty() {
        let mut max_min_vec = Vec::new();
        non_partition_fields
            .iter()
            .for_each(|f| max_min_schema_for_fields(&mut max_min_vec, f));

        stats_parsed_fields.extend(["minValues", "maxValues"].into_iter().map(|name| {
            ArrowField::new(
                name,
                ArrowDataType::Struct(max_min_vec.clone().into()),
                true,
            )
        }));

        let mut null_count_vec = Vec::new();
        non_partition_fields
            .iter()
            .for_each(|f| null_count_schema_for_fields(&mut null_count_vec, f));
        let null_count_struct = ArrowField::new(
            "nullCount",
            ArrowDataType::Struct(null_count_vec.into()),
            true,
        );

        stats_parsed_fields.push(null_count_struct);
    }
    let mut add_fields = ADD_FIELDS.clone();
    add_fields.push(ArrowField::new(
        "stats_parsed",
        ArrowDataType::Struct(stats_parsed_fields.into()),
        true,
    ));
    if !partition_fields.is_empty() {
        add_fields.push(ArrowField::new(
            "partitionValues_parsed",
            ArrowDataType::Struct(partition_fields.into()),
            true,
        ));
    }

    // create remove fields with or without extendedFileMetadata
    let mut remove_fields = REMOVE_FIELDS.clone();
    if use_extended_remove_schema {
        remove_fields.extend(REMOVE_EXTENDED_FILE_METADATA_FIELDS.clone());
    }

    // include add and remove fields in checkpoint schema
    let mut schema_fields = SCHEMA_FIELDS.clone();
    schema_fields.push(ArrowField::new(
        "add",
        ArrowDataType::Struct(add_fields.into()),
        true,
    ));
    schema_fields.push(ArrowField::new(
        "remove",
        ArrowDataType::Struct(remove_fields.into()),
        true,
    ));

    let arrow_schema = ArrowSchema::new(schema_fields);

    std::sync::Arc::new(arrow_schema)
}

fn max_min_schema_for_fields(dest: &mut Vec<ArrowField>, f: &ArrowField) {
    match f.data_type() {
        ArrowDataType::Struct(struct_fields) => {
            let mut child_dest = Vec::new();

            for f in struct_fields {
                max_min_schema_for_fields(&mut child_dest, f);
            }

            dest.push(ArrowField::new(
                f.name(),
                ArrowDataType::Struct(child_dest.into()),
                true,
            ));
        }
        // don't compute min or max for list or map types
        ArrowDataType::List(_) | ArrowDataType::Map(_, _) => { /* noop */ }
        _ => {
            let f = f.clone();
            dest.push(f);
        }
    }
}

fn null_count_schema_for_fields(dest: &mut Vec<ArrowField>, f: &ArrowField) {
    match f.data_type() {
        ArrowDataType::Struct(struct_fields) => {
            let mut child_dest = Vec::new();

            for f in struct_fields {
                null_count_schema_for_fields(&mut child_dest, f);
            }

            dest.push(ArrowField::new(
                f.name(),
                ArrowDataType::Struct(child_dest.into()),
                true,
            ));
        }
        _ => {
            let f = ArrowField::new(f.name(), ArrowDataType::Int64, true);
            dest.push(f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn delta_log_schema_for_table_test() {
        // NOTE: We should future proof the checkpoint schema in case action schema changes.
        // See https://github.com/delta-io/delta-rs/issues/287

        let table_schema = ArrowSchema::new(vec![
            ArrowField::new("pcol", ArrowDataType::Int32, true),
            ArrowField::new("col1", ArrowDataType::Int32, true),
        ]);
        let partition_columns = vec!["pcol".to_string()];
        let log_schema =
            delta_log_schema_for_table(table_schema.clone(), partition_columns.as_slice(), false);

        // verify top-level schema contains all expected fields and they are named correctly.
        let expected_fields = vec!["metaData", "protocol", "txn", "remove", "add"];
        for f in log_schema.fields().iter() {
            assert!(expected_fields.contains(&f.name().as_str()));
        }
        assert_eq!(5, log_schema.fields().len());

        // verify add fields match as expected. a lot of transformation goes into these.
        let add_fields: Vec<_> = log_schema
            .fields()
            .iter()
            .filter(|f| f.name() == "add")
            .flat_map(|f| {
                if let ArrowDataType::Struct(fields) = f.data_type() {
                    fields.iter().cloned()
                } else {
                    unreachable!();
                }
            })
            .collect();
        assert_eq!(9, add_fields.len());
        let add_field_map: HashMap<_, _> = add_fields
            .iter()
            .map(|f| (f.name().to_owned(), f.clone()))
            .collect();
        let partition_values_parsed = add_field_map.get("partitionValues_parsed").unwrap();
        if let ArrowDataType::Struct(fields) = partition_values_parsed.data_type() {
            assert_eq!(1, fields.len());
            let field = fields.get(0).unwrap().to_owned();
            assert_eq!(
                Arc::new(ArrowField::new("pcol", ArrowDataType::Int32, true)),
                field
            );
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
                            let data_type = if k == "nullCount" {
                                ArrowDataType::Int64
                            } else {
                                ArrowDataType::Int32
                            };
                            assert_eq!(Arc::new(ArrowField::new("col1", data_type, true)), field);
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

        // verify extended remove schema fields **ARE NOT** included when `use_extended_remove_schema` is false.
        let num_remove_fields = log_schema
            .fields()
            .iter()
            .filter(|f| f.name() == "remove")
            .flat_map(|f| {
                if let ArrowDataType::Struct(fields) = f.data_type() {
                    fields.iter().cloned()
                } else {
                    unreachable!();
                }
            })
            .count();
        assert_eq!(4, num_remove_fields);

        // verify extended remove schema fields **ARE** included when `use_extended_remove_schema` is true.
        let log_schema =
            delta_log_schema_for_table(table_schema, partition_columns.as_slice(), true);
        let remove_fields: Vec<_> = log_schema
            .fields()
            .iter()
            .filter(|f| f.name() == "remove")
            .flat_map(|f| {
                if let ArrowDataType::Struct(fields) = f.data_type() {
                    fields.iter().cloned()
                } else {
                    unreachable!();
                }
            })
            .collect();
        assert_eq!(7, remove_fields.len());
        let expected_fields = vec![
            "path",
            "deletionTimestamp",
            "dataChange",
            "extendedFileMetadata",
            "partitionValues",
            "size",
            "tags",
        ];
        for f in remove_fields.iter() {
            assert!(expected_fields.contains(&f.name().as_str()));
        }
    }

    #[test]
    fn test_arrow_from_delta_decimal_type() {
        let precision = 20;
        let scale = 2;
        let decimal_type = format!["decimal({precision},{scale})"];
        let decimal_field = crate::SchemaDataType::primitive(decimal_type);
        assert_eq!(
            <ArrowDataType as TryFrom<&crate::SchemaDataType>>::try_from(&decimal_field).unwrap(),
            ArrowDataType::Decimal128(precision, scale)
        );
    }

    #[test]
    fn test_arrow_from_delta_wrong_decimal_type() {
        let precision = 20;
        let scale = "wrong";
        let decimal_type = format!["decimal({precision},{scale})"];
        let _error = format!("Invalid precision or scale decimal type for Arrow: {scale}");
        let decimal_field = crate::SchemaDataType::primitive(decimal_type);
        assert!(matches!(
            <ArrowDataType as TryFrom<&crate::SchemaDataType>>::try_from(&decimal_field)
                .unwrap_err(),
            arrow::error::ArrowError::SchemaError(_error),
        ));
    }

    #[test]
    fn test_arrow_from_delta_timestamp_type() {
        let timestamp_field = crate::SchemaDataType::primitive("timestamp".to_string());
        assert_eq!(
            <ArrowDataType as TryFrom<&crate::SchemaDataType>>::try_from(&timestamp_field).unwrap(),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_delta_from_arrow_timestamp_type() {
        let timestamp_field = ArrowDataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            <crate::SchemaDataType as TryFrom<&ArrowDataType>>::try_from(&timestamp_field).unwrap(),
            crate::SchemaDataType::primitive("timestamp".to_string())
        );
    }

    #[test]
    fn test_delta_from_arrow_timestamp_type_with_tz() {
        let timestamp_field =
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string().into()));
        assert_eq!(
            <crate::SchemaDataType as TryFrom<&ArrowDataType>>::try_from(&timestamp_field).unwrap(),
            crate::SchemaDataType::primitive("timestamp".to_string())
        );
    }

    #[test]
    fn test_delta_from_arrow_map_type() {
        let arrow_map = ArrowDataType::Map(
            Arc::new(ArrowField::new(
                "key_value",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("key", ArrowDataType::Int8, false),
                        ArrowField::new("value", ArrowDataType::Binary, true),
                    ]
                    .into(),
                ),
                true,
            )),
            false,
        );
        let converted_map: crate::SchemaDataType = (&arrow_map).try_into().unwrap();

        assert_eq!(
            converted_map,
            crate::SchemaDataType::map(crate::SchemaTypeMap::new(
                Box::new(crate::SchemaDataType::primitive("byte".to_string())),
                Box::new(crate::SchemaDataType::primitive("binary".to_string())),
                true,
            ))
        );
    }

    #[test]
    fn test_record_batch_from_map_type() {
        let keys = vec!["0", "1", "5", "6", "7"];
        let values: Vec<&[u8]> = vec![
            b"test_val_1",
            b"test_val_2",
            b"long_test_val_3",
            b"4",
            b"test_val_5",
        ];
        let entry_offsets = vec![0u32, 1, 1, 4, 5, 5];
        let num_rows = keys.len();

        let map_array = arrow::array::MapArray::new_from_strings(
            keys.into_iter(),
            &arrow::array::BinaryArray::from(values),
            entry_offsets.as_slice(),
        )
        .expect("Could not create a map array");

        let schema = <arrow::datatypes::Schema as TryFrom<&crate::Schema>>::try_from(
            &crate::Schema::new(vec![crate::SchemaField::new(
                "example".to_string(),
                crate::SchemaDataType::map(crate::SchemaTypeMap::new(
                    Box::new(crate::SchemaDataType::primitive("string".to_string())),
                    Box::new(crate::SchemaDataType::primitive("binary".to_string())),
                    false,
                )),
                false,
                HashMap::new(),
            )]),
        )
        .expect("Could not get schema");

        let record_batch =
            arrow::record_batch::RecordBatch::try_new(Arc::new(schema), vec![Arc::new(map_array)])
                .expect("Failed to create RecordBatch");

        assert_eq!(record_batch.num_columns(), 1);
        assert_eq!(record_batch.num_rows(), num_rows);
    }

    #[test]
    fn test_max_min_schema_for_fields() {
        let mut max_min_vec: Vec<ArrowField> = Vec::new();
        let fields = [
            ArrowField::new("simple", ArrowDataType::Int32, true),
            ArrowField::new(
                "struct",
                ArrowDataType::Struct(
                    vec![ArrowField::new("simple", ArrowDataType::Int32, true)].into(),
                ),
                true,
            ),
            ArrowField::new(
                "list",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "simple",
                    ArrowDataType::Int32,
                    true,
                ))),
                true,
            ),
            ArrowField::new(
                "map",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "struct",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new("key", ArrowDataType::Int32, true),
                                ArrowField::new("value", ArrowDataType::Int32, true),
                            ]
                            .into(),
                        ),
                        true,
                    )),
                    true,
                ),
                true,
            ),
        ];

        let expected = vec![fields[0].clone(), fields[1].clone()];

        fields
            .iter()
            .for_each(|f| max_min_schema_for_fields(&mut max_min_vec, f));

        assert_eq!(max_min_vec, expected);
    }

    #[test]
    fn test_null_count_schema_for_fields() {
        let mut null_count_vec: Vec<ArrowField> = Vec::new();
        let fields = [
            ArrowField::new("int32", ArrowDataType::Int32, true),
            ArrowField::new("int64", ArrowDataType::Int64, true),
            ArrowField::new("Utf8", ArrowDataType::Utf8, true),
            ArrowField::new(
                "list",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "simple",
                    ArrowDataType::Int32,
                    true,
                ))),
                true,
            ),
            ArrowField::new(
                "map",
                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "struct",
                        ArrowDataType::Struct(
                            vec![
                                ArrowField::new("key", ArrowDataType::Int32, true),
                                ArrowField::new("value", ArrowDataType::Int32, true),
                            ]
                            .into(),
                        ),
                        true,
                    )),
                    true,
                ),
                true,
            ),
            ArrowField::new(
                "struct",
                ArrowDataType::Struct(
                    vec![ArrowField::new("int32", ArrowDataType::Int32, true)].into(),
                ),
                true,
            ),
        ];
        let expected = vec![
            ArrowField::new(fields[0].name(), ArrowDataType::Int64, true),
            ArrowField::new(fields[1].name(), ArrowDataType::Int64, true),
            ArrowField::new(fields[2].name(), ArrowDataType::Int64, true),
            ArrowField::new(fields[3].name(), ArrowDataType::Int64, true),
            ArrowField::new(fields[4].name(), ArrowDataType::Int64, true),
            ArrowField::new(
                fields[5].name(),
                ArrowDataType::Struct(
                    vec![ArrowField::new("int32", ArrowDataType::Int64, true)].into(),
                ),
                true,
            ),
        ];
        fields
            .iter()
            .for_each(|f| null_count_schema_for_fields(&mut null_count_vec, f));
        assert_eq!(null_count_vec, expected);
    }

    /*
     * This test validates the trait implementation of
     * TryFrom<&Arc<ArrowField>> for schema::SchemaField which is required with Arrow 37 since
     * iterators on Fields will give an &Arc<ArrowField>
     */
    #[test]
    fn tryfrom_arrowfieldref_with_structs() {
        let field = Arc::new(ArrowField::new(
            "test_struct",
            ArrowDataType::Struct(
                vec![
                    ArrowField::new("key", ArrowDataType::Int32, true),
                    ArrowField::new("value", ArrowDataType::Int32, true),
                ]
                .into(),
            ),
            true,
        ));
        let _converted: schema::SchemaField = field.as_ref().try_into().unwrap();
    }
}
