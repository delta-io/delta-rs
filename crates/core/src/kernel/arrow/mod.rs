//! Conversions between Delta and Arrow data types

use std::sync::Arc;

use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use lazy_static::lazy_static;

use super::{ActionType, ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};

pub(crate) mod extract;
pub(crate) mod json;

const MAP_ROOT_DEFAULT: &str = "entries";
const MAP_KEY_DEFAULT: &str = "keys";
const MAP_VALUE_DEFAULT: &str = "values";
const LIST_ROOT_DEFAULT: &str = "item";

impl TryFrom<ActionType> for ArrowField {
    type Error = ArrowError;

    fn try_from(value: ActionType) -> Result<Self, Self::Error> {
        value.schema_field().try_into()
    }
}

impl TryFrom<&StructType> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(s: &StructType) -> Result<Self, ArrowError> {
        let fields = s
            .fields()
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<ArrowField>, ArrowError>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<&StructField> for ArrowField {
    type Error = ArrowError;

    fn try_from(f: &StructField) -> Result<Self, ArrowError> {
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| Ok((key.clone(), serde_json::to_string(val)?)))
            .collect::<Result<_, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;

        let field = ArrowField::new(
            f.name(),
            ArrowDataType::try_from(f.data_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata);

        Ok(field)
    }
}

impl TryFrom<&ArrayType> for ArrowField {
    type Error = ArrowError;
    fn try_from(a: &ArrayType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            LIST_ROOT_DEFAULT,
            ArrowDataType::try_from(a.element_type())?,
            // TODO check how to handle nullability
            a.contains_null(),
        ))
    }
}

impl TryFrom<&MapType> for ArrowField {
    type Error = ArrowError;

    fn try_from(a: &MapType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            MAP_ROOT_DEFAULT,
            ArrowDataType::Struct(
                vec![
                    ArrowField::new(
                        MAP_KEY_DEFAULT,
                        ArrowDataType::try_from(a.key_type())?,
                        false,
                    ),
                    ArrowField::new(
                        MAP_VALUE_DEFAULT,
                        ArrowDataType::try_from(a.value_type())?,
                        a.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            // always non-null
            false,
        ))
    }
}

impl TryFrom<&DataType> for ArrowDataType {
    type Error = ArrowError;

    fn try_from(t: &DataType) -> Result<Self, ArrowError> {
        match t {
            DataType::Primitive(p) => {
                match p {
                    PrimitiveType::String => Ok(ArrowDataType::Utf8),
                    PrimitiveType::Long => Ok(ArrowDataType::Int64), // undocumented type
                    PrimitiveType::Integer => Ok(ArrowDataType::Int32),
                    PrimitiveType::Short => Ok(ArrowDataType::Int16),
                    PrimitiveType::Byte => Ok(ArrowDataType::Int8),
                    PrimitiveType::Float => Ok(ArrowDataType::Float32),
                    PrimitiveType::Double => Ok(ArrowDataType::Float64),
                    PrimitiveType::Boolean => Ok(ArrowDataType::Boolean),
                    PrimitiveType::Binary => Ok(ArrowDataType::Binary),
                    PrimitiveType::Decimal(precision, scale) => {
                        if precision <= &38 {
                            Ok(ArrowDataType::Decimal128(*precision, *scale))
                        } else if precision <= &76 {
                            Ok(ArrowDataType::Decimal256(*precision, *scale))
                        } else {
                            Err(ArrowError::SchemaError(format!(
                                "Precision too large to be represented in Arrow: {}",
                                precision
                            )))
                        }
                    }
                    PrimitiveType::Date => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone. Stored as 4 bytes integer representing days since 1970-01-01
                        Ok(ArrowDataType::Date32)
                    }
                    PrimitiveType::Timestamp => {
                        // Issue: https://github.com/delta-io/delta/issues/643
                        Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                }
            }
            DataType::Struct(s) => Ok(ArrowDataType::Struct(
                s.fields()
                    .iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(ArrowDataType::List(Arc::new(a.as_ref().try_into()?))),
            DataType::Map(m) => Ok(ArrowDataType::Map(Arc::new(m.as_ref().try_into()?), false)),
        }
    }
}

impl TryFrom<&ArrowSchema> for StructType {
    type Error = ArrowError;

    fn try_from(arrow_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        let new_fields: Result<Vec<StructField>, _> = arrow_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().try_into())
            .collect();
        Ok(StructType::new(new_fields?))
    }
}

impl TryFrom<ArrowSchemaRef> for StructType {
    type Error = ArrowError;

    fn try_from(arrow_schema: ArrowSchemaRef) -> Result<Self, ArrowError> {
        arrow_schema.as_ref().try_into()
    }
}

impl TryFrom<&ArrowField> for StructField {
    type Error = ArrowError;

    fn try_from(arrow_field: &ArrowField) -> Result<Self, ArrowError> {
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(arrow_field.metadata().iter().map(|(k, v)| (k.clone(), v))))
    }
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = ArrowError;

    fn try_from(arrow_datatype: &ArrowDataType) -> Result<Self, ArrowError> {
        match arrow_datatype {
            ArrowDataType::Utf8 => Ok(DataType::Primitive(PrimitiveType::String)),
            ArrowDataType::LargeUtf8 => Ok(DataType::Primitive(PrimitiveType::String)),
            ArrowDataType::Int64 => Ok(DataType::Primitive(PrimitiveType::Long)), // undocumented type
            ArrowDataType::Int32 => Ok(DataType::Primitive(PrimitiveType::Integer)),
            ArrowDataType::Int16 => Ok(DataType::Primitive(PrimitiveType::Short)),
            ArrowDataType::Int8 => Ok(DataType::Primitive(PrimitiveType::Byte)),
            ArrowDataType::UInt64 => Ok(DataType::Primitive(PrimitiveType::Long)), // undocumented type
            ArrowDataType::UInt32 => Ok(DataType::Primitive(PrimitiveType::Integer)),
            ArrowDataType::UInt16 => Ok(DataType::Primitive(PrimitiveType::Short)),
            ArrowDataType::UInt8 => Ok(DataType::Primitive(PrimitiveType::Byte)),
            ArrowDataType::Float32 => Ok(DataType::Primitive(PrimitiveType::Float)),
            ArrowDataType::Float64 => Ok(DataType::Primitive(PrimitiveType::Double)),
            ArrowDataType::Boolean => Ok(DataType::Primitive(PrimitiveType::Boolean)),
            ArrowDataType::Binary => Ok(DataType::Primitive(PrimitiveType::Binary)),
            ArrowDataType::FixedSizeBinary(_) => Ok(DataType::Primitive(PrimitiveType::Binary)),
            ArrowDataType::LargeBinary => Ok(DataType::Primitive(PrimitiveType::Binary)),
            ArrowDataType::Decimal128(p, s) => {
                Ok(DataType::Primitive(PrimitiveType::Decimal(*p, *s)))
            }
            ArrowDataType::Decimal256(p, s) => {
                Ok(DataType::Primitive(PrimitiveType::Decimal(*p, *s)))
            }
            ArrowDataType::Date32 => Ok(DataType::Primitive(PrimitiveType::Date)),
            ArrowDataType::Date64 => Ok(DataType::Primitive(PrimitiveType::Date)),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
                Ok(DataType::Primitive(PrimitiveType::Timestamp))
            }
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(DataType::Primitive(PrimitiveType::Timestamp))
            }
            ArrowDataType::Struct(fields) => {
                let converted_fields: Result<Vec<StructField>, _> = fields
                    .iter()
                    .map(|field| field.as_ref().try_into())
                    .collect();
                Ok(DataType::Struct(Box::new(StructType::new(
                    converted_fields?,
                ))))
            }
            ArrowDataType::List(field) => Ok(DataType::Array(Box::new(ArrayType::new(
                (*field).data_type().try_into()?,
                (*field).is_nullable(),
            )))),
            ArrowDataType::LargeList(field) => Ok(DataType::Array(Box::new(ArrayType::new(
                (*field).data_type().try_into()?,
                (*field).is_nullable(),
            )))),
            ArrowDataType::FixedSizeList(field, _) => Ok(DataType::Array(Box::new(
                ArrayType::new((*field).data_type().try_into()?, (*field).is_nullable()),
            ))),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = struct_fields[0].data_type().try_into()?;
                    let value_type = struct_fields[1].data_type().try_into()?;
                    let value_type_nullable = struct_fields[1].is_nullable();
                    Ok(DataType::Map(Box::new(MapType::new(
                        key_type,
                        value_type,
                        value_type_nullable,
                    ))))
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

macro_rules! arrow_map {
    ($fieldname: ident, null) => {
        ArrowField::new(
            stringify!($fieldname),
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    MAP_ROOT_DEFAULT,
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new(MAP_KEY_DEFAULT, ArrowDataType::Utf8, false),
                            ArrowField::new(MAP_VALUE_DEFAULT, ArrowDataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )
    };
    ($fieldname: ident, not_null) => {
        ArrowField::new(
            stringify!($fieldname),
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    MAP_ROOT_DEFAULT,
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new(MAP_KEY_DEFAULT, ArrowDataType::Utf8, false),
                            ArrowField::new(MAP_VALUE_DEFAULT, ArrowDataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )
    };
}

macro_rules! arrow_field {
    ($fieldname:ident, $type_qual:ident, null) => {
        ArrowField::new(stringify!($fieldname), ArrowDataType::$type_qual, true)
    };
    ($fieldname:ident, $type_qual:ident, not_null) => {
        ArrowField::new(stringify!($fieldname), ArrowDataType::$type_qual, false)
    };
}

macro_rules! arrow_list {
    ($fieldname:ident, $element_name:ident, $type_qual:ident, null) => {
        ArrowField::new(
            stringify!($fieldname),
            ArrowDataType::List(Arc::new(ArrowField::new(
                stringify!($element_name),
                ArrowDataType::$type_qual,
                true,
            ))),
            true,
        )
    };
    ($fieldname:ident, $element_name:ident, $type_qual:ident, not_null) => {
        ArrowField::new(
            stringify!($fieldname),
            ArrowDataType::List(Arc::new(ArrowField::new(
                stringify!($element_name),
                ArrowDataType::$type_qual,
                true,
            ))),
            false,
        )
    };
}

macro_rules! arrow_struct {
    ($fieldname:ident, [$($inner:tt)+], null) => {
        ArrowField::new(
            stringify!($fieldname),
            ArrowDataType::Struct(
                arrow_defs! [$($inner)+].into()
            ),
            true
        )
    };
    ($fieldname:ident, [$($inner:tt)+], not_null) => {
        ArrowField::new(
            stringify!($fieldname),
            ArrowDataType::Struct(
                arrow_defs! [$($inner)+].into()
            ),
            false
        )
    }
}

macro_rules! arrow_def {
    ($fieldname:ident $(null)?) => {
        arrow_map!($fieldname, null)
    };
    ($fieldname:ident not_null) => {
        arrow_map!($fieldname, not_null)
    };
    ($fieldname:ident[$inner_name:ident]{$type_qual:ident} $(null)?) => {
        arrow_list!($fieldname, $inner_name, $type_qual, null)
    };
    ($fieldname:ident[$inner_name:ident]{$type_qual:ident} not_null) => {
        arrow_list!($fieldname, $inner_name, $type_qual, not_null)
    };
    ($fieldname:ident:$type_qual:ident $(null)?) => {
        arrow_field!($fieldname, $type_qual, null)
    };
    ($fieldname:ident:$type_qual:ident not_null) => {
        arrow_field!($fieldname, $type_qual, not_null)
    };
    ($fieldname:ident[$($inner:tt)+] $(null)?) => {
        arrow_struct!($fieldname, [$($inner)+], null)
    };
    ($fieldname:ident[$($inner:tt)+] not_null) => {
        arrow_struct!($fieldname, [$($inner)+], not_null)
    }
}

/// A helper macro to create more readable Arrow field definitions, delimited by commas
///
/// The argument patterns are as follows:
///
/// fieldname (null|not_null)?      -- An arrow field of type map with name "fieldname" consisting of Utf8 key-value pairs, and an
///                                    optional nullability qualifier (null if not specified).
///
/// fieldname:type (null|not_null)? --  An Arrow field consisting of an atomic type. For example,
///                                     id:Utf8 gets mapped to ArrowField::new("id", ArrowDataType::Utf8, true).
///                                     where customerCount:Int64 not_null gets mapped to gets mapped to
///                                     ArrowField::new("customerCount", ArrowDataType::Utf8, true)
///
/// fieldname[list_element]{list_element_type} (null|not_null)? --  An Arrow list, with the name of the elements wrapped in square brackets
///                                                                 and the type of the list elements wrapped in curly brackets. For example,
///                                                                 customers[name]{Utf8} is an nullable arrow field of type arrow list consisting
///                                                                 of elements called "name" with type Utf8.
///
/// fieldname[element1, element2, element3, ....] (null|not_null)? -- An arrow struct with name "fieldname" consisting of elements adhering to any of the patterns
///                                                                   documented, including additional structs arbitrarily nested up to the recursion
///                                                                   limit for Rust macros.
macro_rules! arrow_defs {
    () => {
        vec![] as Vec<ArrowField>
    };
    ($($fieldname:ident$(:$type_qual:ident)?$([$($inner:tt)+])?$({$list_type_qual:ident})? $($nullable:ident)?),+) => {
        vec![
            $(arrow_def!($fieldname$(:$type_qual)?$([$($inner)+])?$({$list_type_qual})? $($nullable)?)),+
        ]
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
        static ref SCHEMA_FIELDS: Vec<ArrowField> = arrow_defs![
                metaData[
                    id:Utf8,
                    name:Utf8,
                    description:Utf8,
                    schemaString:Utf8,
                    createdTime:Int64,
                    partitionColumns[element]{Utf8},
                    configuration,
                    format[provider:Utf8, options]
                ],
                protocol[
                    minReaderVersion:Int32,
                    minWriterVersion:Int32
                ],
                txn[
                    appId:Utf8,
                    version:Int64
                ]
        ];
        static ref ADD_FIELDS: Vec<ArrowField> = arrow_defs![
            path:Utf8,
            size:Int64,
            modificationTime:Int64,
            dataChange:Boolean,
            stats:Utf8,
            partitionValues,
            tags,
            deletionVector[
                storageType:Utf8 not_null,
                pathOrInlineDv:Utf8 not_null,
                offset:Int32 null,
                sizeInBytes:Int32 not_null,
                cardinality:Int64 not_null
            ]
        ];
        static ref REMOVE_FIELDS: Vec<ArrowField> = arrow_defs![
            path: Utf8,
            deletionTimestamp: Int64,
            dataChange: Boolean,
            extendedFileMetadata: Boolean
        ];
        static ref REMOVE_EXTENDED_FILE_METADATA_FIELDS: Vec<ArrowField> =
            arrow_defs![size: Int64, partitionValues, tags];
    };

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

            if !child_dest.is_empty() {
                dest.push(ArrowField::new(
                    f.name(),
                    ArrowDataType::Struct(child_dest.into()),
                    true,
                ));
            }
        }
        // don't compute min or max for list, map or binary types
        ArrowDataType::List(_) | ArrowDataType::Map(_, _) | ArrowDataType::Binary => { /* noop */ }
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
    use arrow::array::ArrayData;
    use arrow_array::Array;
    use arrow_array::{make_array, ArrayRef, MapArray, StringArray, StructArray};
    use arrow_buffer::{Buffer, ToByteSlice};
    use arrow_schema::Field;

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
        let expected_fields = ["metaData", "protocol", "txn", "remove", "add"];
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
        let field_names: Vec<&String> = add_fields.iter().map(|v| v.name()).collect();
        assert_eq!(
            vec![
                "path",
                "size",
                "modificationTime",
                "dataChange",
                "stats",
                "partitionValues",
                "tags",
                "deletionVector",
                "stats_parsed",
                "partitionValues_parsed"
            ],
            field_names
        );
        let add_field_map: HashMap<_, _> = add_fields
            .iter()
            .map(|f| (f.name().to_owned(), f.clone()))
            .collect();
        let partition_values_parsed = add_field_map.get("partitionValues_parsed").unwrap();
        if let ArrowDataType::Struct(fields) = partition_values_parsed.data_type() {
            assert_eq!(1, fields.len());
            let field = fields.first().unwrap().to_owned();
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
                            let field = fields.first().unwrap().to_owned();
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
        let expected_fields = [
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
        let decimal_field = DataType::Primitive(PrimitiveType::Decimal(precision, scale));
        assert_eq!(
            <ArrowDataType as TryFrom<&DataType>>::try_from(&decimal_field).unwrap(),
            ArrowDataType::Decimal128(precision, scale)
        );
    }

    #[test]
    fn test_arrow_from_delta_timestamp_type() {
        let timestamp_field = DataType::Primitive(PrimitiveType::Timestamp);
        assert_eq!(
            <ArrowDataType as TryFrom<&DataType>>::try_from(&timestamp_field).unwrap(),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_delta_from_arrow_timestamp_type() {
        let timestamp_field = ArrowDataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            <DataType as TryFrom<&ArrowDataType>>::try_from(&timestamp_field).unwrap(),
            DataType::Primitive(PrimitiveType::Timestamp)
        );
    }

    #[test]
    fn test_delta_from_arrow_timestamp_type_with_tz() {
        let timestamp_field =
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string().into()));
        assert_eq!(
            <DataType as TryFrom<&ArrowDataType>>::try_from(&timestamp_field).unwrap(),
            DataType::Primitive(PrimitiveType::Timestamp)
        );
    }

    #[test]
    fn test_delta_from_arrow_map_type() {
        let arrow_map = ArrowDataType::Map(
            Arc::new(ArrowField::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("key", ArrowDataType::Int8, false),
                        ArrowField::new("value", ArrowDataType::Binary, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let converted_map: DataType = (&arrow_map).try_into().unwrap();

        assert_eq!(
            converted_map,
            DataType::Map(Box::new(MapType::new(
                DataType::Primitive(PrimitiveType::Byte),
                DataType::Primitive(PrimitiveType::Binary),
                true,
            )))
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

        // Copied the function `new_from_string` with the patched code from https://github.com/apache/arrow-rs/pull/4808
        // This should be reverted back [`MapArray::new_from_strings`] once arrow is upgraded in this project.
        fn new_from_strings<'a>(
            keys: impl Iterator<Item = &'a str>,
            values: &dyn Array,
            entry_offsets: &[u32],
        ) -> Result<MapArray, ArrowError> {
            let entry_offsets_buffer = Buffer::from(entry_offsets.to_byte_slice());
            let keys_data = StringArray::from_iter_values(keys);

            let keys_field = Arc::new(Field::new("keys", ArrowDataType::Utf8, false));
            let values_field = Arc::new(Field::new(
                "values",
                values.data_type().clone(),
                values.null_count() > 0,
            ));

            let entry_struct = StructArray::from(vec![
                (keys_field, Arc::new(keys_data) as ArrayRef),
                (values_field, make_array(values.to_data())),
            ]);

            let map_data_type = ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    entry_struct.data_type().clone(),
                    false,
                )),
                false,
            );

            let map_data = ArrayData::builder(map_data_type)
                .len(entry_offsets.len() - 1)
                .add_buffer(entry_offsets_buffer)
                .add_child_data(entry_struct.into_data())
                .build()?;

            Ok(MapArray::from(map_data))
        }

        let map_array = new_from_strings(
            keys.into_iter(),
            &arrow::array::BinaryArray::from(values),
            entry_offsets.as_slice(),
        )
        .expect("Could not create a map array");

        let schema =
            <arrow::datatypes::Schema as TryFrom<&StructType>>::try_from(&StructType::new(vec![
                StructField::new(
                    "example".to_string(),
                    DataType::Map(Box::new(MapType::new(
                        DataType::Primitive(PrimitiveType::String),
                        DataType::Primitive(PrimitiveType::Binary),
                        false,
                    ))),
                    false,
                ),
            ]))
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
            ArrowField::new("binary", ArrowDataType::Binary, true),
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
        let _converted: StructField = field.as_ref().try_into().unwrap();
    }
}
