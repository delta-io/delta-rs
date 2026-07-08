//! Delta Table partition handling logic.
use std::convert::TryFrom;

use delta_kernel::expressions::{Expression, JunctionPredicateOp, Predicate, Scalar};
use delta_kernel::schema::StructType;
use serde::{Serialize, Serializer};

use crate::errors::{DeltaResult, DeltaTableError};

/// A special value used in Hive to represent the null partition in partitioned tables
pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

/// A Enum used for selecting the partition value operation when filtering a DeltaTable partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionValue {
    /// The partition value with the equal operator
    Equal(String),
    /// The partition value with the not equal operator
    NotEqual(String),
    /// The partition value with the greater than operator
    GreaterThan(String),
    /// The partition value with the greater than or equal operator
    GreaterThanOrEqual(String),
    /// The partition value with the less than operator
    LessThan(String),
    /// The partition value with the less than or equal operator
    LessThanOrEqual(String),
    /// The partition values with the in operator
    In(Vec<String>),
    /// The partition values with the not in operator
    NotIn(Vec<String>),
}

/// A Struct used for filtering a DeltaTable partition by key and value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionFilter {
    /// The key of the PartitionFilter
    pub key: String,
    /// The value of the PartitionFilter
    pub value: PartitionValue,
}

/// Create desired string representation for PartitionFilter.
/// Used in places like predicate in operationParameters, etc.
impl Serialize for PartitionFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match &self.value {
            PartitionValue::Equal(value) => format!("{} = '{value}'", self.key),
            PartitionValue::NotEqual(value) => format!("{} != '{value}'", self.key),
            PartitionValue::GreaterThan(value) => format!("{} > '{value}'", self.key),
            PartitionValue::GreaterThanOrEqual(value) => format!("{} >= '{value}'", self.key),
            PartitionValue::LessThan(value) => format!("{} < '{value}'", self.key),
            PartitionValue::LessThanOrEqual(value) => format!("{} <= '{value}'", self.key),
            // used upper case for IN and NOT similar to SQL
            PartitionValue::In(values) => {
                let quoted_values: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
                format!("{} IN ({})", self.key, quoted_values.join(", "))
            }
            PartitionValue::NotIn(values) => {
                let quoted_values: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
                format!("{} NOT IN ({})", self.key, quoted_values.join(", "))
            }
        };
        serializer.serialize_str(&s)
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, value).
impl TryFrom<(&str, &str, &str)> for PartitionFilter {
    type Error = DeltaTableError;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, value).
    /// Returns a DeltaTableError in case of a malformed filter.
    fn try_from(filter: (&str, &str, &str)) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::Equal(value.to_owned()),
            }),
            (key, "!=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotEqual(value.to_owned()),
            }),
            (key, ">", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThan(value.to_owned()),
            }),
            (key, ">=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThanOrEqual(value.to_owned()),
            }),
            (key, "<", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThan(value.to_owned()),
            }),
            (key, "<=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThanOrEqual(value.to_owned()),
            }),
            (_, _, _) => Err(DeltaTableError::InvalidPartitionFilter {
                partition_filter: format!("{filter:?}"),
            }),
        }
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, list(value)).
impl TryFrom<(&str, &str, &[&str])> for PartitionFilter {
    type Error = DeltaTableError;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, list(value)).
    /// Returns a DeltaTableError in case of a malformed filter.
    fn try_from(filter: (&str, &str, &[&str])) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "in", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::In(value.iter().map(|x| x.to_string()).collect()),
            }),
            (key, "not in", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotIn(value.iter().map(|x| x.to_string()).collect()),
            }),
            (_, _, _) => Err(DeltaTableError::InvalidPartitionFilter {
                partition_filter: format!("{filter:?}"),
            }),
        }
    }
}

/// A Struct DeltaTablePartition used to represent a partition of a DeltaTable.
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaTablePartition {
    /// The key of the DeltaTable partition.
    pub key: String,
    /// The value of the DeltaTable partition.
    pub value: Scalar,
}

impl Eq for DeltaTablePartition {}

impl DeltaTablePartition {
    /// Create a DeltaTable partition from a Tuple of (key, value).
    pub fn from_partition_value(partition_value: (&str, &Scalar)) -> Self {
        let (k, v) = partition_value;
        DeltaTablePartition {
            key: k.to_owned(),
            value: v.to_owned(),
        }
    }
}

///
/// A HivePartition string is represented by a "key=value" format.
///
/// ```rust
/// # use delta_kernel::expressions::Scalar;
/// use deltalake_core::DeltaTablePartition;
///
/// let hive_part = "ds=2023-01-01";
/// let partition = DeltaTablePartition::try_from(hive_part).unwrap();
/// assert_eq!("ds", partition.key);
/// assert_eq!(Scalar::String("2023-01-01".into()), partition.value);
/// ```
impl TryFrom<&str> for DeltaTablePartition {
    type Error = DeltaTableError;

    /// Try to create a DeltaTable partition from a HivePartition string.
    /// Returns a DeltaTableError if the string is not in the form of a HivePartition.
    fn try_from(partition: &str) -> Result<Self, DeltaTableError> {
        let partition_split: Vec<&str> = partition.split('=').collect();
        match partition_split {
            partition_split if partition_split.len() == 2 => Ok(DeltaTablePartition {
                key: partition_split[0].to_owned(),
                value: Scalar::String(partition_split[1].to_owned()),
            }),
            _ => Err(DeltaTableError::PartitionError {
                partition: partition.to_string(),
            }),
        }
    }
}

/// The value of a `(column, op, value)` filter literal: a single partition-value
/// encoded string, or a set of them for `in` / `not in`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilterValue<'a> {
    /// A single encoded value, compared with one of `=`, `!=`, `<`, `<=`, `>`, `>=`.
    Scalar(&'a str),
    /// A set of encoded values, compared with `in` or `not in`.
    Set(Vec<&'a str>),
}

/// A `(column, op, value)` comparison, mirroring the tuple filters accepted by
/// the Python bindings.
pub type FilterLiteral<'a> = (&'a str, &'a str, FilterValue<'a>);

/// Translate a single filter literal into a kernel [`Predicate`].
///
/// The raw value is parsed against the schema type of `column`. A null scalar
/// under `=` / `!=` becomes an IS [NOT] NULL check: in SQL NULL compares equal
/// to nothing, itself included, but these filters have always allowed equality
/// against the null partition value.
pub fn literal_to_kernel_predicate(
    literal: &FilterLiteral<'_>,
    table_schema: &StructType,
) -> DeltaResult<Predicate> {
    let (column, op, value) = literal;
    let op_matches_value = match value {
        FilterValue::Scalar(_) => matches!(*op, "=" | "!=" | "<" | "<=" | ">" | ">="),
        FilterValue::Set(_) => matches!(*op, "in" | "not in"),
    };
    if column.is_empty() || !op_matches_value {
        return Err(invalid_filter_error(literal));
    }
    let Some(field) = table_schema.field(column) else {
        return Err(DeltaTableError::SchemaMismatch {
            msg: format!("Field '{column}' is not a root table field."),
        });
    };
    let Some(dt) = field.data_type().as_primitive_opt() else {
        return Err(DeltaTableError::SchemaMismatch {
            msg: format!("Field '{}' is not a primitive type", field.name()),
        });
    };

    let col = Expression::column([field.name()]);
    Ok(match (*op, value) {
        ("=", FilterValue::Scalar(raw)) => {
            let scalar = dt.parse_scalar(raw)?;
            if scalar.is_null() {
                col.is_null()
            } else {
                col.eq(scalar)
            }
        }
        ("!=", FilterValue::Scalar(raw)) => {
            let scalar = dt.parse_scalar(raw)?;
            if scalar.is_null() {
                col.is_not_null()
            } else {
                col.ne(scalar)
            }
        }
        ("<", FilterValue::Scalar(raw)) => col.lt(dt.parse_scalar(raw)?),
        ("<=", FilterValue::Scalar(raw)) => col.le(dt.parse_scalar(raw)?),
        (">", FilterValue::Scalar(raw)) => col.gt(dt.parse_scalar(raw)?),
        (">=", FilterValue::Scalar(raw)) => col.ge(dt.parse_scalar(raw)?),
        (op @ ("in" | "not in"), FilterValue::Set(raws)) => {
            let values = raws
                .iter()
                .map(|v| dt.parse_scalar(v))
                .collect::<Result<Vec<_>, _>>()?;
            let (term, junction): (Box<dyn Fn(Scalar) -> Predicate>, _) = if op == "in" {
                (Box::new(|v| col.clone().eq(v)), JunctionPredicateOp::Or)
            } else {
                (Box::new(|v| col.clone().ne(v)), JunctionPredicateOp::And)
            };
            let predicates = values.into_iter().map(term).collect::<Vec<_>>();
            Predicate::junction(junction, predicates)
        }
        _ => unreachable!("op/value shapes checked above"),
    })
}

fn invalid_filter_error(literal: &FilterLiteral<'_>) -> DeltaTableError {
    let (column, op, value) = literal;
    let value = match value {
        FilterValue::Scalar(v) => format!("{v:?}"),
        FilterValue::Set(vs) => format!("{vs:?}"),
    };
    DeltaTableError::InvalidPartitionFilter {
        partition_filter: format!("({column:?}, {op:?}, {value})"),
    }
}

/// Translate a conjunction (AND) of filter literals into a kernel [`Predicate`].
///
/// Errors on an empty conjunction: an empty AND is vacuously true and would
/// silently match every file.
pub fn conjunction_to_kernel_predicate(
    literals: &[FilterLiteral<'_>],
    table_schema: &StructType,
) -> DeltaResult<Predicate> {
    if literals.is_empty() {
        return Err(DeltaTableError::Generic(
            "empty conjunction in filter; pass no filter to match all files".to_string(),
        ));
    }
    let mut predicates = literals
        .iter()
        .map(|literal| literal_to_kernel_predicate(literal, table_schema))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(match predicates.len() {
        1 => predicates.pop().unwrap(),
        _ => Predicate::junction(JunctionPredicateOp::And, predicates),
    })
}

/// Translate filters in disjunctive normal form -- an OR across conjunctions
/// (AND groups) of `(column, op, value)` literals -- into a kernel [`Predicate`].
pub fn dnf_to_kernel_predicate(
    dnf: &[Vec<FilterLiteral<'_>>],
    table_schema: &StructType,
) -> DeltaResult<Predicate> {
    if dnf.is_empty() {
        return Err(DeltaTableError::Generic(
            "empty filter; pass no filter to match all files".to_string(),
        ));
    }
    let mut groups = dnf
        .iter()
        .map(|conjunction| conjunction_to_kernel_predicate(conjunction, table_schema))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(match groups.len() {
        1 => groups.pop().unwrap(),
        _ => Predicate::junction(JunctionPredicateOp::Or, groups),
    })
}

/// Translate a conjunction of [`PartitionFilter`]s into a kernel [`Predicate`].
///
/// Unlike [`dnf_to_kernel_predicate`], an empty slice yields an empty AND
/// junction, which is vacuously true: callers treat "no filters" as "match
/// every file".
pub fn to_kernel_predicate(
    filters: &[PartitionFilter],
    table_schema: &StructType,
) -> DeltaResult<Predicate> {
    let predicates = filters
        .iter()
        .map(|filter| filter_to_kernel_predicate(filter, table_schema))
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(Predicate::junction(JunctionPredicateOp::And, predicates))
}

fn filter_to_kernel_predicate(
    filter: &PartitionFilter,
    table_schema: &StructType,
) -> DeltaResult<Predicate> {
    let (op, value) = match &filter.value {
        PartitionValue::Equal(v) => ("=", FilterValue::Scalar(v)),
        PartitionValue::NotEqual(v) => ("!=", FilterValue::Scalar(v)),
        PartitionValue::GreaterThan(v) => (">", FilterValue::Scalar(v)),
        PartitionValue::GreaterThanOrEqual(v) => (">=", FilterValue::Scalar(v)),
        PartitionValue::LessThan(v) => ("<", FilterValue::Scalar(v)),
        PartitionValue::LessThanOrEqual(v) => ("<=", FilterValue::Scalar(v)),
        PartitionValue::In(vs) => (
            "in",
            FilterValue::Set(vs.iter().map(String::as_str).collect()),
        ),
        PartitionValue::NotIn(vs) => (
            "not in",
            FilterValue::Set(vs.iter().map(String::as_str).collect()),
        ),
    };
    literal_to_kernel_predicate(&(filter.key.as_str(), op, value), table_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::StructField;
    use delta_kernel::schema::{DataType, PrimitiveType};
    use serde_json::json;

    fn check_json_serialize(filter: PartitionFilter, expected_json: &str) {
        assert_eq!(serde_json::to_value(filter).unwrap(), json!(expected_json))
    }

    #[test]
    fn test_serialize_partition_filter() {
        check_json_serialize(
            PartitionFilter::try_from(("date", "=", "2022-05-22")).unwrap(),
            "date = '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", "!=", "2022-05-22")).unwrap(),
            "date != '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", ">", "2022-05-22")).unwrap(),
            "date > '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", ">=", "2022-05-22")).unwrap(),
            "date >= '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", "<", "2022-05-22")).unwrap(),
            "date < '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", "<=", "2022-05-22")).unwrap(),
            "date <= '2022-05-22'",
        );
        check_json_serialize(
            PartitionFilter::try_from(("date", "in", vec!["2023-11-04", "2023-06-07"].as_slice()))
                .unwrap(),
            "date IN ('2023-11-04', '2023-06-07')",
        );
        check_json_serialize(
            PartitionFilter::try_from((
                "date",
                "not in",
                vec!["2023-11-04", "2023-06-07"].as_slice(),
            ))
            .unwrap(),
            "date NOT IN ('2023-11-04', '2023-06-07')",
        );
    }

    #[test]
    fn tryfrom_invalid() {
        let buf = "this-is-not-a-partition";
        let partition = DeltaTablePartition::try_from(buf);
        assert!(partition.is_err());
    }

    #[test]
    fn tryfrom_valid() {
        let buf = "ds=2024-04-01";
        let partition = DeltaTablePartition::try_from(buf);
        assert!(partition.is_ok());
        let partition = partition.unwrap();
        assert_eq!(partition.key, "ds");
        assert_eq!(partition.value, Scalar::String("2024-04-01".into()));
    }

    #[test]
    fn test_create_delta_table_partition() {
        let year = "2021".to_string();
        let path = format!("year={year}");
        assert_eq!(
            DeltaTablePartition::try_from(path.as_ref()).unwrap(),
            DeltaTablePartition {
                key: "year".into(),
                value: Scalar::String(year),
            }
        );

        let _wrong_path = "year=2021/month=";
        assert!(matches!(
            DeltaTablePartition::try_from(_wrong_path).unwrap_err(),
            DeltaTableError::PartitionError {
                partition: _wrong_path
            },
        ))
    }

    #[test]
    fn test_filter_to_kernel_predicate_equal() {
        let schema = StructType::try_new(vec![
            StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
            StructField::new("age", DataType::Primitive(PrimitiveType::Integer), true),
        ])
        .unwrap();
        let filter = PartitionFilter {
            key: "name".to_string(),
            value: PartitionValue::Equal("Alice".to_string()),
        };

        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();

        let expected = Expression::column(["name"]).eq(Scalar::String("Alice".into()));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_filter_to_kernel_predicate_not_equal() {
        let schema = StructType::try_new(vec![StructField::new(
            "status",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();
        let filter = PartitionFilter {
            key: "status".to_string(),
            value: PartitionValue::NotEqual("inactive".to_string()),
        };

        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();

        let expected = Expression::column(["status"]).ne(Scalar::String("inactive".into()));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_filter_to_kernel_predicate_comparisons() {
        let schema = StructType::try_new(vec![
            StructField::new("score", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("price", DataType::Primitive(PrimitiveType::Long), true),
        ])
        .unwrap();

        // Test less than
        let filter = PartitionFilter {
            key: "score".to_string(),
            value: PartitionValue::LessThan("100".to_string()),
        };
        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();
        let expected = Expression::column(["score"]).lt(Scalar::Integer(100));
        assert_eq!(predicate, expected);

        // Test less than or equal
        let filter = PartitionFilter {
            key: "score".to_string(),
            value: PartitionValue::LessThanOrEqual("100".to_string()),
        };
        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();
        let expected = Expression::column(["score"]).le(Scalar::Integer(100));
        assert_eq!(predicate, expected);

        // Test greater than
        let filter = PartitionFilter {
            key: "price".to_string(),
            value: PartitionValue::GreaterThan("50".to_string()),
        };
        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();
        let expected = Expression::column(["price"]).gt(Scalar::Long(50));
        assert_eq!(predicate, expected);

        // Test greater than or equal
        let filter = PartitionFilter {
            key: "price".to_string(),
            value: PartitionValue::GreaterThanOrEqual("50".to_string()),
        };
        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();
        let expected = Expression::column(["price"]).ge(Scalar::Long(50));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_filter_to_kernel_predicate_in_operations() {
        let schema = StructType::try_new(vec![StructField::new(
            "category",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();

        let column = Expression::column(["category"]);
        let categories = [
            Scalar::String("books".to_string()),
            Scalar::String("electronics".to_string()),
        ];

        // Test In operation
        let filter = PartitionFilter {
            key: "category".to_string(),
            value: PartitionValue::In(vec!["books".to_string(), "electronics".to_string()]),
        };
        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();
        let expected_inner = categories
            .clone()
            .into_iter()
            .map(|s| column.clone().eq(s))
            .collect::<Vec<_>>();
        let expected = Predicate::junction(JunctionPredicateOp::Or, expected_inner);
        assert_eq!(predicate, expected);

        // Test NotIn operation
        let filter = PartitionFilter {
            key: "category".to_string(),
            value: PartitionValue::NotIn(vec!["books".to_string(), "electronics".to_string()]),
        };
        let predicate = filter_to_kernel_predicate(&filter, &schema).unwrap();
        let expected_inner = categories
            .into_iter()
            .map(|s| column.clone().ne(s))
            .collect::<Vec<_>>();
        let expected = Predicate::junction(JunctionPredicateOp::And, expected_inner);
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_filter_to_kernel_predicate_empty_in_list() {
        let schema = StructType::try_new(vec![StructField::new(
            "tag",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();

        let filter = PartitionFilter {
            key: "tag".to_string(),
            value: PartitionValue::In(vec![]),
        };
        let result = filter_to_kernel_predicate(&filter, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_filter_to_kernel_predicate_field_not_found() {
        let schema = StructType::try_new(vec![StructField::new(
            "existing_field",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();

        let filter = PartitionFilter {
            key: "nonexistent_field".to_string(),
            value: PartitionValue::Equal("value".to_string()),
        };

        let result = filter_to_kernel_predicate(&filter, &schema);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn test_filter_to_kernel_predicate_non_primitive_field() {
        let nested_struct = StructType::try_new(vec![StructField::new(
            "inner",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();
        let schema = StructType::try_new(vec![StructField::new(
            "nested",
            DataType::Struct(Box::new(nested_struct)),
            true,
        )])
        .unwrap();

        let filter = PartitionFilter {
            key: "nested".to_string(),
            value: PartitionValue::Equal("value".to_string()),
        };

        let result = filter_to_kernel_predicate(&filter, &schema);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn test_filter_to_kernel_predicate_different_data_types() {
        let schema = StructType::try_new(vec![
            StructField::new(
                "bool_field",
                DataType::Primitive(PrimitiveType::Boolean),
                true,
            ),
            StructField::new("date_field", DataType::Primitive(PrimitiveType::Date), true),
            StructField::new(
                "timestamp_field",
                DataType::Primitive(PrimitiveType::Timestamp),
                true,
            ),
            StructField::new(
                "double_field",
                DataType::Primitive(PrimitiveType::Double),
                true,
            ),
            StructField::new(
                "float_field",
                DataType::Primitive(PrimitiveType::Float),
                true,
            ),
        ])
        .unwrap();

        // Test boolean field
        let filter = PartitionFilter {
            key: "bool_field".to_string(),
            value: PartitionValue::Equal("true".to_string()),
        };
        assert!(filter_to_kernel_predicate(&filter, &schema).is_ok());

        // Test date field
        let filter = PartitionFilter {
            key: "date_field".to_string(),
            value: PartitionValue::GreaterThan("2023-01-01".to_string()),
        };
        assert!(filter_to_kernel_predicate(&filter, &schema).is_ok());

        // Test float field
        let filter = PartitionFilter {
            key: "float_field".to_string(),
            value: PartitionValue::LessThan("3.14".to_string()),
        };
        assert!(filter_to_kernel_predicate(&filter, &schema).is_ok());
    }

    fn dnf_test_schema() -> StructType {
        StructType::try_new(vec![
            StructField::new("year", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("month", DataType::Primitive(PrimitiveType::Integer), true),
        ])
        .unwrap()
    }

    #[test]
    fn test_dnf_to_kernel_predicate_or_of_ands() {
        let schema = dnf_test_schema();
        let dnf = vec![
            vec![
                ("year", "=", FilterValue::Scalar("2020")),
                ("month", "=", FilterValue::Scalar("2")),
            ],
            vec![("year", "=", FilterValue::Scalar("2021"))],
        ];

        let predicate = dnf_to_kernel_predicate(&dnf, &schema).unwrap();

        let expected = Predicate::junction(
            JunctionPredicateOp::Or,
            vec![
                Predicate::junction(
                    JunctionPredicateOp::And,
                    vec![
                        Expression::column(["year"]).eq(Scalar::Integer(2020)),
                        Expression::column(["month"]).eq(Scalar::Integer(2)),
                    ],
                ),
                Expression::column(["year"]).eq(Scalar::Integer(2021)),
            ],
        );
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_dnf_to_kernel_predicate_single_conjunction_unwrapped() {
        let schema = dnf_test_schema();
        let literal = ("year", ">=", FilterValue::Scalar("2021"));

        let predicate = dnf_to_kernel_predicate(&[vec![literal.clone()]], &schema).unwrap();

        assert_eq!(
            predicate,
            literal_to_kernel_predicate(&literal, &schema).unwrap()
        );
    }

    #[test]
    fn test_dnf_to_kernel_predicate_empty_errors() {
        let schema = dnf_test_schema();
        assert!(matches!(
            dnf_to_kernel_predicate(&[], &schema).unwrap_err(),
            DeltaTableError::Generic(_)
        ));
        assert!(matches!(
            dnf_to_kernel_predicate(&[vec![]], &schema).unwrap_err(),
            DeltaTableError::Generic(_)
        ));
    }

    #[test]
    fn test_literal_to_kernel_predicate_invalid_op() {
        let schema = dnf_test_schema();
        let result =
            literal_to_kernel_predicate(&("year", "like", FilterValue::Scalar("2021")), &schema);
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::InvalidPartitionFilter { .. }
        ));

        // scalar ops reject set values and vice versa
        let result =
            literal_to_kernel_predicate(&("year", "=", FilterValue::Set(vec!["2021"])), &schema);
        assert!(result.is_err());
        let result =
            literal_to_kernel_predicate(&("year", "in", FilterValue::Scalar("2021")), &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_to_kernel_predicate_invalid_scalar_value() {
        let schema = StructType::try_new(vec![StructField::new(
            "number",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )])
        .unwrap();

        let filter = PartitionFilter {
            key: "number".to_string(),
            value: PartitionValue::Equal("not_a_number".to_string()),
        };

        let result = filter_to_kernel_predicate(&filter, &schema);
        assert!(result.is_err());
    }
}
