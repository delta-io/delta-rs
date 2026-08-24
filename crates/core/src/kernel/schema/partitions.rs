//! Delta Table partition handling logic.
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use delta_kernel::expressions::{Expression, JunctionPredicateOp, Predicate, Scalar};
use delta_kernel::schema::StructType;

use crate::errors::{DeltaResult, DeltaTableError};

/// A special value used in Hive to represent the null partition in partitioned tables
pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

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

/// The comparison operator of a `(column, op, value)` filter literal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterOp {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `in`
    In,
    /// `not in`
    NotIn,
}

impl FilterOp {
    /// The operator string accepted by [`FromStr`], e.g. `"="` or `"not in"`.
    pub fn as_str(self) -> &'static str {
        match self {
            FilterOp::Eq => "=",
            FilterOp::Ne => "!=",
            FilterOp::Lt => "<",
            FilterOp::Le => "<=",
            FilterOp::Gt => ">",
            FilterOp::Ge => ">=",
            FilterOp::In => "in",
            FilterOp::NotIn => "not in",
        }
    }

    /// Set operators compare against a [`FilterValue::Set`], scalar operators
    /// against a [`FilterValue::Scalar`].
    fn matches_value(self, value: &FilterValue<'_>) -> bool {
        matches!(self, FilterOp::In | FilterOp::NotIn) == matches!(value, FilterValue::Set(_))
    }
}

impl fmt::Display for FilterOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for FilterOp {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "=" => FilterOp::Eq,
            "!=" => FilterOp::Ne,
            "<" => FilterOp::Lt,
            "<=" => FilterOp::Le,
            ">" => FilterOp::Gt,
            ">=" => FilterOp::Ge,
            "in" => FilterOp::In,
            "not in" => FilterOp::NotIn,
            _ => {
                return Err(DeltaTableError::InvalidPartitionFilter {
                    partition_filter: format!("unknown operator {s:?}"),
                });
            }
        })
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
pub type FilterLiteral<'a> = (&'a str, FilterOp, FilterValue<'a>);

/// Validate a raw `(column, op, value)` tuple into a [`FilterLiteral`],
/// parsing the operator string.
///
/// This is the boundary where stringly-typed filters (e.g. from FFI) enter:
/// an unknown operator, an empty column name, or an operator/value shape
/// mismatch all yield the pinned `InvalidPartitionFilter` error.
pub fn filter_literal<'a>(
    column: &'a str,
    op: &str,
    value: FilterValue<'a>,
) -> DeltaResult<FilterLiteral<'a>> {
    match op.parse::<FilterOp>() {
        Ok(parsed) if !column.is_empty() && parsed.matches_value(&value) => {
            Ok((column, parsed, value))
        }
        _ => Err(invalid_filter_error(column, op, &value)),
    }
}

/// Render a filter literal as the predicate string recorded in commit metadata
/// (`operationParameters.predicate`), e.g. `key = 'value'` or `key IN ('a', 'b')`.
pub fn literal_to_predicate_string(literal: &FilterLiteral<'_>) -> String {
    let (column, op, value) = literal;
    match value {
        FilterValue::Scalar(v) => format!("{column} {op} '{v}'"),
        // upper case for IN and NOT IN, similar to SQL
        FilterValue::Set(vs) => {
            let op = match op {
                FilterOp::In => "IN",
                FilterOp::NotIn => "NOT IN",
                other => other.as_str(),
            };
            let quoted: Vec<String> = vs.iter().map(|v| format!("'{v}'")).collect();
            format!("{column} {op} ({})", quoted.join(", "))
        }
    }
}

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
    if column.is_empty() || !op.matches_value(value) {
        return Err(invalid_filter_error(column, op.as_str(), value));
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
    Ok(match (op, value) {
        // NOTE: In SQL NULL is not equal to anything, including itself. However when specifying partition filters
        // we have allowed to equality against null. So here we have to handle null values explicitly by using
        // is_null and is_not_null methods directly.
        (FilterOp::Eq, FilterValue::Scalar(raw)) => {
            let scalar = dt.parse_scalar(raw)?;
            if scalar.is_null() {
                col.is_null()
            } else {
                col.eq(scalar)
            }
        }
        (FilterOp::Ne, FilterValue::Scalar(raw)) => {
            let scalar = dt.parse_scalar(raw)?;
            if scalar.is_null() {
                col.is_not_null()
            } else {
                col.ne(scalar)
            }
        }
        (FilterOp::Lt, FilterValue::Scalar(raw)) => col.lt(dt.parse_scalar(raw)?),
        (FilterOp::Le, FilterValue::Scalar(raw)) => col.le(dt.parse_scalar(raw)?),
        (FilterOp::Gt, FilterValue::Scalar(raw)) => col.gt(dt.parse_scalar(raw)?),
        (FilterOp::Ge, FilterValue::Scalar(raw)) => col.ge(dt.parse_scalar(raw)?),
        (op @ (FilterOp::In | FilterOp::NotIn), FilterValue::Set(raws)) => {
            let values = raws
                .iter()
                .map(|v| dt.parse_scalar(v))
                .collect::<Result<Vec<_>, _>>()?;
            let (term, junction): (Box<dyn Fn(Scalar) -> Predicate>, _) =
                if matches!(op, FilterOp::In) {
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

fn invalid_filter_error(column: &str, op: &str, value: &FilterValue<'_>) -> DeltaTableError {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::StructField;
    use delta_kernel::schema::{DataType, PrimitiveType};

    #[test]
    fn test_filter_op_from_str() {
        let cases = [
            ("=", FilterOp::Eq),
            ("!=", FilterOp::Ne),
            ("<", FilterOp::Lt),
            ("<=", FilterOp::Le),
            (">", FilterOp::Gt),
            (">=", FilterOp::Ge),
            ("in", FilterOp::In),
            ("not in", FilterOp::NotIn),
        ];
        for (raw, op) in cases {
            assert_eq!(raw.parse::<FilterOp>().unwrap(), op);
            assert_eq!(op.as_str(), raw);
        }
        for raw in ["==", "IN", "NOT IN", "like", "=>", ""] {
            assert!(raw.parse::<FilterOp>().is_err(), "{raw:?} must not parse");
        }
    }

    #[test]
    fn test_filter_literal_parsing() {
        assert_eq!(
            filter_literal("date", "=", FilterValue::Scalar("2022-05-22")).unwrap(),
            ("date", FilterOp::Eq, FilterValue::Scalar("2022-05-22")),
        );
        assert_eq!(
            filter_literal("date", "not in", FilterValue::Set(vec!["a", "b"])).unwrap(),
            ("date", FilterOp::NotIn, FilterValue::Set(vec!["a", "b"])),
        );

        // unknown op, empty column, and op/value shape mismatches all surface
        // the pinned InvalidPartitionFilter message
        let err = filter_literal("col", "=>", FilterValue::Scalar("3")).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"Invalid partition filter found: ("col", "=>", "3")."#
        );
        let err = filter_literal("col", "=", FilterValue::Set(vec!["3", "20"])).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"Invalid partition filter found: ("col", "=", ["3", "20"])."#
        );
        assert!(filter_literal("col", "in", FilterValue::Scalar("3")).is_err());
        assert!(filter_literal("", "=", FilterValue::Scalar("3")).is_err());
    }

    #[test]
    fn test_literal_to_predicate_string() {
        let cases = [
            (
                ("date", FilterOp::Eq, FilterValue::Scalar("2022-05-22")),
                "date = '2022-05-22'",
            ),
            (
                ("date", FilterOp::Ne, FilterValue::Scalar("2022-05-22")),
                "date != '2022-05-22'",
            ),
            (
                ("date", FilterOp::Gt, FilterValue::Scalar("2022-05-22")),
                "date > '2022-05-22'",
            ),
            (
                ("date", FilterOp::Ge, FilterValue::Scalar("2022-05-22")),
                "date >= '2022-05-22'",
            ),
            (
                ("date", FilterOp::Lt, FilterValue::Scalar("2022-05-22")),
                "date < '2022-05-22'",
            ),
            (
                ("date", FilterOp::Le, FilterValue::Scalar("2022-05-22")),
                "date <= '2022-05-22'",
            ),
            (
                (
                    "date",
                    FilterOp::In,
                    FilterValue::Set(vec!["2023-11-04", "2023-06-07"]),
                ),
                "date IN ('2023-11-04', '2023-06-07')",
            ),
            (
                (
                    "date",
                    FilterOp::NotIn,
                    FilterValue::Set(vec!["2023-11-04", "2023-06-07"]),
                ),
                "date NOT IN ('2023-11-04', '2023-06-07')",
            ),
        ];
        for (literal, expected) in cases {
            assert_eq!(literal_to_predicate_string(&literal), expected);
        }
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
    fn test_literal_to_kernel_predicate_equal() {
        let schema = StructType::try_new(vec![
            StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
            StructField::new("age", DataType::Primitive(PrimitiveType::Integer), true),
        ])
        .unwrap();
        let literal = ("name", FilterOp::Eq, FilterValue::Scalar("Alice"));

        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();

        let expected = Expression::column(["name"]).eq(Scalar::String("Alice".into()));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_literal_to_kernel_predicate_not_equal() {
        let schema = StructType::try_new(vec![StructField::new(
            "status",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();
        let literal = ("status", FilterOp::Ne, FilterValue::Scalar("inactive"));

        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();

        let expected = Expression::column(["status"]).ne(Scalar::String("inactive".into()));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_literal_to_kernel_predicate_comparisons() {
        let schema = StructType::try_new(vec![
            StructField::new("score", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("price", DataType::Primitive(PrimitiveType::Long), true),
        ])
        .unwrap();

        let literal = ("score", FilterOp::Lt, FilterValue::Scalar("100"));
        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();
        let expected = Expression::column(["score"]).lt(Scalar::Integer(100));
        assert_eq!(predicate, expected);

        let literal = ("score", FilterOp::Le, FilterValue::Scalar("100"));
        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();
        let expected = Expression::column(["score"]).le(Scalar::Integer(100));
        assert_eq!(predicate, expected);

        let literal = ("price", FilterOp::Gt, FilterValue::Scalar("50"));
        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();
        let expected = Expression::column(["price"]).gt(Scalar::Long(50));
        assert_eq!(predicate, expected);

        let literal = ("price", FilterOp::Ge, FilterValue::Scalar("50"));
        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();
        let expected = Expression::column(["price"]).ge(Scalar::Long(50));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_literal_to_kernel_predicate_in_operations() {
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

        let literal = (
            "category",
            FilterOp::In,
            FilterValue::Set(vec!["books", "electronics"]),
        );
        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();
        let expected_inner = categories
            .clone()
            .into_iter()
            .map(|s| column.clone().eq(s))
            .collect::<Vec<_>>();
        let expected = Predicate::junction(JunctionPredicateOp::Or, expected_inner);
        assert_eq!(predicate, expected);

        let literal = (
            "category",
            FilterOp::NotIn,
            FilterValue::Set(vec!["books", "electronics"]),
        );
        let predicate = literal_to_kernel_predicate(&literal, &schema).unwrap();
        let expected_inner = categories
            .into_iter()
            .map(|s| column.clone().ne(s))
            .collect::<Vec<_>>();
        let expected = Predicate::junction(JunctionPredicateOp::And, expected_inner);
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_literal_to_kernel_predicate_empty_in_list() {
        let schema = StructType::try_new(vec![StructField::new(
            "tag",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();

        let literal = ("tag", FilterOp::In, FilterValue::Set(vec![]));
        assert!(literal_to_kernel_predicate(&literal, &schema).is_ok());
    }

    #[test]
    fn test_literal_to_kernel_predicate_field_not_found() {
        let schema = StructType::try_new(vec![StructField::new(
            "existing_field",
            DataType::Primitive(PrimitiveType::String),
            true,
        )])
        .unwrap();

        let literal = ("nonexistent_field", FilterOp::Eq, FilterValue::Scalar("v"));
        let result = literal_to_kernel_predicate(&literal, &schema);
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn test_literal_to_kernel_predicate_non_primitive_field() {
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

        let literal = ("nested", FilterOp::Eq, FilterValue::Scalar("value"));
        let result = literal_to_kernel_predicate(&literal, &schema);
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn test_literal_to_kernel_predicate_different_data_types() {
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

        let literal = ("bool_field", FilterOp::Eq, FilterValue::Scalar("true"));
        assert!(literal_to_kernel_predicate(&literal, &schema).is_ok());

        let literal = (
            "date_field",
            FilterOp::Gt,
            FilterValue::Scalar("2023-01-01"),
        );
        assert!(literal_to_kernel_predicate(&literal, &schema).is_ok());

        let literal = ("float_field", FilterOp::Lt, FilterValue::Scalar("3.14"));
        assert!(literal_to_kernel_predicate(&literal, &schema).is_ok());
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
                ("year", FilterOp::Eq, FilterValue::Scalar("2020")),
                ("month", FilterOp::Eq, FilterValue::Scalar("2")),
            ],
            vec![("year", FilterOp::Eq, FilterValue::Scalar("2021"))],
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
        let literal = ("year", FilterOp::Ge, FilterValue::Scalar("2021"));

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
    fn test_literal_to_kernel_predicate_shape_mismatch() {
        let schema = dnf_test_schema();

        // scalar ops reject set values and vice versa
        let result = literal_to_kernel_predicate(
            &("year", FilterOp::Eq, FilterValue::Set(vec!["2021"])),
            &schema,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::InvalidPartitionFilter { .. }
        ));
        let result = literal_to_kernel_predicate(
            &("year", FilterOp::In, FilterValue::Scalar("2021")),
            &schema,
        );
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::InvalidPartitionFilter { .. }
        ));
    }

    #[test]
    fn test_literal_to_kernel_predicate_invalid_scalar_value() {
        let schema = StructType::try_new(vec![StructField::new(
            "number",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )])
        .unwrap();

        let literal = ("number", FilterOp::Eq, FilterValue::Scalar("not_a_number"));
        assert!(literal_to_kernel_predicate(&literal, &schema).is_err());
    }
}
