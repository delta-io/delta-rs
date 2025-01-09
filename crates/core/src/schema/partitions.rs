//! Delta Table partition handling logic.
use std::convert::TryFrom;

use delta_kernel::expressions::{ArrayData, BinaryOperator, Expression, Scalar, UnaryOperator};
use delta_kernel::schema::{ArrayType, Schema};
use delta_kernel::DeltaResult as KernelResult;
use serde::{Serialize, Serializer};

use crate::errors::DeltaTableError;
use crate::DeltaResult;

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

fn filter_to_expression(filter: &PartitionFilter, schema: &Schema) -> DeltaResult<Expression> {
    let field = schema.field(&filter.key).ok_or_else(|| {
        DeltaTableError::generic(format!(
            "Partition column not defined in schema: {}",
            &filter.key
        ))
    })?;
    let col = Expression::column([field.name().as_str()]);
    let partition_type = field.data_type().as_primitive_opt().ok_or_else(|| {
        DeltaTableError::InvalidPartitionFilter {
            partition_filter: filter.key.to_string(),
        }
    })?;
    let to_literal = |value: &str| -> KernelResult<_> {
        Ok(Expression::literal(partition_type.parse_scalar(value)?))
    };

    match &filter.value {
        PartitionValue::Equal(value) => {
            let literal = to_literal(value.as_str())?;
            if matches!(literal, Expression::Literal(Scalar::Null(_))) {
                Ok(col.is_null())
            } else {
                Ok(col.eq(literal))
            }
        }
        PartitionValue::NotEqual(value) => {
            let literal = to_literal(value.as_str())?;
            let expression = if matches!(literal, Expression::Literal(Scalar::Null(_))) {
                col.is_null()
            } else {
                col.eq(literal)
            };
            Ok(Expression::unary(UnaryOperator::Not, expression))
        }
        PartitionValue::GreaterThan(value) => Ok(col.gt(to_literal(value.as_str())?)),
        PartitionValue::GreaterThanOrEqual(value) => Ok(col.gt_eq(to_literal(value.as_str())?)),
        PartitionValue::LessThan(value) => Ok(col.lt(to_literal(value.as_str())?)),
        PartitionValue::LessThanOrEqual(value) => Ok(col.lt_eq(to_literal(value.as_str())?)),
        PartitionValue::In(values) | PartitionValue::NotIn(values) => {
            let values = values
                .iter()
                .map(|v| partition_type.parse_scalar(v))
                .collect::<KernelResult<Vec<_>>>()?;
            let array = Expression::literal(Scalar::Array(ArrayData::new(
                ArrayType::new(field.data_type().clone(), false),
                values,
            )));
            match &filter.value {
                PartitionValue::In(_) => Ok(Expression::binary(BinaryOperator::In, col, array)),
                PartitionValue::NotIn(_) => {
                    Ok(Expression::binary(BinaryOperator::NotIn, col, array))
                }
                _ => unreachable!(),
            }
        }
    }
}

/// Partition filters methods for filtering the DeltaTable partitions.
impl PartitionFilter {
    pub fn to_expression(&self, schema: &Schema) -> DeltaResult<Expression> {
        filter_to_expression(self, schema)
    }
}

/// Create desired string representation for PartitionFilter.
/// Used in places like predicate in operationParameters, etc.
impl Serialize for PartitionFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match &self.value {
            PartitionValue::Equal(value) => format!("{} = '{}'", self.key, value),
            PartitionValue::NotEqual(value) => format!("{} != '{}'", self.key, value),
            PartitionValue::GreaterThan(value) => format!("{} > '{}'", self.key, value),
            PartitionValue::GreaterThanOrEqual(value) => format!("{} >= '{}'", self.key, value),
            PartitionValue::LessThan(value) => format!("{} < '{}'", self.key, value),
            PartitionValue::LessThanOrEqual(value) => format!("{} <= '{}'", self.key, value),
            // used upper case for IN and NOT similar to SQL
            PartitionValue::In(values) => {
                let quoted_values: Vec<String> =
                    values.iter().map(|v| format!("'{}'", v)).collect();
                format!("{} IN ({})", self.key, quoted_values.join(", "))
            }
            PartitionValue::NotIn(values) => {
                let quoted_values: Vec<String> =
                    values.iter().map(|v| format!("'{}'", v)).collect();
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
        let partition_splitted: Vec<&str> = partition.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(DeltaTablePartition {
                key: partition_splitted[0].to_owned(),
                value: Scalar::String(partition_splitted[1].to_owned()),
            }),
            _ => Err(DeltaTableError::PartitionError {
                partition: partition.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
