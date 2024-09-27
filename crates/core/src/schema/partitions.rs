//! Delta Table partition handling logic.
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;

use delta_kernel::expressions::Scalar;
use serde::{Serialize, Serializer};

use crate::errors::DeltaTableError;
use crate::kernel::{scalars::ScalarExt, DataType, PrimitiveType};

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

#[derive(Clone, Debug, PartialEq)]
struct ScalarHelper<'a>(&'a Scalar);

impl PartialOrd for ScalarHelper<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Scalar::*;
        match (self.0, other.0) {
            (Null(_), Null(_)) => Some(Ordering::Equal),
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (Long(a), Long(b)) => a.partial_cmp(b),
            (Short(a), Short(b)) => a.partial_cmp(b),
            (Byte(a), Byte(b)) => a.partial_cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),
            (String(a), String(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (TimestampNtz(a), TimestampNtz(b)) => a.partial_cmp(b),
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Binary(a), Binary(b)) => a.partial_cmp(b),
            (Decimal(a, p1, s1), Decimal(b, p2, s2)) => {
                // TODO implement proper decimal comparison
                if p1 != p2 || s1 != s2 {
                    return None;
                };
                a.partial_cmp(b)
            }
            // TODO should we make an assumption about the ordering of nulls?
            // rigth now this is only used for internal purposes.
            (Null(_), _) => Some(Ordering::Less),
            (_, Null(_)) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

/// A Struct used for filtering a DeltaTable partition by key and value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionFilter {
    /// The key of the PartitionFilter
    pub key: String,
    /// The value of the PartitionFilter
    pub value: PartitionValue,
}

fn compare_typed_value(
    partition_value: &Scalar,
    filter_value: &str,
    data_type: &DataType,
) -> Option<Ordering> {
    match data_type {
        DataType::Primitive(primitive_type) => {
            let other = primitive_type.parse_scalar(filter_value).ok()?;
            ScalarHelper(partition_value).partial_cmp(&ScalarHelper(&other))
        }
        // NOTE: complex types are not supported as partition columns
        _ => None,
    }
}

/// Partition filters methods for filtering the DeltaTable partitions.
impl PartitionFilter {
    /// Indicates if a DeltaTable partition matches with the partition filter by key and value.
    pub fn match_partition(&self, partition: &DeltaTablePartition, data_type: &DataType) -> bool {
        if self.key != partition.key {
            return false;
        }
        if self.value == PartitionValue::Equal("".to_string()) {
            return partition.value.is_null();
        }

        match &self.value {
            PartitionValue::Equal(value) => {
                if let DataType::Primitive(PrimitiveType::Timestamp) = data_type {
                    compare_typed_value(&partition.value, value, data_type)
                        .map(|x| x.is_eq())
                        .unwrap_or(false)
                } else {
                    partition.value.serialize() == *value
                }
            }
            PartitionValue::NotEqual(value) => {
                if let DataType::Primitive(PrimitiveType::Timestamp) = data_type {
                    compare_typed_value(&partition.value, value, data_type)
                        .map(|x| !x.is_eq())
                        .unwrap_or(false)
                } else {
                    !(partition.value.serialize() == *value)
                }
            }
            PartitionValue::GreaterThan(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_gt())
                    .unwrap_or(false)
            }
            PartitionValue::GreaterThanOrEqual(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_ge())
                    .unwrap_or(false)
            }
            PartitionValue::LessThan(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_lt())
                    .unwrap_or(false)
            }
            PartitionValue::LessThanOrEqual(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_le())
                    .unwrap_or(false)
            }
            PartitionValue::In(value) => value.contains(&partition.value.serialize()),
            PartitionValue::NotIn(value) => !value.contains(&partition.value.serialize()),
        }
    }

    /// Indicates if one of the DeltaTable partition among the list
    /// matches with the partition filter.
    pub fn match_partitions(
        &self,
        partitions: &[DeltaTablePartition],
        partition_col_data_types: &HashMap<&String, &DataType>,
    ) -> bool {
        let data_type = partition_col_data_types.get(&self.key).unwrap().to_owned();
        partitions
            .iter()
            .any(|partition| self.match_partition(partition, data_type))
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
                value: Scalar::String(year.into()),
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
    fn test_match_partition() {
        let partition_2021 = DeltaTablePartition {
            key: "year".into(),
            value: Scalar::String("2021".into()),
        };
        let partition_2020 = DeltaTablePartition {
            key: "year".into(),
            value: Scalar::String("2020".into()),
        };
        let partition_2019 = DeltaTablePartition {
            key: "year".into(),
            value: Scalar::String("2019".into()),
        };

        let partition_year_2020_filter = PartitionFilter {
            key: "year".to_string(),
            value: PartitionValue::Equal("2020".to_string()),
        };
        let partition_month_12_filter = PartitionFilter {
            key: "month".to_string(),
            value: PartitionValue::Equal("12".to_string()),
        };
        let string_type = DataType::Primitive(PrimitiveType::String);

        assert!(!partition_year_2020_filter.match_partition(&partition_2021, &string_type));
        assert!(partition_year_2020_filter.match_partition(&partition_2020, &string_type));
        assert!(!partition_year_2020_filter.match_partition(&partition_2019, &string_type));
        assert!(!partition_month_12_filter.match_partition(&partition_2019, &string_type));

        /* TODO: To be re-enabled at a future date, needs some type futzing
        let partition_2020_12_31_23_59_59 = DeltaTablePartition {
            key: "time".into(),
            value: PrimitiveType::TimestampNtz.parse_scalar("2020-12-31 23:59:59").expect("Failed to parse timestamp"),
        };

        let partition_time_2020_12_31_23_59_59_filter = PartitionFilter {
            key: "time".to_string(),
            value: PartitionValue::Equal("2020-12-31 23:59:59.000000".into()),
        };

        assert!(partition_time_2020_12_31_23_59_59_filter.match_partition(
            &partition_2020_12_31_23_59_59,
            &DataType::Primitive(PrimitiveType::TimestampNtz)
        ));
        assert!(!partition_time_2020_12_31_23_59_59_filter
            .match_partition(&partition_2020_12_31_23_59_59, &string_type));
        */
    }

    #[test]
    fn test_match_filters() {
        let partitions = vec![
            DeltaTablePartition {
                key: "year".into(),
                value: Scalar::String("2021".into()),
            },
            DeltaTablePartition {
                key: "month".into(),
                value: Scalar::String("12".into()),
            },
        ];

        let string_type = DataType::Primitive(PrimitiveType::String);
        let partition_data_types: HashMap<&String, &DataType> = vec![
            (&partitions[0].key, &string_type),
            (&partitions[1].key, &string_type),
        ]
        .into_iter()
        .collect();

        let valid_filters = PartitionFilter {
            key: "year".to_string(),
            value: PartitionValue::Equal("2021".to_string()),
        };

        let valid_filter_month = PartitionFilter {
            key: "month".to_string(),
            value: PartitionValue::Equal("12".to_string()),
        };

        let invalid_filter = PartitionFilter {
            key: "year".to_string(),
            value: PartitionValue::Equal("2020".to_string()),
        };

        assert!(valid_filters.match_partitions(&partitions, &partition_data_types),);
        assert!(valid_filter_month.match_partitions(&partitions, &partition_data_types),);
        assert!(!invalid_filter.match_partitions(&partitions, &partition_data_types),);
    }
}
